import io
import asyncio
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from PIL import Image

Image.MAX_IMAGE_PIXELS = None

SIZES = {
    "sm": 400,
    "md": 1920,
    "lg": 3840,
}
JPEG_QUALITY = 92

# Max cached thumbnails per size tier
CACHE_LIMITS = {
    "sm": 2000,
    "md": 300,
    "lg": 500,
}

# Thread pool for blocking I/O (HDD reads, image processing)
# Separate pools so prefetch doesn't starve user requests
_executor = ThreadPoolExecutor(max_workers=4)
_prefetch_executor = ThreadPoolExecutor(max_workers=2)
_prefetching = False

# In-memory LRU caches: size -> OrderedDict{ (image_id) -> bytes }
_cache: dict[str, OrderedDict[int, bytes]] = {
    "sm": OrderedDict(),
    "md": OrderedDict(),
    "lg": OrderedDict(),
}
_cache_lock = threading.Lock()

# Orientation detections pending DB write: image_id -> 'landscape'|'portrait'
_orientation_queue: dict[int, str] = {}
_orientation_lock = threading.Lock()


def has_cached(size: str, image_id: int) -> bool:
    with _cache_lock:
        return image_id in _cache[size]


def _get_cached(size: str, image_id: int) -> bytes | None:
    with _cache_lock:
        if image_id in _cache[size]:
            _cache[size].move_to_end(image_id)
            return _cache[size][image_id]
    return None


def _put_cached(size: str, image_id: int, data: bytes):
    with _cache_lock:
        cache = _cache[size]
        cache[image_id] = data
        cache.move_to_end(image_id)
        limit = CACHE_LIMITS[size]
        while len(cache) > limit:
            cache.popitem(last=False)


def _generate_thumbnail_sync(filepath: str, size: str, image_id: int) -> bytes:
    """Generate a thumbnail synchronously. Returns JPEG bytes."""
    # Check cache first (another thread may have generated it)
    existing = _get_cached(size, image_id)
    if existing:
        return existing

    target_width = SIZES[size]

    try:
        ext = filepath.lower().rsplit(".", 1)[-1]
        if ext in ("dng", "cr3"):
            import rawpy
            with rawpy.imread(filepath) as raw:
                rgb = raw.postprocess(use_camera_wb=True, no_auto_bright=True)
            img = Image.fromarray(rgb)
        else:
            img = Image.open(filepath)
            img.load()  # Force read from disk

        # Detect orientation and aspect ratio from original dimensions
        orientation = 'landscape' if img.width >= img.height else 'portrait'
        aspect_ratio = round(img.width / img.height, 4) if img.height > 0 else 1.5
        with _orientation_lock:
            _orientation_queue[image_id] = (orientation, aspect_ratio)

        # Don't upscale — only downscale
        if img.width > target_width:
            ratio = target_width / img.width
            new_height = int(img.height * ratio)
            img = img.resize((target_width, new_height), Image.LANCZOS)

        if img.mode != "RGB":
            img = img.convert("RGB")

        buf = io.BytesIO()
        img.save(buf, "JPEG", quality=JPEG_QUALITY)
        data = buf.getvalue()

        _put_cached(size, image_id, data)
        return data
    except Exception as e:
        print(f"Thumbnail error for {filepath}: {e}")
        return b""


async def get_thumbnail(filepath: str, size: str, image_id: int) -> bytes:
    """Get or generate a thumbnail. Returns JPEG bytes."""
    cached = _get_cached(size, image_id)
    if cached:
        return cached

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor, _generate_thumbnail_sync, filepath, size, image_id
    )


def cache_stats() -> dict:
    """Return current cache occupancy per size."""
    with _cache_lock:
        return {size: len(cache) for size, cache in _cache.items()}


async def run_prefetch_worker():
    """Continuously generate thumbnails for unculled images in the background."""
    global _prefetching
    _prefetching = True
    loop = asyncio.get_event_loop()

    import db

    while _prefetching:
        try:
            generated = 0

            # Prefetch sm thumbnails for kept images in small batches
            conn = await db.get_db()
            try:
                cursor = await conn.execute(
                    "SELECT id, filepath FROM images WHERE status IN ('kept', 'maybe') ORDER BY comparisons ASC LIMIT 50",
                )
                kept_rows = await cursor.fetchall()
            finally:
                await conn.close()

            for row in kept_rows:
                if not _prefetching:
                    break
                if not has_cached("sm", row["id"]):
                    await loop.run_in_executor(
                        _prefetch_executor, _generate_thumbnail_sync, row["filepath"], "sm", row["id"]
                    )
                    generated += 1
                    # Yield to event loop after every thumbnail so requests aren't starved
                    await asyncio.sleep(0.05)

            # Flush orientation detections to DB
            with _orientation_lock:
                pending = dict(_orientation_queue)
                _orientation_queue.clear()
            if pending:
                conn = await db.get_db()
                try:
                    for img_id, (orient, ar) in pending.items():
                        await conn.execute(
                            "UPDATE images SET orientation = ?, aspect_ratio = ? WHERE id = ? AND orientation IS NULL",
                            (orient, ar, img_id),
                        )
                    await conn.commit()
                finally:
                    await conn.close()

            await asyncio.sleep(2 if generated == 0 else 0.5)

        except Exception as e:
            print(f"Prefetch worker error: {e}")
            await asyncio.sleep(5)


def stop_prefetch():
    global _prefetching
    _prefetching = False
