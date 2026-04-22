import asyncio
import hashlib
import io
import os
import shutil
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

from PIL import Image, ImageOps

Image.MAX_IMAGE_PIXELS = None

SIZES = {
    "sm": 400,
    "md": 1920,
    "lg": 3840,
}
JPEG_QUALITY = 92
CACHE_VERSION = "v2"
DISK_CACHE_DIR = os.getenv(
    "PHOTOARCHIVE_THUMB_CACHE_DIR",
    os.path.join(os.path.dirname(__file__), ".thumbcache"),
)
BROWSER_CACHE_MAX_AGE = 86400
BROWSER_CACHE_STALE_WHILE_REVALIDATE = 604800
_executor_workers = 4
_prefetch_workers_count = 2

# Max cached thumbnails per size tier
CACHE_LIMITS = {
    "sm": 2000,
    "md": 300,
    "lg": 500,
}

# Thread pool for blocking I/O (HDD reads, image processing)
# Separate pools so prefetch doesn't starve user requests
_executor = ThreadPoolExecutor(max_workers=_executor_workers, thread_name_prefix="thumb")
_prefetch_executor = ThreadPoolExecutor(max_workers=_prefetch_workers_count, thread_name_prefix="thumb-prefetch")
_prefetching = False

# In-memory LRU caches: size -> OrderedDict{ cache_key -> bytes }
_cache: dict[str, OrderedDict[str, bytes]] = {
    "sm": OrderedDict(),
    "md": OrderedDict(),
    "lg": OrderedDict(),
}
_cache_lock = threading.Lock()

# Shared in-flight work so a burst of requests only generates each thumb once.
_inflight: dict[tuple[str, str], asyncio.Task[bytes]] = {}

# Orientation detections pending DB write: image_id -> (orientation, aspect_ratio)
_orientation_queue: dict[int, tuple[str, float]] = {}
_orientation_lock = threading.Lock()


def _build_cache_key(filepath: str, size: str, image_id: int) -> str:
    try:
        stat = os.stat(filepath)
        signature = (
            f"{CACHE_VERSION}|{size}|{image_id}|{SIZES[size]}|{JPEG_QUALITY}|"
            f"{stat.st_size}|{stat.st_mtime_ns}|{filepath}"
        )
    except OSError:
        signature = (
            f"{CACHE_VERSION}|{size}|{image_id}|{SIZES[size]}|{JPEG_QUALITY}|"
            f"missing|{filepath}"
        )
    return hashlib.sha1(signature.encode("utf-8", "surrogateescape")).hexdigest()


def get_etag(filepath: str, size: str, image_id: int) -> str:
    return f"\"{_build_cache_key(filepath, size, image_id)}\""


def response_headers(filepath: str, size: str, image_id: int) -> dict[str, str]:
    return {
        "Cache-Control": (
            f"public, max-age={BROWSER_CACHE_MAX_AGE}, "
            f"stale-while-revalidate={BROWSER_CACHE_STALE_WHILE_REVALIDATE}"
        ),
        "ETag": get_etag(filepath, size, image_id),
    }


def _disk_path(size: str, cache_key: str) -> str:
    return os.path.join(DISK_CACHE_DIR, size, cache_key[:2], f"{cache_key}.jpg")


def _ensure_disk_cache_dirs():
    if not DISK_CACHE_DIR:
        return
    for size in SIZES:
        os.makedirs(os.path.join(DISK_CACHE_DIR, size), exist_ok=True)


def has_cached(size: str, filepath: str, image_id: int) -> bool:
    cache_key = _build_cache_key(filepath, size, image_id)
    with _cache_lock:
        if cache_key in _cache[size]:
            return True
    return os.path.exists(_disk_path(size, cache_key))


def _get_cached(size: str, cache_key: str) -> bytes | None:
    with _cache_lock:
        if cache_key in _cache[size]:
            _cache[size].move_to_end(cache_key)
            return _cache[size][cache_key]
    return None


def _put_cached(size: str, cache_key: str, data: bytes):
    if not data:
        return
    with _cache_lock:
        cache = _cache[size]
        cache[cache_key] = data
        cache.move_to_end(cache_key)
        limit = CACHE_LIMITS[size]
        while len(cache) > limit:
            cache.popitem(last=False)


def _read_disk_cached(size: str, cache_key: str) -> bytes | None:
    path = _disk_path(size, cache_key)
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None
    except OSError:
        return None


def _write_disk_cached(size: str, cache_key: str, data: bytes):
    if not data or not DISK_CACHE_DIR:
        return
    path = _disk_path(size, cache_key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp_path = f"{path}.{threading.get_ident()}.tmp"
    with open(temp_path, "wb") as f:
        f.write(data)
    os.replace(temp_path, path)


def _load_cached(size: str, cache_key: str) -> bytes | None:
    cached = _get_cached(size, cache_key)
    if cached is not None:
        return cached

    cached = _read_disk_cached(size, cache_key)
    if cached is not None:
        _put_cached(size, cache_key, cached)
    return cached


def _replace_executor(current: ThreadPoolExecutor, workers: int, prefix: str) -> ThreadPoolExecutor:
    replacement = ThreadPoolExecutor(max_workers=workers, thread_name_prefix=prefix)
    try:
        current.shutdown(wait=False, cancel_futures=False)
    except TypeError:
        current.shutdown(wait=False)
    return replacement


def configure(config: dict):
    global JPEG_QUALITY, DISK_CACHE_DIR
    global BROWSER_CACHE_MAX_AGE, BROWSER_CACHE_STALE_WHILE_REVALIDATE
    global _executor_workers, _prefetch_workers_count, _executor, _prefetch_executor

    SIZES["sm"] = int(config.get("thumb_size_sm", SIZES["sm"]))
    SIZES["md"] = int(config.get("thumb_size_md", SIZES["md"]))
    SIZES["lg"] = int(config.get("thumb_size_lg", SIZES["lg"]))
    CACHE_LIMITS["sm"] = int(config.get("cache_limit_sm", CACHE_LIMITS["sm"]))
    CACHE_LIMITS["md"] = int(config.get("cache_limit_md", CACHE_LIMITS["md"]))
    CACHE_LIMITS["lg"] = int(config.get("cache_limit_lg", CACHE_LIMITS["lg"]))
    JPEG_QUALITY = int(config.get("jpeg_quality", JPEG_QUALITY))
    BROWSER_CACHE_MAX_AGE = int(config.get("browser_cache_max_age", BROWSER_CACHE_MAX_AGE))
    BROWSER_CACHE_STALE_WHILE_REVALIDATE = int(
        config.get(
            "browser_cache_stale_while_revalidate",
            BROWSER_CACHE_STALE_WHILE_REVALIDATE,
        )
    )

    disk_cache_dir = (config.get("disk_cache_dir") or "").strip()
    if disk_cache_dir:
        DISK_CACHE_DIR = disk_cache_dir
        _ensure_disk_cache_dirs()

    with _cache_lock:
        for size, cache in _cache.items():
            while len(cache) > CACHE_LIMITS[size]:
                cache.popitem(last=False)

    user_workers = int(config.get("user_workers", _executor_workers))
    if user_workers != _executor_workers:
        _executor = _replace_executor(_executor, user_workers, "thumb")
        _executor_workers = user_workers

    prefetch_workers = int(config.get("prefetch_workers", _prefetch_workers_count))
    if prefetch_workers != _prefetch_workers_count:
        _prefetch_executor = _replace_executor(
            _prefetch_executor, prefetch_workers, "thumb-prefetch"
        )
        _prefetch_workers_count = prefetch_workers


def clear_cache() -> dict:
    with _cache_lock:
        memory_before = {size: len(cache) for size, cache in _cache.items()}
        for cache in _cache.values():
            cache.clear()

    disk_removed = 0
    if DISK_CACHE_DIR and os.path.isdir(DISK_CACHE_DIR):
        for size in SIZES:
            size_dir = os.path.join(DISK_CACHE_DIR, size)
            if not os.path.isdir(size_dir):
                continue
            for entry in os.scandir(size_dir):
                if entry.is_dir(follow_symlinks=False):
                    disk_removed += sum(
                        1 for _root, _dirs, files in os.walk(entry.path) for _file in files
                    )
                    shutil.rmtree(entry.path, ignore_errors=True)
                elif entry.is_file(follow_symlinks=False) and entry.name.endswith(".jpg"):
                    try:
                        os.remove(entry.path)
                        disk_removed += 1
                    except OSError:
                        pass

    return {
        "memory_entries_cleared": sum(memory_before.values()),
        "memory_before": memory_before,
        "disk_files_removed": disk_removed,
        "disk_cache_dir": DISK_CACHE_DIR,
    }


def _generate_thumbnail_sync(filepath: str, size: str, image_id: int, cache_key: str) -> bytes:
    """Generate a thumbnail synchronously. Returns JPEG bytes."""
    existing = _load_cached(size, cache_key)
    if existing is not None:
        return existing

    target_width = SIZES[size]
    img = None

    try:
        ext = filepath.lower().rsplit(".", 1)[-1]
        if ext in ("dng", "cr3"):
            import rawpy

            with rawpy.imread(filepath) as raw:
                rgb = raw.postprocess(use_camera_wb=True, no_auto_bright=True)
            img = Image.fromarray(rgb)
        else:
            with Image.open(filepath) as source:
                source.load()
                img = ImageOps.exif_transpose(source)
                if img is source:
                    img = source.copy()

        orientation = "landscape" if img.width >= img.height else "portrait"
        aspect_ratio = round(img.width / img.height, 4) if img.height > 0 else 1.5
        with _orientation_lock:
            _orientation_queue[image_id] = (orientation, aspect_ratio)

        if img.width > target_width:
            ratio = target_width / img.width
            new_height = max(1, int(img.height * ratio))
            img = img.resize((target_width, new_height), Image.LANCZOS)

        if img.mode != "RGB":
            img = img.convert("RGB")

        buf = io.BytesIO()
        img.save(buf, "JPEG", quality=JPEG_QUALITY, progressive=(size != "sm"))
        data = buf.getvalue()

        _put_cached(size, cache_key, data)
        _write_disk_cached(size, cache_key, data)
        return data
    except Exception as e:
        print(f"Thumbnail error for {filepath}: {e}")
        return b""
    finally:
        if img is not None:
            try:
                img.close()
            except Exception:
                pass


async def _run_thumbnail_job(filepath: str, size: str, image_id: int, cache_key: str, executor: ThreadPoolExecutor) -> bytes:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        executor, _generate_thumbnail_sync, filepath, size, image_id, cache_key
    )


async def _get_thumbnail_with_executor(filepath: str, size: str, image_id: int, executor: ThreadPoolExecutor) -> bytes:
    cache_key = _build_cache_key(filepath, size, image_id)
    cached = _load_cached(size, cache_key)
    if cached is not None:
        return cached

    inflight_key = (size, cache_key)
    task = _inflight.get(inflight_key)
    if task is None:
        task = asyncio.create_task(
            _run_thumbnail_job(filepath, size, image_id, cache_key, executor)
        )
        _inflight[inflight_key] = task

    try:
        return await task
    finally:
        if _inflight.get(inflight_key) is task and task.done():
            _inflight.pop(inflight_key, None)


async def get_thumbnail(filepath: str, size: str, image_id: int) -> bytes:
    """Get or generate a thumbnail. Returns JPEG bytes."""
    return await _get_thumbnail_with_executor(filepath, size, image_id, _executor)


async def prefetch_images(images: list[dict], size: str, limit: int | None = None) -> int:
    """Schedule thumbnail generation without blocking the caller."""
    if size not in SIZES or not images:
        return 0

    scheduled = 0
    for img in images:
        if limit is not None and scheduled >= limit:
            break
        image_id = img.get("id")
        filepath = img.get("filepath")
        if image_id is None or not filepath:
            continue
        if has_cached(size, filepath, image_id):
            continue
        asyncio.create_task(
            _get_thumbnail_with_executor(filepath, size, image_id, _prefetch_executor)
        )
        scheduled += 1
    return scheduled


def cache_stats() -> dict:
    """Return current cache occupancy per size."""
    with _cache_lock:
        return {size: len(cache) for size, cache in _cache.items()}


async def run_prefetch_worker():
    """Continuously generate thumbnails for kept images in the background."""
    global _prefetching
    _prefetching = True

    import db

    while _prefetching:
        try:
            generated = 0

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
                if not has_cached("sm", row["filepath"], row["id"]):
                    await _get_thumbnail_with_executor(
                        row["filepath"], "sm", row["id"], _prefetch_executor
                    )
                    generated += 1
                    await asyncio.sleep(0.05)

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
