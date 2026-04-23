import asyncio
import hashlib
import io
import os
import shutil
import sqlite3
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

import db
from PIL import Image, ImageOps

Image.MAX_IMAGE_PIXELS = None

THUMB_TIERS = ("sm", "md", "lg")
FULL_TIER = "full"
ALL_TIERS = THUMB_TIERS + (FULL_TIER,)
SIZES = {
    "sm": 400,
    "md": 1920,
    "lg": 3840,
}
THUMB_QUALITY = 92
CACHE_VERSION = "v3"
SSD_CACHE_DIR = os.getenv(
    "PHOTOARCHIVE_THUMB_CACHE_DIR",
    os.path.join(os.path.dirname(__file__), ".thumbcache"),
)
SSD_CACHE_BYTES = 10 * 1024 * 1024 * 1024
MEMORY_CACHE_BYTES = 512 * 1024 * 1024
PREGENERATE_ON_IDLE = True
PREGENERATE_IDLE_SECONDS = 5.0
PREGENERATE_SCAN_BATCH = 256
PREGENERATE_GENERATE_BATCH = 48
BROWSER_CACHE_MAX_AGE = 86400
BROWSER_CACHE_STALE_WHILE_REVALIDATE = 604800
_executor_workers = 4
_prefetch_workers_count = 2

_disk_allocations = {tier: 0 for tier in ALL_TIERS}
_executor = ThreadPoolExecutor(max_workers=_executor_workers, thread_name_prefix="thumb")
_prefetch_executor = ThreadPoolExecutor(
    max_workers=_prefetch_workers_count,
    thread_name_prefix="thumb-prefetch",
)

# In-memory thumbnail LRU: (size, image_id) -> (source_signature, jpeg_bytes)
_memory_cache: OrderedDict[tuple[str, int], tuple[str, bytes]] = OrderedDict()
_memory_cache_bytes = 0
_cache_lock = threading.Lock()
_meta_lock = threading.Lock()

# Shared in-flight work so a burst of requests only performs one source read.
_inflight: dict[tuple[str, int, str], asyncio.Task[object]] = {}

# Orientation detections pending DB write: image_id -> (orientation, aspect_ratio)
_orientation_queue: dict[int, tuple[str, float]] = {}
_orientation_lock = threading.Lock()

_last_user_activity = time.monotonic()
_prefetching = False
_pregen_manual_mode = False
_pregen_manual_pause = False
_pregen_scan_offsets = {tier: 0 for tier in THUMB_TIERS}
_last_thumb_config_signature = ""
_pregen_status = {
    "enabled": True,
    "manual_mode": False,
    "manual_pause": False,
    "state": "idle",
    "message": "",
    "active_phase": None,
    "started_at": None,
    "last_generated_at": None,
    "generated_this_session": 0,
    "last_error": "",
}

JPEG_EXTENSIONS = {".jpg", ".jpeg"}
RAW_EXTENSIONS = {
    ".arw",
    ".cr2",
    ".cr3",
    ".dng",
    ".nef",
    ".orf",
    ".raf",
    ".rw2",
}


def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _as_bool(value, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in ("", "0", "false", "no", "off")
    return bool(value)


def _current_time() -> float:
    return time.time()


def note_user_activity():
    global _last_user_activity
    _last_user_activity = time.monotonic()


def _ensure_disk_cache_dirs():
    if not SSD_CACHE_DIR:
        return
    os.makedirs(SSD_CACHE_DIR, exist_ok=True)
    for size in THUMB_TIERS:
        os.makedirs(os.path.join(SSD_CACHE_DIR, size), exist_ok=True)
    os.makedirs(os.path.join(SSD_CACHE_DIR, FULL_TIER), exist_ok=True)


def _allocate_disk_budget(total_bytes: int) -> dict[str, int]:
    allocations = {tier: 0 for tier in ALL_TIERS}
    remaining = max(0, int(total_bytes))
    targets = {
        "sm": 300 * 1024 * 1024,
        "md": 4 * 1024 * 1024 * 1024,
        "lg": 12 * 1024 * 1024 * 1024,
    }
    for tier in THUMB_TIERS:
        if remaining <= 0:
            break
        allocations[tier] = min(targets[tier], remaining)
        remaining -= allocations[tier]
    allocations[FULL_TIER] = max(0, remaining)
    return allocations


_source_stat_cache: dict[str, str] = {}

def _get_source_bits(filepath: str) -> str:
    """Cache os.stat results per filepath to avoid repeated HDD stat calls."""
    cached = _source_stat_cache.get(filepath)
    if cached is not None:
        return cached
    try:
        stat = os.stat(filepath)
        bits = f"{stat.st_size}|{stat.st_mtime_ns}|{filepath}"
    except OSError:
        bits = f"missing|{filepath}"
    _source_stat_cache[filepath] = bits
    return bits

def _build_source_signature(filepath: str, size: str, image_id: int) -> str:
    source_bits = _get_source_bits(filepath)

    if size == FULL_TIER:
        signature = f"{CACHE_VERSION}|full|{image_id}|{source_bits}"
    else:
        signature = (
            f"{CACHE_VERSION}|thumb|{size}|{image_id}|{SIZES[size]}|"
            f"{THUMB_QUALITY}|{source_bits}"
        )
    return hashlib.sha1(signature.encode("utf-8", "surrogateescape")).hexdigest()


def get_etag(filepath: str, size: str, image_id: int) -> str:
    return f"\"{_build_source_signature(filepath, size, image_id)}\""


def response_headers(filepath: str, size: str, image_id: int) -> dict[str, str]:
    return {
        "Cache-Control": (
            f"public, max-age={BROWSER_CACHE_MAX_AGE}, "
            f"stale-while-revalidate={BROWSER_CACHE_STALE_WHILE_REVALIDATE}"
        ),
        "ETag": get_etag(filepath, size, image_id),
    }


def _thumbnail_disk_path(size: str, image_id: int) -> str:
    return os.path.join(SSD_CACHE_DIR, size, f"{image_id}.jpg")


def _full_disk_path(image_id: int, filepath: str) -> str:
    ext = os.path.splitext(filepath)[1].lower() or ".bin"
    return os.path.join(SSD_CACHE_DIR, FULL_TIER, f"{image_id}{ext}")


def _replace_executor(
    current: ThreadPoolExecutor,
    workers: int,
    prefix: str,
) -> ThreadPoolExecutor:
    replacement = ThreadPoolExecutor(max_workers=workers, thread_name_prefix=prefix)
    try:
        current.shutdown(wait=False, cancel_futures=False)
    except TypeError:
        current.shutdown(wait=False)
    return replacement


def _memory_get_fast(size: str, image_id: int) -> bytes | None:
    """Fast memory check — no signature validation."""
    key = (size, image_id)
    with _cache_lock:
        entry = _memory_cache.get(key)
        if entry is None:
            return None
        _, data = entry
        _memory_cache.move_to_end(key)
        return data


def _memory_get(size: str, image_id: int, source_signature: str) -> bytes | None:
    key = (size, image_id)
    global _memory_cache_bytes

    with _cache_lock:
        entry = _memory_cache.get(key)
        if entry is None:
            return None
        cached_signature, data = entry
        if cached_signature != source_signature:
            _memory_cache_bytes -= len(data)
            _memory_cache.pop(key, None)
            return None
        _memory_cache.move_to_end(key)
        return data


def _memory_put(size: str, image_id: int, source_signature: str, data: bytes):
    if not data or MEMORY_CACHE_BYTES <= 0:
        return

    key = (size, image_id)
    global _memory_cache_bytes

    with _cache_lock:
        existing = _memory_cache.pop(key, None)
        if existing is not None:
            _memory_cache_bytes -= len(existing[1])

        _memory_cache[key] = (source_signature, data)
        _memory_cache.move_to_end(key)
        _memory_cache_bytes += len(data)

        while _memory_cache and _memory_cache_bytes > MEMORY_CACHE_BYTES:
            _old_key, (_, old_data) = _memory_cache.popitem(last=False)
            _memory_cache_bytes -= len(old_data)


def _clear_memory_cache() -> dict:
    global _memory_cache_bytes
    with _cache_lock:
        counts = {size: 0 for size in THUMB_TIERS}
        for size, _image_id in _memory_cache.keys():
            counts[size] = counts.get(size, 0) + 1
        entries_cleared = len(_memory_cache)
        bytes_cleared = _memory_cache_bytes
        _memory_cache.clear()
        _memory_cache_bytes = 0
    return {
        "entries_cleared": entries_cleared,
        "bytes_cleared": bytes_cleared,
        "counts": counts,
    }


def _clear_memory_tiers(tiers: tuple[str, ...]):
    global _memory_cache_bytes
    with _cache_lock:
        for key in list(_memory_cache.keys()):
            if key[0] not in tiers:
                continue
            _signature, data = _memory_cache.pop(key)
            _memory_cache_bytes -= len(data)


def _memory_stats() -> dict:
    with _cache_lock:
        tiers = {size: {"count": 0, "bytes": 0} for size in THUMB_TIERS}
        for (size, _image_id), (_signature, data) in _memory_cache.items():
            tiers[size]["count"] += 1
            tiers[size]["bytes"] += len(data)
        return {
            "limit_bytes": MEMORY_CACHE_BYTES,
            "used_bytes": _memory_cache_bytes,
            "tiers": tiers,
        }


def _remove_cache_entry_locked(conn: sqlite3.Connection, row: sqlite3.Row):
    path = row["path"]
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except OSError:
        pass
    conn.execute(
        "DELETE FROM cache_entries WHERE cache_root = ? AND size = ? AND image_id = ?",
        (row["cache_root"], row["size"], row["image_id"]),
    )


def _enforce_tier_budget_locked(conn: sqlite3.Connection, size: str):
    budget = _disk_allocations.get(size, 0)
    rows = conn.execute(
        "SELECT cache_root, size, image_id, path, size_bytes FROM cache_entries "
        "WHERE cache_root = ? AND size = ?",
        (SSD_CACHE_DIR, size),
    ).fetchall()
    total = sum(int(row["size_bytes"]) for row in rows)

    if budget <= 0:
        for row in rows:
            _remove_cache_entry_locked(conn, row)
        conn.commit()
        return

    if total <= budget:
        return

    evict_rows = conn.execute(
        "SELECT cache_root, size, image_id, path, size_bytes FROM cache_entries "
        "WHERE cache_root = ? AND size = ? "
        "ORDER BY last_accessed ASC, image_id ASC",
        (SSD_CACHE_DIR, size),
    ).fetchall()
    for row in evict_rows:
        _remove_cache_entry_locked(conn, row)
        total -= int(row["size_bytes"])
        if total <= budget:
            break
    conn.commit()


def _enforce_all_disk_budgets():
    with _meta_lock:
        conn = _db_connect()
        try:
            for size in ALL_TIERS:
                _enforce_tier_budget_locked(conn, size)
        finally:
            conn.close()


def _clear_disk_tiers(tiers: tuple[str, ...]):
    with _meta_lock:
        conn = _db_connect()
        try:
            placeholders = ",".join("?" for _ in tiers)
            rows = conn.execute(
                f"SELECT cache_root, size, image_id, path FROM cache_entries "
                f"WHERE cache_root = ? AND size IN ({placeholders})",
                (SSD_CACHE_DIR, *tiers),
            ).fetchall()
            for row in rows:
                _remove_cache_entry_locked(conn, row)
            conn.commit()
        finally:
            conn.close()

    for tier in tiers:
        tier_dir = os.path.join(SSD_CACHE_DIR, tier)
        if not os.path.isdir(tier_dir):
            continue
        for entry in os.scandir(tier_dir):
            if entry.is_file(follow_symlinks=False):
                try:
                    os.remove(entry.path)
                except OSError:
                    pass


def _get_disk_entry(
    size: str,
    image_id: int,
    source_signature: str,
    touch: bool = True,
) -> sqlite3.Row | None:
    if not SSD_CACHE_DIR or _disk_allocations.get(size, 0) <= 0:
        return None

    with _meta_lock:
        conn = _db_connect()
        try:
            row = conn.execute(
                "SELECT cache_root, size, image_id, path, source_signature, size_bytes "
                "FROM cache_entries WHERE cache_root = ? AND size = ? AND image_id = ?",
                (SSD_CACHE_DIR, size, image_id),
            ).fetchone()
            if row is None:
                return None
            if row["source_signature"] != source_signature or not os.path.exists(row["path"]):
                stale_row = conn.execute(
                    "SELECT cache_root, size, image_id, path FROM cache_entries "
                    "WHERE cache_root = ? AND size = ? AND image_id = ?",
                    (SSD_CACHE_DIR, size, image_id),
                ).fetchone()
                if stale_row is not None:
                    _remove_cache_entry_locked(conn, stale_row)
                    conn.commit()
                return None
            if touch:
                conn.execute(
                    "UPDATE cache_entries SET last_accessed = ? "
                    "WHERE cache_root = ? AND size = ? AND image_id = ?",
                    (_current_time(), SSD_CACHE_DIR, size, image_id),
                )
                conn.commit()
            return row
        finally:
            conn.close()


# In-memory index: (size, image_id) -> disk path. Built on startup, updated on writes.
_disk_path_index: dict[tuple[str, int], str] = {}
_disk_index_built = False


def _build_disk_path_index():
    """Load all cache entry paths into memory for fast lookup."""
    global _disk_index_built
    if not SSD_CACHE_DIR:
        _disk_index_built = True
        return
    with _meta_lock:
        conn = _db_connect()
        try:
            rows = conn.execute(
                "SELECT size, image_id, path FROM cache_entries WHERE cache_root = ?",
                (SSD_CACHE_DIR,),
            ).fetchall()
        finally:
            conn.close()
    for row in rows:
        _disk_path_index[(row["size"], row["image_id"])] = row["path"]
    _disk_index_built = True


def _index_disk_entry(size: str, image_id: int, path: str):
    """Update the in-memory index when a new cache entry is written."""
    _disk_path_index[(size, image_id)] = path


def fast_disk_read(size: str, image_id: int) -> bytes | None:
    """Fast path: read thumbnail from SSD via in-memory index. No SQLite, no locks, no HDD stat."""
    if not _disk_index_built:
        _build_disk_path_index()
    path = _disk_path_index.get((size, image_id))
    if path is None:
        return None
    try:
        with open(path, "rb") as f:
            return f.read()
    except OSError:
        return None


def _read_disk_thumbnail(size: str, image_id: int, source_signature: str) -> bytes | None:
    row = _get_disk_entry(size, image_id, source_signature)
    if row is None:
        return None

    try:
        with open(row["path"], "rb") as f:
            data = f.read()
    except OSError:
        with _meta_lock:
            conn = _db_connect()
            try:
                stale_row = conn.execute(
                    "SELECT cache_root, size, image_id, path FROM cache_entries "
                    "WHERE cache_root = ? AND size = ? AND image_id = ?",
                    (SSD_CACHE_DIR, size, image_id),
                ).fetchone()
                if stale_row is not None:
                    _remove_cache_entry_locked(conn, stale_row)
                    conn.commit()
            finally:
                conn.close()
        return None

    _memory_put(size, image_id, source_signature, data)
    return data


def _store_disk_entry(size: str, image_id: int, source_signature: str, path: str, size_bytes: int):
    with _meta_lock:
        conn = _db_connect()
        try:
            previous = conn.execute(
                "SELECT cache_root, size, image_id, path FROM cache_entries "
                "WHERE cache_root = ? AND size = ? AND image_id = ?",
                (SSD_CACHE_DIR, size, image_id),
            ).fetchone()
            if previous is not None and previous["path"] != path:
                _remove_cache_entry_locked(conn, previous)

            conn.execute(
                "INSERT OR REPLACE INTO cache_entries "
                "(cache_root, size, image_id, path, source_signature, size_bytes, last_accessed, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    SSD_CACHE_DIR,
                    size,
                    image_id,
                    path,
                    source_signature,
                    int(size_bytes),
                    _current_time(),
                    _current_time(),
                ),
            )
            _enforce_tier_budget_locked(conn, size)
            conn.commit()
            _index_disk_entry(size, image_id, path)
        finally:
            conn.close()


def _write_thumbnail_to_disk(size: str, image_id: int, source_signature: str, data: bytes):
    budget = _disk_allocations.get(size, 0)
    if not SSD_CACHE_DIR or budget <= 0 or len(data) > budget:
        return

    path = _thumbnail_disk_path(size, image_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp_path = f"{path}.{threading.get_ident()}.tmp"
    with open(temp_path, "wb") as f:
        f.write(data)
    os.replace(temp_path, path)
    _store_disk_entry(size, image_id, source_signature, path, len(data))


def _cache_full_image_sync(filepath: str, image_id: int, source_signature: str) -> str:
    if not os.path.exists(filepath):
        return filepath

    budget = _disk_allocations.get(FULL_TIER, 0)
    if not SSD_CACHE_DIR or budget <= 0:
        return filepath

    try:
        source_size = os.path.getsize(filepath)
    except OSError:
        return filepath

    if source_size > budget:
        return filepath

    row = _get_disk_entry(FULL_TIER, image_id, source_signature)
    if row is not None:
        return row["path"]

    path = _full_disk_path(image_id, filepath)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp_path = f"{path}.{threading.get_ident()}.tmp"
    shutil.copyfile(filepath, temp_path)
    os.replace(temp_path, path)
    _store_disk_entry(FULL_TIER, image_id, source_signature, path, source_size)

    row = _get_disk_entry(FULL_TIER, image_id, source_signature)
    if row is not None:
        return row["path"]
    return filepath


def _load_raw_preview(filepath: str, max_target: int) -> Image.Image | None:
    import rawpy

    try:
        with rawpy.imread(filepath) as raw:
            thumb = raw.extract_thumb()
        if thumb.format == rawpy.ThumbFormat.JPEG:
            with Image.open(io.BytesIO(thumb.data)) as source:
                source.load()
                img = ImageOps.exif_transpose(source)
                if img is source:
                    img = source.copy()
        elif thumb.format == rawpy.ThumbFormat.BITMAP:
            img = Image.fromarray(thumb.data)
        else:
            return None

        if max(img.width, img.height) >= max_target:
            return img
        img.close()
    except Exception:
        return None
    return None


def _load_source_image(filepath: str, max_target: int, prefer_draft: bool) -> Image.Image:
    ext = os.path.splitext(filepath)[1].lower()
    if ext in RAW_EXTENSIONS:
        preview = _load_raw_preview(filepath, max_target)
        if preview is not None:
            return preview

        import rawpy

        with rawpy.imread(filepath) as raw:
            rgb = raw.postprocess(use_camera_wb=True, no_auto_bright=True)
        return Image.fromarray(rgb)

    with Image.open(filepath) as source:
        if prefer_draft and ext in JPEG_EXTENSIONS:
            source.draft("RGB", (max_target * 2, max_target * 2))
        source.load()
        img = ImageOps.exif_transpose(source)
        if img is source:
            img = source.copy()
        return img


def _resize_to_long_side(img: Image.Image, target_long_side: int) -> Image.Image:
    long_side = max(img.width, img.height)
    if long_side <= target_long_side:
        return img.copy()

    scale = target_long_side / long_side
    new_size = (
        max(1, int(round(img.width * scale))),
        max(1, int(round(img.height * scale))),
    )
    return img.resize(new_size, Image.LANCZOS)


def _queue_orientation(image_id: int, img: Image.Image):
    orientation = "landscape" if img.width >= img.height else "portrait"
    aspect_ratio = round(img.width / img.height, 4) if img.height > 0 else 1.5
    with _orientation_lock:
        _orientation_queue[image_id] = (orientation, aspect_ratio)


def _planned_thumbnail_sizes(filepath: str, image_id: int, requested_size: str) -> list[str]:
    needed = []
    for size in THUMB_TIERS:
        should_consider = size == requested_size or _disk_allocations.get(size, 0) > 0
        if not should_consider:
            continue
        source_signature = _build_source_signature(filepath, size, image_id)
        if _memory_get(size, image_id, source_signature) is not None:
            continue
        if _get_disk_entry(size, image_id, source_signature, touch=False) is not None:
            continue
        needed.append(size)
    return sorted(needed, key=lambda tier: SIZES[tier], reverse=True)


def _generate_missing_thumbnails_sync(filepath: str, requested_size: str, image_id: int):
    needed_sizes = _planned_thumbnail_sizes(filepath, image_id, requested_size)
    if not needed_sizes:
        return

    img = None
    current = None

    try:
        max_target = max(SIZES[size] for size in needed_sizes)
        prefer_draft = max_target <= SIZES["sm"]
        img = _load_source_image(filepath, max_target, prefer_draft=prefer_draft)
        _queue_orientation(image_id, img)

        current = img
        for index, size in enumerate(needed_sizes):
            if index == 0:
                variant = _resize_to_long_side(current, SIZES[size])
            else:
                variant = _resize_to_long_side(current, SIZES[size])

            if variant.mode != "RGB":
                converted = variant.convert("RGB")
                variant.close()
                variant = converted

            buf = io.BytesIO()
            variant.save(
                buf,
                "JPEG",
                quality=THUMB_QUALITY,
                progressive=(size != "sm"),
            )
            data = buf.getvalue()
            source_signature = _build_source_signature(filepath, size, image_id)
            _memory_put(size, image_id, source_signature, data)
            _write_thumbnail_to_disk(size, image_id, source_signature, data)

            if current is not img:
                current.close()
            current = variant
    except Exception as e:
        print(f"Thumbnail error for {filepath}: {e}")
    finally:
        if current is not None and current is not img:
            try:
                current.close()
            except Exception:
                pass
        if img is not None:
            try:
                img.close()
            except Exception:
                pass


def has_cached(size: str, filepath: str, image_id: int) -> bool:
    source_signature = _build_source_signature(filepath, size, image_id)
    if size in THUMB_TIERS and _memory_get(size, image_id, source_signature) is not None:
        return True
    return _get_disk_entry(size, image_id, source_signature, touch=False) is not None


async def _run_thumbnail_job(
    filepath: str,
    size: str,
    image_id: int,
    executor: ThreadPoolExecutor,
):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        executor,
        _generate_missing_thumbnails_sync,
        filepath,
        size,
        image_id,
    )


async def _ensure_thumbnail_with_executor(
    filepath: str,
    size: str,
    image_id: int,
    executor: ThreadPoolExecutor,
    *,
    note_activity: bool,
) -> bytes:
    if note_activity:
        note_user_activity()

    source_signature = _build_source_signature(filepath, size, image_id)
    cached = _memory_get(size, image_id, source_signature)
    if cached is not None:
        return cached

    cached = _read_disk_thumbnail(size, image_id, source_signature)
    if cached is not None:
        return cached

    inflight_key = ("thumb", image_id, source_signature)
    task = _inflight.get(inflight_key)
    if task is None:
        task = asyncio.create_task(
            _run_thumbnail_job(filepath, size, image_id, executor)
        )
        _inflight[inflight_key] = task

    try:
        await task
    finally:
        if _inflight.get(inflight_key) is task and task.done():
            _inflight.pop(inflight_key, None)

    cached = _memory_get(size, image_id, source_signature)
    if cached is not None:
        return cached
    return _read_disk_thumbnail(size, image_id, source_signature) or b""


async def get_thumbnail(filepath: str, size: str, image_id: int) -> bytes:
    return await _ensure_thumbnail_with_executor(
        filepath,
        size,
        image_id,
        _executor,
        note_activity=True,
    )


async def prefetch_images(images: list[dict], size: str, limit: int | None = None) -> int:
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
            _ensure_thumbnail_with_executor(
                filepath,
                size,
                image_id,
                _prefetch_executor,
                note_activity=False,
            )
        )
        scheduled += 1
    return scheduled


async def _run_full_image_job(filepath: str, image_id: int):
    loop = asyncio.get_running_loop()
    source_signature = _build_source_signature(filepath, FULL_TIER, image_id)
    return await loop.run_in_executor(
        _executor,
        _cache_full_image_sync,
        filepath,
        image_id,
        source_signature,
    )


async def get_full_image_path(filepath: str, image_id: int) -> str:
    note_user_activity()
    source_signature = _build_source_signature(filepath, FULL_TIER, image_id)
    row = _get_disk_entry(FULL_TIER, image_id, source_signature)
    if row is not None:
        return row["path"]

    inflight_key = ("full", image_id, source_signature)
    task = _inflight.get(inflight_key)
    if task is None:
        task = asyncio.create_task(_run_full_image_job(filepath, image_id))
        _inflight[inflight_key] = task

    try:
        result = await task
    finally:
        if _inflight.get(inflight_key) is task and task.done():
            _inflight.pop(inflight_key, None)

    return str(result)


def load_embedding_image(filepath: str, image_id: int) -> Image.Image:
    source_signature = _build_source_signature(filepath, "md", image_id)
    cached = _memory_get("md", image_id, source_signature)
    if cached is None:
        cached = _read_disk_thumbnail("md", image_id, source_signature)

    if cached is not None:
        with Image.open(io.BytesIO(cached)) as source:
            source.load()
            img = source.copy()
        if img.mode != "RGB":
            converted = img.convert("RGB")
            img.close()
            img = converted
        return img

    img = _load_source_image(filepath, SIZES["md"], prefer_draft=False)
    if max(img.width, img.height) > 1024:
        resized = _resize_to_long_side(img, 1024)
        img.close()
        img = resized
    if img.mode != "RGB":
        converted = img.convert("RGB")
        img.close()
        img = converted
    return img


async def flush_orientation_updates():
    with _orientation_lock:
        pending = dict(_orientation_queue)
        _orientation_queue.clear()

    if not pending:
        return

    await db.batch_set_orientations(
        [(orientation, aspect_ratio, image_id) for image_id, (orientation, aspect_ratio) in pending.items()]
    )


def _set_pregen_state(state: str, message: str = "", phase: str | None = None, error: str = ""):
    _pregen_status["enabled"] = PREGENERATE_ON_IDLE
    _pregen_status["manual_mode"] = _pregen_manual_mode
    _pregen_status["manual_pause"] = _pregen_manual_pause
    _pregen_status["state"] = state
    _pregen_status["message"] = message
    _pregen_status["active_phase"] = phase
    _pregen_status["last_error"] = error
    if _pregen_status["started_at"] is None and state == "running":
        _pregen_status["started_at"] = _current_time()


async def _cache_target_total() -> int:
    conn = await db.get_db()
    try:
        cursor = await conn.execute(
            "SELECT COUNT(*) AS c FROM images WHERE status IN ('kept', 'maybe')"
        )
        row = await cursor.fetchone()
        return int(row["c"] if row else 0)
    finally:
        await conn.close()


async def _pregen_candidate_batch(size: str, offset: int, limit: int):
    conn = await db.get_db()
    try:
        cursor = await conn.execute(
            "SELECT i.id, i.filepath, c.source_signature "
            "FROM images i "
            "LEFT JOIN cache_entries c "
            "  ON c.cache_root = ? AND c.size = ? AND c.image_id = i.id "
            "WHERE i.status IN ('kept', 'maybe') "
            "ORDER BY i.filepath ASC "
            "LIMIT ? OFFSET ?",
            (SSD_CACHE_DIR, size, limit, offset),
        )
        return await cursor.fetchall()
    finally:
        await conn.close()


async def _run_pregen_phase(size: str) -> int:
    offset = _pregen_scan_offsets[size]
    rows = await _pregen_candidate_batch(size, offset, PREGENERATE_SCAN_BATCH)
    if not rows:
        _pregen_scan_offsets[size] = 0
        rows = await _pregen_candidate_batch(size, 0, PREGENERATE_SCAN_BATCH)
        if not rows:
            return 0

    _pregen_scan_offsets[size] = offset + len(rows)
    generated = 0

    for row in rows:
        if not _prefetching or _pregen_manual_pause:
            break
        if not _pregen_manual_mode and (time.monotonic() - _last_user_activity) < PREGENERATE_IDLE_SECONDS:
            break

        current_signature = _build_source_signature(row["filepath"], size, row["id"])
        if row["source_signature"] == current_signature and has_cached(size, row["filepath"], row["id"]):
            continue

        await _ensure_thumbnail_with_executor(
            row["filepath"],
            size,
            row["id"],
            _prefetch_executor,
            note_activity=False,
        )
        generated += 1
        _pregen_status["last_generated_at"] = _current_time()
        _pregen_status["generated_this_session"] += 1
        await asyncio.sleep(0.02)

        if generated >= PREGENERATE_GENERATE_BATCH:
            break

    if len(rows) < PREGENERATE_SCAN_BATCH:
        _pregen_scan_offsets[size] = 0
    return generated


def cache_stats() -> dict:
    memory = _memory_stats()
    disk_tiers = {
        size: {
            "count": 0,
            "bytes": 0,
            "budget_bytes": _disk_allocations.get(size, 0),
        }
        for size in ALL_TIERS
    }

    with _meta_lock:
        conn = _db_connect()
        try:
            rows = conn.execute(
                "SELECT size, COUNT(*) AS count, COALESCE(SUM(size_bytes), 0) AS bytes "
                "FROM cache_entries WHERE cache_root = ? GROUP BY size",
                (SSD_CACHE_DIR,),
            ).fetchall()
        finally:
            conn.close()

    for row in rows:
        if row["size"] in disk_tiers:
            disk_tiers[row["size"]]["count"] = int(row["count"])
            disk_tiers[row["size"]]["bytes"] = int(row["bytes"])

    disk_used = sum(info["bytes"] for info in disk_tiers.values())
    return {
        "memory": memory,
        "disk": {
            "root": SSD_CACHE_DIR,
            "limit_bytes": SSD_CACHE_BYTES,
            "used_bytes": disk_used,
            "tiers": disk_tiers,
        },
    }


def _thumb_config_signature() -> str:
    return f"{CACHE_VERSION}|{SIZES['sm']}|{SIZES['md']}|{SIZES['lg']}|{THUMB_QUALITY}"


def get_pregen_status(target_total: int = 0) -> dict:
    stats = cache_stats()
    phases = {}
    for size in THUMB_TIERS:
        count = stats["disk"]["tiers"][size]["count"]
        progress_pct = round((count / target_total) * 100, 1) if target_total > 0 else 0.0
        phases[size] = {
            "count": count,
            "total": target_total,
            "progress_pct": progress_pct,
            "budget_bytes": stats["disk"]["tiers"][size]["budget_bytes"],
        }

    return {
        **dict(_pregen_status),
        "idle_seconds": round(max(0.0, time.monotonic() - _last_user_activity), 2),
        "phases": phases,
    }


def clear_cache() -> dict:
    memory = _clear_memory_cache()

    disk_removed = 0
    if SSD_CACHE_DIR and os.path.isdir(SSD_CACHE_DIR):
        for _root, _dirs, files in os.walk(SSD_CACHE_DIR):
            disk_removed += len(files)
        shutil.rmtree(SSD_CACHE_DIR, ignore_errors=True)
    _ensure_disk_cache_dirs()

    with _meta_lock:
        conn = _db_connect()
        try:
            conn.execute("DELETE FROM cache_entries WHERE cache_root = ?", (SSD_CACHE_DIR,))
            conn.commit()
        finally:
            conn.close()

    return {
        "memory_entries_cleared": memory["entries_cleared"],
        "memory_bytes_cleared": memory["bytes_cleared"],
        "memory_before": memory["counts"],
        "disk_files_removed": disk_removed,
        "ssd_cache_dir": SSD_CACHE_DIR,
    }


def configure(config: dict):
    global THUMB_QUALITY, SSD_CACHE_DIR, SSD_CACHE_BYTES, MEMORY_CACHE_BYTES
    global PREGENERATE_ON_IDLE, _memory_cache_bytes
    global BROWSER_CACHE_MAX_AGE, BROWSER_CACHE_STALE_WHILE_REVALIDATE
    global _executor_workers, _prefetch_workers_count, _executor, _prefetch_executor
    global _disk_allocations, _last_thumb_config_signature, _pregen_manual_pause

    SIZES["sm"] = int(config.get("thumb_size_sm", SIZES["sm"]))
    SIZES["md"] = int(config.get("thumb_size_md", SIZES["md"]))
    SIZES["lg"] = int(config.get("thumb_size_lg", SIZES["lg"]))
    THUMB_QUALITY = int(config.get("thumb_quality", config.get("jpeg_quality", THUMB_QUALITY)))
    BROWSER_CACHE_MAX_AGE = int(config.get("browser_cache_max_age", BROWSER_CACHE_MAX_AGE))
    BROWSER_CACHE_STALE_WHILE_REVALIDATE = int(
        config.get(
            "browser_cache_stale_while_revalidate",
            BROWSER_CACHE_STALE_WHILE_REVALIDATE,
        )
    )
    memory_cache_gb = config.get("memory_cache_gb")
    if memory_cache_gb is None:
        try:
            memory_cache_gb = float(config.get("memory_cache_mb", 512)) / 1024.0
        except (TypeError, ValueError):
            memory_cache_gb = 0.5
    try:
        MEMORY_CACHE_BYTES = max(0, int(float(memory_cache_gb) * 1024 * 1024 * 1024))
    except (TypeError, ValueError):
        MEMORY_CACHE_BYTES = int(0.5 * 1024 * 1024 * 1024)
    SSD_CACHE_BYTES = max(0, int(config.get("ssd_cache_gb", 10))) * 1024 * 1024 * 1024
    PREGENERATE_ON_IDLE = _as_bool(config.get("pregenerate_on_idle"), PREGENERATE_ON_IDLE)

    disk_cache_dir = (
        config.get("ssd_cache_dir")
        or config.get("disk_cache_dir")
        or SSD_CACHE_DIR
    )
    SSD_CACHE_DIR = os.path.abspath(str(disk_cache_dir).strip() or SSD_CACHE_DIR)
    _ensure_disk_cache_dirs()

    new_thumb_config_signature = _thumb_config_signature()
    if _last_thumb_config_signature and new_thumb_config_signature != _last_thumb_config_signature:
        _clear_memory_tiers(THUMB_TIERS)
        _clear_disk_tiers(THUMB_TIERS)
    _last_thumb_config_signature = new_thumb_config_signature

    _disk_allocations = _allocate_disk_budget(SSD_CACHE_BYTES)

    if not PREGENERATE_ON_IDLE and not _pregen_manual_mode:
        _pregen_manual_pause = True
    elif PREGENERATE_ON_IDLE and not _pregen_manual_mode:
        _pregen_manual_pause = False

    with _cache_lock:
        while _memory_cache and _memory_cache_bytes > MEMORY_CACHE_BYTES:
            _key, (_signature, data) = _memory_cache.popitem(last=False)
            _memory_cache_bytes -= len(data)

    user_workers = int(config.get("user_workers", _executor_workers))
    if user_workers != _executor_workers:
        _executor = _replace_executor(_executor, user_workers, "thumb")
        _executor_workers = user_workers

    prefetch_workers = int(config.get("prefetch_workers", _prefetch_workers_count))
    if prefetch_workers != _prefetch_workers_count:
        _prefetch_executor = _replace_executor(
            _prefetch_executor,
            prefetch_workers,
            "thumb-prefetch",
        )
        _prefetch_workers_count = prefetch_workers

    _enforce_all_disk_budgets()
    _build_disk_path_index()


def start_pregeneration() -> dict:
    global _pregen_manual_mode, _pregen_manual_pause
    _pregen_manual_mode = True
    _pregen_manual_pause = False
    _pregen_status["started_at"] = _current_time()
    _set_pregen_state("running", "Pre-generating cache on demand.")
    return dict(_pregen_status)


def stop_pregeneration() -> dict:
    global _pregen_manual_mode, _pregen_manual_pause
    _pregen_manual_mode = False
    _pregen_manual_pause = True
    _set_pregen_state("paused", "Pre-generation paused by user.")
    return dict(_pregen_status)


async def run_prefetch_worker():
    global _prefetching
    _prefetching = True

    while _prefetching:
        try:
            await flush_orientation_updates()

            if _pregen_manual_pause:
                _set_pregen_state("paused", "Pre-generation paused by user.")
                await asyncio.sleep(1)
                continue

            auto_enabled = PREGENERATE_ON_IDLE
            idle_seconds = time.monotonic() - _last_user_activity
            if not _pregen_manual_mode and not auto_enabled:
                _set_pregen_state("disabled", "Idle pre-generation is disabled in Settings.")
                await asyncio.sleep(2)
                continue

            if not _pregen_manual_mode and idle_seconds < PREGENERATE_IDLE_SECONDS:
                _set_pregen_state(
                    "waiting_for_idle",
                    f"Waiting for {PREGENERATE_IDLE_SECONDS:.0f}s of user idle time.",
                )
                await asyncio.sleep(1)
                continue

            target_total = await _cache_target_total()
            if target_total <= 0:
                _set_pregen_state("idle", "No kept or maybe images to pre-generate.")
                await asyncio.sleep(5)
                continue

            phases = [size for size in THUMB_TIERS if _disk_allocations.get(size, 0) > 0]
            if not phases:
                _set_pregen_state("idle", "No SSD thumbnail budget is available.")
                await asyncio.sleep(5)
                continue

            generated = 0
            for phase in phases:
                _set_pregen_state("running", f"Pre-generating {phase} thumbnails…", phase=phase)
                phase_generated = await _run_pregen_phase(phase)
                generated += phase_generated
                await flush_orientation_updates()
                if phase_generated > 0:
                    break

            if generated == 0:
                status = get_pregen_status(target_total)
                phase_parts = []
                for phase in phases:
                    phase_parts.append(
                        f"{phase}: {status['phases'][phase]['count']}/{target_total}"
                    )
                _set_pregen_state(
                    "complete",
                    "Cache is warm for current budget. " + " · ".join(phase_parts),
                )
                await asyncio.sleep(2)
            else:
                await asyncio.sleep(0.1)

        except Exception as e:
            _set_pregen_state("error", "Pre-generation worker hit an error.", error=str(e))
            print(f"Prefetch worker error: {e}")
            await asyncio.sleep(5)


def stop_prefetch():
    global _prefetching
    _prefetching = False
