import asyncio
import hashlib
import io
import os
import shutil
import sqlite3
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import db
import resource_governor
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
CACHE_PROFILE = "original_heavy"
SSD_CACHE_DIR = os.getenv(
    "PHOTOARCHIVE_THUMB_CACHE_DIR",
    os.path.join(os.path.dirname(__file__), ".thumbcache"),
)
SSD_CACHE_BYTES = 10 * 1024 * 1024 * 1024
MEMORY_CACHE_BYTES = 512 * 1024 * 1024
PREGENERATE_ON_IDLE = True
PREGENERATE_IDLE_SECONDS = 1.0
PREGENERATE_SCAN_BATCH = 1024
PREGENERATE_GENERATE_BATCH = 16
PREGENERATE_BATCH_PAUSE_SECONDS = 0.25
THUMBNAIL_RETRY_SECONDS = 6 * 60 * 60
BROWSER_CACHE_MAX_AGE = 86400
BROWSER_CACHE_STALE_WHILE_REVALIDATE = 604800
HOT_LG_RESERVE_FRACTION = 0.35
HOT_LG_RESERVE_MIN_BYTES = 2 * 1024 * 1024 * 1024
HOT_LG_RESERVE_MAX_BYTES = 64 * 1024 * 1024 * 1024
COLD_CACHE_ACCESS_OFFSET_SECONDS = 45 * 24 * 60 * 60
_executor_workers = 4
_prefetch_workers_count = 6

_disk_allocations = {tier: 0 for tier in ALL_TIERS}
_executor = ThreadPoolExecutor(max_workers=_executor_workers, thread_name_prefix="thumb")
_prefetch_executor = ThreadPoolExecutor(
    max_workers=_prefetch_workers_count,
    thread_name_prefix="thumb-prefetch",
)

# In-memory thumbnail LRU: (size, image_id) -> (source_signature, jpeg_bytes)
_memory_cache: OrderedDict[tuple[str, int], tuple[str, bytes]] = OrderedDict()
_memory_cache_bytes = 0
_memory_tier_bytes = {size: 0 for size in THUMB_TIERS}
_cache_lock = threading.Lock()
_meta_lock = threading.Lock()

# Shared in-flight work so a burst of requests only performs one source read.
_inflight: dict[tuple[str, int, str], asyncio.Task[object]] = {}
_thumbnail_retry_after: dict[tuple[str, int, str], float] = {}

# Write-behind queue for cache DB entries — reduces _meta_lock contention.
_write_queue: list[tuple[str, int, str, str, int, float]] = []  # (size, image_id, sig, path, bytes, access)
_write_queue_lock = threading.Lock()

# Orientation detections pending DB write: image_id -> (orientation, aspect_ratio)
_orientation_queue: dict[int, tuple[str, float]] = {}
_orientation_lock = threading.Lock()

_last_user_activity = time.monotonic()
_prefetching = False
_pregen_manual_mode = False
_pregen_manual_pause = False
_pregen_scan_offsets = {tier: 0 for tier in THUMB_TIERS}
_last_thumb_config_signature = ""
_thumb_config_changed_at = 0.0
_replace_stale_thumbnails = False
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
_pregen_history = deque()
_pregen_session_started_at: float | None = None
_pregen_session_generated = 0

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


_persistent_conn: sqlite3.Connection | None = None


def _db_connect() -> sqlite3.Connection:
    """Return persistent connection (under _meta_lock, so safe to share)."""
    global _persistent_conn
    if _persistent_conn is not None:
        try:
            _persistent_conn.execute("SELECT 1")
            return _persistent_conn
        except sqlite3.ProgrammingError:
            _persistent_conn = None

    if _persistent_conn is None:
        conn = sqlite3.connect(db.DB_PATH, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cache_metadata ("
            "cache_root TEXT PRIMARY KEY, "
            "thumb_config_signature TEXT NOT NULL, "
            "thumb_config_changed_at REAL NOT NULL, "
            "replace_stale_thumbnails INTEGER NOT NULL DEFAULT 0"
            ")"
        )
        try:
            conn.execute(
                "ALTER TABLE cache_metadata "
                "ADD COLUMN replace_stale_thumbnails INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        conn.commit()
        _persistent_conn = conn
    return _persistent_conn


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


def _cache_access_time(*, hot: bool) -> float:
    now = _current_time()
    return now if hot else now - COLD_CACHE_ACCESS_OFFSET_SECONDS


def note_user_activity():
    global _last_user_activity
    _last_user_activity = time.monotonic()


def get_idle_seconds() -> float:
    return max(0.0, time.monotonic() - _last_user_activity)


def _ensure_disk_cache_dirs():
    if not SSD_CACHE_DIR:
        return
    os.makedirs(SSD_CACHE_DIR, exist_ok=True)
    for size in THUMB_TIERS:
        os.makedirs(os.path.join(SSD_CACHE_DIR, size), exist_ok=True)
    os.makedirs(os.path.join(SSD_CACHE_DIR, FULL_TIER), exist_ok=True)


SSD_REMAINDER_PROFILES = {
    "browse_fast": {"lg": 0.80, FULL_TIER: 0.20},
    "balanced": {"lg": 0.55, FULL_TIER: 0.45},
    "original_heavy": {"lg": 0.35, FULL_TIER: 0.65},
}

MEMORY_CACHE_PROFILES = {
    "browse_fast": {"sm": 0.30, "md": 0.50, "lg": 0.20},
    "balanced": {"sm": 0.20, "md": 0.45, "lg": 0.35},
    "original_heavy": {"sm": 0.15, "md": 0.45, "lg": 0.40},
}


def _normalize_ratios(values: dict[str, float], tiers: tuple[str, ...]) -> dict[str, float]:
    cleaned = {tier: max(0.0, float(values.get(tier, 0.0) or 0.0)) for tier in tiers}
    total = sum(cleaned.values())
    if total <= 0:
        return {tier: 0.0 for tier in tiers}
    return {tier: cleaned[tier] / total for tier in tiers}


def _active_memory_ratios() -> dict[str, float]:
    return _normalize_ratios(
        MEMORY_CACHE_PROFILES.get(CACHE_PROFILE, MEMORY_CACHE_PROFILES["original_heavy"]),
        THUMB_TIERS,
    )


def _allocate_by_ratios(total_bytes: int, ratios: dict[str, float], tiers: tuple[str, ...]) -> dict[str, int]:
    total = max(0, int(total_bytes))
    allocations = {tier: 0 for tier in tiers}
    if total <= 0:
        return allocations

    assigned = 0
    active_tiers = [tier for tier in tiers if ratios.get(tier, 0.0) > 0]
    for tier in active_tiers[:-1]:
        amount = int(total * ratios[tier])
        allocations[tier] = amount
        assigned += amount
    if active_tiers:
        allocations[active_tiers[-1]] = max(0, total - assigned)
    return allocations


def _quality_size_factor() -> float:
    quality = max(40, min(100, int(THUMB_QUALITY)))
    if quality >= 92:
        return 1.0 + (quality - 92) * 0.08
    if quality >= 80:
        return 0.45 + ((quality - 80) / 12.0) * 0.55
    if quality >= 60:
        return 0.28 + ((quality - 60) / 20.0) * 0.17
    return 0.18 + ((quality - 40) / 20.0) * 0.10


def estimated_tier_bytes(size: str) -> int:
    base = {
        "sm": 33 * 1024,
        "md": 450 * 1024,
        "lg": 1750 * 1024,
        FULL_TIER: 20 * 1024 * 1024,
    }
    if size == FULL_TIER:
        return base[FULL_TIER]
    return max(1, int(base.get(size, base["md"]) * _quality_size_factor()))


def _cache_archive_estimates() -> dict:
    fallbacks = {tier: estimated_tier_bytes(tier) for tier in ALL_TIERS}
    estimates = {
        "active_images": 0,
        "total_images": 0,
        "avg_bytes": dict(fallbacks),
        "sample_count": {tier: 0 for tier in ALL_TIERS},
        "needed_bytes": {tier: 0 for tier in ALL_TIERS},
    }
    try:
        with _meta_lock:
            conn = _db_connect()
            row = conn.execute(
                "SELECT "
                "SUM(CASE WHEN status IN ('kept', 'maybe') THEN 1 ELSE 0 END) AS active_images, "
                "COUNT(*) AS total_images "
                "FROM images"
            ).fetchone()
            active_images = int(row["active_images"] or 0)
            total_images = int(row["total_images"] or 0)
            rows = conn.execute(
                "SELECT size, COUNT(*) AS count, COALESCE(SUM(size_bytes), 0) AS bytes "
                "FROM cache_entries WHERE cache_root = ? AND (size = ? OR created_at >= ?) GROUP BY size",
                (SSD_CACHE_DIR, FULL_TIER, _thumb_config_changed_at),
            ).fetchall()
    except Exception:
        active_images = 0
        total_images = 0
        rows = []

    for row in rows:
        size = row["size"]
        if size not in estimates["avg_bytes"]:
            continue
        count = int(row["count"] or 0)
        size_bytes = int(row["bytes"] or 0)
        if count > 0 and size_bytes > 0:
            estimates["avg_bytes"][size] = max(1, int(size_bytes / count))
            estimates["sample_count"][size] = count

    estimates["active_images"] = active_images
    estimates["total_images"] = total_images
    for tier in THUMB_TIERS:
        estimates["needed_bytes"][tier] = estimates["avg_bytes"][tier] * active_images
    estimates["needed_bytes"][FULL_TIER] = estimates["avg_bytes"][FULL_TIER] * total_images
    return estimates


def cache_archive_estimates() -> dict:
    return _cache_archive_estimates()


def _allocate_weighted_capped(total_bytes: int, weights: dict[str, float], caps: dict[str, int]) -> dict[str, int]:
    tiers = tuple(caps.keys())
    ratios = _normalize_ratios(weights, tiers)
    allocations = {tier: 0 for tier in tiers}
    remaining = max(0, int(total_bytes))
    if remaining <= 0:
        return allocations

    rough = _allocate_by_ratios(remaining, ratios, tiers)
    for tier in tiers:
        allocations[tier] = min(max(0, int(caps.get(tier, 0))), rough.get(tier, 0))
    remaining -= sum(allocations.values())

    for tier in sorted(tiers, key=lambda item: ratios.get(item, 0.0), reverse=True):
        if remaining <= 0:
            break
        room = max(0, int(caps.get(tier, 0)) - allocations[tier])
        take = min(room, remaining)
        allocations[tier] += take
        remaining -= take
    return allocations


def _allocate_disk_budget(total_bytes: int) -> dict[str, int]:
    total = max(0, int(total_bytes))
    allocations = {tier: 0 for tier in ALL_TIERS}
    if total <= 0:
        return allocations

    estimates = _cache_archive_estimates()
    needed = estimates["needed_bytes"]
    remaining = total

    for tier in ("sm", "md"):
        amount = min(remaining, int(needed.get(tier, 0) or 0))
        allocations[tier] = amount
        remaining -= amount
        if remaining <= 0:
            return allocations

    remainder_weights = SSD_REMAINDER_PROFILES.get(
        CACHE_PROFILE,
        SSD_REMAINDER_PROFILES["original_heavy"],
    )
    remainder = _allocate_weighted_capped(
        remaining,
        remainder_weights,
        {
            "lg": int(needed.get("lg", 0) or 0),
            FULL_TIER: int(needed.get(FULL_TIER, 0) or 0),
        },
    )
    allocations["lg"] = remainder["lg"]
    allocations[FULL_TIER] = remainder[FULL_TIER]

    leftover = remaining - allocations["lg"] - allocations[FULL_TIER]
    if leftover > 0:
        allocations["md"] += leftover
    return allocations


def _background_tier_budget(size: str) -> int:
    budget = int(_disk_allocations.get(size, 0) or 0)
    if size != "lg" or budget <= 0:
        return budget

    needed = int(_cache_archive_estimates()["needed_bytes"].get("lg", 0) or 0)
    if needed <= 0 or budget >= int(needed * 0.95):
        return budget

    reserve = min(
        budget,
        max(
            HOT_LG_RESERVE_MIN_BYTES,
            min(HOT_LG_RESERVE_MAX_BYTES, int(budget * HOT_LG_RESERVE_FRACTION)),
        ),
    )
    return max(0, budget - reserve)


def cache_budget_config() -> dict:
    memory_allocations = _allocate_by_ratios(MEMORY_CACHE_BYTES, _active_memory_ratios(), THUMB_TIERS)
    ssd_ratios = _normalize_ratios(_disk_allocations, ALL_TIERS)
    return {
        "profile": CACHE_PROFILE,
        "ssd_ratios": ssd_ratios,
        "memory_ratios": _active_memory_ratios(),
        "ssd_allocations": dict(_disk_allocations),
        "memory_allocations": memory_allocations,
    }


_SOURCE_STAT_CACHE_MAX = 25000
_SOURCE_STAT_CACHE_TTL_SECONDS = 60.0
_source_stat_cache: OrderedDict[str, tuple[float, str]] = OrderedDict()

def _get_source_bits(filepath: str) -> str:
    """Cache os.stat results per filepath to avoid repeated HDD stat calls."""
    now = time.monotonic()
    cached = _source_stat_cache.get(filepath)
    if cached is not None and now - cached[0] <= _SOURCE_STAT_CACHE_TTL_SECONDS:
        _source_stat_cache.move_to_end(filepath)
        return cached[1]
    try:
        stat = os.stat(filepath)
        bits = f"{stat.st_size}|{stat.st_mtime_ns}|{filepath}"
    except OSError:
        bits = f"missing|{filepath}"
    _source_stat_cache[filepath] = (now, bits)
    if len(_source_stat_cache) > _SOURCE_STAT_CACHE_MAX:
        _source_stat_cache.popitem(last=False)
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


def _source_missing(filepath: str) -> bool:
    return _get_source_bits(filepath).startswith("missing|")


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
    entry = _memory_get_entry_fast(size, image_id)
    return entry[1] if entry is not None else None


def _memory_get_entry_fast(size: str, image_id: int) -> tuple[str, bytes] | None:
    """Fast memory check that also returns the cached signature."""
    key = (size, image_id)
    with _cache_lock:
        entry = _memory_cache.get(key)
        if entry is None:
            return None
        signature, data = entry
        _memory_cache.move_to_end(key)
        return signature, data


def _memory_get(size: str, image_id: int, source_signature: str) -> bytes | None:
    key = (size, image_id)

    with _cache_lock:
        entry = _memory_cache.get(key)
        if entry is None:
            return None
        cached_signature, data = entry
        if cached_signature != source_signature:
            _memory_remove_locked(key)
            return None
        _memory_cache.move_to_end(key)
        return data


def _memory_tier_budget(size: str) -> int:
    if MEMORY_CACHE_BYTES <= 0:
        return 0
    ratios = _active_memory_ratios()
    return int(MEMORY_CACHE_BYTES * ratios.get(size, 0.0))


def _memory_remove_locked(key: tuple[str, int]) -> bool:
    global _memory_cache_bytes
    entry = _memory_cache.pop(key, None)
    if entry is None:
        return False
    size = key[0]
    data_len = len(entry[1])
    _memory_cache_bytes -= data_len
    _memory_tier_bytes[size] = max(0, _memory_tier_bytes.get(size, 0) - data_len)
    return True


def _evict_memory_oldest_locked(size: str | None = None) -> bool:
    for key in list(_memory_cache.keys()):
        if size is None or key[0] == size:
            return _memory_remove_locked(key)
    return False


def _enforce_memory_budget_locked():
    for size in THUMB_TIERS:
        budget = _memory_tier_budget(size)
        while _memory_tier_bytes.get(size, 0) > budget:
            if not _evict_memory_oldest_locked(size):
                break

    while _memory_cache and _memory_cache_bytes > MEMORY_CACHE_BYTES:
        if not _evict_memory_oldest_locked():
            break


def _memory_put(size: str, image_id: int, source_signature: str, data: bytes):
    if not data or MEMORY_CACHE_BYTES <= 0:
        return
    tier_budget = _memory_tier_budget(size)
    if tier_budget <= 0 or len(data) > tier_budget:
        return

    key = (size, image_id)
    global _memory_cache_bytes

    with _cache_lock:
        _memory_remove_locked(key)

        _memory_cache[key] = (source_signature, data)
        _memory_cache.move_to_end(key)
        data_len = len(data)
        _memory_cache_bytes += data_len
        _memory_tier_bytes[size] = _memory_tier_bytes.get(size, 0) + data_len
        _enforce_memory_budget_locked()


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
        for size in THUMB_TIERS:
            _memory_tier_bytes[size] = 0
    return {
        "entries_cleared": entries_cleared,
        "bytes_cleared": bytes_cleared,
        "counts": counts,
    }


def _clear_memory_tiers(tiers: tuple[str, ...]):
    with _cache_lock:
        for key in list(_memory_cache.keys()):
            if key[0] not in tiers:
                continue
            _memory_remove_locked(key)


def _memory_stats() -> dict:
    with _cache_lock:
        tiers = {size: {"count": 0, "bytes": 0} for size in THUMB_TIERS}
        for (size, _image_id), (_signature, data) in _memory_cache.items():
            tiers[size]["count"] += 1
            tiers[size]["bytes"] += len(data)
        for size in THUMB_TIERS:
            tiers[size]["budget_bytes"] = _memory_tier_budget(size)
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
    _unindex_disk_entry(row["size"], row["image_id"])


_tier_byte_totals: dict[str, int] = {}  # running totals, populated lazily


def _tier_bytes(conn: sqlite3.Connection, size: str) -> int:
    """Get running total for a tier, initializing from DB if needed."""
    total = _tier_byte_totals.get(size)
    if total is None:
        row = conn.execute(
            "SELECT COALESCE(SUM(size_bytes), 0) AS t FROM cache_entries "
            "WHERE cache_root = ? AND size = ?",
            (SSD_CACHE_DIR, size),
        ).fetchone()
        total = int(row["t"])
        _tier_byte_totals[size] = total
    return total


def _enforce_tier_budget_locked(conn: sqlite3.Connection, size: str):
    budget = _disk_allocations.get(size, 0)
    total = _tier_bytes(conn, size)

    if budget <= 0:
        rows = conn.execute(
            "SELECT cache_root, size, image_id, path, size_bytes FROM cache_entries "
            "WHERE cache_root = ? AND size = ?",
            (SSD_CACHE_DIR, size),
        ).fetchall()
        for row in rows:
            _remove_cache_entry_locked(conn, row)
            total -= int(row["size_bytes"])
        conn.commit()
        _tier_byte_totals[size] = max(0, total)
        return

    if total <= budget:
        return

    # Evict cold background entries before recently viewed/warmed images.
    # Elo is a secondary tie-breaker once access recency has separated hot
    # session cache from overnight cache building.
    evict_rows = conn.execute(
        "SELECT c.cache_root, c.size, c.image_id, c.path, c.size_bytes "
        "FROM cache_entries c "
        "LEFT JOIN images i ON c.image_id = i.id "
        "WHERE c.cache_root = ? AND c.size = ? "
        "ORDER BY c.last_accessed ASC, COALESCE(i.elo, 1200) ASC",
        (SSD_CACHE_DIR, size),
    ).fetchall()
    for row in evict_rows:
        _remove_cache_entry_locked(conn, row)
        total -= int(row["size_bytes"])
        if total <= budget:
            break
    conn.commit()
    _tier_byte_totals[size] = max(0, total)


def _enforce_all_disk_budgets():
    _tier_byte_totals.clear()  # force re-read from DB
    with _meta_lock:
        conn = _db_connect()
        for size in ALL_TIERS:
            _enforce_tier_budget_locked(conn, size)


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
        row = conn.execute(
            "SELECT cache_root, size, image_id, path, source_signature, size_bytes "
            "FROM cache_entries WHERE cache_root = ? AND size = ? AND image_id = ?",
            (SSD_CACHE_DIR, size, image_id),
        ).fetchone()
        if row is None:
            return None
        if row["source_signature"] != source_signature:
            return None
        if not os.path.exists(row["path"]):
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
        _index_disk_entry(size, image_id, row["path"], row["source_signature"])
        return row


def touch_cached(size: str, filepath: str, image_id: int) -> bool:
    if size not in ALL_TIERS:
        return False
    source_signature = _build_source_signature(filepath, size, image_id)
    return touch_cached_signature(size, image_id, source_signature)


def touch_cached_signature(size: str, image_id: int, source_signature: str | None = None) -> bool:
    if not SSD_CACHE_DIR or _disk_allocations.get(size, 0) <= 0:
        return False
    with _meta_lock:
        conn = _db_connect()
        if source_signature:
            cursor = conn.execute(
                "UPDATE cache_entries SET last_accessed = ? "
                "WHERE cache_root = ? AND size = ? AND image_id = ? AND source_signature = ?",
                (_current_time(), SSD_CACHE_DIR, size, image_id, source_signature),
            )
        else:
            cursor = conn.execute(
                "UPDATE cache_entries SET last_accessed = ? "
                "WHERE cache_root = ? AND size = ? AND image_id = ?",
                (_current_time(), SSD_CACHE_DIR, size, image_id),
            )
        conn.commit()
        return cursor.rowcount > 0


# In-memory index: (size, image_id) -> (disk path, source signature).
# Built on startup and updated on writes/evictions so hot thumbnail requests
# can skip SQLite without believing stale evicted files still exist.
_disk_path_index: dict[tuple[str, int], tuple[str, str]] = {}
_disk_index_lock = threading.Lock()
_disk_index_built = False


def _build_disk_path_index():
    """Load all cache entry paths into memory for fast lookup."""
    global _disk_index_built
    if not SSD_CACHE_DIR:
        _clear_disk_index()
        _disk_index_built = True
        return
    with _meta_lock:
        conn = _db_connect()
        rows = conn.execute(
            "SELECT size, image_id, path, source_signature FROM cache_entries WHERE cache_root = ?",
            (SSD_CACHE_DIR,),
        ).fetchall()
    new_index = {}
    for row in rows:
        new_index[(row["size"], row["image_id"])] = (row["path"], row["source_signature"])
    with _disk_index_lock:
        _disk_path_index.clear()
        _disk_path_index.update(new_index)
    _disk_index_built = True


def _index_disk_entry(size: str, image_id: int, path: str, source_signature: str):
    """Update the in-memory index when a new cache entry is written."""
    with _disk_index_lock:
        _disk_path_index[(size, image_id)] = (path, source_signature)


def _unindex_disk_entry(size: str, image_id: int):
    with _disk_index_lock:
        _disk_path_index.pop((size, image_id), None)


def _clear_disk_index(tiers: tuple[str, ...] | None = None):
    global _disk_index_built
    with _disk_index_lock:
        if tiers is None:
            _disk_path_index.clear()
            _disk_index_built = False
            return
        for key in list(_disk_path_index.keys()):
            if key[0] in tiers:
                _disk_path_index.pop(key, None)


def fast_disk_has(size: str, image_id: int, source_signature: str | None = None) -> bool:
    if not _disk_index_built:
        _build_disk_path_index()
    with _disk_index_lock:
        entry = _disk_path_index.get((size, image_id))
    if entry is None:
        return False
    path, cached_signature = entry
    if source_signature is not None and cached_signature != source_signature:
        return False
    if os.path.exists(path):
        return True
    _unindex_disk_entry(size, image_id)
    return False


def fast_disk_path_entry(
    size: str,
    image_id: int,
    source_signature: str | None = None,
) -> tuple[str, str] | None:
    if not _disk_index_built:
        _build_disk_path_index()
    with _disk_index_lock:
        entry = _disk_path_index.get((size, image_id))
    if entry is None:
        return None
    path, cached_signature = entry
    if source_signature is not None and cached_signature != source_signature:
        return None
    if os.path.exists(path):
        return cached_signature, path
    _unindex_disk_entry(size, image_id)
    return None


def fast_disk_read_entry(
    size: str,
    image_id: int,
    source_signature: str | None = None,
    *,
    populate_memory: bool = False,
) -> tuple[str, bytes] | None:
    if not _disk_index_built:
        _build_disk_path_index()
    with _disk_index_lock:
        entry = _disk_path_index.get((size, image_id))
    if entry is None:
        return None
    path, cached_signature = entry
    if source_signature is not None and cached_signature != source_signature:
        return None
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError:
        _unindex_disk_entry(size, image_id)
        return None
    if populate_memory and size in THUMB_TIERS:
        _memory_put(size, image_id, cached_signature, data)
    return cached_signature, data


def fast_disk_read(size: str, image_id: int) -> bytes | None:
    """Fast path: read thumbnail from SSD via in-memory index. No SQLite, no locks, no HDD stat."""
    entry = fast_disk_read_entry(size, image_id)
    return entry[1] if entry is not None else None


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
            stale_row = conn.execute(
                "SELECT cache_root, size, image_id, path FROM cache_entries "
                "WHERE cache_root = ? AND size = ? AND image_id = ?",
                (SSD_CACHE_DIR, size, image_id),
            ).fetchone()
            if stale_row is not None:
                _remove_cache_entry_locked(conn, stale_row)
                conn.commit()
        return None

    _memory_put(size, image_id, source_signature, data)
    return data


def _store_disk_entry(
    size: str,
    image_id: int,
    source_signature: str,
    path: str,
    size_bytes: int,
    *,
    hot: bool = True,
):
    now = _current_time()
    access_time = _cache_access_time(hot=hot)
    with _meta_lock:
        conn = _db_connect()
        previous = conn.execute(
            "SELECT cache_root, size, image_id, path, size_bytes FROM cache_entries "
            "WHERE cache_root = ? AND size = ? AND image_id = ?",
            (SSD_CACHE_DIR, size, image_id),
        ).fetchone()
        old_bytes = 0
        if previous is not None:
            old_bytes = int(previous["size_bytes"])
            if previous["path"] != path:
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
                access_time,
                now,
            ),
        )
        # Update running total: add new, subtract old (if replacing)
        if size in _tier_byte_totals:
            _tier_byte_totals[size] += int(size_bytes) - old_bytes
        _enforce_tier_budget_locked(conn, size)
        conn.commit()
        _index_disk_entry(size, image_id, path, source_signature)


def _write_thumbnail_to_disk(size: str, image_id: int, source_signature: str, data: bytes, *, hot: bool):
    budget = _disk_allocations.get(size, 0)
    if not SSD_CACHE_DIR or budget <= 0 or len(data) > budget:
        return

    path = _thumbnail_disk_path(size, image_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp_path = f"{path}.{threading.get_ident()}.tmp"
    with open(temp_path, "wb") as f:
        f.write(data)
    os.replace(temp_path, path)
    # Update in-memory index immediately so has_cached sees it.
    _index_disk_entry(size, image_id, path, source_signature)
    # Queue DB write for bulk flush instead of acquiring _meta_lock per thumbnail.
    with _write_queue_lock:
        _write_queue.append((size, image_id, source_signature, path, len(data), _cache_access_time(hot=hot)))
    _maybe_flush_write_queue()


_WRITE_FLUSH_SIZE = 30  # flush after this many queued writes


def _flush_write_queue():
    """Flush pending cache DB writes in a single transaction."""
    with _write_queue_lock:
        if not _write_queue:
            return
        # Keep only the newest write per cache key. Pregeneration can queue the
        # same image/tier more than once while older work is still flushing.
        latest = {}
        for entry in _write_queue:
            latest[(entry[0], entry[1])] = entry
        batch = list(latest.values())
        _write_queue.clear()

    now = _current_time()
    with _meta_lock:
        conn = _db_connect()
        now = _current_time()
        for size, image_id, source_signature, path, size_bytes, access_time in batch:
            previous = conn.execute(
                "SELECT cache_root, size, image_id, path, size_bytes FROM cache_entries "
                "WHERE cache_root = ? AND size = ? AND image_id = ?",
                (SSD_CACHE_DIR, size, image_id),
            ).fetchone()
            old_bytes = int(previous["size_bytes"]) if previous is not None else 0
            if previous is not None and previous["path"] != path:
                _remove_cache_entry_locked(conn, previous)

            conn.execute(
                "INSERT OR REPLACE INTO cache_entries "
                "(cache_root, size, image_id, path, source_signature, size_bytes, last_accessed, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (SSD_CACHE_DIR, size, image_id, path, source_signature, int(size_bytes), access_time, now),
            )
            if size in _tier_byte_totals:
                _tier_byte_totals[size] += int(size_bytes) - old_bytes
            _index_disk_entry(size, image_id, path, source_signature)
        for size in {entry[0] for entry in batch}:
            _enforce_tier_budget_locked(conn, size)
        conn.commit()


def _maybe_flush_write_queue():
    """Flush if enough writes have accumulated — called from worker threads."""
    with _write_queue_lock:
        should_flush = len(_write_queue) >= _WRITE_FLUSH_SIZE
    if should_flush:
        _flush_write_queue()


def _cache_full_image_sync(filepath: str, image_id: int, source_signature: str, hot: bool = True) -> str:
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
    _store_disk_entry(FULL_TIER, image_id, source_signature, path, source_size, hot=hot)

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
        if ext in JPEG_EXTENSIONS:
            # Always use draft mode for JPEGs — decodes at reduced resolution
            # via libjpeg DCT scaling, cutting both I/O and decode time.
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

    # Fast integer pre-downscale with reduce() when source is much larger,
    # then final filter for quality.
    factor = max(1, long_side // (target_long_side * 2))
    if factor > 1:
        img = img.reduce(factor)
        long_side = max(img.width, img.height)

    scale = target_long_side / long_side
    new_size = (
        max(1, int(round(img.width * scale))),
        max(1, int(round(img.height * scale))),
    )
    # Use BILINEAR for large targets (≥1920px) where the downscale ratio is small
    # and quality difference is imperceptible. LANCZOS for smaller sizes.
    resample = Image.BILINEAR if target_long_side >= 1920 else Image.LANCZOS
    return img.resize(new_size, resample)


def _queue_orientation(image_id: int, img: Image.Image):
    orientation = "landscape" if img.width >= img.height else "portrait"
    aspect_ratio = round(img.width / img.height, 4) if img.height > 0 else 1.5
    with _orientation_lock:
        _orientation_queue[image_id] = (orientation, aspect_ratio)


def _planned_thumbnail_sizes(
    filepath: str,
    image_id: int,
    requested_size: str,
    *,
    include_smaller_tiers: bool = False,
    allow_stale_fallback: bool = True,
) -> list[str]:
    if _source_missing(filepath):
        return []

    needed = []
    now = time.time()
    if include_smaller_tiers:
        requested_long_side = SIZES.get(requested_size, 0)
        candidate_sizes = tuple(
            size for size in THUMB_TIERS
            if SIZES[size] <= requested_long_side and (size == requested_size or _disk_allocations.get(size, 0) > 0)
        )
    else:
        candidate_sizes = (requested_size,)
    for size in candidate_sizes:
        if size not in THUMB_TIERS:
            continue
        source_signature = _build_source_signature(filepath, size, image_id)
        if _thumbnail_retry_after.get((size, image_id, source_signature), 0) > now:
            continue
        if _memory_get(size, image_id, source_signature) is not None:
            continue
        # Fast check via in-memory index before expensive DB query.
        if fast_disk_has(size, image_id, source_signature):
            continue
        if allow_stale_fallback and fast_disk_has(size, image_id):
            continue
        if _get_disk_entry(size, image_id, source_signature, touch=False) is not None:
            continue
        needed.append(size)
    if include_smaller_tiers:
        return sorted(needed, key=lambda tier: SIZES[tier], reverse=True)
    return needed


def _generate_missing_thumbnails_sync(
    filepath: str,
    requested_size: str,
    image_id: int,
    *,
    include_smaller_tiers: bool = False,
    hot: bool = False,
    allow_stale_fallback: bool = True,
):
    needed_sizes = _planned_thumbnail_sizes(
        filepath,
        image_id,
        requested_size,
        include_smaller_tiers=include_smaller_tiers,
        allow_stale_fallback=allow_stale_fallback,
    )
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
            _write_thumbnail_to_disk(size, image_id, source_signature, data, hot=hot)
            _thumbnail_retry_after.pop((size, image_id, source_signature), None)

            if current is not img:
                current.close()
            current = variant
    except Exception as e:
        retry_until = time.time() + THUMBNAIL_RETRY_SECONDS
        for size in needed_sizes:
            source_signature = _build_source_signature(filepath, size, image_id)
            _thumbnail_retry_after[(size, image_id, source_signature)] = retry_until
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
    # Fast check via in-memory index — avoids SQLite while still rejecting
    # stale signatures and evicted files.
    if fast_disk_has(size, image_id, source_signature):
        return True
    return _get_disk_entry(size, image_id, source_signature, touch=False) is not None


def has_cached_fast(size: str, image_id: int) -> bool:
    return _memory_get_entry_fast(size, image_id) is not None or fast_disk_has(size, image_id)


async def _run_thumbnail_job(
    filepath: str,
    size: str,
    image_id: int,
    executor: ThreadPoolExecutor,
    include_smaller_tiers: bool,
    hot: bool,
    allow_stale_fallback: bool,
):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        executor,
        partial(
            _generate_missing_thumbnails_sync,
            filepath,
            size,
            image_id,
            include_smaller_tiers=include_smaller_tiers,
            hot=hot,
            allow_stale_fallback=allow_stale_fallback,
        ),
    )


async def _ensure_thumbnail_with_executor(
    filepath: str,
    size: str,
    image_id: int,
    executor: ThreadPoolExecutor,
    *,
    note_activity: bool,
    include_smaller_tiers: bool = False,
    allow_stale_fallback: bool = True,
) -> bytes:
    if note_activity:
        note_user_activity()

    source_signature = _build_source_signature(filepath, size, image_id)
    cached = _memory_get(size, image_id, source_signature)
    if cached is not None:
        return cached

    # Try lock-free fast path via in-memory index before DB round-trip
    disk_entry = fast_disk_read_entry(
        size,
        image_id,
        None if allow_stale_fallback else source_signature,
    )
    if disk_entry is not None:
        return disk_entry[1]

    cached = _read_disk_thumbnail(size, image_id, source_signature)
    if cached is not None:
        return cached
    if _source_missing(filepath):
        return b""

    inflight_key = ("thumb", image_id, source_signature)
    task = _inflight.get(inflight_key)
    if task is None:
        task = asyncio.create_task(
            _run_thumbnail_job(
                filepath,
                size,
                image_id,
                executor,
                include_smaller_tiers,
                note_activity,
                allow_stale_fallback,
            )
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
    disk_entry = fast_disk_read_entry(
        size,
        image_id,
        None if allow_stale_fallback else source_signature,
    )
    if disk_entry is not None:
        return disk_entry[1]
    return _read_disk_thumbnail(size, image_id, source_signature) or b""


async def get_thumbnail(filepath: str, size: str, image_id: int) -> bytes:
    return await _ensure_thumbnail_with_executor(
        filepath,
        size,
        image_id,
        _executor,
        note_activity=True,
        include_smaller_tiers=False,
    )


async def prefetch_images(
    images: list[dict],
    size: str,
    limit: int | None = None,
    *,
    hot: bool = False,
) -> int:
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
        source_signature = _build_source_signature(filepath, size, image_id)
        require_current = _replace_stale_thumbnails
        if require_current:
            if _memory_get(size, image_id, source_signature) is not None:
                continue
            if fast_disk_has(size, image_id, source_signature):
                if hot:
                    touch_cached_signature(size, image_id, source_signature)
                continue
        elif _memory_get_entry_fast(size, image_id) is not None:
            continue
        if not require_current and fast_disk_has(size, image_id):
            if hot:
                touch_cached_signature(size, image_id, None)
            continue
        if has_cached(size, filepath, image_id):
            if hot:
                touch_cached(size, filepath, image_id)
            continue
        asyncio.create_task(
            _ensure_thumbnail_with_executor(
                filepath,
                size,
                image_id,
                _prefetch_executor,
                note_activity=hot,
                include_smaller_tiers=True,
                allow_stale_fallback=not require_current,
            )
        )
        scheduled += 1
    return scheduled


async def _run_full_image_job(filepath: str, image_id: int, hot: bool):
    loop = asyncio.get_running_loop()
    source_signature = _build_source_signature(filepath, FULL_TIER, image_id)
    return await loop.run_in_executor(
        _executor,
        _cache_full_image_sync,
        filepath,
        image_id,
        source_signature,
        hot,
    )


def get_cached_full_image_path(filepath: str, image_id: int) -> str | None:
    source_signature = _build_source_signature(filepath, FULL_TIER, image_id)
    row = _get_disk_entry(FULL_TIER, image_id, source_signature)
    return row["path"] if row is not None else None


async def schedule_full_image_cache(filepath: str, image_id: int, *, hot: bool = True):
    if not SSD_CACHE_DIR or _disk_allocations.get(FULL_TIER, 0) <= 0:
        return
    if not os.path.exists(filepath):
        return

    source_signature = _build_source_signature(filepath, FULL_TIER, image_id)
    if touch_cached_signature(FULL_TIER, image_id, source_signature):
        return
    inflight_key = ("full", image_id, source_signature)
    task = _inflight.get(inflight_key)
    if task is not None:
        return

    task = asyncio.create_task(_run_full_image_job(filepath, image_id, hot))
    _inflight[inflight_key] = task

    async def _release_when_done():
        try:
            await task
        except Exception:
            pass
        finally:
            if _inflight.get(inflight_key) is task:
                _inflight.pop(inflight_key, None)

    asyncio.create_task(_release_when_done())


async def get_full_image_path(filepath: str, image_id: int) -> str:
    note_user_activity()
    source_signature = _build_source_signature(filepath, FULL_TIER, image_id)
    row = _get_disk_entry(FULL_TIER, image_id, source_signature)
    if row is not None:
        return row["path"]

    inflight_key = ("full", image_id, source_signature)
    task = _inflight.get(inflight_key)
    if task is None:
        task = asyncio.create_task(_run_full_image_job(filepath, image_id, True))
        _inflight[inflight_key] = task

    try:
        result = await task
    finally:
        if _inflight.get(inflight_key) is task and task.done():
            _inflight.pop(inflight_key, None)

    return str(result)


def load_embedding_image(filepath: str, image_id: int, *, require_cached: bool = False) -> Image.Image | None:
    """Load an image for embedding. Prefers SSD-cached md thumbnails for speed,
    but falls back to reading the original file from HDD if no cache exists."""
    # Try fast path first (no HDD stat)
    data = _memory_get_fast("md", image_id)
    if data is None:
        data = fast_disk_read("md", image_id)

    # Fallback to signature-validated read
    if data is None:
        source_signature = _build_source_signature(filepath, "md", image_id)
        data = _memory_get("md", image_id, source_signature)
        if data is None:
            data = _read_disk_thumbnail("md", image_id, source_signature)

    if data is not None:
        with Image.open(io.BytesIO(data)) as source:
            source.load()
            img = source.copy()
        if img.mode != "RGB":
            converted = img.convert("RGB")
            img.close()
            img = converted
        return img

    if require_cached:
        return None

    # No cached thumbnail — load original from HDD and resize to md
    try:
        md_size = SIZES["md"]
        img = _load_source_image(filepath, md_size, prefer_draft=False)
        resized = _resize_to_long_side(img, md_size)
        if resized is not img:
            img.close()
        img = resized
        if img.mode != "RGB":
            converted = img.convert("RGB")
            img.close()
            img = converted
        return img
    except Exception:
        return None


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


def _record_pregen_batch(count: int):
    global _pregen_session_generated, _pregen_session_started_at
    if count <= 0:
        return
    now = _current_time()
    if _pregen_session_started_at is None:
        _pregen_session_started_at = now
    _pregen_session_generated += count
    _pregen_history.append({"ended_at": now, "count": int(count)})
    cutoff = now - 10 * 60
    while _pregen_history and _pregen_history[0]["ended_at"] < cutoff:
        _pregen_history.popleft()


def _pregen_rates() -> tuple[float, float]:
    now = _current_time()
    cutoff = now - 10 * 60
    while _pregen_history and _pregen_history[0]["ended_at"] < cutoff:
        _pregen_history.popleft()
    recent_count = sum(item["count"] for item in _pregen_history)
    recent_window = max(1.0, min(10 * 60.0, now - _pregen_history[0]["ended_at"])) if _pregen_history else 0.0
    recent_rate = (recent_count / recent_window) * 60.0 if recent_window > 0 else 0.0
    session_window = max(1.0, now - _pregen_session_started_at) if _pregen_session_started_at else 0.0
    overall_rate = (_pregen_session_generated / session_window) * 60.0 if session_window > 0 else 0.0
    return recent_rate, overall_rate


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
            "INDEXED BY idx_images_active_filepath "
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


async def _run_pregen_phase(size: str, generate_batch: int | None = None) -> int:
    generate_batch = generate_batch or PREGENERATE_GENERATE_BATCH
    background_budget = _background_tier_budget(size)
    if background_budget <= 0:
        return 0

    with _meta_lock:
        conn = _db_connect()
        if _tier_bytes(conn, size) >= background_budget and not _replace_stale_thumbnails:
            return 0

    offset = _pregen_scan_offsets[size]
    rows = await _pregen_candidate_batch(size, offset, PREGENERATE_SCAN_BATCH)
    if not rows:
        _pregen_scan_offsets[size] = 0
        rows = await _pregen_candidate_batch(size, 0, PREGENERATE_SCAN_BATCH)
        if not rows:
            return 0

    _pregen_scan_offsets[size] = offset + len(rows)

    # Filter to uncached candidates
    pending = []
    for row in rows:
        if not _prefetching or _pregen_manual_pause:
            break
        if not _pregen_manual_mode and (time.monotonic() - _last_user_activity) < PREGENERATE_IDLE_SECONDS:
            break
        if _source_missing(row["filepath"]):
            continue
        current_signature = _build_source_signature(row["filepath"], size, row["id"])
        if row["source_signature"] == current_signature and fast_disk_has(size, row["id"], current_signature):
            continue
        if row["source_signature"] and row["source_signature"] != current_signature:
            if not _replace_stale_thumbnails and fast_disk_has(size, row["id"]):
                continue
        pending.append(row)
        if len(pending) >= generate_batch:
            break

    if not pending:
        return 0

    # Submit all pending jobs at once — the thread pool's internal queue keeps
    # the HDD saturated by starting the next read as soon as a thread frees up.
    tasks = [
        _ensure_thumbnail_with_executor(
            row["filepath"], size, row["id"],
            _prefetch_executor,
            note_activity=False,
            include_smaller_tiers=True,
            allow_stale_fallback=False,
        )
        for row in pending
    ]
    await asyncio.gather(*tasks)
    _flush_write_queue()
    generated = len(pending)
    _pregen_status["last_generated_at"] = _current_time()
    _pregen_status["generated_this_session"] += generated
    _record_pregen_batch(generated)

    if len(rows) < PREGENERATE_SCAN_BATCH:
        _pregen_scan_offsets[size] = 0
    return generated


def cache_stats() -> dict:
    memory = _memory_stats()
    disk_tiers = {
        size: {
            "count": 0,
            "bytes": 0,
            "current_count": 0,
            "current_bytes": 0,
            "stale_count": 0,
            "replacement_mode": False,
            "budget_bytes": _disk_allocations.get(size, 0),
        }
        for size in ALL_TIERS
    }

    with _meta_lock:
        conn = _db_connect()
        rows = conn.execute(
            "SELECT size, COUNT(*) AS count, COALESCE(SUM(size_bytes), 0) AS bytes "
            "FROM cache_entries WHERE cache_root = ? GROUP BY size",
            (SSD_CACHE_DIR,),
        ).fetchall()
        if _replace_stale_thumbnails and _thumb_config_changed_at > 0:
            current_rows = conn.execute(
                "SELECT size, COUNT(*) AS count, COALESCE(SUM(size_bytes), 0) AS bytes "
                "FROM cache_entries "
                "WHERE cache_root = ? AND (size = ? OR created_at >= ?) "
                "GROUP BY size",
                (SSD_CACHE_DIR, FULL_TIER, _thumb_config_changed_at),
            ).fetchall()
        else:
            current_rows = rows

    for row in rows:
        if row["size"] in disk_tiers:
            disk_tiers[row["size"]]["count"] = int(row["count"])
            disk_tiers[row["size"]]["bytes"] = int(row["bytes"])
    for row in current_rows:
        if row["size"] in disk_tiers:
            disk_tiers[row["size"]]["current_count"] = int(row["count"])
            disk_tiers[row["size"]]["current_bytes"] = int(row["bytes"])
    for size, info in disk_tiers.items():
        if not _replace_stale_thumbnails or size == FULL_TIER:
            info["current_count"] = info["count"]
            info["current_bytes"] = info["bytes"]
        info["stale_count"] = max(0, info["count"] - info["current_count"])
        info["replacement_mode"] = bool(
            _replace_stale_thumbnails
            and size in THUMB_TIERS
            and info["stale_count"] > 0
        )

    disk_used = sum(info["bytes"] for info in disk_tiers.values())
    return {
        "memory": memory,
        "disk": {
            "root": SSD_CACHE_DIR,
            "limit_bytes": SSD_CACHE_BYTES,
            "used_bytes": disk_used,
            "tiers": disk_tiers,
        },
        "thumbnail_config": {
            "changed_at": _thumb_config_changed_at,
            "replace_stale_thumbnails": _replace_stale_thumbnails,
        },
    }


def _thumb_config_signature() -> str:
    return f"{CACHE_VERSION}|{SIZES['sm']}|{SIZES['md']}|{SIZES['lg']}|{THUMB_QUALITY}"


def _sync_thumb_config_metadata(new_signature: str, *, replace_thumbnail_cache: bool):
    global _last_thumb_config_signature, _thumb_config_changed_at, _replace_stale_thumbnails
    now = _current_time()

    with _meta_lock:
        conn = _db_connect()
        row = conn.execute(
            "SELECT thumb_config_signature, thumb_config_changed_at, replace_stale_thumbnails "
            "FROM cache_metadata WHERE cache_root = ?",
            (SSD_CACHE_DIR,),
        ).fetchone()
        previous_signature = row["thumb_config_signature"] if row else _last_thumb_config_signature
        previous_changed_at = float(row["thumb_config_changed_at"]) if row else _thumb_config_changed_at
        previous_replace_stale = bool(row["replace_stale_thumbnails"]) if row else _replace_stale_thumbnails

    changed = bool(previous_signature and previous_signature != new_signature)
    if changed:
        _clear_memory_tiers(THUMB_TIERS)
        _thumb_config_changed_at = now
        _replace_stale_thumbnails = bool(replace_thumbnail_cache)
        for tier in THUMB_TIERS:
            _pregen_scan_offsets[tier] = 0
    else:
        _thumb_config_changed_at = previous_changed_at or 0.0
        _replace_stale_thumbnails = previous_replace_stale

    with _meta_lock:
        conn = _db_connect()
        conn.execute(
            "INSERT OR REPLACE INTO cache_metadata "
            "(cache_root, thumb_config_signature, thumb_config_changed_at, replace_stale_thumbnails) "
            "VALUES (?, ?, ?, ?)",
            (
                SSD_CACHE_DIR,
                new_signature,
                _thumb_config_changed_at,
                1 if _replace_stale_thumbnails else 0,
            ),
        )
        conn.commit()

    _last_thumb_config_signature = new_signature


def get_pregen_status(target_total: int = 0, stats: dict | None = None) -> dict:
    stats = stats or cache_stats()
    phases = {}
    remaining = 0
    for size in THUMB_TIERS:
        tier_stats = stats["disk"]["tiers"][size]
        count = int(
            tier_stats.get("progress_count")
            if tier_stats.get("progress_count") is not None
            else (
                tier_stats.get("current_count", 0)
                if tier_stats.get("replacement_mode")
                else tier_stats.get("count", 0)
            )
        )
        available_count = int(tier_stats.get("count", 0))
        current_count = int(tier_stats.get("current_count", count))
        target = target_total
        progress_pct = round((count / target_total) * 100, 1) if target_total > 0 else 0.0
        if target_total > 0:
            background_budget = _background_tier_budget(size)
            avg_bytes = (
                int(tier_stats.get("current_bytes", 0) / current_count)
                if current_count > 0 and tier_stats.get("current_bytes", 0) > 0
                else int(tier_stats["bytes"] / available_count)
                if available_count > 0 and tier_stats["bytes"] > 0
                else estimated_tier_bytes(size)
            )
            if avg_bytes > 0 and background_budget > 0:
                target = min(target_total, max(count, int(background_budget / avg_bytes)))
            remaining += max(0, target - count)
        phases[size] = {
            "count": count,
            "available_count": available_count,
            "current_count": current_count,
            "stale_count": max(0, available_count - current_count),
            "replacement_mode": bool(tier_stats.get("replacement_mode")),
            "total": target,
            "progress_pct": round((count / target) * 100, 1) if target > 0 else progress_pct,
            "budget_bytes": tier_stats["budget_bytes"],
            "background_budget_bytes": _background_tier_budget(size),
            "remaining": max(0, target - count),
        }

    recent_rate, overall_rate = _pregen_rates()
    effective_rate = recent_rate if recent_rate > 0 else overall_rate
    eta_seconds = int((remaining / effective_rate) * 60) if remaining > 0 and effective_rate > 0 else None
    replacement_mode = any(phase["replacement_mode"] for phase in phases.values())

    return {
        **dict(_pregen_status),
        "idle_seconds": round(max(0.0, time.monotonic() - _last_user_activity), 2),
        "phases": phases,
        "remaining": remaining,
        "recent_images_per_min": round(recent_rate, 2),
        "overall_images_per_min": round(overall_rate, 2),
        "eta_seconds": eta_seconds,
        "replacement_mode": replacement_mode,
    }


def clear_cache() -> dict:
    global _replace_stale_thumbnails
    memory = _clear_memory_cache()
    _flush_write_queue()

    disk_removed = 0
    if SSD_CACHE_DIR and os.path.isdir(SSD_CACHE_DIR):
        for _root, _dirs, files in os.walk(SSD_CACHE_DIR):
            disk_removed += len(files)
        shutil.rmtree(SSD_CACHE_DIR, ignore_errors=True)
    _ensure_disk_cache_dirs()
    _clear_disk_index()
    _tier_byte_totals.clear()
    _source_stat_cache.clear()

    with _meta_lock:
        conn = _db_connect()
        conn.execute("DELETE FROM cache_entries WHERE cache_root = ?", (SSD_CACHE_DIR,))
        conn.execute(
            "UPDATE cache_metadata SET replace_stale_thumbnails = 0 WHERE cache_root = ?",
            (SSD_CACHE_DIR,),
        )
        conn.commit()

    _replace_stale_thumbnails = False

    return {
        "memory_entries_cleared": memory["entries_cleared"],
        "memory_bytes_cleared": memory["bytes_cleared"],
        "memory_before": memory["counts"],
        "disk_files_removed": disk_removed,
        "ssd_cache_dir": SSD_CACHE_DIR,
    }


def configure(config: dict):
    global THUMB_QUALITY, SSD_CACHE_DIR, SSD_CACHE_BYTES, MEMORY_CACHE_BYTES
    global CACHE_PROFILE
    global PREGENERATE_ON_IDLE, PREGENERATE_GENERATE_BATCH, PREGENERATE_BATCH_PAUSE_SECONDS
    global _memory_cache_bytes
    global BROWSER_CACHE_MAX_AGE, BROWSER_CACHE_STALE_WHILE_REVALIDATE
    global _executor_workers, _prefetch_workers_count, _executor, _prefetch_executor
    global _disk_allocations, _last_thumb_config_signature, _pregen_manual_pause, _thumb_config_changed_at
    global _replace_stale_thumbnails

    _flush_write_queue()
    old_cache_dir = SSD_CACHE_DIR
    replace_thumbnail_cache = _as_bool(config.get("_replace_thumbnail_cache"), False)

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
    profile = str(config.get("cache_profile", CACHE_PROFILE)).strip().lower()
    CACHE_PROFILE = profile if profile in {"browse_fast", "balanced", "original_heavy"} else "original_heavy"
    PREGENERATE_ON_IDLE = _as_bool(config.get("pregenerate_on_idle"), PREGENERATE_ON_IDLE)
    PREGENERATE_GENERATE_BATCH = max(
        4,
        min(64, int(config.get("pregen_generate_batch", PREGENERATE_GENERATE_BATCH))),
    )
    PREGENERATE_BATCH_PAUSE_SECONDS = max(
        0.0,
        min(5.0, float(config.get("pregen_batch_pause_ms", 250)) / 1000.0),
    )

    disk_cache_dir = (
        config.get("ssd_cache_dir")
        or config.get("disk_cache_dir")
        or SSD_CACHE_DIR
    )
    SSD_CACHE_DIR = os.path.abspath(str(disk_cache_dir).strip() or SSD_CACHE_DIR)
    if SSD_CACHE_DIR != old_cache_dir:
        _clear_disk_index()
        _tier_byte_totals.clear()
    _ensure_disk_cache_dirs()

    _sync_thumb_config_metadata(
        _thumb_config_signature(),
        replace_thumbnail_cache=replace_thumbnail_cache,
    )

    _disk_allocations = _allocate_disk_budget(SSD_CACHE_BYTES)

    if not PREGENERATE_ON_IDLE and not _pregen_manual_mode:
        _pregen_manual_pause = True
    elif PREGENERATE_ON_IDLE and not _pregen_manual_mode:
        _pregen_manual_pause = False

    with _cache_lock:
        _enforce_memory_budget_locked()

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
    _target_total_cache = 0
    _target_total_at = 0.0

    while _prefetching:
        try:
            _flush_write_queue()
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

            now = time.monotonic()
            if now - _target_total_at > 30:
                _target_total_cache = await _cache_target_total()
                _target_total_at = now
            target_total = _target_total_cache
            if target_total <= 0:
                _set_pregen_state("idle", "No images available to warm.")
                await asyncio.sleep(5)
                continue

            decision = resource_governor.get_background_decision(get_idle_seconds())
            _pregen_status["governor"] = decision.to_dict()
            if decision.pause:
                _set_pregen_state(
                    "throttled",
                    f"Background work paused: {decision.reason}.",
                )
                await asyncio.sleep(decision.sleep_seconds)
                continue

            phase_order = ("md", "sm", "lg")
            phases = [size for size in phase_order if _disk_allocations.get(size, 0) > 0]
            if not phases:
                _set_pregen_state("idle", "No SSD thumbnail budget is available.")
                await asyncio.sleep(5)
                continue

            generated = 0
            for phase in phases:
                _set_pregen_state(
                    "running",
                    f"Pre-generating {phase} thumbnails ({decision.mode}: {decision.reason})…",
                    phase=phase,
                )
                phase_generated = await _run_pregen_phase(
                    phase,
                    generate_batch=decision.thumbnail_batch_size,
                )
                generated += phase_generated

            await flush_orientation_updates()

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
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(max(PREGENERATE_BATCH_PAUSE_SECONDS, decision.thumbnail_pause_seconds))

        except Exception as e:
            _set_pregen_state("error", "Pre-generation worker hit an error.", error=str(e))
            print(f"Prefetch worker error: {e}")
            await asyncio.sleep(5)


def stop_prefetch():
    global _prefetching
    _prefetching = False
