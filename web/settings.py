import copy
import json
import os
import threading

WEB_DIR = os.path.dirname(__file__)
SETTINGS_PATH = os.path.join(WEB_DIR, "settings.local.json")


def _default_model_dir(model_id: str) -> str:
    safe = model_id.replace("/", "--").replace("\\", "--").replace(":", "-")
    return os.path.join(WEB_DIR, ".models", safe)


DEFAULT_SETTINGS = {
    "thumb_size_sm": 400,
    "thumb_size_md": 1920,
    "thumb_size_lg": 3840,
    "thumb_quality": 92,
    "ssd_cache_dir": os.path.join(WEB_DIR, ".thumbcache"),
    "ssd_cache_gb": 100,
    "memory_cache_gb": 0.5,
    "cache_profile": "original_heavy",
    "pregenerate_on_idle": True,
    "background_thumb_workers": 2,
    "pregen_generate_batch": 16,
    "pregen_batch_pause_ms": 250,
    "embed_batch_pause_ms": 250,
    "embed_batch_size": 8,
    "embed_model_id": "Qwen/Qwen3-VL-Embedding-2B",
    "embed_model_revision": "main",
    "embed_model_dir": _default_model_dir("Qwen/Qwen3-VL-Embedding-2B"),
    "search_similarity_threshold": 0.35,
}

INT_RANGES = {
    "thumb_size_sm": (64, 4096),
    "thumb_size_md": (128, 8192),
    "thumb_size_lg": (128, 8192),
    "thumb_quality": (40, 100),
    "ssd_cache_gb": (0, 4096),
    "background_thumb_workers": (1, 4),
    "pregen_generate_batch": (4, 64),
    "pregen_batch_pause_ms": (0, 5000),
    "embed_batch_pause_ms": (0, 5000),
    "embed_batch_size": (1, 32),
}

FLOAT_RANGES = {
    "memory_cache_gb": (0.0, 64.0),
    "search_similarity_threshold": (0.1, 0.8),
}

_lock = threading.Lock()
_settings = None

CACHE_PROFILES = ("browse_fast", "balanced", "original_heavy")

BROWSER_CACHE_MAX_AGE = 86400
BROWSER_CACHE_STALE_WHILE_REVALIDATE = 604800


def _system_memory_gb() -> float | None:
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        phys_pages = os.sysconf("SC_PHYS_PAGES")
        if page_size <= 0 or phys_pages <= 0:
            return None
        return (page_size * phys_pages) / (1024 ** 3)
    except (AttributeError, OSError, ValueError):
        return None


def _clamp(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, value))


def _derive_runtime_tuning(memory_cache_gb: float) -> dict:
    cpu_count = max(1, os.cpu_count() or 4)
    system_memory_gb = _system_memory_gb()

    user_workers = _clamp(cpu_count - 1, 2, 12)
    if system_memory_gb is not None:
        if system_memory_gb < 8:
            user_workers = min(user_workers, 4)
        elif system_memory_gb < 16:
            user_workers = min(user_workers, 6)

    cache_aggression = max(1, min(4, int(memory_cache_gb / 0.5) if memory_cache_gb > 0 else 1))
    prefetch_target = min(user_workers - 2, user_workers // 2 + cache_aggression - 1)
    prefetch_workers = _clamp(max(1, prefetch_target), 1, 3)
    warm_factor = max(1, prefetch_workers * cache_aggression)

    return {
        "cpu_count": cpu_count,
        "system_memory_gb": round(system_memory_gb, 1) if system_memory_gb is not None else None,
        "user_workers": user_workers,
        "prefetch_workers": prefetch_workers,
        "scan_prefetch_limit": _clamp(warm_factor * 8, 24, 96),
        "review_prefetch_limit": _clamp(warm_factor * 6, 12, 48),
        "compare_prefetch_limit": _clamp(warm_factor * 4, 8, 32),
        "mosaic_prefetch_limit": _clamp(warm_factor * 6, 12, 48),
        "browser_cache_max_age": BROWSER_CACHE_MAX_AGE,
        "browser_cache_stale_while_revalidate": BROWSER_CACHE_STALE_WHILE_REVALIDATE,
    }


def _resolve_cache_dir(path: str, default: str) -> str:
    value = (path or "").strip()
    if not value:
        return default
    if not os.path.isabs(value):
        value = os.path.abspath(os.path.join(WEB_DIR, value))
    if value == os.path.sep:
        return default
    return value


def normalize_settings(raw: dict | None) -> dict:
    normalized = copy.deepcopy(DEFAULT_SETTINGS)
    if not isinstance(raw, dict):
        return normalized

    if "thumb_quality" not in raw and "jpeg_quality" in raw:
        raw = {**raw, "thumb_quality": raw.get("jpeg_quality")}
    if "ssd_cache_dir" not in raw and "disk_cache_dir" in raw:
        raw = {**raw, "ssd_cache_dir": raw.get("disk_cache_dir")}
    if "memory_cache_gb" not in raw and "memory_cache_mb" in raw:
        try:
            raw = {
                **raw,
                "memory_cache_gb": max(0.0, float(raw.get("memory_cache_mb", 0)) / 1024.0),
            }
        except (TypeError, ValueError):
            pass
    if "memory_cache_gb" not in raw:
        cache_limit_values = [
            raw.get("cache_limit_sm"),
            raw.get("cache_limit_md"),
            raw.get("cache_limit_lg"),
        ]
        if any(value is not None for value in cache_limit_values):
            try:
                approx_entries = sum(int(value or 0) for value in cache_limit_values)
                raw = {
                    **raw,
                    "memory_cache_gb": max(0.25, min(64.0, (approx_entries // 8 or 512) / 1024.0)),
                }
            except (TypeError, ValueError):
                pass

    model_id = (raw.get("embed_model_id") or normalized["embed_model_id"]).strip()
    if not model_id:
        model_id = normalized["embed_model_id"]
    normalized["embed_model_id"] = model_id

    revision = (raw.get("embed_model_revision") or normalized["embed_model_revision"]).strip()
    normalized["embed_model_revision"] = revision or "main"

    normalized["ssd_cache_dir"] = _resolve_cache_dir(
        raw.get("ssd_cache_dir", normalized["ssd_cache_dir"]),
        DEFAULT_SETTINGS["ssd_cache_dir"],
    )
    normalized["embed_model_dir"] = _resolve_cache_dir(
        raw.get("embed_model_dir", _default_model_dir(model_id)),
        _default_model_dir(model_id),
    )

    profile = str(raw.get("cache_profile", normalized["cache_profile"])).strip().lower()
    normalized["cache_profile"] = profile if profile in CACHE_PROFILES else DEFAULT_SETTINGS["cache_profile"]

    for key, (min_value, max_value) in INT_RANGES.items():
        value = raw.get(key, normalized[key])
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = normalized[key]
        normalized[key] = max(min_value, min(max_value, value))

    for key, (min_value, max_value) in FLOAT_RANGES.items():
        value = raw.get(key, normalized[key])
        try:
            value = float(value)
        except (TypeError, ValueError):
            value = normalized[key]
        normalized[key] = round(max(min_value, min(max_value, value)), 2)

    if normalized["thumb_size_sm"] > normalized["thumb_size_md"]:
        normalized["thumb_size_md"] = normalized["thumb_size_sm"]
    if normalized["thumb_size_md"] > normalized["thumb_size_lg"]:
        normalized["thumb_size_lg"] = normalized["thumb_size_md"]

    normalized["pregenerate_on_idle"] = bool(raw.get("pregenerate_on_idle", normalized["pregenerate_on_idle"]))
    normalized.update(_derive_runtime_tuning(normalized["memory_cache_gb"]))
    normalized["prefetch_workers"] = min(
        normalized["prefetch_workers"],
        normalized["background_thumb_workers"],
    )

    return normalized


def load_settings(force: bool = False) -> dict:
    global _settings
    with _lock:
        if _settings is not None and not force:
            return copy.deepcopy(_settings)

        raw = {}
        if os.path.exists(SETTINGS_PATH):
            try:
                with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                    raw = json.load(f)
            except Exception:
                raw = {}

        _settings = normalize_settings(raw)
        return copy.deepcopy(_settings)


def get_settings() -> dict:
    return load_settings()


def save_settings(raw: dict | None) -> dict:
    global _settings
    normalized = normalize_settings(raw)
    persisted = {key: normalized[key] for key in DEFAULT_SETTINGS}
    temp_path = f"{SETTINGS_PATH}.tmp"

    with _lock:
        os.makedirs(os.path.dirname(SETTINGS_PATH), exist_ok=True)
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(persisted, f, indent=2, sort_keys=True)
        os.replace(temp_path, SETTINGS_PATH)
        _settings = normalized
        return copy.deepcopy(_settings)


def reset_settings() -> dict:
    global _settings
    with _lock:
        try:
            os.remove(SETTINGS_PATH)
        except FileNotFoundError:
            pass
        _settings = normalize_settings({})
        return copy.deepcopy(_settings)


def settings_metadata() -> dict:
    return {
        "settings_path": SETTINGS_PATH,
        "defaults": copy.deepcopy(DEFAULT_SETTINGS),
        "cache_profiles": list(CACHE_PROFILES),
    }
