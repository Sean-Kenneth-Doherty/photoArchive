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
    "ssd_cache_gb": 10,
    "memory_cache_gb": 0.5,
    "pregenerate_on_idle": True,
    "user_workers": 4,
    "prefetch_workers": 2,
    "browser_cache_max_age": 86400,
    "browser_cache_stale_while_revalidate": 604800,
    "scan_prefetch_limit": 20,
    "cull_prefetch_limit": 24,
    "compare_prefetch_limit": 16,
    "mosaic_prefetch_limit": 24,
    "embed_model_id": "Qwen/Qwen3-VL-Embedding-2B",
    "embed_model_revision": "main",
    "embed_model_dir": _default_model_dir("Qwen/Qwen3-VL-Embedding-2B"),
}

INT_RANGES = {
    "thumb_size_sm": (64, 4096),
    "thumb_size_md": (128, 8192),
    "thumb_size_lg": (128, 8192),
    "thumb_quality": (40, 100),
    "ssd_cache_gb": (0, 4096),
    "user_workers": (1, 32),
    "prefetch_workers": (1, 16),
    "browser_cache_max_age": (0, 31536000),
    "browser_cache_stale_while_revalidate": (0, 31536000),
    "scan_prefetch_limit": (0, 200),
    "cull_prefetch_limit": (0, 200),
    "compare_prefetch_limit": (0, 200),
    "mosaic_prefetch_limit": (0, 200),
}

FLOAT_RANGES = {
    "memory_cache_gb": (0.0, 64.0),
}

_lock = threading.Lock()
_settings = None


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
    temp_path = f"{SETTINGS_PATH}.tmp"

    with _lock:
        os.makedirs(os.path.dirname(SETTINGS_PATH), exist_ok=True)
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(normalized, f, indent=2, sort_keys=True)
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
        _settings = copy.deepcopy(DEFAULT_SETTINGS)
        return copy.deepcopy(_settings)


def settings_metadata() -> dict:
    return {
        "settings_path": SETTINGS_PATH,
        "defaults": copy.deepcopy(DEFAULT_SETTINGS),
    }
