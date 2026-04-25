"""
Embedding worker for photoArchive.

Background worker that embeds images using Qwen3-VL-Embedding-2B (int4).
Embeddings power: text search, find similar, Elo propagation, duplicate
detection, and auto-collections.
"""

import asyncio
import logging
import os
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import numpy as np

# Dedicated executors — separate CPU prep from GPU encode
_embed_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="embed-gpu")
_preload_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="embed-preload")

import ai_models
import db
import embed_cache
import resource_governor
import settings
import thumbnails

log = logging.getLogger("embedding_worker")
log.setLevel(logging.INFO)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

EMBEDDING_DIM = 2048  # Native output dimension for Qwen3-VL-Embedding-2B
INITIAL_EMBED_BATCH_SIZE = 4
DEFAULT_EMBED_BATCH_SIZE = 8
EMBED_BATCH_GROWTH_SUCCESS_BATCHES = 12
EMBED_OOM_GROWTH_COOLDOWN_SECONDS = 600
EMBED_SPEED_WINDOW_SECONDS = 1800
EMBED_CANDIDATE_MULTIPLIER = 16
EMBED_RETRY_SECONDS = 600

# Module-level reference for text search (set by run_embedding_worker on startup)
_model = None
_loaded_model_dir = None
_loaded_model_id = None
_loaded_model_revision = None
_worker_status = {
    "state": "idle",
    "message": "",
    "ready": False,
    "manual_pause": False,
    "model_id": "",
    "model_dir": "",
    "last_error": "",
    "last_batch_size": 0,
    "last_batch_seconds": 0.0,
    "last_embedded_at": None,
    "session_embedded": 0,
    "session_started_at": None,
    "session_embed_seconds": 0.0,
    "session_wall_seconds": 0.0,
    "recent_images_per_min": 0.0,
    "recent_wall_images_per_min": 0.0,
    "overall_images_per_min": 0.0,
    "overall_wall_images_per_min": 0.0,
    "active_batch_size": INITIAL_EMBED_BATCH_SIZE,
    "target_batch_size": DEFAULT_EMBED_BATCH_SIZE,
    "successful_batches_at_size": 0,
    "last_batch_failures": 0,
    "last_batch_stage_seconds": {},
    "last_candidate_query_seconds": 0.0,
    "last_candidate_count": 0,
    "last_candidate_window_size": 0,
    "last_ready_count": 0,
    "last_cooled_down_count": 0,
    "next_retry_at": None,
    "oom_backoffs": 0,
    "last_oom_at": None,
    "batch_growth_paused_until": None,
}
_embedding_history = deque()
_embed_retry_after: dict[int, float] = {}
_embedding_manual_pause = False
_batch_control = {
    "active_batch_size": INITIAL_EMBED_BATCH_SIZE,
    "successful_batches": 0,
    "oom_backoffs": 0,
    "last_oom_at": None,
    "growth_paused_until": None,
}


def _target_embed_batch_size(config: dict | None = None) -> int:
    config = config or settings.get_settings()
    try:
        target = int(config.get("embed_batch_size", DEFAULT_EMBED_BATCH_SIZE))
    except (TypeError, ValueError):
        target = DEFAULT_EMBED_BATCH_SIZE
    return max(1, min(32, target))


def _refresh_batch_status(config: dict | None = None) -> tuple[int, int]:
    target = _target_embed_batch_size(config)
    active = int(_batch_control.get("active_batch_size") or INITIAL_EMBED_BATCH_SIZE)
    active = max(1, min(active, target))
    _batch_control["active_batch_size"] = active
    _worker_status.update({
        "active_batch_size": active,
        "target_batch_size": target,
        "successful_batches_at_size": int(_batch_control.get("successful_batches") or 0),
        "oom_backoffs": int(_batch_control.get("oom_backoffs") or 0),
        "last_oom_at": _batch_control.get("last_oom_at"),
        "batch_growth_paused_until": _batch_control.get("growth_paused_until"),
    })
    return active, target


def _note_successful_embedding_batch(config: dict | None = None):
    active, target = _refresh_batch_status(config)
    if active >= target:
        _batch_control["successful_batches"] = 0
        _refresh_batch_status(config)
        return

    _batch_control["successful_batches"] += 1
    now = time.time()
    growth_paused_until = _batch_control.get("growth_paused_until")
    can_grow = growth_paused_until is None or now >= growth_paused_until
    if (
        can_grow
        and _batch_control["successful_batches"] >= EMBED_BATCH_GROWTH_SUCCESS_BATCHES
    ):
        _batch_control["active_batch_size"] = min(target, active + 1)
        _batch_control["successful_batches"] = 0
    _refresh_batch_status(config)


def _note_cuda_oom(config: dict | None = None) -> int:
    active, _target = _refresh_batch_status(config)
    now = time.time()
    _batch_control["active_batch_size"] = max(1, active // 2)
    _batch_control["successful_batches"] = 0
    _batch_control["oom_backoffs"] = int(_batch_control.get("oom_backoffs") or 0) + 1
    _batch_control["last_oom_at"] = now
    _batch_control["growth_paused_until"] = now + EMBED_OOM_GROWTH_COOLDOWN_SECONDS
    _refresh_batch_status(config)
    return int(_batch_control["active_batch_size"])


def _is_cuda_oom_error(error) -> bool:
    name = type(error).__name__.lower()
    text = str(error).lower()
    return (
        "outofmemoryerror" in name
        or "cuda out of memory" in text
        or ("cuda" in text and "out of memory" in text)
    )


def _clear_cuda_cache():
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception:
        pass


def _set_worker_status(state: str, message: str = "", ready: bool = False, last_error: str = ""):
    config = settings.get_settings()
    _refresh_batch_status(config)
    _worker_status.update({
        "state": state,
        "message": message,
        "ready": ready,
        "manual_pause": _embedding_manual_pause,
        "model_id": config["embed_model_id"],
        "model_dir": config["embed_model_dir"],
        "last_error": last_error,
    })


def _recompute_speed_metrics(now: float | None = None):
    now = now or time.time()
    cutoff = now - EMBED_SPEED_WINDOW_SECONDS
    while _embedding_history and _embedding_history[0]["ended_at"] < cutoff:
        _embedding_history.popleft()

    recent_images = sum(item["count"] for item in _embedding_history)
    recent_seconds = sum(item["seconds"] for item in _embedding_history)
    recent_wall_seconds = sum(item.get("wall_seconds", item["seconds"]) for item in _embedding_history)
    overall_images = _worker_status["session_embedded"]
    overall_seconds = _worker_status["session_embed_seconds"]
    overall_wall_seconds = _worker_status["session_wall_seconds"]

    _worker_status["recent_images_per_min"] = (
        round((recent_images / recent_seconds) * 60, 2) if recent_seconds > 0 else 0.0
    )
    _worker_status["recent_wall_images_per_min"] = (
        round((recent_images / recent_wall_seconds) * 60, 2) if recent_wall_seconds > 0 else 0.0
    )
    _worker_status["overall_images_per_min"] = (
        round((overall_images / overall_seconds) * 60, 2) if overall_seconds > 0 else 0.0
    )
    _worker_status["overall_wall_images_per_min"] = (
        round((overall_images / overall_wall_seconds) * 60, 2) if overall_wall_seconds > 0 else 0.0
    )


def _record_embedding_batch(
    count: int,
    seconds: float,
    *,
    wall_seconds: float | None = None,
    stage_seconds: dict | None = None,
    failures: int = 0,
):
    wall_seconds = seconds if wall_seconds is None else wall_seconds
    if stage_seconds is not None:
        _worker_status["last_batch_stage_seconds"] = {
            key: round(max(float(value), 0.0), 3)
            for key, value in stage_seconds.items()
        }
    _worker_status["last_batch_failures"] = int(failures)

    if count <= 0:
        return

    now = time.time()
    if not _worker_status["session_started_at"]:
        _worker_status["session_started_at"] = now

    seconds = max(float(seconds), 0.001)
    wall_seconds = max(float(wall_seconds), 0.001)
    _worker_status["last_batch_size"] = count
    _worker_status["last_batch_seconds"] = round(seconds, 3)
    _worker_status["last_embedded_at"] = now
    _worker_status["session_embedded"] += count
    _worker_status["session_embed_seconds"] += seconds
    _worker_status["session_wall_seconds"] += wall_seconds
    _embedding_history.append({
        "ended_at": now,
        "count": count,
        "seconds": seconds,
        "wall_seconds": wall_seconds,
    })
    _recompute_speed_metrics(now)


def _schedule_embed_retry(image_id: int, error: str):
    wait_seconds = EMBED_RETRY_SECONDS
    if "No such file" in error or "FileNotFoundError" in error:
        wait_seconds = EMBED_RETRY_SECONDS
    elif "cannot identify image file" in error:
        wait_seconds = EMBED_RETRY_SECONDS * 3

    _embed_retry_after[image_id] = time.time() + wait_seconds


def _select_ready_candidates(rows, limit: int | None = None):
    now = time.time()
    selected = []
    cooled_down = 0
    next_retry_at = None

    for row in rows:
        retry_after = _embed_retry_after.get(row["id"], 0)
        if retry_after > now:
            cooled_down += 1
            if next_retry_at is None or retry_after < next_retry_at:
                next_retry_at = retry_after
            continue

        selected.append(row)
        if limit is not None and len(selected) >= limit:
            break

    return selected, cooled_down, next_retry_at


def get_worker_status() -> dict:
    _refresh_batch_status()
    _recompute_speed_metrics()
    _worker_status["manual_pause"] = _embedding_manual_pause
    return dict(_worker_status)


def pause_embedding_worker() -> dict:
    global _embedding_manual_pause
    _embedding_manual_pause = True
    _set_worker_status("paused", "Embedding paused by user.", ready=_model is not None)
    return get_worker_status()


def resume_embedding_worker() -> dict:
    global _embedding_manual_pause
    _embedding_manual_pause = False
    _set_worker_status("idle", "Embedding will resume automatically.", ready=_model is not None)
    return get_worker_status()


def _load_model(model_dir: str, model_id: str):
    """Load the embedding model strictly from the local filesystem."""
    import torch
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(
        model_dir,
        model_kwargs={
            "quantization_config": {
                "load_in_4bit": True,
                "bnb_4bit_compute_dtype": torch.float16,
                "bnb_4bit_use_double_quant": True,
                "bnb_4bit_quant_type": "nf4",
            },
            "torch_dtype": torch.float16,
        },
        trust_remote_code=True,
        local_files_only=True,
    )
    log.info(f"{model_id} loaded from {model_dir} (int4, {EMBEDDING_DIM}-dim)")
    return model


def _load_image_for_embedding(image_id: int, path: str):
    return thumbnails.load_embedding_image(path, image_id, require_cached=True)


def _preload_images(
    image_refs: list[tuple[int, str]],
) -> tuple[list, list[int], list[str | None]]:
    """CPU stage: load images from SSD cache and resize. No GPU work."""
    errors = [None] * len(image_refs)
    valid = []
    valid_indices = []
    for i, (image_id, path) in enumerate(image_refs):
        img = None
        try:
            img = _load_image_for_embedding(image_id, path)
            if img is None:
                errors[i] = "md thumbnail not cached yet"
                continue
            valid.append(img)
            valid_indices.append(i)
        except Exception as e:
            errors[i] = f"{type(e).__name__}: {e}"
            log.debug(f"Skipping image {path}: {e}")
            if img is not None:
                try:
                    img.close()
                except Exception:
                    pass
    return valid, valid_indices, errors


def _timed_preload_images(
    image_refs: list[tuple[int, str]],
) -> tuple[list, list[int], list[str | None], float]:
    started = time.perf_counter()
    valid, valid_indices, errors = _preload_images(image_refs)
    return valid, valid_indices, errors, time.perf_counter() - started


def _close_preloaded_images(valid: list):
    for img in valid:
        try:
            img.close()
        except Exception:
            pass


def _encode_images(
    model, valid: list, valid_indices: list[int], n_refs: int,
) -> tuple[list[np.ndarray | None], list[str | None]]:
    """GPU stage: encode pre-loaded PIL images."""
    results = [None] * n_refs
    errors = [None] * n_refs

    if not valid:
        return results, errors

    try:
        embeddings = model.encode(valid, normalize_embeddings=True)
        for idx, valid_i in enumerate(valid_indices):
            results[valid_i] = embeddings[idx].astype(np.float32)
    except Exception as e:
        if _is_cuda_oom_error(e):
            raise
        failure = f"{type(e).__name__}: {e}"
        for valid_i in valid_indices:
            errors[valid_i] = failure
    finally:
        _close_preloaded_images(valid)
    return results, errors


_text_cache: dict[str, np.ndarray] = {}
_TEXT_CACHE_MAX = 100

def encode_text(query: str) -> np.ndarray | None:
    """Encode a text query into an embedding. Cached for repeat queries."""
    if _model is None:
        return None
    cached = _text_cache.get(query)
    if cached is not None:
        return cached
    embedding = _model.encode(
        [query],
        prompt="Retrieve images relevant to the query.",
        normalize_embeddings=True,
    )
    vec = embedding[0].astype(np.float32)
    if len(_text_cache) >= _TEXT_CACHE_MAX:
        _text_cache.pop(next(iter(_text_cache)))  # evict oldest
    _text_cache[query] = vec
    return vec


def vec_to_blob(vec: np.ndarray) -> bytes:
    return np.asarray(vec, dtype=np.float32).tobytes()


def blob_to_vec(blob: bytes) -> np.ndarray:
    return np.frombuffer(blob, dtype=np.float32).copy()


def _row_image_ref(row) -> tuple[int, str]:
    return int(row["id"]), row["filepath"]


def _schedule_preload(loop, rows):
    image_refs = [_row_image_ref(row) for row in rows]
    return loop.run_in_executor(_preload_executor, _timed_preload_images, image_refs)


async def _discard_preload_future(future):
    if future is None:
        return
    try:
        valid, _valid_indices, _errors, _seconds = await future
    except Exception:
        return
    _close_preloaded_images(valid)


async def _process_embedding_candidates(
    loop,
    model,
    rows,
    *,
    batch_pause_seconds: float = 0.0,
) -> dict:
    """Embed one candidate window, splitting it into adaptive chunks."""
    index = 0
    stored_total = 0
    failed_total = 0
    chunks_completed = 0
    first_failure_error = None
    preload_future = None
    preload_rows = None

    while index < len(rows):
        if _embedding_manual_pause:
            break

        active_batch_size, _target = _refresh_batch_status()
        if preload_future is None:
            preload_rows = rows[index:index + active_batch_size]
            preload_future = _schedule_preload(loop, preload_rows)

        chunk_rows = preload_rows or []
        chunk_len = len(chunk_rows)
        if chunk_len <= 0:
            break

        wall_started = time.perf_counter()
        preload_seconds = 0.0
        encode_seconds = 0.0
        store_seconds = 0.0
        pause_seconds = 0.0

        try:
            valid, valid_indices, preload_errors, preload_seconds = await preload_future
        except Exception as e:
            valid = []
            valid_indices = []
            preload_errors = [f"{type(e).__name__}: {e}"] * chunk_len
            preload_seconds = time.perf_counter() - wall_started

        next_index = index + chunk_len
        next_future = None
        next_rows = None
        if next_index < len(rows):
            next_active_batch_size, _target = _refresh_batch_status()
            next_rows = rows[next_index:next_index + next_active_batch_size]
            next_future = _schedule_preload(loop, next_rows)

        try:
            encode_started = time.perf_counter()
            vectors, encode_errors = await loop.run_in_executor(
                _embed_executor, _encode_images, model, valid, valid_indices, chunk_len
            )
            encode_seconds = time.perf_counter() - encode_started
        except Exception as e:
            if not _is_cuda_oom_error(e):
                await _discard_preload_future(next_future)
                raise

            await loop.run_in_executor(_embed_executor, _clear_cuda_cache)
            _note_cuda_oom()
            await _discard_preload_future(next_future)

            stage_seconds = {
                "candidate_query": float(_worker_status.get("last_candidate_query_seconds") or 0.0),
                "preload": preload_seconds,
                "encode": time.perf_counter() - encode_started,
                "store": 0.0,
                "pause": 0.0,
                "wall": time.perf_counter() - wall_started,
            }
            _record_embedding_batch(
                0,
                0.0,
                wall_seconds=stage_seconds["wall"],
                stage_seconds=stage_seconds,
                failures=chunk_len if chunk_len <= 1 else 0,
            )

            if chunk_len <= 1:
                failure = f"{type(e).__name__}: {e}"
                first_failure_error = first_failure_error or failure
                failed_total += chunk_len
                for row in chunk_rows:
                    _schedule_embed_retry(int(row["id"]), failure)
                index = next_index

            preload_future = None
            preload_rows = None
            await asyncio.sleep(0)
            continue

        errors = [preload_errors[i] or encode_errors[i] for i in range(chunk_len)]
        batch = []
        cached_vectors = []
        failed = []
        for row, vec, error in zip(chunk_rows, vectors, errors):
            image_id = int(row["id"])
            if vec is not None:
                batch.append((image_id, vec_to_blob(vec)))
                cached_vectors.append((image_id, vec))
                _embed_retry_after.pop(image_id, None)
            elif error:
                failed.append((image_id, error))
                first_failure_error = first_failure_error or error
                _schedule_embed_retry(image_id, error)

        store_started = time.perf_counter()
        if batch:
            await db.store_embeddings_batch(batch)
            embed_cache.add_vectors(cached_vectors)
            embedded_count = await db.get_embedding_count()
            log.info(f"Embedded {len(batch)} images (total: {embedded_count})")
            _note_successful_embedding_batch()
        store_seconds = time.perf_counter() - store_started

        if batch_pause_seconds:
            pause_started = time.perf_counter()
            await asyncio.sleep(batch_pause_seconds)
            pause_seconds = time.perf_counter() - pause_started

        wall_seconds = time.perf_counter() - wall_started
        stage_seconds = {
            "candidate_query": float(_worker_status.get("last_candidate_query_seconds") or 0.0),
            "preload": preload_seconds,
            "encode": encode_seconds,
            "store": store_seconds,
            "pause": pause_seconds,
            "wall": wall_seconds,
        }
        work_seconds = max(wall_seconds - pause_seconds, 0.001)
        _record_embedding_batch(
            len(batch),
            work_seconds,
            wall_seconds=wall_seconds,
            stage_seconds=stage_seconds,
            failures=len(failed),
        )

        stored_total += len(batch)
        failed_total += len(failed)
        chunks_completed += 1
        index = next_index
        preload_future = next_future
        preload_rows = next_rows

    return {
        "stored": stored_total,
        "failed": failed_total,
        "chunks": chunks_completed,
        "first_error": first_failure_error,
    }


async def run_embedding_worker():
    """Main background loop: embed images for search, similarity, and Elo propagation."""
    loop = asyncio.get_running_loop()

    global _model, _loaded_model_dir, _loaded_model_id, _loaded_model_revision


    while True:
        try:
            config = settings.get_settings()
            model_id = config["embed_model_id"]
            model_revision = config["embed_model_revision"]
            model_dir = config["embed_model_dir"]
            decision = resource_governor.get_background_decision(thumbnails.get_idle_seconds())
            _worker_status["governor"] = decision.to_dict()
            batch_pause_seconds = max(
                0.0,
                min(
                    5.0,
                    max(
                        float(config.get("embed_batch_pause_ms", 250)) / 1000.0,
                        decision.embedding_pause_seconds,
                    ),
                ),
            )
            model_installed = ai_models.model_files_present(model_dir)

            if _embedding_manual_pause:
                _set_worker_status("paused", "Embedding paused by user.", ready=_model is not None)
                await asyncio.sleep(1)
                continue

            if decision.pause:
                _set_worker_status(
                    "throttled",
                    f"Background embedding paused: {decision.reason}.",
                    ready=_model is not None,
                )
                await asyncio.sleep(decision.sleep_seconds)
                continue

            if not model_installed:
                _model = None
                _loaded_model_dir = None
                _loaded_model_id = None
                _loaded_model_revision = None
                has_model = False
                _set_worker_status(
                    "waiting_for_model",
                    f"Install {model_id} from Settings to enable AI features.",
                    ready=False,
                )
                await asyncio.sleep(10)
                continue

            needs_model_load = (
                _model is None
                or _loaded_model_dir != model_dir
                or _loaded_model_id != model_id
                or _loaded_model_revision != model_revision
            )
            if needs_model_load and not decision.can_start_heavy_work:
                _set_worker_status(
                    "throttled",
                    f"Model loading deferred until the desktop is idle and quiet: {decision.reason}.",
                    ready=False,
                )
                await asyncio.sleep(max(5.0, decision.embedding_pause_seconds))
                continue

            if needs_model_load:
                _set_worker_status("loading_model", f"Loading {model_id} from disk…", ready=False)
                _model = await loop.run_in_executor(_embed_executor, _load_model, model_dir, model_id)
                _loaded_model_dir = model_dir
                _loaded_model_id = model_id
                _loaded_model_revision = model_revision
                _set_worker_status("ready", f"{model_id} loaded locally.", ready=True)

            # Phase 1: Embed unembedded images with pipelined CPU/GPU
            active_batch_size, _target_batch_size = _refresh_batch_status(config)
            candidate_limit = max(1, active_batch_size * EMBED_CANDIDATE_MULTIPLIER)
            query_started = time.perf_counter()
            candidates = await db.get_unembedded_images(
                limit=candidate_limit,
                md_cache_root=thumbnails.SSD_CACHE_DIR,
            )
            query_seconds = time.perf_counter() - query_started
            unembedded, cooled_down, next_retry_at = _select_ready_candidates(candidates)
            _worker_status.update({
                "last_candidate_query_seconds": round(query_seconds, 3),
                "last_candidate_count": len(candidates),
                "last_candidate_window_size": candidate_limit,
                "last_ready_count": len(unembedded),
                "last_cooled_down_count": cooled_down,
                "next_retry_at": next_retry_at,
            })
            if unembedded:
                _set_worker_status(
                    "embedding",
                    f"Embedding {len(unembedded)} images in batches up to {active_batch_size}…",
                    ready=True,
                )
                result = await _process_embedding_candidates(
                    loop,
                    _model,
                    unembedded,
                    batch_pause_seconds=batch_pause_seconds,
                )

                if result["failed"] and not result["stored"]:
                    sample_error = result.get("first_error") or "image unavailable"
                    _set_worker_status(
                        "embedding",
                        f"Skipping {result['failed']} unavailable files for now; retrying other images. Latest error: {sample_error}",
                        ready=True,
                    )
                continue

            if candidates and cooled_down:
                wait_for = max(1, int(next_retry_at - time.time())) if next_retry_at else 5
                _set_worker_status(
                    "embedding",
                    f"Waiting to retry {cooled_down} unavailable files in about {wait_for}s.",
                    ready=True,
                )
                await asyncio.sleep(min(wait_for, 10))
                continue

            _set_worker_status("idle", "Waiting for new images…", ready=True)
            await asyncio.sleep(2)

        except Exception as e:
            _set_worker_status("error", str(e), ready=False, last_error=str(e))
            log.error(f"Embedding worker error: {e}", exc_info=True)
            await asyncio.sleep(10)
