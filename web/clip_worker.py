"""
Qwen3-VL-Embedding + active learning taste model for photoArchive.

Background worker that:
1. Embeds kept/maybe images using Qwen3-VL-Embedding-8B (int4 quantized)
2. Trains a Ridge regression (embedding -> predicted Elo) on comparison data
3. Computes per-image uncertainty for smart mosaic selection
4. Provides text-to-image search via shared embedding space
"""

import asyncio
import logging
import os
import struct
import time
from collections import deque

import numpy as np

import ai_models
import db
import settings
import thumbnails

log = logging.getLogger("clip_worker")
log.setLevel(logging.INFO)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

EMBEDDING_DIM = 2048  # Native output dimension for Qwen3-VL-Embedding-2B
BATCH_SIZE = 4  # Small batches for VL model
RETRAIN_EVERY = 50
EMBED_SPEED_WINDOW_SECONDS = 300
EMBED_CANDIDATE_MULTIPLIER = 16
EMBED_RETRY_SECONDS = 600

# Module-level reference for text search (set by run_clip_worker on startup)
_model = None
_loaded_model_dir = None
_loaded_model_id = None
_loaded_model_revision = None
_worker_status = {
    "state": "idle",
    "message": "",
    "ready": False,
    "model_id": "",
    "model_dir": "",
    "last_error": "",
    "last_batch_size": 0,
    "last_batch_seconds": 0.0,
    "last_embedded_at": None,
    "session_embedded": 0,
    "session_started_at": None,
    "session_embed_seconds": 0.0,
    "recent_images_per_min": 0.0,
    "overall_images_per_min": 0.0,
}
_embedding_history = deque()
_embed_retry_after: dict[int, float] = {}


def _set_worker_status(state: str, message: str = "", ready: bool = False, last_error: str = ""):
    config = settings.get_settings()
    _worker_status.update({
        "state": state,
        "message": message,
        "ready": ready,
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
    overall_images = _worker_status["session_embedded"]
    overall_seconds = _worker_status["session_embed_seconds"]

    _worker_status["recent_images_per_min"] = (
        round((recent_images / recent_seconds) * 60, 2) if recent_seconds > 0 else 0.0
    )
    _worker_status["overall_images_per_min"] = (
        round((overall_images / overall_seconds) * 60, 2) if overall_seconds > 0 else 0.0
    )


def _record_embedding_batch(count: int, seconds: float):
    if count <= 0:
        return

    now = time.time()
    if not _worker_status["session_started_at"]:
        _worker_status["session_started_at"] = now

    seconds = max(float(seconds), 0.001)
    _worker_status["last_batch_size"] = count
    _worker_status["last_batch_seconds"] = round(seconds, 3)
    _worker_status["last_embedded_at"] = now
    _worker_status["session_embedded"] += count
    _worker_status["session_embed_seconds"] += seconds
    _embedding_history.append({
        "ended_at": now,
        "count": count,
        "seconds": seconds,
    })
    _recompute_speed_metrics(now)


def _schedule_embed_retry(image_id: int, error: str):
    wait_seconds = EMBED_RETRY_SECONDS
    if "No such file" in error or "FileNotFoundError" in error:
        wait_seconds = EMBED_RETRY_SECONDS
    elif "cannot identify image file" in error:
        wait_seconds = EMBED_RETRY_SECONDS * 3

    _embed_retry_after[image_id] = time.time() + wait_seconds


def _select_ready_candidates(rows):
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
        if len(selected) >= BATCH_SIZE:
            break

    return selected, cooled_down, next_retry_at


def get_worker_status() -> dict:
    _recompute_speed_metrics()
    return dict(_worker_status)


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
    return thumbnails.load_embedding_image(path, image_id)


def _embed_images(
    model,
    image_refs: list[tuple[int, str]],
) -> tuple[list[np.ndarray | None], list[str | None]]:
    """Embed images from file paths. Returns vectors plus per-image error strings."""
    from PIL import Image as PILImage

    results = [None] * len(image_refs)
    errors = [None] * len(image_refs)
    valid = []
    valid_indices = []
    for i, (image_id, path) in enumerate(image_refs):
        img = None
        try:
            img = _load_image_for_embedding(image_id, path)
            # Resize to max 1024px on long side to keep VRAM reasonable
            max_side = max(img.size)
            if max_side > 1024:
                scale = 1024 / max_side
                img = img.resize((int(img.width * scale), int(img.height * scale)), PILImage.LANCZOS)
            if img.mode != "RGB":
                img = img.convert("RGB")
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

    if not valid:
        return results, errors

    try:
        embeddings = model.encode(valid, normalize_embeddings=True)
        for idx, valid_i in enumerate(valid_indices):
            results[valid_i] = embeddings[idx].astype(np.float32)
        return results, errors
    except Exception as e:
        failure = f"{type(e).__name__}: {e}"
        for valid_i in valid_indices:
            errors[valid_i] = failure
        return results, errors
    finally:
        for img in valid:
            try:
                img.close()
            except Exception:
                pass


def encode_text(query: str) -> np.ndarray | None:
    """Encode a text query into an embedding. Returns None if model not loaded."""
    if _model is None:
        return None
    embedding = _model.encode(
        [query],
        prompt="Retrieve images relevant to the query.",
        normalize_embeddings=True,
    )
    return embedding[0].astype(np.float32)


def vec_to_blob(vec: np.ndarray) -> bytes:
    return struct.pack(f"{EMBEDDING_DIM}f", *vec)


def blob_to_vec(blob: bytes) -> np.ndarray:
    return np.array(struct.unpack(f"{EMBEDDING_DIM}f", blob), dtype=np.float32)


def _train_taste_model(training_rows):
    """Train Ridge regression on (embedding, elo) pairs. Returns model params."""
    from sklearn.linear_model import Ridge

    X = np.array([blob_to_vec(row["embedding"]) for row in training_rows])
    y = np.array([row["elo"] for row in training_rows])

    model = Ridge(alpha=1.0)
    model.fit(X, y)

    # Precompute (X^T X + alpha*I)^{-1} for uncertainty estimation
    XtX = X.T @ X + model.alpha * np.eye(EMBEDDING_DIM)
    XtX_inv = np.linalg.inv(XtX)

    return model.coef_, model.intercept_, XtX_inv


def _predict_all(all_rows, coef, intercept, XtX_inv):
    """Predict Elo and uncertainty for all embedded images."""
    results = []
    for row in all_rows:
        vec = blob_to_vec(row["embedding"])
        predicted_elo = float(vec @ coef + intercept)
        uncertainty = float(vec @ XtX_inv @ vec)
        results.append((predicted_elo, uncertainty, row["image_id"]))
    return results


async def run_clip_worker():
    """Main background loop: embed, train, predict."""
    loop = asyncio.get_running_loop()

    global _model, _loaded_model_dir, _loaded_model_id, _loaded_model_revision

    last_train_count = 0
    has_model = False

    while True:
        try:
            config = settings.get_settings()
            model_id = config["embed_model_id"]
            model_revision = config["embed_model_revision"]
            model_dir = config["embed_model_dir"]
            model_installed = ai_models.model_files_present(model_dir)

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

            if (
                _model is None
                or _loaded_model_dir != model_dir
                or _loaded_model_id != model_id
                or _loaded_model_revision != model_revision
            ):
                _set_worker_status("loading_model", f"Loading {model_id} from disk…", ready=False)
                _model = await loop.run_in_executor(None, _load_model, model_dir, model_id)
                _loaded_model_dir = model_dir
                _loaded_model_id = model_id
                _loaded_model_revision = model_revision
                last_train_count = await db.get_comparison_count()
                has_model = False
                _set_worker_status("ready", f"{model_id} loaded locally.", ready=True)

            # Phase 1: Embed unembedded images
            candidates = await db.get_unembedded_images(limit=BATCH_SIZE * EMBED_CANDIDATE_MULTIPLIER)
            unembedded, cooled_down, next_retry_at = _select_ready_candidates(candidates)
            if unembedded:
                _set_worker_status("embedding", f"Embedding {len(unembedded)} images…", ready=True)
                image_refs = [(row["id"], row["filepath"]) for row in unembedded]
                embed_started = time.perf_counter()

                vectors, errors = await loop.run_in_executor(
                    None, _embed_images, _model, image_refs
                )

                batch = []
                failed = []
                for row, vec, error in zip(unembedded, vectors, errors):
                    if vec is not None:
                        batch.append((row["id"], vec_to_blob(vec)))
                        _embed_retry_after.pop(row["id"], None)
                    elif error:
                        failed.append((row["id"], error))
                        _schedule_embed_retry(row["id"], error)

                if batch:
                    await db.store_embeddings_batch(batch)
                    _record_embedding_batch(len(batch), time.perf_counter() - embed_started)
                    embedded_count = await db.get_embedding_count()
                    log.info(f"Embedded {len(batch)} images (total: {embedded_count})")
                    has_model = False

                if failed and not batch:
                    sample_error = failed[0][1]
                    _set_worker_status(
                        "embedding",
                        f"Skipping {len(failed)} unavailable files for now; retrying other images. Latest error: {sample_error}",
                        ready=True,
                    )
                await asyncio.sleep(0.1)
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

            # Phase 2: Train/retrain the taste model
            current_count = await db.get_comparison_count()
            if not has_model or current_count - last_train_count >= RETRAIN_EVERY:
                _set_worker_status("training", "Updating taste model predictions…", ready=True)
                training_data = await db.get_training_data()
                if len(training_data) >= 20:
                    coef, intercept, XtX_inv = await loop.run_in_executor(
                        None, _train_taste_model, training_data
                    )

                    all_embeddings = await db.get_all_embeddings()
                    predictions = await loop.run_in_executor(
                        None, _predict_all, all_embeddings, coef, intercept, XtX_inv
                    )

                    if predictions:
                        await db.bulk_update_predictions(predictions)

                    last_train_count = current_count
                    has_model = True
                    log.info(
                        f"Taste model trained on {len(training_data)} images, "
                        f"predicted {len(predictions)} ratings"
                    )

            _set_worker_status("idle", "Waiting for new images or comparisons…", ready=True)
            await asyncio.sleep(5)

        except Exception as e:
            _set_worker_status("error", str(e), ready=False, last_error=str(e))
            log.error(f"Embedding worker error: {e}", exc_info=True)
            await asyncio.sleep(10)
