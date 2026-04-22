"""
Qwen3-VL-Embedding + active learning taste model for photoArchive.

Background worker that:
1. Embeds kept/maybe images using Qwen3-VL-Embedding-8B (int4 quantized)
2. Trains a Ridge regression (embedding -> predicted Elo) on comparison data
3. Computes per-image uncertainty for smart mosaic selection
4. Provides text-to-image search via shared embedding space
"""

import logging
import os
import struct
import asyncio

import numpy as np

import ai_models
import db
import settings

log = logging.getLogger("clip_worker")
log.setLevel(logging.INFO)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

EMBEDDING_DIM = 2048  # Matryoshka truncation from 4096 -> 2048
BATCH_SIZE = 4  # Small batches for VL model
RETRAIN_EVERY = 50

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
}


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


def get_worker_status() -> dict:
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
    model.truncate_dim = EMBEDDING_DIM
    log.info(f"{model_id} loaded from {model_dir} (int4, {EMBEDDING_DIM}-dim)")
    return model


def _embed_images(model, image_paths: list[str]) -> list[np.ndarray]:
    """Embed images from file paths. Returns list of numpy arrays (or None for failures)."""
    from PIL import Image

    valid = []
    valid_indices = []
    for i, path in enumerate(image_paths):
        try:
            img = Image.open(path).convert("RGB")
            # Resize to max 1024px on long side to keep VRAM reasonable
            max_side = max(img.size)
            if max_side > 1024:
                scale = 1024 / max_side
                img = img.resize((int(img.width * scale), int(img.height * scale)), Image.LANCZOS)
            valid.append(img)
            valid_indices.append(i)
        except Exception as e:
            log.debug(f"Skipping image {path}: {e}")

    if not valid:
        return [None] * len(image_paths)

    embeddings = model.encode(valid, normalize_embeddings=True)

    results = [None] * len(image_paths)
    for idx, valid_i in enumerate(valid_indices):
        results[valid_i] = embeddings[idx].astype(np.float32)
    return results


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
    batches_since_train = 0
    TRAIN_EVERY_N_BATCHES = 5  # train more frequently since batches are small

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
                batches_since_train = 0
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
                batches_since_train = 0
                _set_worker_status("ready", f"{model_id} loaded locally.", ready=True)

            # Phase 1: Embed unembedded images
            unembedded = await db.get_unembedded_images(limit=BATCH_SIZE)
            if unembedded:
                _set_worker_status("embedding", f"Embedding {len(unembedded)} images…", ready=True)
                image_paths = [row["filepath"] for row in unembedded]

                vectors = await loop.run_in_executor(
                    None, _embed_images, _model, image_paths
                )

                batch = []
                for row, vec in zip(unembedded, vectors):
                    if vec is not None:
                        batch.append((row["id"], vec_to_blob(vec)))

                if batch:
                    await db.store_embeddings_batch(batch)
                    embedded_count = await db.get_embedding_count()
                    log.info(f"Embedded {len(batch)} images (total: {embedded_count})")

                batches_since_train += 1
                await asyncio.sleep(0.1)

                if batches_since_train < TRAIN_EVERY_N_BATCHES:
                    continue

            # Phase 2: Train/retrain the taste model
            batches_since_train = 0
            current_count = await db.get_comparison_count()
            if not has_model or current_count - last_train_count >= RETRAIN_EVERY or unembedded:
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
