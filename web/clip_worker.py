"""
CLIP embedding + active learning taste model for PhotoRanker.

Background worker that:
1. Embeds kept/maybe images using CLIP ViT-B/32
2. Trains a Ridge regression (embedding -> predicted Elo) on comparison data
3. Computes per-image uncertainty for smart mosaic selection
"""

import asyncio
import struct
import logging
import numpy as np

import db
import thumbnails

log = logging.getLogger("clip_worker")
log.setLevel(logging.INFO)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

EMBEDDING_DIM = 512
BATCH_SIZE = 64
RETRAIN_EVERY = 50  # retrain after this many new comparisons


def _load_clip_model():
    """Load CLIP ViT-B/32 on GPU (falls back to CPU)."""
    import open_clip
    import torch

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-B-32", pretrained="laion2b_s34b_b79k", device=device
    )
    model.eval()
    log.info(f"CLIP model loaded on {device}")
    return model, preprocess, device


def _embed_batch(model, preprocess, device, jpeg_bytes_list: list[bytes]) -> list[np.ndarray]:
    """Run CLIP inference on a batch of JPEG byte buffers."""
    import torch
    from PIL import Image
    import io

    tensors = []
    valid_indices = []
    for i, data in enumerate(jpeg_bytes_list):
        if not data:
            continue
        try:
            img = Image.open(io.BytesIO(data)).convert("RGB")
            tensors.append(preprocess(img))
            valid_indices.append(i)
        except Exception as e:
            log.debug(f"Skipping image {i}: {e}")

    if not tensors:
        return [None] * len(jpeg_bytes_list)

    batch = torch.stack(tensors).to(device)
    with torch.no_grad():
        features = model.encode_image(batch)
        features = features / features.norm(dim=-1, keepdim=True)  # L2 normalize
        features = features.cpu().numpy().astype(np.float32)

    results = [None] * len(jpeg_bytes_list)
    for idx, valid_i in enumerate(valid_indices):
        results[valid_i] = features[idx]
    return results


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
    loop = asyncio.get_event_loop()

    log.info("CLIP worker starting — loading model...")
    model, preprocess, device = await loop.run_in_executor(None, _load_clip_model)

    last_train_count = await db.get_comparison_count()
    has_model = False
    batches_since_train = 0
    TRAIN_EVERY_N_BATCHES = 10  # train every ~640 new embeddings

    while True:
        try:
            # Phase 1: Embed unembedded images
            unembedded = await db.get_unembedded_images(limit=BATCH_SIZE)
            if unembedded:
                # Get thumbnails via existing cache/pipeline
                jpeg_list = []
                for row in unembedded:
                    try:
                        data = await thumbnails.get_thumbnail(row["filepath"], "sm", row["id"])
                        jpeg_list.append(data)
                    except Exception:
                        jpeg_list.append(None)

                vectors = await loop.run_in_executor(
                    None, _embed_batch, model, preprocess, device, jpeg_list
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
                await asyncio.sleep(0.1)  # yield before next batch

                # Train periodically during embedding, not just after
                if batches_since_train < TRAIN_EVERY_N_BATCHES:
                    continue

            # Phase 2: Train/retrain the taste model
            batches_since_train = 0
            current_count = await db.get_comparison_count()
            if not has_model or current_count - last_train_count >= RETRAIN_EVERY or unembedded:
                training_data = await db.get_training_data()
                if len(training_data) >= 20:  # need minimum data
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

            await asyncio.sleep(5)

        except Exception as e:
            log.error(f"CLIP worker error: {e}")
            await asyncio.sleep(10)
