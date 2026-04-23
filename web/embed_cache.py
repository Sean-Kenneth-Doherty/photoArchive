"""
Shared in-memory cache for the embedding matrix.

Used by: search, find similar, duplicates, collections, Elo propagation.
Rebuilt when the embedding count changes (new images embedded).
"""

import numpy as np
import db

_cache = {
    "image_ids": None,
    "id_to_idx": None,
    "matrix": None,
    "count": 0,
}


async def get_matrix():
    """Return (image_ids, matrix) from cache, rebuilding if needed."""
    current_count = await db.get_embedding_count()
    if _cache["matrix"] is not None and _cache["count"] == current_count:
        return _cache["image_ids"], _cache["matrix"]

    all_embeddings = await db.get_all_embeddings()
    if not all_embeddings:
        return None, None

    image_ids = [r["image_id"] for r in all_embeddings]
    # Use np.frombuffer instead of struct.unpack — 7x faster
    matrix = np.array([
        np.frombuffer(r["embedding"], dtype=np.float32)
        for r in all_embeddings
    ])

    _cache["image_ids"] = image_ids
    _cache["id_to_idx"] = {img_id: i for i, img_id in enumerate(image_ids)}
    _cache["matrix"] = matrix
    _cache["count"] = current_count

    return image_ids, matrix


def get_vector(image_id: int):
    """Get a single image's embedding vector from cache. Returns None if not cached."""
    if _cache["matrix"] is None or _cache["id_to_idx"] is None:
        return None
    idx = _cache["id_to_idx"].get(image_id)
    if idx is None:
        return None
    return _cache["matrix"][idx]
