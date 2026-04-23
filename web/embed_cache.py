"""
Shared in-memory cache for the embedding matrix.

Used by: search, find similar, duplicates, collections, Elo propagation.
Rebuilt when the embedding count changes (new images embedded).
"""

import numpy as np
import asyncio
import time
import db

COUNT_CHECK_TTL_SECONDS = 1.0
MATRIX_GROWTH_MIN_ROWS = 512
MATRIX_GROWTH_FACTOR = 1.10

_cache = {
    "image_ids": None,
    "id_to_idx": None,
    "matrix": None,
    "count": 0,
    "checked_at": 0.0,
}
_rebuild_lock = asyncio.Lock()


def _matrix_view():
    matrix = _cache["matrix"]
    image_ids = _cache["image_ids"]
    if matrix is None or image_ids is None:
        return None
    return matrix[:len(image_ids)]


def _rows_to_matrix(rows):
    if not rows:
        return [], None

    image_ids = [r["image_id"] for r in rows]
    dim = len(rows[0]["embedding"]) // 4
    capacity = max(
        len(rows),
        int(len(rows) * MATRIX_GROWTH_FACTOR) + MATRIX_GROWTH_MIN_ROWS,
    )
    matrix = np.empty((capacity, dim), dtype=np.float32)
    for i, row in enumerate(rows):
        matrix[i] = np.frombuffer(row["embedding"], dtype=np.float32, count=dim)
    return image_ids, matrix


async def get_matrix():
    """Return (image_ids, matrix) from cache, rebuilding if needed."""
    now = time.monotonic()
    if (
        _cache["matrix"] is not None
        and now - _cache["checked_at"] < COUNT_CHECK_TTL_SECONDS
    ):
        return _cache["image_ids"], _matrix_view()

    async with _rebuild_lock:
        now = time.monotonic()
        if (
            _cache["matrix"] is not None
            and now - _cache["checked_at"] < COUNT_CHECK_TTL_SECONDS
        ):
            return _cache["image_ids"], _matrix_view()

        current_count = await db.get_embedding_count()
        _cache["checked_at"] = now
        if _cache["matrix"] is not None and _cache["count"] == current_count:
            return _cache["image_ids"], _matrix_view()

        all_embeddings = await db.get_all_embeddings()
        if not all_embeddings:
            _cache.update({
                "image_ids": None,
                "id_to_idx": None,
                "matrix": None,
                "count": 0,
                "checked_at": now,
            })
            return None, None

        image_ids, matrix = _rows_to_matrix(all_embeddings)

        # Build new cache atomically to avoid partial reads from concurrent callers
        new_cache = {
            "image_ids": image_ids,
            "id_to_idx": {img_id: i for i, img_id in enumerate(image_ids)},
            "matrix": matrix,
            "count": len(image_ids),
            "checked_at": now,
        }
        _cache.update(new_cache)

        return image_ids, _matrix_view()


def add_vectors(rows: list[tuple[int, np.ndarray]]):
    """Append freshly stored vectors to the warm cache without a full DB rebuild."""
    if not rows or _cache["matrix"] is None or _cache["image_ids"] is None:
        return

    id_to_idx = _cache["id_to_idx"] or {}
    new_rows = [(image_id, vec) for image_id, vec in rows if image_id not in id_to_idx]
    if not new_rows:
        _cache["checked_at"] = time.monotonic()
        return

    image_ids = list(_cache["image_ids"])
    matrix = _cache["matrix"]
    old_count = len(image_ids)
    dim = matrix.shape[1]
    new_count = old_count + len(new_rows)

    if new_count > matrix.shape[0]:
        new_capacity = max(
            new_count,
            int(new_count * MATRIX_GROWTH_FACTOR) + MATRIX_GROWTH_MIN_ROWS,
        )
        grown = np.empty((new_capacity, dim), dtype=np.float32)
        grown[:old_count] = matrix[:old_count]
        matrix = grown

    for offset, (image_id, vec) in enumerate(new_rows):
        matrix[old_count + offset] = np.asarray(vec, dtype=np.float32)
        id_to_idx[image_id] = old_count + offset
        image_ids.append(image_id)

    _cache.update({
        "image_ids": image_ids,
        "id_to_idx": id_to_idx,
        "matrix": matrix,
        "count": new_count,
        "checked_at": time.monotonic(),
    })


def invalidate():
    """Force the next get_matrix() call to verify/rebuild the active set."""
    _cache["checked_at"] = 0.0
    _cache["count"] = -1


def get_index() -> dict[int, int]:
    return _cache["id_to_idx"] or {}


def get_vector(image_id: int):
    """Get a single image's embedding vector from cache. Returns None if not cached."""
    if _cache["matrix"] is None or _cache["id_to_idx"] is None:
        return None
    idx = _cache["id_to_idx"].get(image_id)
    if idx is None:
        return None
    return _cache["matrix"][idx]
