"""
Shared in-memory cache for the embedding matrix.

Used by: search, find similar, duplicates, collections, Elo propagation.
Rebuilt when the embedding count changes (new images embedded).
"""

import numpy as np
import asyncio
import json
import os
import sqlite3
import time
import db

COUNT_CHECK_TTL_SECONDS = 1.0
MATRIX_GROWTH_MIN_ROWS = 512
MATRIX_GROWTH_FACTOR = 1.10
SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), ".embedcache")
SNAPSHOT_MATRIX_PATH = os.path.join(SNAPSHOT_DIR, "matrix.npy")
SNAPSHOT_IDS_PATH = os.path.join(SNAPSHOT_DIR, "image_ids.npy")
SNAPSHOT_META_PATH = os.path.join(SNAPSHOT_DIR, "meta.json")

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


def _rows_to_matrix(rows, *, overallocate: bool = True):
    if not rows:
        return [], None

    image_ids = [r[0] for r in rows]
    dim = len(rows[0][1]) // 4
    capacity = len(rows)
    if overallocate:
        capacity = max(
            len(rows),
            int(len(rows) * MATRIX_GROWTH_FACTOR) + MATRIX_GROWTH_MIN_ROWS,
        )
    matrix = np.empty((capacity, dim), dtype=np.float32)
    for i, (_image_id, blob) in enumerate(rows):
        matrix[i] = np.frombuffer(blob, dtype=np.float32, count=dim)
    return image_ids, matrix


def _db_file_signature() -> list[list[str | int]]:
    signature = []
    for path in (db.DB_PATH, f"{db.DB_PATH}-wal", f"{db.DB_PATH}-shm"):
        try:
            stat = os.stat(path)
            signature.append([os.path.basename(path), stat.st_size, stat.st_mtime_ns])
        except OSError:
            signature.append([os.path.basename(path), -1, -1])
    return signature


def _load_snapshot_sync(expected_count: int):
    try:
        with open(SNAPSHOT_META_PATH, "r", encoding="utf-8") as f:
            meta = json.load(f)
        if int(meta.get("count", -1)) != expected_count:
            return None
        if meta.get("db_signature") != _db_file_signature():
            return None
        # Load into RAM rather than returning a memmap. Search/similar should
        # pay a predictable warmup cost instead of page-faulting during the
        # first similarity matmul.
        ids = np.load(SNAPSHOT_IDS_PATH)
        matrix = np.load(SNAPSHOT_MATRIX_PATH)
        if len(ids) != expected_count or matrix.shape[0] != expected_count:
            return None
        return ids.astype(np.int64).tolist(), matrix
    except Exception:
        return None


def _save_snapshot_sync(image_ids: list[int], matrix: np.ndarray):
    try:
        os.makedirs(SNAPSHOT_DIR, exist_ok=True)
        ids_tmp = f"{SNAPSHOT_IDS_PATH}.tmp"
        matrix_tmp = f"{SNAPSHOT_MATRIX_PATH}.tmp"
        meta_tmp = f"{SNAPSHOT_META_PATH}.tmp"
        np.save(ids_tmp, np.asarray(image_ids, dtype=np.int64))
        np.save(matrix_tmp, np.asarray(matrix[:len(image_ids)], dtype=np.float32))
        with open(meta_tmp, "w", encoding="utf-8") as f:
            json.dump({
                "count": len(image_ids),
                "db_signature": _db_file_signature(),
                "created_at": time.time(),
            }, f)
        os.replace(f"{ids_tmp}.npy", SNAPSHOT_IDS_PATH)
        os.replace(f"{matrix_tmp}.npy", SNAPSHOT_MATRIX_PATH)
        os.replace(meta_tmp, SNAPSHOT_META_PATH)
    except Exception:
        pass


def _get_embedding_count_sync() -> int:
    conn = sqlite3.connect(db.DB_PATH, timeout=30)
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM embeddings e "
            "JOIN images i ON e.image_id = i.id "
            "WHERE i.status IN ('kept', 'maybe')"
        ).fetchone()[0]
    finally:
        conn.close()


def _load_embeddings_sync(expected_count: int):
    snapshot = _load_snapshot_sync(expected_count)
    if snapshot is not None:
        return snapshot

    conn = sqlite3.connect(db.DB_PATH, timeout=30)
    try:
        rows = conn.execute(
            "SELECT e.image_id, e.embedding FROM embeddings e "
            "JOIN images i ON e.image_id = i.id "
            "WHERE i.status IN ('kept', 'maybe')"
        ).fetchall()
    finally:
        conn.close()
    image_ids, matrix = _rows_to_matrix(rows)
    if image_ids and matrix is not None:
        _save_snapshot_sync(image_ids, matrix)
    return image_ids, matrix


def _remove_snapshot_sync():
    for path in (SNAPSHOT_MATRIX_PATH, SNAPSHOT_IDS_PATH, SNAPSHOT_META_PATH):
        try:
            os.remove(path)
        except OSError:
            pass


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

        current_count = _get_embedding_count_sync()
        _cache["checked_at"] = now
        if _cache["matrix"] is not None and _cache["count"] == current_count:
            return _cache["image_ids"], _matrix_view()

        image_ids, matrix = _load_embeddings_sync(current_count)
        if not image_ids or matrix is None:
            _cache.update({
                "image_ids": None,
                "id_to_idx": None,
                "matrix": None,
                "count": 0,
                "checked_at": now,
            })
            return None, None

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
