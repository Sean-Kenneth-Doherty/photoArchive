import aiosqlite
import os
import time as _time

DB_PATH = os.path.join(os.path.dirname(__file__), "photoarchive.db")
EXPECTED_EMBEDDING_DIM = 2048  # Qwen3-VL-Embedding-2B native dimension

SCHEMA = """
CREATE TABLE IF NOT EXISTS images (
    id INTEGER PRIMARY KEY,
    filename TEXT NOT NULL,
    filepath TEXT NOT NULL UNIQUE,
    elo REAL DEFAULT 1200.0,
    comparisons INTEGER DEFAULT 0,
    status TEXT DEFAULT 'kept',
    flag TEXT DEFAULT 'unflagged',
    orientation TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS comparisons (
    id INTEGER PRIMARY KEY,
    winner_id INTEGER REFERENCES images(id),
    loser_id INTEGER REFERENCES images(id),
    mode TEXT,
    elo_before_winner REAL,
    elo_before_loser REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_images_status ON images(status);
CREATE INDEX IF NOT EXISTS idx_images_elo ON images(elo DESC);
CREATE INDEX IF NOT EXISTS idx_images_comparisons ON images(comparisons);
CREATE INDEX IF NOT EXISTS idx_comparisons_pair ON comparisons(winner_id, loser_id);

-- Composite indexes for fast sorted queries with status filter
CREATE INDEX IF NOT EXISTS idx_images_status_elo ON images(status, elo DESC);
CREATE INDEX IF NOT EXISTS idx_images_status_elo_asc ON images(status, elo ASC);
CREATE INDEX IF NOT EXISTS idx_images_status_id ON images(status, id DESC);
CREATE INDEX IF NOT EXISTS idx_images_status_comparisons ON images(status, comparisons DESC);
CREATE INDEX IF NOT EXISTS idx_images_status_filename ON images(status, filename ASC);
CREATE INDEX IF NOT EXISTS idx_images_status_orient_elo ON images(status, orientation, elo DESC);
CREATE INDEX IF NOT EXISTS idx_images_status_comps_elo ON images(status, comparisons, elo DESC);

-- Hot-path partial indexes for the active Library/Compare working set.
-- These avoid temp B-tree sorts caused by status IN ('kept', 'maybe').
CREATE INDEX IF NOT EXISTS idx_images_active_elo
ON images(elo DESC) WHERE status IN ('kept', 'maybe');
CREATE INDEX IF NOT EXISTS idx_images_active_elo_asc
ON images(elo ASC) WHERE status IN ('kept', 'maybe');
CREATE INDEX IF NOT EXISTS idx_images_active_comparisons
ON images(comparisons DESC) WHERE status IN ('kept', 'maybe');
CREATE INDEX IF NOT EXISTS idx_images_active_comparisons_asc
ON images(comparisons ASC) WHERE status IN ('kept', 'maybe');
CREATE INDEX IF NOT EXISTS idx_images_active_filename
ON images(filename ASC) WHERE status IN ('kept', 'maybe');
CREATE INDEX IF NOT EXISTS idx_images_active_id
ON images(id DESC) WHERE status IN ('kept', 'maybe');
CREATE INDEX IF NOT EXISTS idx_images_active_filepath
ON images(filepath ASC) WHERE status IN ('kept', 'maybe');
CREATE INDEX IF NOT EXISTS idx_images_active_orientation_elo
ON images(orientation, elo DESC) WHERE status IN ('kept', 'maybe');

CREATE TABLE IF NOT EXISTS embeddings (
    image_id INTEGER PRIMARY KEY REFERENCES images(id),
    embedding BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cache_entries (
    cache_root TEXT NOT NULL,
    size TEXT NOT NULL,
    image_id INTEGER NOT NULL REFERENCES images(id),
    path TEXT NOT NULL,
    source_signature TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    last_accessed REAL NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (cache_root, size, image_id)
);

CREATE INDEX IF NOT EXISTS idx_cache_entries_root_size_access
ON cache_entries(cache_root, size, last_accessed);

-- For pregen candidate batch query (ORDER BY filepath ASC with status filter)
CREATE INDEX IF NOT EXISTS idx_images_status_filepath ON images(status, filepath ASC);

-- For LRU eviction ordering (avoids TEMP B-TREE sort during budget enforcement)
CREATE INDEX IF NOT EXISTS idx_cache_entries_root_size_accessed_id
ON cache_entries(cache_root, size, last_accessed, image_id);

CREATE TABLE IF NOT EXISTS cache_metadata (
    cache_root TEXT PRIMARY KEY,
    thumb_config_signature TEXT NOT NULL,
    thumb_config_changed_at REAL NOT NULL,
    replace_stale_thumbnails INTEGER NOT NULL DEFAULT 0
);
"""

_stats_cache = {"data": None, "expires": 0}


def _invalidate_stats_cache():
    _stats_cache["data"] = None
    _stats_cache["expires"] = 0


def invalidate_stats_cache():
    _invalidate_stats_cache()


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH, timeout=30)
    db.row_factory = aiosqlite.Row
    return db


async def init_db():
    db_exists = os.path.exists(DB_PATH)
    db = await get_db()
    try:
        if not db_exists:
            await db.execute("PRAGMA journal_mode=WAL")
        await db.executescript(SCHEMA)
        # Migrations: add columns if missing
        for col, defn in [
            ("orientation", "TEXT DEFAULT NULL"),
            ("flag", "TEXT DEFAULT 'unflagged'"),
            ("predicted_elo", "REAL DEFAULT NULL"),
            ("uncertainty", "REAL DEFAULT NULL"),
            ("aspect_ratio", "REAL DEFAULT NULL"),
        ]:
            try:
                await db.execute(f"ALTER TABLE images ADD COLUMN {col} {defn}")
            except Exception:
                pass  # Column already exists
        try:
            await db.execute(
                "ALTER TABLE cache_metadata "
                "ADD COLUMN replace_stale_thumbnails INTEGER NOT NULL DEFAULT 0"
            )
        except Exception:
            pass  # Column already exists
        await db.execute("CREATE INDEX IF NOT EXISTS idx_images_flag ON images(flag)")
        # Backfill aspect_ratio from orientation for images that don't have it yet
        await db.execute(
            "UPDATE images SET aspect_ratio = 1.5 WHERE orientation = 'landscape' AND aspect_ratio IS NULL"
        )
        await db.execute(
            "UPDATE images SET aspect_ratio = 0.6667 WHERE orientation = 'portrait' AND aspect_ratio IS NULL"
        )
        # Check embedding dimension — if it changed (model upgrade), clear old embeddings
        cursor = await db.execute("SELECT embedding FROM embeddings LIMIT 1")
        row = await cursor.fetchone()
        if row:
            import struct
            expected_bytes = EXPECTED_EMBEDDING_DIM * 4  # 4 bytes per float32
            if len(row["embedding"]) != expected_bytes:
                await db.execute("DELETE FROM embeddings")
                await db.execute("UPDATE images SET predicted_elo = NULL, uncertainty = NULL")
        await db.commit()
    finally:
        await db.close()


async def set_image_orientation(image_id: int, orientation: str):
    db = await get_db()
    try:
        await db.execute("UPDATE images SET orientation = ? WHERE id = ?", (orientation, image_id))
        await db.commit()
    finally:
        await db.close()


async def get_unclassified_images(limit: int = 200):
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, filepath FROM images WHERE orientation IS NULL LIMIT ?",
            (limit,),
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def batch_set_orientations(updates: list[tuple[str, float, int]]):
    """Set orientation and aspect_ratio for multiple images. Each tuple: (orientation, aspect_ratio, image_id)."""
    db = await get_db()
    try:
        await db.executemany(
            "UPDATE images SET orientation = ?, aspect_ratio = ? WHERE id = ?",
            updates,
        )
        await db.commit()
    finally:
        await db.close()


async def insert_images_batch(rows: list[tuple[str, str]]):
    """Insert (filename, filepath) pairs, ignoring duplicates."""
    db = await get_db()
    try:
        await db.executemany(
            "INSERT OR IGNORE INTO images (filename, filepath, status) VALUES (?, ?, 'kept')",
            rows,
        )
        await db.commit()
        _invalidate_stats_cache()
    finally:
        await db.close()


async def get_recent_active_images(limit: int = 10):
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, filename, filepath FROM images WHERE status IN ('kept', 'maybe') ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def set_image_status(image_id: int, status: str):
    db = await get_db()
    try:
        await db.execute(
            "UPDATE images SET status = ? WHERE id = ?", (status, image_id)
        )
        await db.commit()
        _invalidate_stats_cache()
    finally:
        await db.close()


async def set_image_flag(image_id: int, flag: str):
    db = await get_db()
    try:
        await db.execute(
            "UPDATE images SET flag = ? WHERE id = ?", (flag, image_id)
        )
        await db.commit()
    finally:
        await db.close()


async def get_active_images_for_pairing():
    """Get active images sorted by Elo for Swiss-system pairing."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, filename, filepath, elo, comparisons, flag, orientation, aspect_ratio FROM images "
            "INDEXED BY idx_images_active_elo "
            "WHERE status IN ('kept', 'maybe') ORDER BY elo DESC"
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def get_past_matchups() -> set[tuple[int, int]]:
    """Return set of (min_id, max_id) tuples for all past matchups."""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT winner_id, loser_id FROM comparisons")
        rows = await cursor.fetchall()
        return {(min(r["winner_id"], r["loser_id"]), max(r["winner_id"], r["loser_id"])) for r in rows}
    finally:
        await db.close()


async def record_comparison(winner_id: int, loser_id: int, mode: str, elo_before_winner: float, elo_before_loser: float, new_winner_elo: float, new_loser_elo: float):
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO comparisons (winner_id, loser_id, mode, elo_before_winner, elo_before_loser) VALUES (?, ?, ?, ?, ?)",
            (winner_id, loser_id, mode, elo_before_winner, elo_before_loser),
        )
        await db.execute(
            "UPDATE images SET elo = ?, comparisons = comparisons + 1 WHERE id = ?",
            (new_winner_elo, winner_id),
        )
        await db.execute(
            "UPDATE images SET elo = ?, comparisons = comparisons + 1 WHERE id = ?",
            (new_loser_elo, loser_id),
        )
        await db.commit()
        _invalidate_stats_cache()
    finally:
        await db.close()


async def undo_last_comparison():
    """Undo the last comparison, restoring Elo ratings."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, winner_id, loser_id, elo_before_winner, elo_before_loser FROM comparisons ORDER BY id DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        if row:
            await db.execute(
                "UPDATE images SET elo = ?, comparisons = MAX(comparisons - 1, 0) WHERE id = ?",
                (row["elo_before_winner"], row["winner_id"]),
            )
            await db.execute(
                "UPDATE images SET elo = ?, comparisons = MAX(comparisons - 1, 0) WHERE id = ?",
                (row["elo_before_loser"], row["loser_id"]),
            )
            await db.execute("DELETE FROM comparisons WHERE id = ?", (row["id"],))
            await db.commit()
            _invalidate_stats_cache()
            return {"winner_id": row["winner_id"], "loser_id": row["loser_id"]}
        return None
    finally:
        await db.close()


RANKING_SORTS = {
    "elo": "elo DESC",
    "elo_asc": "elo ASC",
    "comparisons": "comparisons DESC",
    "least_compared": "comparisons ASC",
    "filename": "filename ASC",
    "filename_desc": "filename DESC",
    "newest": "id DESC",
    "oldest": "id ASC",
}
RANKING_INDEXES = {
    "elo": "idx_images_active_elo",
    "elo_asc": "idx_images_active_elo_asc",
    "comparisons": "idx_images_active_comparisons",
    "least_compared": "idx_images_active_comparisons_asc",
    "filename": "idx_images_active_filename",
    "filename_desc": "idx_images_active_filename",
    "newest": "idx_images_active_id",
    "oldest": "idx_images_active_id",
}

STAR_THRESHOLDS = {5: 1500, 4: 1350, 3: 1250, 2: 1150, 1: 0}

async def get_rankings(limit: int = 100, offset: int = 0, sort: str = "elo",
                       orientation: str = "", compared: str = "", min_stars: int = 0,
                       folder: str = "", flag: str = "", id_filter: set = None):
    db = await get_db()
    try:
        order = RANKING_SORTS.get(sort, "elo DESC")
        conditions = ["status IN ('kept', 'maybe')"]
        params = []

        if orientation in ("landscape", "portrait"):
            conditions.append("orientation = ?")
            params.append(orientation)

        if compared == "compared":
            conditions.append("comparisons > 0")
        elif compared == "uncompared":
            conditions.append("comparisons = 0")
        elif compared == "confident":
            conditions.append("comparisons >= 10")

        if min_stars > 0 and min_stars in STAR_THRESHOLDS:
            conditions.append("elo >= ?")
            params.append(STAR_THRESHOLDS[min_stars])

        if folder:
            conditions.append("filepath LIKE ?")
            params.append(f"%/{folder}/%")

        if flag in ("picked", "unflagged", "rejected"):
            conditions.append("COALESCE(flag, 'unflagged') = ?")
            params.append(flag)

        if id_filter is not None:
            if not id_filter:
                return []
            placeholders = ",".join("?" * len(id_filter))
            conditions.append(f"id IN ({placeholders})")
            params.extend(id_filter)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        # Skip index hint when using id_filter — SQLite picks the right plan
        if id_filter is not None:
            cursor = await db.execute(
                f"SELECT id, filename, filepath, elo, comparisons, status, flag, aspect_ratio "
                f"FROM images WHERE {where} ORDER BY {order} LIMIT ? OFFSET ?",
                params,
            )
        else:
            index_name = RANKING_INDEXES.get(sort, "idx_images_active_elo")
            if orientation in ("landscape", "portrait") and sort == "elo":
                index_name = "idx_images_active_orientation_elo"
            cursor = await db.execute(
                f"SELECT id, filename, filepath, elo, comparisons, status, flag, aspect_ratio "
                f"FROM images INDEXED BY {index_name} "
                f"WHERE {where} ORDER BY {order} LIMIT ? OFFSET ?",
                params,
            )
        return await cursor.fetchall()
    finally:
        await db.close()


async def get_stats():
    if _stats_cache["data"] and _time.time() < _stats_cache["expires"]:
        return _stats_cache["data"]
    db = await get_db()
    try:
        # Single query for all status counts
        cursor = await db.execute(
            "SELECT status, COUNT(*) as c FROM images GROUP BY status"
        )
        counts = {row["status"]: row["c"] for row in await cursor.fetchall()}

        cursor = await db.execute("SELECT COUNT(*) as c FROM comparisons")
        total_comparisons = (await cursor.fetchone())["c"]

        total = sum(counts.values())
        result = {
            "total_images": total,
            "kept": counts.get("kept", 0),
            "maybe": counts.get("maybe", 0),
            "rejected": counts.get("rejected", 0),
            "total_comparisons": total_comparisons,
        }
        _stats_cache["data"] = result
        _stats_cache["expires"] = _time.time() + 2  # cache for 2 seconds
        return result
    finally:
        await db.close()


async def get_image_by_id(image_id: int):
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM images WHERE id = ?", (image_id,))
        return await cursor.fetchone()
    finally:
        await db.close()


async def get_images_by_ids(image_ids: list[int]) -> dict[int, dict]:
    """Fetch multiple images by ID in a single query. Returns {id: row_dict}."""
    if not image_ids:
        return {}
    db = await get_db()
    try:
        placeholders = ",".join("?" for _ in image_ids)
        cursor = await db.execute(
            f"SELECT * FROM images WHERE id IN ({placeholders})", image_ids
        )
        rows = await cursor.fetchall()
        return {row["id"]: dict(row) for row in rows}
    finally:
        await db.close()


async def get_top_images(limit: int = 50):
    """Get top N images by Elo for top-tier refinement."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, filename, filepath, elo, comparisons, flag FROM images "
            "INDEXED BY idx_images_active_elo "
            "WHERE status IN ('kept', 'maybe') ORDER BY elo DESC LIMIT ?",
            (limit,),
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def get_scan_folder():
    """Get the common root folder from scanned images."""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT filepath FROM images ORDER BY RANDOM() LIMIT 50")
        rows = await cursor.fetchall()
        if not rows:
            return None
        dirs = [os.path.dirname(row["filepath"]) for row in rows]
        return os.path.commonpath(dirs)
    finally:
        await db.close()


# --- Embedding / Active Learning ---

async def get_unembedded_images(limit: int = 64, md_cache_root: str = ""):
    """Get kept/maybe images that don't have CLIP embeddings yet."""
    db = await get_db()
    try:
        if md_cache_root:
            cursor = await db.execute(
                "SELECT i.id, i.filepath FROM images i "
                "INDEXED BY idx_images_active_id "
                "WHERE i.status IN ('kept', 'maybe') "
                "AND EXISTS ("
                "  SELECT 1 FROM cache_entries c "
                "  WHERE c.cache_root = ? AND c.size = 'md' AND c.image_id = i.id"
                ") "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM embeddings e WHERE e.image_id = i.id"
                ") "
                "ORDER BY i.id ASC "
                "LIMIT ?",
                (md_cache_root, limit),
            )
        else:
            cursor = await db.execute(
                "SELECT i.id, i.filepath FROM images i "
                "INDEXED BY idx_images_active_id "
                "WHERE i.status IN ('kept', 'maybe') "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM embeddings e WHERE e.image_id = i.id"
                ") "
                "ORDER BY i.id ASC "
                "LIMIT ?",
                (limit,),
            )
        return await cursor.fetchall()
    finally:
        await db.close()


async def store_embeddings_batch(rows: list[tuple[int, bytes]]):
    """Store CLIP embedding blobs. Each row: (image_id, embedding_bytes)."""
    db = await get_db()
    try:
        await db.executemany(
            "INSERT OR REPLACE INTO embeddings (image_id, embedding) VALUES (?, ?)",
            rows,
        )
        await db.commit()
    finally:
        await db.close()


async def get_all_embeddings():
    """Get all embeddings for prediction pass."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT e.image_id, e.embedding FROM embeddings e "
            "JOIN images i ON e.image_id = i.id "
            "WHERE i.status IN ('kept', 'maybe')"
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def get_embedding_count() -> int:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT COUNT(*) as c FROM embeddings e "
            "JOIN images i ON e.image_id = i.id "
            "WHERE i.status IN ('kept', 'maybe')"
        )
        return (await cursor.fetchone())["c"]
    finally:
        await db.close()
