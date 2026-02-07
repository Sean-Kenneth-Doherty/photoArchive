import aiosqlite
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "photoranker.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS images (
    id INTEGER PRIMARY KEY,
    filename TEXT NOT NULL,
    filepath TEXT NOT NULL UNIQUE,
    elo REAL DEFAULT 1200.0,
    comparisons INTEGER DEFAULT 0,
    status TEXT DEFAULT 'kept',
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
"""


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    return db


async def init_db():
    db = await get_db()
    try:
        await db.executescript(SCHEMA)
        # Migration: add orientation column if missing
        try:
            await db.execute("ALTER TABLE images ADD COLUMN orientation TEXT DEFAULT NULL")
        except Exception:
            pass  # Column already exists
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


async def get_unculled_by_orientation(orientation: str, limit: int = 12):
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, filename, filepath FROM images WHERE status = 'unculled' AND orientation = ? ORDER BY RANDOM() LIMIT ?",
            (orientation, limit),
        )
        return await cursor.fetchall()
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


async def batch_set_orientations(updates: list[tuple[str, int]]):
    """Set orientation for multiple images. Each tuple: (orientation, image_id)."""
    db = await get_db()
    try:
        await db.executemany(
            "UPDATE images SET orientation = ? WHERE id = ?",
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
            "INSERT OR IGNORE INTO images (filename, filepath) VALUES (?, ?)",
            rows,
        )
        await db.commit()
    finally:
        await db.close()


async def get_unculled_images(limit: int = 10, offset: int = 0):
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, filename, filepath FROM images WHERE status = 'unculled' ORDER BY RANDOM() LIMIT ?",
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
    finally:
        await db.close()


async def set_image_status_and_elo(image_id: int, status: str, new_elo: float):
    db = await get_db()
    try:
        await db.execute(
            "UPDATE images SET status = ?, elo = ? WHERE id = ?", (status, new_elo, image_id)
        )
        await db.commit()
    finally:
        await db.close()


async def batch_cull(decisions: list[tuple[int, str, float]]):
    """Apply multiple cull decisions at once. Each tuple: (image_id, status, new_elo)."""
    db = await get_db()
    try:
        for image_id, status, new_elo in decisions:
            await db.execute(
                "UPDATE images SET status = ?, elo = ? WHERE id = ?",
                (status, new_elo, image_id),
            )
        await db.commit()
    finally:
        await db.close()


async def undo_last_cull():
    """Undo the last cull decision by reverting the most recently culled image to unculled."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, status FROM images WHERE status != 'unculled' ORDER BY rowid DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        if row:
            await db.execute("UPDATE images SET status = 'unculled' WHERE id = ?", (row["id"],))
            await db.commit()
            return {"id": row["id"], "previous_status": row["status"]}
        return None
    finally:
        await db.close()


async def get_kept_images_for_pairing():
    """Get kept images sorted by Elo for Swiss-system pairing."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, filename, filepath, elo, comparisons FROM images "
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
            return {"winner_id": row["winner_id"], "loser_id": row["loser_id"]}
        return None
    finally:
        await db.close()


async def get_rankings(limit: int = 100, offset: int = 0):
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, filename, filepath, elo, comparisons, status FROM images "
            "WHERE status IN ('kept', 'maybe') ORDER BY elo DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def get_stats():
    db = await get_db()
    try:
        cursor = await db.execute("SELECT COUNT(*) as total FROM images")
        total = (await cursor.fetchone())["total"]

        cursor = await db.execute("SELECT COUNT(*) as c FROM images WHERE status = 'unculled'")
        unculled = (await cursor.fetchone())["c"]

        cursor = await db.execute("SELECT COUNT(*) as c FROM images WHERE status = 'kept'")
        kept = (await cursor.fetchone())["c"]

        cursor = await db.execute("SELECT COUNT(*) as c FROM images WHERE status = 'maybe'")
        maybe = (await cursor.fetchone())["c"]

        cursor = await db.execute("SELECT COUNT(*) as c FROM images WHERE status = 'rejected'")
        rejected = (await cursor.fetchone())["c"]

        cursor = await db.execute("SELECT COUNT(*) as c FROM comparisons")
        total_comparisons = (await cursor.fetchone())["c"]

        return {
            "total_images": total,
            "unculled": unculled,
            "kept": kept,
            "maybe": maybe,
            "rejected": rejected,
            "total_comparisons": total_comparisons,
        }
    finally:
        await db.close()


async def get_image_by_id(image_id: int):
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM images WHERE id = ?", (image_id,))
        return await cursor.fetchone()
    finally:
        await db.close()


async def get_top_images(limit: int = 50):
    """Get top N images by Elo for top-tier refinement."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, filename, filepath, elo, comparisons FROM images "
            "WHERE status IN ('kept', 'maybe') ORDER BY elo DESC LIMIT ?",
            (limit,),
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def get_scan_folder():
    """Get the folder path from the most recently scanned image."""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT filepath FROM images LIMIT 1")
        row = await cursor.fetchone()
        if row:
            return os.path.dirname(row["filepath"])
        return None
    finally:
        await db.close()
