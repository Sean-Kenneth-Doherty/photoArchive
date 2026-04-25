import aiosqlite
import os
import time as _time

DB_PATH = os.path.join(os.path.dirname(__file__), "photoarchive.db")
EXPECTED_EMBEDDING_DIM = 2048  # Qwen3-VL-Embedding-2B native dimension

SCHEMA = """
CREATE TABLE IF NOT EXISTS catalog_sources (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    included INTEGER NOT NULL DEFAULT 1,
    online INTEGER NOT NULL DEFAULT 1,
    image_count INTEGER NOT NULL DEFAULT 0,
    active_image_count INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL DEFAULT (strftime('%s', 'now')),
    last_scan_at REAL DEFAULT NULL,
    last_seen_at REAL DEFAULT NULL,
    removed_at REAL DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS images (
    id INTEGER PRIMARY KEY,
    source_id INTEGER REFERENCES catalog_sources(id),
    filename TEXT NOT NULL,
    filepath TEXT NOT NULL UNIQUE,
    elo REAL DEFAULT 1200.0,
    comparisons INTEGER DEFAULT 0,
    propagated_updates INTEGER DEFAULT 0,
    status TEXT DEFAULT 'kept',
    flag TEXT DEFAULT 'unflagged',
    orientation TEXT DEFAULT NULL,
    date_taken TEXT DEFAULT NULL,
    camera_make TEXT DEFAULT NULL,
    camera_model TEXT DEFAULT NULL,
    lens TEXT DEFAULT NULL,
    file_ext TEXT DEFAULT NULL,
    file_size INTEGER DEFAULT NULL,
    file_modified_at REAL DEFAULT NULL,
    width INTEGER DEFAULT NULL,
    height INTEGER DEFAULT NULL,
    latitude REAL DEFAULT NULL,
    longitude REAL DEFAULT NULL,
    metadata_scanned_at REAL DEFAULT NULL,
    metadata_version INTEGER DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS comparisons (
    id INTEGER PRIMARY KEY,
    winner_id INTEGER REFERENCES images(id),
    loser_id INTEGER REFERENCES images(id),
    mode TEXT,
    elo_before_winner REAL,
    elo_before_loser REAL,
    action_id TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_images_status ON images(status);
CREATE INDEX IF NOT EXISTS idx_images_source_id ON images(source_id);
CREATE INDEX IF NOT EXISTS idx_catalog_sources_path ON catalog_sources(path);
CREATE INDEX IF NOT EXISTS idx_catalog_sources_active ON catalog_sources(included, online);
CREATE INDEX IF NOT EXISTS idx_images_elo ON images(elo DESC);
CREATE INDEX IF NOT EXISTS idx_images_comparisons ON images(comparisons);
CREATE INDEX IF NOT EXISTS idx_comparisons_pair ON comparisons(winner_id, loser_id);
CREATE INDEX IF NOT EXISTS idx_comparisons_action_id ON comparisons(action_id);

CREATE TABLE IF NOT EXISTS propagation_updates (
    id INTEGER PRIMARY KEY,
    action_id TEXT NOT NULL,
    image_id INTEGER NOT NULL REFERENCES images(id),
    elo_before REAL NOT NULL,
    propagated_updates_before INTEGER NOT NULL,
    elo_after REAL NOT NULL,
    delta REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_propagation_updates_action_id
ON propagation_updates(action_id);

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
CREATE INDEX IF NOT EXISTS idx_images_active_camera
ON images(camera_make, camera_model) WHERE status IN ('kept', 'maybe');
CREATE INDEX IF NOT EXISTS idx_images_active_lens
ON images(lens) WHERE status IN ('kept', 'maybe');

-- Source-aware indexes for the active working set. Source state now determines
-- whether an image is active; status is retained only for old DB compatibility.
CREATE INDEX IF NOT EXISTS idx_images_source_elo
ON images(source_id, elo DESC);
CREATE INDEX IF NOT EXISTS idx_images_source_elo_asc
ON images(source_id, elo ASC);
CREATE INDEX IF NOT EXISTS idx_images_source_comparisons
ON images(source_id, comparisons DESC);
CREATE INDEX IF NOT EXISTS idx_images_source_comparisons_asc
ON images(source_id, comparisons ASC);
CREATE INDEX IF NOT EXISTS idx_images_source_filename
ON images(source_id, filename ASC);
CREATE INDEX IF NOT EXISTS idx_images_source_id_desc
ON images(source_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_images_source_filepath
ON images(source_id, filepath ASC);
CREATE INDEX IF NOT EXISTS idx_images_source_orientation_elo
ON images(source_id, orientation, elo DESC);
CREATE INDEX IF NOT EXISTS idx_images_source_date_taken
ON images(source_id, date_taken DESC);
CREATE INDEX IF NOT EXISTS idx_images_source_file_size
ON images(source_id, file_size DESC);
CREATE INDEX IF NOT EXISTS idx_images_source_file_ext
ON images(source_id, file_ext);
CREATE INDEX IF NOT EXISTS idx_images_source_camera
ON images(source_id, camera_make, camera_model);
CREATE INDEX IF NOT EXISTS idx_images_source_lens
ON images(source_id, lens);
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
_filter_options_cache = {"data": None, "expires": 0}


def normalize_source_path(path: str) -> str:
    """Return the canonical local path used as a catalog source key."""
    return os.path.realpath(os.path.abspath(os.path.expanduser(path or "")))


def source_display_name(path: str) -> str:
    normalized = normalize_source_path(path)
    return os.path.basename(normalized.rstrip(os.sep)) or normalized


def active_source_join(image_alias: str = "i", source_alias: str = "s") -> str:
    return f"JOIN catalog_sources {source_alias} ON {source_alias}.id = {image_alias}.source_id"


def active_source_condition(source_alias: str = "s") -> str:
    return f"{source_alias}.included = 1 AND {source_alias}.online = 1"


def _chunked(values: list[int], chunk_size: int = 500):
    for start in range(0, len(values), chunk_size):
        yield values[start:start + chunk_size]


def _invalidate_stats_cache():
    _stats_cache["data"] = None
    _stats_cache["expires"] = 0


def _invalidate_filter_options_cache():
    _filter_options_cache["data"] = None
    _filter_options_cache["expires"] = 0


def invalidate_stats_cache():
    _invalidate_stats_cache()


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH, timeout=30)
    db.row_factory = aiosqlite.Row
    return db


async def _ensure_catalog_source(conn, path: str, *, included: bool = True, last_scan_at=None):
    normalized = normalize_source_path(path)
    display_name = source_display_name(normalized)
    online = 1 if os.path.isdir(normalized) else 0
    now = _time.time()
    await conn.execute(
        "INSERT INTO catalog_sources "
        "(path, display_name, included, online, created_at, last_scan_at, last_seen_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(path) DO UPDATE SET "
        "display_name = excluded.display_name, "
        "included = CASE WHEN excluded.included = 1 THEN 1 ELSE catalog_sources.included END, "
        "online = excluded.online, "
        "last_scan_at = COALESCE(excluded.last_scan_at, catalog_sources.last_scan_at), "
        "last_seen_at = excluded.last_seen_at, "
        "removed_at = CASE WHEN excluded.included = 1 THEN NULL ELSE catalog_sources.removed_at END",
        (normalized, display_name, 1 if included else 0, online, now, last_scan_at, now),
    )
    cursor = await conn.execute("SELECT * FROM catalog_sources WHERE path = ?", (normalized,))
    return await cursor.fetchone()


async def _update_source_counts(conn, source_id: int | None = None):
    params = []
    where = ""
    if source_id is not None:
        where = " WHERE id = ?"
        params.append(source_id)
    await conn.execute(
        "UPDATE catalog_sources SET image_count = ("
        "  SELECT COUNT(*) FROM images WHERE images.source_id = catalog_sources.id"
        f"){where}",
        params,
    )
    await conn.execute(
        "UPDATE catalog_sources SET active_image_count = CASE "
        "WHEN included = 1 AND online = 1 THEN image_count ELSE 0 END"
        f"{where}",
        params,
    )


async def _migrate_catalog_sources(conn):
    cursor = await conn.execute(
        "UPDATE images SET flag = 'rejected' "
        "WHERE status = 'rejected' AND COALESCE(flag, 'unflagged') = 'unflagged'"
    )
    if cursor.rowcount:
        _invalidate_filter_options_cache()

    # Status used to control membership in older versions. Sources now own
    # membership, so normalize old statuses after preserving rejection as a flag.
    await conn.execute("UPDATE images SET status = 'kept' WHERE COALESCE(status, '') != 'kept'")

    await conn.execute(
        "UPDATE images SET source_id = NULL "
        "WHERE source_id IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM catalog_sources WHERE catalog_sources.id = images.source_id)"
    )

    cursor = await conn.execute(
        "SELECT filepath FROM images WHERE source_id IS NULL ORDER BY filepath"
    )
    rows = await cursor.fetchall()
    if rows:
        dirs = [os.path.dirname(row["filepath"]) for row in rows if row["filepath"]]
        try:
            root = os.path.commonpath(dirs) if dirs else os.path.expanduser("~/Pictures")
        except ValueError:
            root = dirs[0] if dirs else os.path.expanduser("~/Pictures")
        source = await _ensure_catalog_source(conn, root, included=True)
        await conn.execute(
            "UPDATE images SET source_id = ? WHERE source_id IS NULL",
            (source["id"],),
        )

    await _update_source_counts(conn)


async def init_db():
    db_exists = os.path.exists(DB_PATH)
    db = await get_db()
    try:
        if not db_exists:
            await db.execute("PRAGMA journal_mode=WAL")
        if db_exists:
            # Existing databases may predate columns now referenced by indexes in
            # SCHEMA. Add those columns first so CREATE INDEX IF NOT EXISTS is safe.
            await db.execute(
                "CREATE TABLE IF NOT EXISTS catalog_sources ("
                "id INTEGER PRIMARY KEY, "
                "path TEXT NOT NULL UNIQUE, "
                "display_name TEXT NOT NULL, "
                "included INTEGER NOT NULL DEFAULT 1, "
                "online INTEGER NOT NULL DEFAULT 1, "
                "image_count INTEGER NOT NULL DEFAULT 0, "
                "active_image_count INTEGER NOT NULL DEFAULT 0, "
                "created_at REAL NOT NULL DEFAULT (strftime('%s', 'now')), "
                "last_scan_at REAL DEFAULT NULL, "
                "last_seen_at REAL DEFAULT NULL, "
                "removed_at REAL DEFAULT NULL"
                ")"
            )
            cursor = await db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'images'"
            )
            if await cursor.fetchone():
                for col, defn in [
                    ("source_id", "INTEGER REFERENCES catalog_sources(id)"),
                    ("orientation", "TEXT DEFAULT NULL"),
                    ("flag", "TEXT DEFAULT 'unflagged'"),
                    ("propagated_updates", "INTEGER DEFAULT 0"),
                    ("predicted_elo", "REAL DEFAULT NULL"),
                    ("uncertainty", "REAL DEFAULT NULL"),
                    ("aspect_ratio", "REAL DEFAULT NULL"),
                    ("date_taken", "TEXT DEFAULT NULL"),
                    ("camera_make", "TEXT DEFAULT NULL"),
                    ("camera_model", "TEXT DEFAULT NULL"),
                    ("lens", "TEXT DEFAULT NULL"),
                    ("file_ext", "TEXT DEFAULT NULL"),
                    ("file_size", "INTEGER DEFAULT NULL"),
                    ("file_modified_at", "REAL DEFAULT NULL"),
                    ("width", "INTEGER DEFAULT NULL"),
                    ("height", "INTEGER DEFAULT NULL"),
                    ("metadata_scanned_at", "REAL DEFAULT NULL"),
                    ("latitude", "REAL DEFAULT NULL"),
                    ("longitude", "REAL DEFAULT NULL"),
                    ("metadata_version", "INTEGER DEFAULT NULL"),
                ]:
                    try:
                        await db.execute(f"ALTER TABLE images ADD COLUMN {col} {defn}")
                    except Exception:
                        pass
        await db.executescript(SCHEMA)
        # Migrations: add columns if missing
        for col, defn in [
            ("source_id", "INTEGER REFERENCES catalog_sources(id)"),
            ("orientation", "TEXT DEFAULT NULL"),
            ("flag", "TEXT DEFAULT 'unflagged'"),
            ("propagated_updates", "INTEGER DEFAULT 0"),
            ("predicted_elo", "REAL DEFAULT NULL"),
            ("uncertainty", "REAL DEFAULT NULL"),
            ("aspect_ratio", "REAL DEFAULT NULL"),
            ("date_taken", "TEXT DEFAULT NULL"),
            ("camera_make", "TEXT DEFAULT NULL"),
            ("camera_model", "TEXT DEFAULT NULL"),
            ("lens", "TEXT DEFAULT NULL"),
            ("file_ext", "TEXT DEFAULT NULL"),
            ("file_size", "INTEGER DEFAULT NULL"),
            ("file_modified_at", "REAL DEFAULT NULL"),
            ("width", "INTEGER DEFAULT NULL"),
            ("height", "INTEGER DEFAULT NULL"),
            ("metadata_scanned_at", "REAL DEFAULT NULL"),
            ("latitude", "REAL DEFAULT NULL"),
            ("longitude", "REAL DEFAULT NULL"),
            ("metadata_version", "INTEGER DEFAULT NULL"),
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
        try:
            await db.execute(
                "ALTER TABLE comparisons "
                "ADD COLUMN action_id TEXT DEFAULT NULL"
            )
        except Exception:
            pass  # Column already exists
        for col, defn in [
            ("display_name", "TEXT DEFAULT ''"),
            ("included", "INTEGER NOT NULL DEFAULT 1"),
            ("online", "INTEGER NOT NULL DEFAULT 1"),
            ("image_count", "INTEGER NOT NULL DEFAULT 0"),
            ("active_image_count", "INTEGER NOT NULL DEFAULT 0"),
            ("created_at", "REAL DEFAULT NULL"),
            ("last_scan_at", "REAL DEFAULT NULL"),
            ("last_seen_at", "REAL DEFAULT NULL"),
            ("removed_at", "REAL DEFAULT NULL"),
        ]:
            try:
                await db.execute(f"ALTER TABLE catalog_sources ADD COLUMN {col} {defn}")
            except Exception:
                pass  # Column already exists
        await db.execute("CREATE INDEX IF NOT EXISTS idx_images_flag ON images(flag)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_comparisons_action_id ON comparisons(action_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_images_source_id ON images(source_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_catalog_sources_path ON catalog_sources(path)")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_catalog_sources_active "
            "ON catalog_sources(included, online)"
        )
        for name, sql in [
            ("idx_images_source_elo", "ON images(source_id, elo DESC)"),
            ("idx_images_source_elo_asc", "ON images(source_id, elo ASC)"),
            ("idx_images_source_comparisons", "ON images(source_id, comparisons DESC)"),
            ("idx_images_source_comparisons_asc", "ON images(source_id, comparisons ASC)"),
            ("idx_images_source_filename", "ON images(source_id, filename ASC)"),
            ("idx_images_source_id_desc", "ON images(source_id, id DESC)"),
            ("idx_images_source_filepath", "ON images(source_id, filepath ASC)"),
            ("idx_images_source_orientation_elo", "ON images(source_id, orientation, elo DESC)"),
            ("idx_images_source_date_taken", "ON images(source_id, date_taken DESC)"),
            ("idx_images_source_file_size", "ON images(source_id, file_size DESC)"),
            ("idx_images_source_file_ext", "ON images(source_id, file_ext)"),
            ("idx_images_source_camera", "ON images(source_id, camera_make, camera_model)"),
            ("idx_images_source_lens", "ON images(source_id, lens)"),
        ]:
            await db.execute(f"CREATE INDEX IF NOT EXISTS {name} {sql}")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_images_active_date_taken "
            "ON images(date_taken DESC) WHERE status IN ('kept', 'maybe')"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_images_active_file_size "
            "ON images(file_size DESC) WHERE status IN ('kept', 'maybe')"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_images_active_file_ext "
            "ON images(file_ext) WHERE status IN ('kept', 'maybe')"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_images_active_camera "
            "ON images(camera_make, camera_model) WHERE status IN ('kept', 'maybe')"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_images_active_lens "
            "ON images(lens) WHERE status IN ('kept', 'maybe')"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_images_gps "
            "ON images(latitude, longitude) WHERE latitude IS NOT NULL"
        )
        # Backfill aspect_ratio from orientation for images that don't have it yet
        await db.execute(
            "UPDATE images SET aspect_ratio = 1.5 WHERE orientation = 'landscape' AND aspect_ratio IS NULL"
        )
        await db.execute(
            "UPDATE images SET aspect_ratio = 0.6667 WHERE orientation = 'portrait' AND aspect_ratio IS NULL"
        )
        await _migrate_catalog_sources(db)
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
            "SELECT i.id, i.filepath FROM images i "
            "JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE i.orientation IS NULL AND s.included = 1 AND s.online = 1 "
            "LIMIT ?",
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
        _invalidate_filter_options_cache()
    finally:
        await db.close()


async def get_images_needing_metadata(limit: int = 100, metadata_version: int = 1):
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT i.id, i.filepath FROM images i "
            "JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1 AND ("
            "i.metadata_scanned_at IS NULL "
            "OR i.metadata_version IS NULL "
            "OR i.metadata_version < ?) "
            "LIMIT ?",
            (metadata_version, limit),
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def batch_update_metadata(updates: list[tuple]):
    """Persist extracted metadata. Tuples are built in app._metadata_update_tuple."""
    if not updates:
        return
    db = await get_db()
    try:
        await db.executemany(
            "UPDATE images SET "
            "date_taken = COALESCE(?, date_taken), "
            "camera_make = COALESCE(?, camera_make), "
            "camera_model = COALESCE(?, camera_model), "
            "lens = COALESCE(?, lens), "
            "file_ext = COALESCE(?, file_ext), "
            "file_size = COALESCE(?, file_size), "
            "file_modified_at = COALESCE(?, file_modified_at), "
            "width = COALESCE(?, width), "
            "height = COALESCE(?, height), "
            "metadata_scanned_at = ?, metadata_version = ?, "
            "orientation = COALESCE(orientation, ?), "
            "aspect_ratio = COALESCE(aspect_ratio, ?), "
            "latitude = COALESCE(?, latitude), "
            "longitude = COALESCE(?, longitude) "
            "WHERE id = ?",
            updates,
        )
        await db.commit()
        _invalidate_filter_options_cache()
    finally:
        await db.close()


def _insert_row_with_file_metadata(row):
    if len(row) >= 5:
        return row[:5]
    filename, filepath = row[:2]
    file_ext = os.path.splitext(filename)[1].lower()
    file_size = None
    file_modified_at = None
    try:
        stat = os.stat(filepath)
        file_size = int(stat.st_size)
        file_modified_at = float(stat.st_mtime)
    except Exception:
        pass
    return filename, filepath, file_ext, file_size, file_modified_at


async def insert_images_batch(rows: list[tuple], source_id: int | None = None):
    """Insert image rows, ignoring duplicates."""
    if not rows:
        return
    db = await get_db()
    try:
        normalized_rows = [_insert_row_with_file_metadata(row) for row in rows]
        if source_id is not None:
            await db.executemany(
                "INSERT OR IGNORE INTO images "
                "(source_id, filename, filepath, status, file_ext, file_size, file_modified_at) "
                "VALUES (?, ?, ?, 'kept', ?, ?, ?)",
                [(source_id, *row) for row in normalized_rows],
            )
            await db.execute(
                "UPDATE images SET source_id = ? "
                "WHERE source_id IS NULL AND filepath IN ("
                + ",".join("?" for _ in normalized_rows)
                + ")",
                [source_id] + [row[1] for row in normalized_rows],
            )
            await _update_source_counts(db, source_id)
        else:
            await db.executemany(
                "INSERT OR IGNORE INTO images "
                "(filename, filepath, status, file_ext, file_size, file_modified_at) "
                "VALUES (?, ?, 'kept', ?, ?, ?)",
                normalized_rows,
            )
        await db.commit()
        _invalidate_stats_cache()
        _invalidate_filter_options_cache()
    finally:
        await db.close()


async def refresh_source_online_states():
    db = await get_db()
    try:
        cursor = await db.execute("SELECT id, path, online FROM catalog_sources")
        rows = await cursor.fetchall()
        now = _time.time()
        updates = []
        for row in rows:
            online = 1 if os.path.isdir(row["path"]) else 0
            if int(row["online"] or 0) != online:
                updates.append((online, now, row["id"]))
        if updates:
            await db.executemany(
                "UPDATE catalog_sources SET online = ?, last_seen_at = ? WHERE id = ?",
                updates,
            )
            await _update_source_counts(db)
            await db.commit()
            _invalidate_stats_cache()
            _invalidate_filter_options_cache()
    finally:
        await db.close()


async def add_or_restore_source(path: str):
    db = await get_db()
    try:
        source = await _ensure_catalog_source(db, path, included=True)
        await _update_source_counts(db, source["id"])
        await db.commit()
        _invalidate_stats_cache()
        _invalidate_filter_options_cache()
        cursor = await db.execute("SELECT * FROM catalog_sources WHERE id = ?", (source["id"],))
        return await cursor.fetchone()
    finally:
        await db.close()


async def mark_source_scan_started(source_id: int):
    db = await get_db()
    try:
        now = _time.time()
        await db.execute(
            "UPDATE catalog_sources SET included = 1, online = 1, removed_at = NULL, last_seen_at = ? "
            "WHERE id = ?",
            (now, source_id),
        )
        await db.commit()
    finally:
        await db.close()


async def mark_source_scan_finished(source_id: int):
    db = await get_db()
    try:
        now = _time.time()
        await db.execute(
            "UPDATE catalog_sources SET last_scan_at = ?, last_seen_at = ?, online = ? WHERE id = ?",
            (now, now, 1, source_id),
        )
        await _update_source_counts(db, source_id)
        await db.commit()
        _invalidate_stats_cache()
        _invalidate_filter_options_cache()
    finally:
        await db.close()


async def get_source(source_id: int):
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM catalog_sources WHERE id = ?", (source_id,))
        return await cursor.fetchone()
    finally:
        await db.close()


async def get_source_by_path(path: str):
    normalized = normalize_source_path(path)
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM catalog_sources WHERE path = ?", (normalized,))
        return await cursor.fetchone()
    finally:
        await db.close()


async def get_catalog_sources():
    await refresh_source_online_states()
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, path, display_name, included, online, image_count, active_image_count, "
            "created_at, last_scan_at, last_seen_at, removed_at "
            "FROM catalog_sources ORDER BY included DESC, display_name COLLATE NOCASE ASC, path ASC"
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def get_catalog_summary():
    sources = [dict(row) for row in await get_catalog_sources()]
    stats = await get_stats()
    return {"sources": sources, "stats": stats}


async def remove_source_keep_data(source_id: int):
    db = await get_db()
    try:
        now = _time.time()
        await db.execute(
            "UPDATE catalog_sources SET included = 0, removed_at = ?, last_seen_at = ? WHERE id = ?",
            (now, now, source_id),
        )
        await _update_source_counts(db, source_id)
        await db.commit()
        _invalidate_stats_cache()
        _invalidate_filter_options_cache()
    finally:
        await db.close()


async def get_source_image_ids(source_id: int) -> list[int]:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT id FROM images WHERE source_id = ?", (source_id,))
        return [int(row["id"]) for row in await cursor.fetchall()]
    finally:
        await db.close()


async def purge_source_catalog_data(source_id: int) -> dict:
    image_ids = await get_source_image_ids(source_id)
    db = await get_db()
    try:
        comparison_count = 0
        comparison_decrements: dict[int, int] = {}
        if image_ids:
            source_id_set = set(image_ids)
            for chunk in _chunked(image_ids):
                placeholders = ",".join("?" for _ in chunk)
                cursor = await db.execute(
                    f"DELETE FROM embeddings WHERE image_id IN ({placeholders})",
                    chunk,
                )
                cursor = await db.execute(
                    f"SELECT winner_id, loser_id FROM comparisons "
                    f"WHERE winner_id IN ({placeholders}) OR loser_id IN ({placeholders})",
                    chunk + chunk,
                )
                for row in await cursor.fetchall():
                    winner_id = int(row["winner_id"])
                    loser_id = int(row["loser_id"])
                    if winner_id in source_id_set and loser_id not in source_id_set:
                        comparison_decrements[loser_id] = comparison_decrements.get(loser_id, 0) + 1
                    elif loser_id in source_id_set and winner_id not in source_id_set:
                        comparison_decrements[winner_id] = comparison_decrements.get(winner_id, 0) + 1
                cursor = await db.execute(
                    f"DELETE FROM comparisons "
                    f"WHERE winner_id IN ({placeholders}) OR loser_id IN ({placeholders})",
                    chunk + chunk,
                )
                comparison_count += max(0, cursor.rowcount or 0)
                await db.execute(
                    f"DELETE FROM cache_entries WHERE image_id IN ({placeholders})",
                    chunk,
                )
                await db.execute(
                    f"DELETE FROM images WHERE id IN ({placeholders})",
                    chunk,
                )
            if comparison_decrements:
                await db.executemany(
                    "UPDATE images SET comparisons = MAX(COALESCE(comparisons, 0) - ?, 0) WHERE id = ?",
                    [(count, image_id) for image_id, count in comparison_decrements.items()],
                )
        await db.execute("DELETE FROM catalog_sources WHERE id = ?", (source_id,))
        await _update_source_counts(db)
        await db.commit()
        _invalidate_stats_cache()
        _invalidate_filter_options_cache()
        return {
            "images_deleted": len(image_ids),
            "comparisons_deleted": comparison_count,
        }
    finally:
        await db.close()


async def get_recent_active_images(limit: int = 10):
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT i.id, i.filename, i.filepath FROM images i "
            "JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1 "
            "ORDER BY i.id DESC LIMIT ?",
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
        _invalidate_filter_options_cache()
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


async def batch_set_image_flags(image_ids: list[int], flag: str, chunk_size: int = 500) -> int:
    if not image_ids:
        return 0
    db = await get_db()
    updated = 0
    try:
        for start in range(0, len(image_ids), chunk_size):
            chunk = image_ids[start:start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            await db.execute(
                f"UPDATE images SET flag = ? WHERE id IN ({placeholders})",
                [flag] + chunk,
            )
            updated += len(chunk)
        await db.commit()
        return updated
    finally:
        await db.close()


async def get_active_images_for_pairing():
    """Get active images sorted by Elo for Swiss-system pairing."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT i.id, i.filename, i.filepath, i.elo, i.comparisons, "
            "i.propagated_updates, i.flag, i.orientation, "
            "i.aspect_ratio, i.date_taken, i.camera_make, i.camera_model, i.lens, "
            "i.file_ext, i.file_size, i.width, i.height, i.file_modified_at, "
            "i.latitude, i.longitude FROM images i "
            "JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1 ORDER BY i.elo DESC"
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


async def record_comparison(
    winner_id: int,
    loser_id: int,
    mode: str,
    elo_before_winner: float,
    elo_before_loser: float,
    new_winner_elo: float,
    new_loser_elo: float,
    action_id: str | None = None,
):
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO comparisons "
            "(winner_id, loser_id, mode, elo_before_winner, elo_before_loser, action_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (winner_id, loser_id, mode, elo_before_winner, elo_before_loser, action_id),
        )
        await db.execute(
            "UPDATE images SET elo = ?, comparisons = COALESCE(comparisons, 0) + 1 WHERE id = ?",
            (new_winner_elo, winner_id),
        )
        await db.execute(
            "UPDATE images SET elo = ?, comparisons = COALESCE(comparisons, 0) + 1 WHERE id = ?",
            (new_loser_elo, loser_id),
        )
        await db.commit()
        _invalidate_stats_cache()
    finally:
        await db.close()


async def undo_last_comparison():
    """Undo the last comparison/action, restoring Elo ratings."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT id, action_id FROM comparisons ORDER BY id DESC LIMIT 1"
        )
        latest = await cursor.fetchone()
        if latest:
            action_id = latest["action_id"]
            if action_id:
                cursor = await db.execute(
                    "SELECT id, winner_id, loser_id, elo_before_winner, elo_before_loser, action_id "
                    "FROM comparisons WHERE action_id = ? ORDER BY id ASC",
                    (action_id,),
                )
                rows = await cursor.fetchall()
            else:
                cursor = await db.execute(
                    "SELECT id, winner_id, loser_id, elo_before_winner, elo_before_loser, action_id "
                    "FROM comparisons WHERE id = ?",
                    (latest["id"],),
                )
                rows = await cursor.fetchall()

            if not rows:
                return None

            propagation_rows = []
            if action_id:
                cursor = await db.execute(
                    "SELECT image_id, elo_before, propagated_updates_before "
                    "FROM propagation_updates WHERE action_id = ? ORDER BY id ASC",
                    (action_id,),
                )
                propagation_rows = await cursor.fetchall()

            for row in propagation_rows:
                await db.execute(
                    "UPDATE images SET elo = ?, propagated_updates = ? WHERE id = ?",
                    (
                        float(row["elo_before"]),
                        int(row["propagated_updates_before"]),
                        int(row["image_id"]),
                    ),
                )

            restore_elo: dict[int, float] = {}
            comparison_decrements: dict[int, int] = {}
            for row in rows:
                winner_id = int(row["winner_id"])
                loser_id = int(row["loser_id"])
                restore_elo.setdefault(winner_id, float(row["elo_before_winner"]))
                restore_elo.setdefault(loser_id, float(row["elo_before_loser"]))
                comparison_decrements[winner_id] = comparison_decrements.get(winner_id, 0) + 1
                comparison_decrements[loser_id] = comparison_decrements.get(loser_id, 0) + 1

            for image_id, elo in restore_elo.items():
                await db.execute(
                    "UPDATE images SET elo = ?, comparisons = MAX(COALESCE(comparisons, 0) - ?, 0) WHERE id = ?",
                    (elo, comparison_decrements.get(image_id, 0), image_id),
                )

            if action_id:
                await db.execute("DELETE FROM comparisons WHERE action_id = ?", (action_id,))
                await db.execute("DELETE FROM propagation_updates WHERE action_id = ?", (action_id,))
            else:
                await db.execute("DELETE FROM comparisons WHERE id = ?", (latest["id"],))
            await db.commit()
            _invalidate_stats_cache()
            last_row = rows[-1]
            return {
                "winner_id": last_row["winner_id"],
                "loser_id": last_row["loser_id"],
                "comparisons_undone": len(rows),
                "propagations_undone": len(propagation_rows),
                "action_id": action_id,
            }
        return None
    finally:
        await db.close()


RANKING_SORTS = {
    "elo": "i.elo DESC",
    "elo_asc": "i.elo ASC",
    "comparisons": "i.comparisons DESC",
    "least_compared": "i.comparisons ASC",
    "filename": "i.filename ASC",
    "filename_desc": "i.filename DESC",
    "newest": "i.id DESC",
    "oldest": "i.id ASC",
    "date_taken": "i.date_taken IS NULL ASC, i.date_taken DESC, i.id DESC",
    "date_taken_asc": "i.date_taken IS NULL ASC, i.date_taken ASC, i.id ASC",
    "file_size": "i.file_size IS NULL ASC, i.file_size DESC, i.id DESC",
    "file_size_asc": "i.file_size IS NULL ASC, i.file_size ASC, i.id ASC",
    "date_modified": "i.file_modified_at IS NULL ASC, i.file_modified_at DESC, i.id DESC",
    "date_modified_asc": "i.file_modified_at IS NULL ASC, i.file_modified_at ASC, i.id ASC",
    "camera": "i.camera_make IS NULL ASC, i.camera_make ASC, i.camera_model ASC, i.id ASC",
    "camera_desc": "i.camera_make IS NULL ASC, i.camera_make DESC, i.camera_model DESC, i.id DESC",
    "resolution": "(i.width * i.height) IS NULL ASC, (i.width * i.height) DESC, i.id DESC",
    "resolution_asc": "(i.width * i.height) IS NULL ASC, (i.width * i.height) ASC, i.id ASC",
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
    "date_taken": None,
    "date_taken_asc": None,
    "file_size": None,
    "file_size_asc": None,
    "date_modified": None,
    "date_modified_asc": None,
    "camera": None,
    "camera_desc": None,
    "resolution": None,
    "resolution_asc": None,
}

STAR_THRESHOLDS = {5: 1500, 4: 1350, 3: 1250, 2: 1150, 1: 0}


def _ranking_filter_parts(
    orientation: str = "", compared: str = "", min_stars: int = 0,
    folder: str = "", flag: str = "", date_taken: str = "",
    file_type: str = "", camera: str = "", lens: str = "",
    visible_thumb_size: str = "", cache_root: str = "",
    text_query: str = "",
) -> tuple[list[str], list]:
    conditions = ["s.included = 1", "s.online = 1"]
    params = []

    if orientation in ("landscape", "portrait"):
        conditions.append("i.orientation = ?")
        params.append(orientation)

    if compared == "compared":
        conditions.append(
            "(i.comparisons > 0 OR i.propagated_updates > 0 OR ABS(COALESCE(i.elo, 1200.0) - 1200.0) > 0.0001)"
        )
    elif compared == "uncompared":
        conditions.append(
            "i.comparisons = 0 AND COALESCE(i.propagated_updates, 0) = 0 "
            "AND ABS(COALESCE(i.elo, 1200.0) - 1200.0) <= 0.0001"
        )
    elif compared == "confident":
        conditions.append("i.comparisons >= 10")

    if min_stars > 0 and min_stars in STAR_THRESHOLDS:
        conditions.append("i.elo >= ?")
        params.append(STAR_THRESHOLDS[min_stars])

    if folder:
        conditions.append("i.filepath LIKE ?")
        params.append(f"%/{folder}/%")

    if flag in ("picked", "unflagged", "rejected"):
        conditions.append("COALESCE(i.flag, 'unflagged') = ?")
        params.append(flag)

    if date_taken == "undated":
        conditions.append("i.date_taken IS NULL")
    elif date_taken.isdigit() and len(date_taken) == 4:
        start = f"{date_taken}-01-01 00:00:00"
        end = f"{int(date_taken) + 1}-01-01 00:00:00"
        conditions.append("i.date_taken >= ? AND i.date_taken < ?")
        params.extend([start, end])

    if file_type:
        normalized_type = file_type.lower()
        if not normalized_type.startswith("."):
            normalized_type = f".{normalized_type}"
        conditions.append("LOWER(i.file_ext) = ?")
        params.append(normalized_type)

    if camera:
        conditions.append(
            "TRIM(COALESCE(i.camera_make, '') || ' ' || COALESCE(i.camera_model, '')) = ?"
        )
        params.append(camera)

    if lens:
        conditions.append("i.lens = ?")
        params.append(lens)

    if text_query:
        escaped = (
            text_query.strip().lower()
            .replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        if escaped:
            pattern = f"%{escaped}%"
            fields = (
                "i.filename",
                "i.filepath",
                "i.date_taken",
                "i.camera_make",
                "i.camera_model",
                "i.lens",
                "i.file_ext",
            )
            conditions.append(
                "("
                + " OR ".join(f"LOWER(COALESCE({field}, '')) LIKE ? ESCAPE '\\'" for field in fields)
                + ")"
            )
            params.extend([pattern] * len(fields))

    if visible_thumb_size and cache_root:
        conditions.append(
            "EXISTS ("
            "  SELECT 1 FROM cache_entries c "
            "  WHERE c.cache_root = ? AND c.size = ? AND c.image_id = i.id"
            ")"
        )
        params.extend([cache_root, visible_thumb_size])

    return conditions, params


async def get_cached_image_ids(
    image_ids: list[int],
    size: str,
    cache_root: str,
    chunk_size: int = 900,
) -> set[int]:
    """Return IDs with a cache_entries row for the exact cache root/tier."""
    if not image_ids or not size or not cache_root:
        return set()
    unique_ids = list(dict.fromkeys(int(image_id) for image_id in image_ids))
    cached: set[int] = set()
    db = await get_db()
    try:
        for chunk in _chunked(unique_ids, chunk_size):
            placeholders = ",".join("?" for _ in chunk)
            cursor = await db.execute(
                "SELECT image_id FROM cache_entries "
                f"WHERE cache_root = ? AND size = ? AND image_id IN ({placeholders})",
                [cache_root, size] + chunk,
            )
            cached.update(int(row["image_id"]) for row in await cursor.fetchall())
        return cached
    finally:
        await db.close()


async def get_rankings(limit: int = 100, offset: int = 0, sort: str = "elo",
                       orientation: str = "", compared: str = "", min_stars: int = 0,
                       folder: str = "", flag: str = "", date_taken: str = "",
                       file_type: str = "", camera: str = "", lens: str = "",
                       id_filter: set = None,
                       visible_thumb_size: str = "", cache_root: str = "",
                       text_query: str = ""):
    db = await get_db()
    try:
        order = RANKING_SORTS.get(sort, "elo DESC")
        conditions, params = _ranking_filter_parts(
            orientation=orientation, compared=compared, min_stars=min_stars,
            folder=folder, flag=flag, date_taken=date_taken,
            file_type=file_type, camera=camera, lens=lens,
            visible_thumb_size=visible_thumb_size, cache_root=cache_root,
            text_query=text_query,
        )

        if id_filter is not None:
            if not id_filter:
                return []
            placeholders = ",".join("?" * len(id_filter))
            conditions.append(f"i.id IN ({placeholders})")
            params.extend(id_filter)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        cursor = await db.execute(
            f"SELECT i.id, i.source_id, i.filename, i.filepath, i.elo, i.comparisons, "
            f"i.propagated_updates, "
            f"i.status, i.flag, i.aspect_ratio, "
            f"i.date_taken, i.camera_make, i.camera_model, i.lens, i.file_ext, i.file_size, "
            f"i.file_modified_at, i.width, i.height, i.latitude, i.longitude, i.created_at "
            f"FROM images i JOIN catalog_sources s ON s.id = i.source_id "
            f"WHERE {where} ORDER BY {order} LIMIT ? OFFSET ?",
            params,
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def count_rankings(orientation: str = "", compared: str = "", min_stars: int = 0,
                         folder: str = "", flag: str = "", date_taken: str = "",
                         file_type: str = "", camera: str = "", lens: str = "",
                         id_filter: set = None,
                         visible_thumb_size: str = "", cache_root: str = "",
                         text_query: str = "") -> int:
    db = await get_db()
    try:
        conditions, params = _ranking_filter_parts(
            orientation=orientation, compared=compared, min_stars=min_stars,
            folder=folder, flag=flag, date_taken=date_taken,
            file_type=file_type, camera=camera, lens=lens,
            visible_thumb_size=visible_thumb_size, cache_root=cache_root,
            text_query=text_query,
        )
        if id_filter is not None:
            if not id_filter:
                return 0
            placeholders = ",".join("?" * len(id_filter))
            conditions.append(f"i.id IN ({placeholders})")
            params.extend(id_filter)
        cursor = await db.execute(
            f"SELECT COUNT(*) AS count FROM images i "
            f"JOIN catalog_sources s ON s.id = i.source_id "
            f"WHERE {' AND '.join(conditions)}",
            params,
        )
        row = await cursor.fetchone()
        return int(row["count"] or 0)
    finally:
        await db.close()


async def get_date_groups(orientation: str = "", compared: str = "", min_stars: int = 0,
                          folder: str = "", flag: str = "", date_taken: str = "",
                          file_type: str = "", camera: str = "", lens: str = "",
                          visible_thumb_size: str = "", cache_root: str = ""):
    db = await get_db()
    try:
        conditions, params = _ranking_filter_parts(
            orientation=orientation, compared=compared, min_stars=min_stars,
            folder=folder, flag=flag, date_taken=date_taken,
            file_type=file_type, camera=camera, lens=lens,
            visible_thumb_size=visible_thumb_size, cache_root=cache_root,
        )
        cursor = await db.execute(
            "SELECT "
            "CASE WHEN i.date_taken IS NOT NULL AND length(i.date_taken) >= 7 "
            "THEN substr(i.date_taken, 1, 7) ELSE '' END AS date_group, "
            "COUNT(*) AS count "
            f"FROM images i JOIN catalog_sources s ON s.id = i.source_id "
            f"WHERE {' AND '.join(conditions)} "
            "GROUP BY date_group ORDER BY date_group DESC",
            params,
        )
        groups = []
        for row in await cursor.fetchall():
            date_group = row["date_group"] or ""
            if date_group:
                try:
                    from datetime import datetime
                    label = datetime.strptime(date_group, "%Y-%m").strftime("%B %Y")
                except ValueError:
                    label = date_group
            else:
                label = "No Date"
            groups.append({"date": date_group, "label": label, "count": row["count"]})
        return groups
    finally:
        await db.close()


async def get_map_markers(orientation: str = "", compared: str = "", min_stars: int = 0,
                          folder: str = "", flag: str = "", date_taken: str = "",
                          file_type: str = "", camera: str = "", lens: str = "",
                          visible_thumb_size: str = "", cache_root: str = ""):
    db = await get_db()
    try:
        conditions, params = _ranking_filter_parts(
            orientation=orientation, compared=compared, min_stars=min_stars,
            folder=folder, flag=flag, date_taken=date_taken,
            file_type=file_type, camera=camera, lens=lens,
        )
        total_cursor = await db.execute(
            f"SELECT COUNT(*) AS count FROM images i "
            f"JOIN catalog_sources s ON s.id = i.source_id "
            f"WHERE {' AND '.join(conditions)}",
            params,
        )
        total_count = int((await total_cursor.fetchone())["count"] or 0)

        gps_conditions = conditions + ["i.latitude IS NOT NULL", "i.longitude IS NOT NULL"]
        gps_total_cursor = await db.execute(
            f"SELECT COUNT(*) AS count FROM images i "
            f"JOIN catalog_sources s ON s.id = i.source_id "
            f"WHERE {' AND '.join(gps_conditions)}",
            params,
        )
        gps_total_count = int((await gps_total_cursor.fetchone())["count"] or 0)

        marker_conditions = list(gps_conditions)
        marker_params = list(params)
        visible_total_count = total_count
        if visible_thumb_size and cache_root:
            marker_conditions.append(
                "EXISTS ("
                "  SELECT 1 FROM cache_entries c "
                "  WHERE c.cache_root = ? AND c.size = ? AND c.image_id = i.id"
                ")"
            )
            marker_params.extend([cache_root, visible_thumb_size])
            visible_conditions = list(conditions) + [
                "EXISTS ("
                "  SELECT 1 FROM cache_entries c "
                "  WHERE c.cache_root = ? AND c.size = ? AND c.image_id = i.id"
                ")"
            ]
            visible_cursor = await db.execute(
                f"SELECT COUNT(*) AS count FROM images i "
                f"JOIN catalog_sources s ON s.id = i.source_id "
                f"WHERE {' AND '.join(visible_conditions)}",
                params + [cache_root, visible_thumb_size],
            )
            visible_total_count = int((await visible_cursor.fetchone())["count"] or 0)

        cursor = await db.execute(
            "SELECT i.id, i.filename, i.latitude, i.longitude FROM images i "
            "JOIN catalog_sources s ON s.id = i.source_id "
            f"WHERE {' AND '.join(marker_conditions)} "
            "ORDER BY i.date_taken DESC, i.id DESC",
            marker_params,
        )
        markers = [
            {
                "id": row["id"],
                "filename": row["filename"],
                "lat": row["latitude"],
                "lng": row["longitude"],
                "thumb_url": f"/api/thumb/sm/{row['id']}",
            }
            for row in await cursor.fetchall()
        ]
        return {
            "markers": markers,
            "total_count": total_count,
            "visible_count": visible_total_count,
            "gps_count": len(markers),
            "gps_total_count": gps_total_count,
            "hidden_pending_thumbnails": max(gps_total_count - len(markers), 0),
        }
    finally:
        await db.close()


async def get_filter_options():
    if _filter_options_cache["data"] and _time.time() < _filter_options_cache["expires"]:
        return _filter_options_cache["data"]
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT substr(date_taken, 1, 4) AS year, COUNT(*) AS count "
            "FROM images i JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1 AND i.date_taken IS NOT NULL "
            "GROUP BY year ORDER BY year DESC"
        )
        years = [
            {"year": row["year"], "count": row["count"]}
            for row in await cursor.fetchall()
            if row["year"]
        ]

        cursor = await db.execute(
            "SELECT LOWER(i.file_ext) AS ext, COUNT(*) AS count "
            "FROM images i JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1 AND i.file_ext IS NOT NULL AND i.file_ext != '' "
            "GROUP BY LOWER(i.file_ext) ORDER BY count DESC, ext ASC"
        )
        file_types = [
            {"ext": row["ext"], "count": row["count"]}
            for row in await cursor.fetchall()
            if row["ext"]
        ]

        cursor = await db.execute(
            "SELECT COUNT(*) AS count FROM images "
            "JOIN catalog_sources s ON s.id = images.source_id "
            "WHERE s.included = 1 AND s.online = 1 AND images.date_taken IS NULL"
        )
        undated = (await cursor.fetchone())["count"]

        cursor = await db.execute(
            "SELECT TRIM(COALESCE(i.camera_make, '') || ' ' || COALESCE(i.camera_model, '')) AS camera, "
            "COUNT(*) AS count "
            "FROM images i JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1 "
            "AND (i.camera_make IS NOT NULL OR i.camera_model IS NOT NULL) "
            "GROUP BY camera ORDER BY count DESC, camera ASC LIMIT 200"
        )
        cameras = [
            {"camera": row["camera"], "count": row["count"]}
            for row in await cursor.fetchall()
            if row["camera"]
        ]

        cursor = await db.execute(
            "SELECT i.lens, COUNT(*) AS count "
            "FROM images i JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1 AND i.lens IS NOT NULL AND i.lens != '' "
            "GROUP BY i.lens ORDER BY count DESC, i.lens ASC LIMIT 200"
        )
        lenses = [
            {"lens": row["lens"], "count": row["count"]}
            for row in await cursor.fetchall()
            if row["lens"]
        ]

        result = {
            "years": years,
            "file_types": file_types,
            "undated": undated,
            "cameras": cameras,
            "lenses": lenses,
        }
        _filter_options_cache["data"] = result
        _filter_options_cache["expires"] = _time.time() + 30
        return result
    finally:
        await db.close()


async def get_stats():
    if _stats_cache["data"] and _time.time() < _stats_cache["expires"]:
        return _stats_cache["data"]
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT "
            "COUNT(*) AS catalog_images, "
            "SUM(CASE WHEN s.included = 1 AND s.online = 1 THEN 1 ELSE 0 END) AS active_images, "
            "SUM(CASE WHEN s.included = 0 THEN 1 ELSE 0 END) AS removed_images, "
            "SUM(CASE WHEN s.included = 1 AND s.online = 0 THEN 1 ELSE 0 END) AS offline_images, "
            "SUM(CASE WHEN s.included = 1 AND s.online = 1 AND COALESCE(i.flag, 'unflagged') = 'picked' THEN 1 ELSE 0 END) AS picked, "
            "SUM(CASE WHEN s.included = 1 AND s.online = 1 AND COALESCE(i.flag, 'unflagged') = 'rejected' THEN 1 ELSE 0 END) AS rejected "
            "FROM images i LEFT JOIN catalog_sources s ON s.id = i.source_id"
        )
        counts = await cursor.fetchone()

        cursor = await db.execute(
            "SELECT COUNT(*) as c FROM comparisons c "
            "JOIN images wi ON wi.id = c.winner_id "
            "JOIN images li ON li.id = c.loser_id "
            "JOIN catalog_sources ws ON ws.id = wi.source_id "
            "JOIN catalog_sources ls ON ls.id = li.source_id "
            "WHERE ws.included = 1 AND ws.online = 1 AND ls.included = 1 AND ls.online = 1"
        )
        direct_comparison_rows = int((await cursor.fetchone())["c"] or 0)
        cursor = await db.execute("SELECT COUNT(*) as c FROM comparisons")
        total_catalog_comparison_rows = int((await cursor.fetchone())["c"] or 0)

        cursor = await db.execute(
            "SELECT "
            "COALESCE(SUM(COALESCE(i.comparisons, 0)), 0) AS image_comparison_count, "
            "COALESCE(SUM(COALESCE(i.propagated_updates, 0)), 0) AS propagated_update_count, "
            "SUM(CASE WHEN COALESCE(i.comparisons, 0) > 0 "
            "      OR COALESCE(i.propagated_updates, 0) > 0 "
            "      OR ABS(COALESCE(i.elo, 1200.0) - 1200.0) > 0.0001 "
            "    THEN 1 ELSE 0 END) AS rated_images "
            "FROM images i LEFT JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1"
        )
        ranking_counts = await cursor.fetchone()

        cursor = await db.execute(
            "SELECT COALESCE(SUM(cnt), 0) AS c FROM ("
            "  SELECT c.winner_id AS image_id, COUNT(*) AS cnt FROM comparisons c GROUP BY c.winner_id "
            "  UNION ALL "
            "  SELECT c.loser_id AS image_id, COUNT(*) AS cnt FROM comparisons c GROUP BY c.loser_id"
            ") x "
            "JOIN images i ON i.id = x.image_id "
            "JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1"
        )
        direct_image_history_count = int((await cursor.fetchone())["c"] or 0)

        cursor = await db.execute(
            "SELECT "
            "COALESCE(SUM(COALESCE(comparisons, 0)), 0) AS image_comparison_count, "
            "COALESCE(SUM(COALESCE(propagated_updates, 0)), 0) AS propagated_update_count "
            "FROM images"
        )
        catalog_ranking_counts = await cursor.fetchone()

        active = int(counts["active_images"] or 0)
        image_comparison_count = int(ranking_counts["image_comparison_count"] or 0)
        propagated_update_count = int(ranking_counts["propagated_update_count"] or 0)
        imported_ranking_without_history = max(0, image_comparison_count - direct_image_history_count)
        ranking_signal_count = direct_comparison_rows + imported_ranking_without_history + propagated_update_count

        catalog_image_comparison_count = int(catalog_ranking_counts["image_comparison_count"] or 0)
        catalog_propagated_update_count = int(catalog_ranking_counts["propagated_update_count"] or 0)
        catalog_imported_without_history = max(
            0,
            catalog_image_comparison_count - (total_catalog_comparison_rows * 2),
        )
        catalog_ranking_signal_count = (
            total_catalog_comparison_rows
            + catalog_imported_without_history
            + catalog_propagated_update_count
        )

        result = {
            "total_images": active,
            "active_images": active,
            "total_catalog_images": int(counts["catalog_images"] or 0),
            "removed_images": int(counts["removed_images"] or 0),
            "offline_images": int(counts["offline_images"] or 0),
            "kept": active,
            "maybe": 0,
            "picked": int(counts["picked"] or 0),
            "rejected": int(counts["rejected"] or 0),
            "total_comparisons": ranking_signal_count,
            "total_catalog_comparisons": catalog_ranking_signal_count,
            "direct_comparison_rows": direct_comparison_rows,
            "direct_catalog_comparison_rows": total_catalog_comparison_rows,
            "rated_images": int(ranking_counts["rated_images"] or 0),
            "ranking_signal_count": ranking_signal_count,
            "catalog_ranking_signal_count": catalog_ranking_signal_count,
            "propagated_update_count": propagated_update_count,
            "imported_ranking_without_history": imported_ranking_without_history,
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


async def get_active_images_by_ids(image_ids: list[int]) -> dict[int, dict]:
    """Fetch active/online images by ID. Returns {id: row_dict}."""
    if not image_ids:
        return {}
    unique_ids = list(dict.fromkeys(int(image_id) for image_id in image_ids))
    db = await get_db()
    try:
        placeholders = ",".join("?" for _ in unique_ids)
        cursor = await db.execute(
            f"SELECT i.* FROM images i "
            f"JOIN catalog_sources s ON s.id = i.source_id "
            f"WHERE s.included = 1 AND s.online = 1 AND i.id IN ({placeholders})",
            unique_ids,
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
            "SELECT i.id, i.filename, i.filepath, i.elo, i.comparisons, "
            "i.propagated_updates, i.flag, i.date_taken, "
            "i.camera_make, i.camera_model, i.lens, i.file_ext, i.file_size, "
            "i.width, i.height, i.file_modified_at, i.latitude, i.longitude FROM images i "
            "JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1 ORDER BY i.elo DESC LIMIT ?",
            (limit,),
        )
        return await cursor.fetchall()
    finally:
        await db.close()


async def get_scan_folder():
    """Get a representative active source folder."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT path FROM catalog_sources WHERE included = 1 "
            "ORDER BY last_scan_at IS NULL ASC, last_scan_at DESC, id DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        if row:
            return row["path"]
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
                "JOIN catalog_sources s ON s.id = i.source_id "
                "WHERE s.included = 1 AND s.online = 1 "
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
                "JOIN catalog_sources s ON s.id = i.source_id "
                "WHERE s.included = 1 AND s.online = 1 "
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
            "JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1"
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
            "JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1"
        )
        return (await cursor.fetchone())["c"]
    finally:
        await db.close()
