"""
Local performance smoke benchmarks for photoArchive.

Run from the repo root:
    web/.venv/bin/python web/bench_perf.py

The default run measures hot DB/API helper paths, embedding-matrix cache
behavior, and already-cached thumbnail reads. It does not generate thumbnails
or start the FastAPI server.
"""

import argparse
import asyncio
import os
import sqlite3
import statistics
import time

import numpy as np

import db
import embed_cache
import settings
import thumbnails


def ms(seconds: float) -> float:
    return round(seconds * 1000.0, 3)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


async def time_async(label: str, iterations: int, fn):
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        await fn()
        times.append(time.perf_counter() - start)
    print(
        f"{label:<34} avg={ms(statistics.mean(times)):>8}ms "
        f"p95={ms(percentile(times, 95)):>8}ms "
        f"min={ms(min(times)):>8}ms"
    )


def time_sync(label: str, iterations: int, fn):
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    print(
        f"{label:<34} avg={ms(statistics.mean(times)):>8}ms "
        f"p95={ms(percentile(times, 95)):>8}ms "
        f"min={ms(min(times)):>8}ms"
    )


def print_counts():
    conn = sqlite3.connect(db.DB_PATH)
    try:
        image_count = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
        comparison_count = conn.execute("SELECT COUNT(*) FROM comparisons").fetchone()[0]
        embedding_count = conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
        cache_count = conn.execute("SELECT COUNT(*) FROM cache_entries").fetchone()[0]
        print(
            f"Dataset: images={image_count:,} comparisons={comparison_count:,} "
            f"embeddings={embedding_count:,} cache_entries={cache_count:,}"
        )
    finally:
        conn.close()


def print_query_plan(name: str, sql: str, params: tuple = ()):
    conn = sqlite3.connect(db.DB_PATH)
    try:
        rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    finally:
        conn.close()
    plan = " | ".join(row[3] for row in rows)
    print(f"{name:<34} {plan}")


RANKING_SQL = {
    "elo": "elo DESC",
    "least_compared": "comparisons ASC",
    "filename": "filename ASC",
    "newest": "id DESC",
    "date_taken": "date_taken IS NULL ASC, date_taken DESC, id DESC",
    "date_modified": "file_modified_at IS NULL ASC, file_modified_at DESC, id DESC",
    "file_size": "file_size IS NULL ASC, file_size DESC, id DESC",
    "resolution": "(width * height) IS NULL ASC, (width * height) DESC, id DESC",
    "camera": "camera_make IS NULL ASC, camera_make ASC, camera_model ASC, id ASC",
}
RANKING_BENCH_INDEXES = {
    "elo": "idx_images_active_elo",
    "least_compared": "idx_images_active_comparisons_asc",
    "filename": "idx_images_active_filename",
    "newest": "idx_images_active_id",
    "date_taken": "idx_images_active_date_taken_sort_desc",
    "date_modified": "idx_images_active_modified_sort_desc",
    "file_size": "idx_images_active_file_size_sort_desc",
    "resolution": "idx_images_active_resolution_sort_desc",
    "camera": "idx_images_active_camera_sort_asc",
}


def fetch_rankings_sync(sort: str):
    order = RANKING_SQL[sort]
    conn = sqlite3.connect(db.DB_PATH)
    try:
        index_name = RANKING_BENCH_INDEXES[sort]
        return conn.execute(
            "SELECT id, filename, filepath, elo, comparisons, status, aspect_ratio "
            f"FROM images INDEXED BY {index_name} WHERE status IN ('kept', 'maybe') "
            "AND missing_at IS NULL "
            f"ORDER BY {order} LIMIT 100"
        ).fetchall()
    finally:
        conn.close()


def fetch_pairing_sync():
    conn = sqlite3.connect(db.DB_PATH)
    try:
        return conn.execute(
            "SELECT i.id, i.filename, i.filepath, i.elo, i.comparisons, i.orientation, i.aspect_ratio "
            "FROM images i INDEXED BY idx_images_active_elo "
            "JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1 "
            "AND i.status IN ('kept', 'maybe') AND i.missing_at IS NULL "
            "ORDER BY i.elo DESC"
        ).fetchall()
    finally:
        conn.close()


def fetch_matchups_sync():
    conn = sqlite3.connect(db.DB_PATH)
    try:
        return conn.execute("SELECT winner_id, loser_id FROM comparisons").fetchall()
    finally:
        conn.close()


def bench_db(iterations: int):
    print("\nDB/API helper timings")
    for sort in ("elo", "least_compared", "filename", "newest", "date_taken", "date_modified", "file_size", "resolution", "camera"):
        time_sync(
            f"get_rankings sort={sort}",
            iterations,
            lambda sort=sort: fetch_rankings_sync(sort),
        )
    time_sync("get_kept_images_for_pairing", max(1, iterations // 3), fetch_pairing_sync)
    time_sync("get_past_matchups", max(1, iterations // 3), fetch_matchups_sync)

    print("\nQuery plans")
    print_query_plan(
        "rankings elo",
        "SELECT id, filename, filepath, elo, comparisons, status, aspect_ratio "
        "FROM images INDEXED BY idx_images_active_elo "
        "WHERE status IN ('kept', 'maybe') AND missing_at IS NULL "
        "ORDER BY elo DESC LIMIT 100",
    )
    print_query_plan(
        "rankings date_taken",
        "SELECT id, filename, filepath, elo, comparisons, status, aspect_ratio "
        "FROM images INDEXED BY idx_images_active_date_taken_sort_desc "
        "WHERE status IN ('kept', 'maybe') AND missing_at IS NULL "
        "ORDER BY date_taken IS NULL ASC, date_taken DESC, id DESC LIMIT 100",
    )
    print_query_plan(
        "pregen md candidates",
        "SELECT i.id, i.source_id, i.filepath, i.file_size, i.file_modified_at "
        "FROM images i INDEXED BY idx_images_source_missing_filepath "
        "JOIN catalog_sources s ON s.id = i.source_id "
        "WHERE s.included = 1 AND s.online = 1 AND i.missing_at IS NULL "
        "AND (i.source_id > ? OR (i.source_id = ? AND (i.filepath > ? OR (i.filepath = ? AND i.id > ?)))) "
        "ORDER BY i.source_id ASC, i.filepath ASC, i.id ASC LIMIT 1024",
        (0, 0, "", "", 0),
    )
    print_query_plan(
        "embedding md-ready",
        "SELECT i.id, i.filepath FROM images i "
        "INDEXED BY idx_images_active_id "
        "WHERE i.status IN ('kept', 'maybe') AND i.missing_at IS NULL "
        "AND EXISTS ("
        "  SELECT 1 FROM cache_entries c "
        "  WHERE c.cache_root = ? AND c.size = 'md' AND c.image_id = i.id"
        ") "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM embeddings e WHERE e.image_id = i.id"
        ") "
        "ORDER BY i.id ASC LIMIT 64",
        (settings.get_settings()["ssd_cache_dir"],),
    )


def top_indices_desc(values, limit: int):
    if len(values) <= limit:
        return np.argsort(values)[::-1]
    candidates = np.argpartition(values, -limit)[-limit:]
    return candidates[np.argsort(values[candidates])[::-1]]


async def bench_embeddings(iterations: int):
    print("\nEmbedding matrix timings")
    start = time.perf_counter()
    image_ids, matrix = await embed_cache.get_matrix()
    cold = time.perf_counter() - start
    if image_ids is None or matrix is None:
        print("embedding matrix unavailable")
        return
    print(f"get_matrix cold-ish                 {ms(cold):>8}ms shape={matrix.shape}")

    await time_async("get_matrix warm", iterations, embed_cache.get_matrix)

    source = matrix[0]
    time_sync("similarity matmul", iterations, lambda: matrix @ source)
    scores = matrix @ source
    time_sync("top-k argpartition", iterations, lambda: top_indices_desc(scores, 50))
    time_sync("top-k full argsort", iterations, lambda: np.argsort(scores)[::-1][:50])


def cached_thumbnail_rows():
    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = {}
        for size in thumbnails.THUMB_TIERS:
            row = conn.execute(
                "SELECT c.size, c.image_id, c.source_signature, c.path, i.filepath "
                "FROM cache_entries c JOIN images i ON i.id = c.image_id "
                "WHERE c.cache_root = ? AND c.size = ? LIMIT 1",
                (thumbnails.SSD_CACHE_DIR, size),
            ).fetchone()
            if row:
                rows[size] = row
        return rows
    finally:
        conn.close()


def bench_thumbnails(iterations: int):
    print("\nThumbnail cache timings")
    thumbnails.configure(settings.load_settings())
    rows = cached_thumbnail_rows()
    if not rows:
        print("no cached thumbnail rows found")
        return

    for size, row in rows.items():
        image_id = int(row["image_id"])
        signature = row["source_signature"]
        time_sync(
            f"fast_disk_read_entry {size}",
            iterations,
            lambda size=size, image_id=image_id, signature=signature: thumbnails.fast_disk_read_entry(
                size,
                image_id,
                signature,
            ),
        )
        entry = thumbnails.fast_disk_read_entry(size, image_id, signature, populate_memory=True)
        if entry is not None:
            time_sync(
                f"memory_get_entry {size}",
                iterations * 20,
                lambda size=size, image_id=image_id: thumbnails._memory_get_entry_fast(size, image_id),
            )
        time_sync(
            f"has_cached {size}",
            iterations,
            lambda size=size, row=row: thumbnails.has_cached(size, row["filepath"], int(row["image_id"])),
        )


async def main():
    parser = argparse.ArgumentParser(description="Run local photoArchive performance smoke benchmarks.")
    parser.add_argument("--iterations", type=int, default=25)
    parser.add_argument("--skip-db", action="store_true")
    parser.add_argument("--skip-embeddings", action="store_true")
    parser.add_argument("--skip-thumbnails", action="store_true")
    args = parser.parse_args()

    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

    await db.init_db()
    print_counts()
    if not args.skip_db:
        bench_db(args.iterations)
    if not args.skip_embeddings:
        await bench_embeddings(max(3, args.iterations // 3))
    if not args.skip_thumbnails:
        bench_thumbnails(args.iterations)


if __name__ == "__main__":
    asyncio.run(main())
