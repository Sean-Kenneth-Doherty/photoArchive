import asyncio
import os
import sqlite3
import sys
import tempfile
import time
import unittest

from PIL import Image

sys.path.insert(0, os.path.dirname(__file__))
import thumbnails  # noqa: E402


class ThumbnailBulkWarmupTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.old_cache_dir = thumbnails.SSD_CACHE_DIR
        self.old_allocations = dict(thumbnails._disk_allocations)
        self.old_get_source_bits = thumbnails._get_source_bits
        self.old_load_source_image = thumbnails._load_source_image
        self.old_memory_bytes = thumbnails.MEMORY_CACHE_BYTES
        self.old_db_connect = thumbnails._db_connect
        self.old_db_path = thumbnails.db.DB_PATH
        self.old_persistent_conn = thumbnails._persistent_conn
        self.old_prefetching = thumbnails._prefetching
        self.old_pregen_manual_mode = thumbnails._pregen_manual_mode
        self.old_pregen_manual_pause = thumbnails._pregen_manual_pause

        thumbnails.SSD_CACHE_DIR = self.tempdir.name
        thumbnails.db.DB_PATH = os.path.join(self.tempdir.name, "thumbnail-cache-test.db")
        thumbnails._persistent_conn = None
        thumbnails._disk_allocations.update({
            "sm": 64 * 1024 * 1024,
            "md": 64 * 1024 * 1024,
            "lg": 64 * 1024 * 1024,
            thumbnails.FULL_TIER: 0,
        })
        thumbnails.MEMORY_CACHE_BYTES = 0
        thumbnails._ensure_disk_cache_dirs()
        thumbnails._clear_memory_cache()
        thumbnails._clear_disk_index()
        thumbnails._tier_byte_totals.clear()
        thumbnails._source_stat_cache.clear()
        thumbnails._thumbnail_retry_after.clear()
        thumbnails._prefetching = True
        thumbnails._pregen_manual_mode = True
        thumbnails._pregen_manual_pause = False
        thumbnails._reset_pregen_bulk_cursor()
        thumbnails._reset_pregen_full_cursor()
        with thumbnails._write_queue_lock:
            thumbnails._write_queue.clear()
        with thumbnails._disk_index_lock:
            thumbnails._disk_index_built = True

    def tearDown(self):
        thumbnails.SSD_CACHE_DIR = self.old_cache_dir
        thumbnails._disk_allocations.clear()
        thumbnails._disk_allocations.update(self.old_allocations)
        thumbnails._get_source_bits = self.old_get_source_bits
        thumbnails._load_source_image = self.old_load_source_image
        thumbnails._db_connect = self.old_db_connect
        if thumbnails._persistent_conn is not None:
            thumbnails._persistent_conn.close()
        thumbnails._persistent_conn = self.old_persistent_conn
        thumbnails.db.DB_PATH = self.old_db_path
        thumbnails.MEMORY_CACHE_BYTES = self.old_memory_bytes
        thumbnails._prefetching = self.old_prefetching
        thumbnails._pregen_manual_mode = self.old_pregen_manual_mode
        thumbnails._pregen_manual_pause = self.old_pregen_manual_pause
        thumbnails._clear_memory_cache()
        thumbnails._clear_disk_index()
        thumbnails._tier_byte_totals.clear()
        thumbnails._source_stat_cache.clear()
        thumbnails._thumbnail_retry_after.clear()
        thumbnails._reset_pregen_bulk_cursor()
        thumbnails._reset_pregen_full_cursor()
        with thumbnails._write_queue_lock:
            thumbnails._write_queue.clear()
        thumbnails._clear_cache_metadata_lock_backoff()
        self.tempdir.cleanup()

    def _make_image(self) -> str:
        path = os.path.join(self.tempdir.name, "source.jpg")
        Image.new("RGB", (1200, 800), color=(120, 80, 40)).save(path, "JPEG", quality=90)
        os.utime(path, (time.time(), 1712345678.25))
        return path

    def _make_original_file(self, name: str, size: int) -> str:
        path = os.path.join(self.tempdir.name, name)
        with open(path, "wb") as f:
            f.write(bytes([len(name) % 251]) * size)
        os.utime(path, (time.time(), 1712345678.25 + size))
        return path

    def _add_catalog_original(self, image_id: int, path: str):
        stat = os.stat(path)
        with sqlite3.connect(thumbnails.db.DB_PATH) as conn:
            conn.executescript(thumbnails.db.SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO catalog_sources "
                "(id, path, display_name, included, online) VALUES (1, ?, 'catalog', 1, 1)",
                (self.tempdir.name,),
            )
            conn.execute(
                "INSERT OR REPLACE INTO images "
                "(id, source_id, filename, filepath, status, file_size, file_modified_at, missing_at) "
                "VALUES (?, 1, ?, ?, 'kept', ?, ?, NULL)",
                (image_id, os.path.basename(path), path, int(stat.st_size), float(stat.st_mtime)),
            )

    def _cache_original_now(self, image_id: int, path: str):
        signature = thumbnails._build_source_signature(path, thumbnails.FULL_TIER, image_id)
        return thumbnails._cache_full_image_sync(path, image_id, signature, hot=False)

    def _catalog_signatures(self, path: str, image_id: int = 1) -> tuple[dict[str, str], int, float]:
        stat = os.stat(path)
        file_size = int(stat.st_size)
        file_modified_at = float(stat.st_mtime)
        signatures = {
            size: thumbnails._build_catalog_source_signature(
                path,
                size,
                image_id,
                file_size,
                file_modified_at,
            )[0]
            for size in thumbnails.THUMB_TIERS
        }
        return signatures, file_size, file_modified_at

    def test_catalog_signature_uses_metadata_before_stat_fallback(self):
        calls = []

        def fake_source_bits(filepath):
            calls.append(filepath)
            return f"321|987654321|{filepath}"

        thumbnails._get_source_bits = fake_source_bits
        path = os.path.join(self.tempdir.name, "photo.jpg")

        signature, source_size, missing = thumbnails._build_catalog_source_signature(
            path,
            "md",
            42,
            123,
            456.5,
        )
        self.assertEqual(source_size, 123)
        self.assertFalse(missing)
        self.assertEqual(calls, [])
        self.assertEqual(
            signature,
            thumbnails._build_source_signature_from_bits("catalog|123|456.500000000|" + path, "md", 42),
        )

        fallback_signature, fallback_size, fallback_missing = thumbnails._build_catalog_source_signature(
            path,
            "md",
            42,
            None,
            None,
        )
        self.assertEqual(calls, [path])
        self.assertEqual(fallback_size, 321)
        self.assertFalse(fallback_missing)
        self.assertEqual(
            fallback_signature,
            thumbnails._build_source_signature_from_bits(f"321|987654321|{path}", "md", 42),
        )

    def test_bulk_generation_writes_all_missing_tiers_from_one_source_load(self):
        path = self._make_image()
        signatures, file_size, _mtime = self._catalog_signatures(path)
        load_count = 0

        def counted_load(*args, **kwargs):
            nonlocal load_count
            load_count += 1
            return self.old_load_source_image(*args, **kwargs)

        thumbnails._load_source_image = counted_load
        metrics = thumbnails._generate_thumbnail_set_sync(
            path,
            1,
            signatures,
            source_bytes=file_size,
        )

        self.assertEqual(load_count, 1)
        self.assertEqual(metrics["source_reads"], 1)
        self.assertEqual(metrics["thumbnails_written"], 3)
        for size in thumbnails.THUMB_TIERS:
            self.assertTrue(os.path.exists(thumbnails._thumbnail_disk_path(size, 1)))

    def test_bulk_generation_marks_missing_source_without_retry(self):
        image_id = 5
        missing_path = os.path.join(self.tempdir.name, "missing.jpg")
        with sqlite3.connect(thumbnails.db.DB_PATH) as conn:
            conn.execute("CREATE TABLE images (id INTEGER PRIMARY KEY, missing_at REAL DEFAULT NULL)")
            conn.execute("INSERT INTO images(id, missing_at) VALUES (?, NULL)", (image_id,))

        signatures = {
            "md": thumbnails._build_source_signature_from_bits(
                f"catalog|123|456.000000000|{missing_path}",
                "md",
                image_id,
            )
        }
        metrics = thumbnails._generate_thumbnail_set_sync(
            missing_path,
            image_id,
            signatures,
            source_bytes=123,
        )

        self.assertEqual(metrics["source_read_failures"], 1)
        with sqlite3.connect(thumbnails.db.DB_PATH) as conn:
            row = conn.execute("SELECT missing_at FROM images WHERE id = ?", (image_id,)).fetchone()
        self.assertIsNotNone(row[0])
        self.assertEqual(thumbnails._thumbnail_retry_after, {})

    def test_bulk_candidate_skips_already_cached_tiers(self):
        path = self._make_image()
        signatures, file_size, file_modified_at = self._catalog_signatures(path)
        thumbnails._generate_thumbnail_set_sync(path, 1, signatures, source_bytes=file_size)

        needed, _source_size = thumbnails._bulk_candidate_signatures(
            {
                "id": 1,
                "filepath": path,
                "file_size": file_size,
                "file_modified_at": file_modified_at,
            },
            {size: 64 * 1024 * 1024 for size in thumbnails.THUMB_TIERS},
            {size: 64 * 1024 * 1024 for size in thumbnails.THUMB_TIERS},
        )
        self.assertEqual(needed, {})

    def test_bulk_candidate_respects_lg_budget_room(self):
        path = self._make_image()
        stat = os.stat(path)
        row = {
            "id": 2,
            "filepath": path,
            "file_size": int(stat.st_size),
            "file_modified_at": float(stat.st_mtime),
        }
        needed, _source_size = thumbnails._bulk_candidate_signatures(
            row,
            {
                "sm": thumbnails.estimated_tier_bytes("sm"),
                "md": thumbnails.estimated_tier_bytes("md"),
                "lg": thumbnails.estimated_tier_bytes("lg") - 1,
            },
            {size: 64 * 1024 * 1024 for size in thumbnails.THUMB_TIERS},
        )
        self.assertIn("sm", needed)
        self.assertIn("md", needed)
        self.assertNotIn("lg", needed)

    def test_full_warmup_copies_until_budget_room_is_used_and_skips_cached(self):
        thumbnails._disk_allocations[thumbnails.FULL_TIER] = 70
        first = self._make_original_file("first.jpg", 30)
        second = self._make_original_file("second.jpg", 40)
        third = self._make_original_file("third.jpg", 40)
        for image_id, path in ((1, first), (2, second), (3, third)):
            self._add_catalog_original(image_id, path)

        self.assertNotEqual(self._cache_original_now(1, first), first)

        warmed = asyncio.run(thumbnails._run_full_warm_batch(generate_batch=10))

        self.assertEqual(warmed, 1)
        self.assertTrue(thumbnails.has_cached(thumbnails.FULL_TIER, first, 1))
        self.assertTrue(thumbnails.has_cached(thumbnails.FULL_TIER, second, 2))
        self.assertFalse(thumbnails.has_cached(thumbnails.FULL_TIER, third, 3))

    def test_full_warmup_does_not_churn_when_full_tier_is_full(self):
        thumbnails._disk_allocations[thumbnails.FULL_TIER] = 80
        first = self._make_original_file("one.jpg", 40)
        second = self._make_original_file("two.jpg", 40)
        third = self._make_original_file("three.jpg", 10)
        for image_id, path in ((1, first), (2, second), (3, third)):
            self._add_catalog_original(image_id, path)

        self.assertNotEqual(self._cache_original_now(1, first), first)
        self.assertNotEqual(self._cache_original_now(2, second), second)

        warmed = asyncio.run(thumbnails._run_full_warm_batch(generate_batch=10))

        self.assertEqual(warmed, 0)
        self.assertTrue(thumbnails.has_cached(thumbnails.FULL_TIER, first, 1))
        self.assertTrue(thumbnails.has_cached(thumbnails.FULL_TIER, second, 2))
        self.assertFalse(thumbnails.has_cached(thumbnails.FULL_TIER, third, 3))

    def test_full_warmup_skips_oversized_originals(self):
        thumbnails._disk_allocations[thumbnails.FULL_TIER] = 32
        path = self._make_original_file("oversized.jpg", 64)
        self._add_catalog_original(1, path)

        warmed = asyncio.run(thumbnails._run_full_warm_batch(generate_batch=10))

        self.assertEqual(warmed, 0)
        self.assertFalse(thumbnails.has_cached(thumbnails.FULL_TIER, path, 1))

    def test_touch_cached_signature_ignores_sqlite_lock(self):
        class LockedConn:
            def execute(self, *_args, **_kwargs):
                raise sqlite3.OperationalError("database is locked")

            def rollback(self):
                pass

        thumbnails._db_connect = lambda: LockedConn()

        self.assertFalse(thumbnails.touch_cached_signature("sm", 1, "sig"))

    def test_touch_cached_signature_ignores_sqlite_connect_lock(self):
        def locked_connect():
            raise sqlite3.OperationalError("database is locked")

        thumbnails._db_connect = locked_connect

        self.assertFalse(thumbnails.touch_cached_signature("sm", 1, "sig"))

    def test_flush_write_queue_requeues_after_sqlite_lock(self):
        class LockedConn:
            def execute(self, *_args, **_kwargs):
                raise sqlite3.OperationalError("database is locked")

            def rollback(self):
                pass

        path = os.path.join(self.tempdir.name, "sm", "1.jpg")
        with thumbnails._write_queue_lock:
            thumbnails._write_queue.append(("sm", 1, "sig", path, 123, time.time()))
        thumbnails._db_connect = lambda: LockedConn()

        self.assertFalse(thumbnails._flush_write_queue())
        with thumbnails._write_queue_lock:
            self.assertEqual(len(thumbnails._write_queue), 1)

    def test_flush_write_queue_requeues_after_sqlite_connect_lock(self):
        path = os.path.join(self.tempdir.name, "sm", "1.jpg")
        with thumbnails._write_queue_lock:
            thumbnails._write_queue.append(("sm", 1, "sig", path, 123, time.time()))

        def locked_connect():
            raise sqlite3.OperationalError("database is locked")

        thumbnails._db_connect = locked_connect

        self.assertFalse(thumbnails._flush_write_queue())
        with thumbnails._write_queue_lock:
            self.assertEqual(len(thumbnails._write_queue), 1)

    def test_get_thumbnail_returns_generated_bytes_when_caches_are_disabled(self):
        path = self._make_image()
        thumbnails._disk_allocations.update({tier: 0 for tier in thumbnails.ALL_TIERS})
        thumbnails.MEMORY_CACHE_BYTES = 0

        data = asyncio.run(thumbnails.get_thumbnail(path, "md", 7))

        self.assertGreater(len(data), 100)
        self.assertFalse(os.path.exists(thumbnails._thumbnail_disk_path("md", 7)))
        self.assertIsNone(
            thumbnails._memory_get("md", 7, thumbnails._build_source_signature(path, "md", 7))
        )

    def test_clear_cache_refuses_unmarked_directory_with_non_cache_files(self):
        user_file = os.path.join(self.tempdir.name, "keep.txt")
        with open(user_file, "w", encoding="utf-8") as f:
            f.write("not cache data")
        marker = thumbnails._cache_marker_path()
        if os.path.exists(marker):
            os.remove(marker)

        result = thumbnails.clear_cache()

        self.assertTrue(result["refused"])
        self.assertTrue(os.path.exists(user_file))

    def test_clear_cache_allows_legacy_cache_layout_and_writes_marker(self):
        marker = thumbnails._cache_marker_path()
        if os.path.exists(marker):
            os.remove(marker)
        path = os.path.join(self.tempdir.name, "sm", "1.jpg")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(b"cache")

        result = thumbnails.clear_cache()

        self.assertNotIn("refused", result)
        self.assertFalse(os.path.exists(path))
        self.assertTrue(os.path.exists(marker))


if __name__ == "__main__":
    unittest.main()
