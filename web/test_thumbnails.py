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

        thumbnails.SSD_CACHE_DIR = self.tempdir.name
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
        thumbnails.MEMORY_CACHE_BYTES = self.old_memory_bytes
        thumbnails._clear_memory_cache()
        thumbnails._clear_disk_index()
        thumbnails._tier_byte_totals.clear()
        thumbnails._source_stat_cache.clear()
        thumbnails._thumbnail_retry_after.clear()
        with thumbnails._write_queue_lock:
            thumbnails._write_queue.clear()
        thumbnails._clear_cache_metadata_lock_backoff()
        self.tempdir.cleanup()

    def _make_image(self) -> str:
        path = os.path.join(self.tempdir.name, "source.jpg")
        Image.new("RGB", (1200, 800), color=(120, 80, 40)).save(path, "JPEG", quality=90)
        os.utime(path, (time.time(), 1712345678.25))
        return path

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


if __name__ == "__main__":
    unittest.main()
