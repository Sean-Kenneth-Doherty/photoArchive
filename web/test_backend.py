import os
import sqlite3
import sys
import tempfile
import unittest

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
import app as app_module  # noqa: E402
import db  # noqa: E402
import embedding_worker  # noqa: E402
import elo_propagation  # noqa: E402
import scanner  # noqa: E402


class JsonRequest:
    def __init__(self, payload):
        self.payload = payload

    async def json(self):
        return self.payload


class BadJsonRequest:
    async def json(self):
        raise ValueError("bad json")


class BackendRankingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.old_db_path = db.DB_PATH
        self.old_schedule_pairing_propagation = app_module._schedule_pairing_propagation
        self.old_get_matrix = elo_propagation.embed_cache.get_matrix
        self.old_get_index = elo_propagation.embed_cache.get_index
        self.old_get_vector = elo_propagation.embed_cache.get_vector
        self.old_encode_text = embedding_worker.encode_text
        self.old_prefetch_images = app_module.thumbnails.prefetch_images
        self.old_schedule_full_image_cache = app_module.thumbnails.schedule_full_image_cache
        self.old_has_cached_fast = app_module.thumbnails.has_cached_fast
        self.old_fast_disk_path_entry = app_module.thumbnails.fast_disk_path_entry
        self.old_settings_path = app_module.settings.SETTINGS_PATH
        self.old_settings_state = app_module.settings._settings

        db.DB_PATH = os.path.join(self.tempdir.name, "photoarchive-test.db")
        app_module.settings.SETTINGS_PATH = os.path.join(self.tempdir.name, "settings.local.json")
        app_module.settings._settings = None
        db.invalidate_stats_cache()
        db.invalidate_cached_image_ids_cache()
        await db.init_db()
        app_module._pairing_cache.update({"data": None, "by_id": None, "valid": False})
        app_module._matchups_cache.update({"data": None, "valid": False})

        def close_scheduled(coro):
            coro.close()

        async def noop_prefetch(*_args, **_kwargs):
            return 0

        app_module._schedule_pairing_propagation = close_scheduled
        app_module.thumbnails.prefetch_images = noop_prefetch

    async def asyncTearDown(self):
        app_module._schedule_pairing_propagation = self.old_schedule_pairing_propagation
        elo_propagation.embed_cache.get_matrix = self.old_get_matrix
        elo_propagation.embed_cache.get_index = self.old_get_index
        elo_propagation.embed_cache.get_vector = self.old_get_vector
        embedding_worker.encode_text = self.old_encode_text
        app_module.thumbnails.prefetch_images = self.old_prefetch_images
        app_module.thumbnails.schedule_full_image_cache = self.old_schedule_full_image_cache
        app_module.thumbnails.has_cached_fast = self.old_has_cached_fast
        app_module.thumbnails.fast_disk_path_entry = self.old_fast_disk_path_entry
        app_module.settings.SETTINGS_PATH = self.old_settings_path
        app_module.settings._settings = self.old_settings_state
        db.DB_PATH = self.old_db_path
        db.invalidate_stats_cache()
        db.invalidate_cached_image_ids_cache()
        self.tempdir.cleanup()

    async def _source(self, name="catalog", *, online=True):
        path = os.path.join(self.tempdir.name, name)
        os.makedirs(path, exist_ok=True)
        source = await db.add_or_restore_source(path)
        if not online:
            conn = await db.get_db()
            try:
                await conn.execute("UPDATE catalog_sources SET online = 0 WHERE id = ?", (source["id"],))
                await conn.commit()
            finally:
                await conn.close()
            db.invalidate_stats_cache()
        return source

    async def _image(
        self,
        source_id,
        filename,
        *,
        elo=1200.0,
        comparisons=0,
        propagated_updates=0,
        missing_at=None,
    ):
        filepath = os.path.join(self.tempdir.name, f"{source_id}-{filename}")
        conn = await db.get_db()
        try:
            cursor = await conn.execute(
                "INSERT INTO images "
                "(source_id, filename, filepath, elo, comparisons, propagated_updates, status, missing_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'kept', ?)",
                (source_id, filename, filepath, elo, comparisons, propagated_updates, missing_at),
            )
            await conn.commit()
            image_id = cursor.lastrowid
        finally:
            await conn.close()
        db.invalidate_stats_cache()
        return image_id

    async def _image_row(self, image_id):
        conn = await db.get_db()
        try:
            cursor = await conn.execute("SELECT * FROM images WHERE id = ?", (image_id,))
            return dict(await cursor.fetchone())
        finally:
            await conn.close()

    async def _cache_entry(self, image_id, size="sm"):
        conn = await db.get_db()
        try:
            now = 12345.0 + int(image_id)
            await conn.execute(
                "INSERT OR REPLACE INTO cache_entries "
                "(cache_root, size, image_id, path, source_signature, size_bytes, last_accessed, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    app_module.thumbnails.SSD_CACHE_DIR,
                    size,
                    image_id,
                    os.path.join(self.tempdir.name, f"{size}-{image_id}.jpg"),
                    f"sig-{size}-{image_id}",
                    123,
                    now,
                    now,
                ),
            )
            await conn.commit()
        finally:
            await conn.close()

    def _stub_text_search(self, image_ids, scores):
        matrix = np.array([[score, 0.0] for score in scores], dtype=np.float32)

        async def fake_get_matrix():
            return image_ids, matrix

        def fake_encode_text(_query):
            return np.array([1.0, 0.0], dtype=np.float32)

        elo_propagation.embed_cache.get_matrix = fake_get_matrix
        embedding_worker.encode_text = fake_encode_text

    async def test_compare_payload_validation_rejects_invalid_and_inactive_images(self):
        source = await self._source()
        a = await self._image(source["id"], "a.jpg")
        b = await self._image(source["id"], "b.jpg")
        offline_source = await self._source("offline", online=False)
        offline = await self._image(offline_source["id"], "offline.jpg")

        response = await app_module.submit_comparison(BadJsonRequest())
        self.assertEqual(response.status_code, 400)

        response = await app_module.mosaic_pick(JsonRequest([]))
        self.assertEqual(response.status_code, 400)

        response = await app_module.submit_comparison(JsonRequest({"winner_id": a, "loser_id": a}))
        self.assertEqual(response.status_code, 400)

        response = await app_module.submit_comparison(JsonRequest({"winner_id": a, "loser_id": offline}))
        self.assertEqual(response.status_code, 400)

        response = await app_module.mosaic_pick(JsonRequest({"winner_id": a, "loser_ids": [b, b]}))
        self.assertEqual(response.status_code, 400)

        response = await app_module.mosaic_pick(JsonRequest({"winner_id": a, "loser_ids": [a]}))
        self.assertEqual(response.status_code, 400)

        response = await app_module.mosaic_pick(JsonRequest({"winner_id": a, "loser_ids": [999999]}))
        self.assertEqual(response.status_code, 400)

    async def test_mosaic_pick_undo_reverts_whole_action(self):
        source = await self._source()
        winner = await self._image(source["id"], "winner.jpg")
        losers = [
            await self._image(source["id"], "loser1.jpg"),
            await self._image(source["id"], "loser2.jpg"),
            await self._image(source["id"], "loser3.jpg"),
        ]

        result = await app_module.mosaic_pick(JsonRequest({"winner_id": winner, "loser_ids": losers}))
        self.assertTrue(result["ok"])
        self.assertEqual(result["pairs_recorded"], 3)
        self.assertTrue(result["action_id"])

        conn = await db.get_db()
        try:
            cursor = await conn.execute("SELECT COUNT(*) AS c, COUNT(DISTINCT action_id) AS actions FROM comparisons")
            counts = await cursor.fetchone()
        finally:
            await conn.close()
        self.assertEqual(counts["c"], 3)
        self.assertEqual(counts["actions"], 1)

        undo = await app_module.compare_undo()
        self.assertTrue(undo["ok"])
        self.assertEqual(undo["comparisons_undone"], 3)

        conn = await db.get_db()
        try:
            cursor = await conn.execute("SELECT COUNT(*) AS c FROM comparisons")
            remaining = await cursor.fetchone()
        finally:
            await conn.close()
        self.assertEqual(remaining["c"], 0)

        for image_id in [winner] + losers:
            row = await self._image_row(image_id)
            self.assertAlmostEqual(row["elo"], 1200.0)
            self.assertEqual(row["comparisons"], 0)

    async def test_propagation_updates_separate_counter(self):
        source = await self._source()
        winner = await self._image(source["id"], "winner.jpg")
        loser = await self._image(source["id"], "loser.jpg")
        neighbor = await self._image(source["id"], "neighbor.jpg")

        image_ids = [winner, loser, neighbor]
        matrix = np.array(
            [
                [1.0, 0.0],
                [-1.0, 0.0],
                [0.995, 0.1],
            ],
            dtype=np.float32,
        )
        matrix /= np.linalg.norm(matrix, axis=1, keepdims=True)

        async def fake_get_matrix():
            return image_ids, matrix

        def fake_get_index():
            return {image_id: idx for idx, image_id in enumerate(image_ids)}

        elo_propagation.embed_cache.get_matrix = fake_get_matrix
        elo_propagation.embed_cache.get_index = fake_get_index

        await elo_propagation.propagate_comparison(winner, loser, k=20.0)

        row = await self._image_row(neighbor)
        self.assertEqual(row["comparisons"], 0)
        self.assertEqual(row["propagated_updates"], 1)
        self.assertGreater(row["elo"], 1200.0)

    async def test_undo_reverts_action_scoped_propagation_updates(self):
        source = await self._source()
        winner = await self._image(source["id"], "winner.jpg")
        loser = await self._image(source["id"], "loser.jpg")
        neighbor = await self._image(source["id"], "neighbor.jpg")
        action_id = "undo-propagation-test"

        image_ids = [winner, loser, neighbor]
        matrix = np.array(
            [
                [1.0, 0.0],
                [-1.0, 0.0],
                [0.995, 0.1],
            ],
            dtype=np.float32,
        )
        matrix /= np.linalg.norm(matrix, axis=1, keepdims=True)

        async def fake_get_matrix():
            return image_ids, matrix

        def fake_get_index():
            return {image_id: idx for idx, image_id in enumerate(image_ids)}

        elo_propagation.embed_cache.get_matrix = fake_get_matrix
        elo_propagation.embed_cache.get_index = fake_get_index

        await db.record_comparison(
            winner,
            loser,
            "swiss",
            1200.0,
            1200.0,
            1210.0,
            1190.0,
            action_id=action_id,
        )
        await elo_propagation.propagate_comparison(winner, loser, k=20.0, action_id=action_id)

        propagated = await self._image_row(neighbor)
        self.assertEqual(propagated["propagated_updates"], 1)
        self.assertGreater(propagated["elo"], 1200.0)

        undo = await app_module.compare_undo()
        self.assertTrue(undo["ok"])
        self.assertEqual(undo["comparisons_undone"], 1)
        self.assertEqual(undo["propagations_undone"], 1)

        restored_neighbor = await self._image_row(neighbor)
        self.assertAlmostEqual(restored_neighbor["elo"], 1200.0)
        self.assertEqual(restored_neighbor["propagated_updates"], 0)

        restored_winner = await self._image_row(winner)
        restored_loser = await self._image_row(loser)
        self.assertAlmostEqual(restored_winner["elo"], 1200.0)
        self.assertAlmostEqual(restored_loser["elo"], 1200.0)
        self.assertEqual(restored_winner["comparisons"], 0)
        self.assertEqual(restored_loser["comparisons"], 0)

        conn = await db.get_db()
        try:
            cursor = await conn.execute("SELECT COUNT(*) AS c FROM propagation_updates")
            remaining = await cursor.fetchone()
        finally:
            await conn.close()
        self.assertEqual(remaining["c"], 0)

    async def test_stats_preserve_imported_ranking_without_history(self):
        source = await self._source()
        await self._image(source["id"], "imported-a.jpg", elo=1300.0, comparisons=5)
        await self._image(source["id"], "imported-b.jpg", elo=1190.0, comparisons=2, propagated_updates=1)
        await self._image(source["id"], "unranked.jpg")

        stats = await db.get_stats()

        self.assertEqual(stats["direct_comparison_rows"], 0)
        self.assertEqual(stats["rated_images"], 2)
        self.assertEqual(stats["imported_ranking_without_history"], 7)
        self.assertEqual(stats["propagated_update_count"], 1)
        self.assertEqual(stats["ranking_signal_count"], 8)
        self.assertEqual(stats["total_comparisons"], 8)

    async def test_init_db_migrates_legacy_comparison_action_id_before_indexes(self):
        original_path = db.DB_PATH
        legacy_path = os.path.join(self.tempdir.name, "legacy-comparisons.db")
        conn = sqlite3.connect(legacy_path)
        try:
            conn.execute(
                "CREATE TABLE comparisons ("
                "id INTEGER PRIMARY KEY, "
                "winner_id INTEGER, "
                "loser_id INTEGER, "
                "mode TEXT, "
                "elo_before_winner REAL, "
                "elo_before_loser REAL, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                ")"
            )
            conn.commit()
        finally:
            conn.close()

        db.DB_PATH = legacy_path
        db.invalidate_stats_cache()
        try:
            await db.init_db()
            conn = sqlite3.connect(legacy_path)
            try:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(comparisons)")}
                indexes = {row[1] for row in conn.execute("PRAGMA index_list(comparisons)")}
            finally:
                conn.close()
        finally:
            db.DB_PATH = original_path
            db.invalidate_stats_cache()

        self.assertIn("action_id", columns)
        self.assertIn("idx_comparisons_action_id", indexes)

    async def test_rankings_query_plan_uses_active_sort_indexes(self):
        source = await self._source()
        first = await self._image(source["id"], "a.jpg", elo=1500)
        second = await self._image(source["id"], "b.jpg", elo=1300)
        conn = await db.get_db()
        try:
            await conn.execute(
                "UPDATE images SET date_taken = ?, file_modified_at = ?, file_size = ?, "
                "width = ?, height = ?, camera_make = ?, camera_model = ? WHERE id = ?",
                ("2024-01-02 03:04:05", 1700000000.0, 200, 4000, 3000, "Fuji", "X-T5", first),
            )
            await conn.execute(
                "UPDATE images SET date_taken = ?, file_modified_at = ?, file_size = ?, "
                "width = ?, height = ?, camera_make = ?, camera_model = ? WHERE id = ?",
                ("2023-01-02 03:04:05", 1600000000.0, 100, 2000, 1000, "Canon", "R5", second),
            )
            await conn.commit()
        finally:
            await conn.close()

        expected = {
            "elo": "idx_images_active_elo",
            "date_taken": "idx_images_active_date_taken_sort_desc",
            "date_modified": "idx_images_active_modified_sort_desc",
            "file_size": "idx_images_active_file_size_sort_desc",
            "resolution": "idx_images_active_resolution_sort_desc",
            "camera": "idx_images_active_camera_sort_asc",
        }
        raw = sqlite3.connect(db.DB_PATH)
        try:
            for sort, index_name in expected.items():
                rows = await db.get_rankings(limit=10, sort=sort)
                self.assertTrue(rows)
                conditions, params = db._ranking_filter_parts()
                image_source = db._ranking_image_source(sort, id_filter=None, text_query="")
                sql = (
                    f"SELECT i.id FROM {image_source} "
                    "JOIN catalog_sources s ON s.id = i.source_id "
                    f"WHERE {' AND '.join(conditions)} "
                    f"ORDER BY {db.RANKING_SORTS[sort]} LIMIT 10"
                )
                plan_rows = raw.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
                plan = " | ".join(row[3] for row in plan_rows)
                self.assertIn(index_name, plan)
                self.assertNotIn("USE TEMP B-TREE", plan)
        finally:
            raw.close()

    async def test_rankings_returns_only_sm_cached_images_with_visible_total_counts(self):
        source = await self._source()
        visible_high = await self._image(source["id"], "visible-high.jpg", elo=1500)
        hidden = await self._image(source["id"], "hidden.jpg", elo=1400)
        visible_low = await self._image(source["id"], "visible-low.jpg", elo=1300)
        await self._cache_entry(visible_high, "sm")
        await self._cache_entry(visible_low, "sm")

        result = await app_module.api_rankings(limit=10, sort="elo")

        self.assertEqual([img["id"] for img in result["images"]], [visible_high, visible_low])
        self.assertEqual(result["visible_images"], 2)
        self.assertEqual(result["total_images"], 3)
        self.assertEqual(result["hidden_pending_thumbnails"], 1)
        self.assertNotIn(hidden, [img["id"] for img in result["images"]])

    async def test_visible_ranking_count_uses_short_ttl_cache_and_invalidation(self):
        source = await self._source()
        first = await self._image(source["id"], "first.jpg")
        second = await self._image(source["id"], "second.jpg")
        await self._cache_entry(first, "sm")

        count = await db.count_rankings(
            visible_thumb_size="sm",
            cache_root=app_module.thumbnails.SSD_CACHE_DIR,
        )
        self.assertEqual(count, 1)

        await self._cache_entry(second, "sm")
        stale_count = await db.count_rankings(
            visible_thumb_size="sm",
            cache_root=app_module.thumbnails.SSD_CACHE_DIR,
        )
        self.assertEqual(stale_count, 1)

        db.invalidate_cached_image_ids_cache()
        refreshed_count = await db.count_rankings(
            visible_thumb_size="sm",
            cache_root=app_module.thumbnails.SSD_CACHE_DIR,
        )
        self.assertEqual(refreshed_count, 2)

    async def test_rescan_marks_missing_files_and_restores_seen_files(self):
        source = await self._source("scan-source")
        first_path = os.path.join(source["path"], "first.jpg")
        second_path = os.path.join(source["path"], "second.jpg")
        for path in (first_path, second_path):
            with open(path, "wb") as f:
                f.write(b"not-a-real-jpeg")

        await scanner.scan_folder(source["path"], source_id=source["id"])
        stats = await db.get_stats()
        self.assertEqual(stats["total_images"], 2)

        os.remove(second_path)
        await scanner.scan_folder(source["path"], source_id=source["id"])

        rows = await db.get_rankings(limit=10)
        self.assertEqual([row["filename"] for row in rows], ["first.jpg"])
        conn = await db.get_db()
        try:
            cursor = await conn.execute(
                "SELECT missing_at FROM images WHERE filepath = ?",
                (second_path,),
            )
            missing = await cursor.fetchone()
        finally:
            await conn.close()
        self.assertIsNotNone(missing["missing_at"])
        stats = await db.get_stats()
        self.assertEqual(stats["total_images"], 1)
        self.assertEqual(stats["total_catalog_images"], 2)

        with open(second_path, "wb") as f:
            f.write(b"back")
        await scanner.scan_folder(source["path"], source_id=source["id"])

        rows = await db.get_rankings(limit=10, sort="filename")
        self.assertEqual([row["filename"] for row in rows], ["first.jpg", "second.jpg"])
        restored = await self._image_row(rows[1]["id"])
        self.assertIsNone(restored["missing_at"])

    async def test_missing_images_are_excluded_from_active_views_and_workers(self):
        source = await self._source()
        active_a = await self._image(source["id"], "active-a.jpg", elo=1500)
        missing = await self._image(source["id"], "missing.jpg", elo=1400, missing_at=12345.0)
        active_b = await self._image(source["id"], "active-b.jpg", elo=1300)
        await self._cache_entry(active_a, "sm")
        await self._cache_entry(missing, "sm")
        await self._cache_entry(active_b, "sm")
        await self._cache_entry(active_a, "md")
        await self._cache_entry(missing, "md")
        await self._cache_entry(active_b, "md")

        rankings = await app_module.api_rankings(limit=10, sort="elo")
        self.assertEqual([img["id"] for img in rankings["images"]], [active_a, active_b])
        self.assertEqual(rankings["visible_images"], 2)
        self.assertEqual(rankings["total_images"], 2)

        mosaic = await app_module.mosaic_next(n=3, strategy="diverse")
        self.assertNotIn(missing, [img["id"] for img in mosaic["images"]])
        self.assertEqual(mosaic["total_images"], 2)

        compare = await app_module.compare_next(n=2, mode="swiss")
        pair_ids = {
            image["id"]
            for pair in compare["pairs"]
            for image in (pair["left"], pair["right"])
        }
        self.assertEqual(pair_ids, {active_a, active_b})

        self.assertNotIn(missing, await db.get_active_images_by_ids([active_a, missing, active_b]))
        self.assertEqual(len(await db.get_unembedded_images(limit=10)), 2)
        stats = await db.get_stats()
        self.assertEqual(stats["total_images"], 2)
        self.assertEqual(stats["total_catalog_images"], 3)

    async def test_mosaic_excludes_images_without_sm_but_reports_filtered_total(self):
        source = await self._source()
        first = await self._image(source["id"], "first.jpg", elo=1500)
        hidden = await self._image(source["id"], "hidden.jpg", elo=1400)
        second = await self._image(source["id"], "second.jpg", elo=1300)
        await self._cache_entry(first, "sm")
        await self._cache_entry(second, "sm")

        result = await app_module.mosaic_next(n=3, strategy="diverse")
        ids = [img["id"] for img in result["images"]]

        self.assertEqual(ids, [first, second])
        self.assertNotIn(hidden, ids)
        self.assertEqual(result["visible_images"], 2)
        self.assertEqual(result["total_images"], 3)
        self.assertEqual(result["hidden_pending_thumbnails"], 1)
        self.assertEqual(result["stats"]["filtered_pool_visible"], 2)
        self.assertEqual(result["stats"]["filtered_pool_total"], 3)

    async def test_compare_next_excludes_images_without_md_thumbnails(self):
        source = await self._source()
        visible_a = await self._image(source["id"], "visible-a.jpg", elo=1500)
        hidden_a = await self._image(source["id"], "hidden-a.jpg", elo=1450)
        visible_b = await self._image(source["id"], "visible-b.jpg", elo=1400)
        hidden_b = await self._image(source["id"], "hidden-b.jpg", elo=1350)
        await self._cache_entry(visible_a, "md")
        await self._cache_entry(visible_b, "md")

        result = await app_module.compare_next(n=2, mode="swiss")
        pair_ids = {
            image["id"]
            for pair in result["pairs"]
            for image in (pair["left"], pair["right"])
        }

        self.assertEqual(pair_ids, {visible_a, visible_b})
        self.assertNotIn(hidden_a, pair_ids)
        self.assertNotIn(hidden_b, pair_ids)
        self.assertEqual(result["visible_images"], 2)
        self.assertEqual(result["total_images"], 4)
        self.assertEqual(result["hidden_pending_thumbnails"], 2)

    async def test_search_skips_uncached_sm_results_and_fills_later_visible_matches(self):
        source = await self._source()
        hidden_best = await self._image(source["id"], "hidden-best.jpg")
        visible_first = await self._image(source["id"], "visible-first.jpg")
        hidden_next = await self._image(source["id"], "hidden-next.jpg")
        visible_second = await self._image(source["id"], "visible-second.jpg")
        await self._cache_entry(visible_first, "sm")
        await self._cache_entry(visible_second, "sm")

        image_ids = [hidden_best, visible_first, hidden_next, visible_second]
        matrix = np.array(
            [
                [0.99, 0.01],
                [0.90, 0.10],
                [0.80, 0.20],
                [0.70, 0.30],
            ],
            dtype=np.float32,
        )

        async def fake_get_matrix():
            return image_ids, matrix

        def fake_encode_text(_query):
            return np.array([1.0, 0.0], dtype=np.float32)

        elo_propagation.embed_cache.get_matrix = fake_get_matrix
        embedding_worker.encode_text = fake_encode_text

        result = await app_module.api_search(q="sunset", limit=2)

        self.assertEqual([img["id"] for img in result["images"]], [visible_first, visible_second])
        self.assertEqual(result["visible_images"], 2)
        self.assertEqual(result["total_images"], 4)
        self.assertEqual(result["hidden_pending_thumbnails"], 2)

    async def test_rankings_search_uses_metadata_fallback_when_ai_is_cold(self):
        source = await self._source()
        visible_match = await self._image(source["id"], "sunset-visible.jpg")
        hidden_match = await self._image(source["id"], "sunset-hidden.jpg")
        visible_miss = await self._image(source["id"], "portrait-visible.jpg")
        await self._cache_entry(visible_match, "sm")
        await self._cache_entry(visible_miss, "sm")

        embedding_worker.encode_text = lambda _query: None

        result = await app_module.api_rankings(q="sunset", sort="similarity", limit=10)

        self.assertEqual([img["id"] for img in result["images"]], [visible_match])
        self.assertEqual(result["search_mode"], "metadata")
        self.assertTrue(result["ai_unavailable"])
        self.assertEqual(result["visible_images"], 1)
        self.assertEqual(result["total_images"], 2)
        self.assertEqual(result["hidden_pending_thumbnails"], 1)
        self.assertNotIn(hidden_match, [img["id"] for img in result["images"]])

    async def test_rankings_search_similarity_defaults_but_other_sorts_keep_pool(self):
        source = await self._source()
        best_match = await self._image(source["id"], "landscape-best.jpg", elo=1200)
        rated_match = await self._image(source["id"], "landscape-rated.jpg", elo=1600)
        miss = await self._image(source["id"], "portrait-miss.jpg", elo=1800)
        for image_id in (best_match, rated_match, miss):
            await self._cache_entry(image_id, "sm")

        self._stub_text_search(
            [best_match, rated_match, miss],
            [0.92, 0.70, 0.10],
        )

        similarity = await app_module.api_rankings(q="landscapes", sort="similarity", limit=10)
        elo_sorted = await app_module.api_rankings(q="landscapes", sort="elo", limit=10)

        self.assertEqual([img["id"] for img in similarity["images"]], [best_match, rated_match])
        self.assertEqual([img["id"] for img in elo_sorted["images"]], [rated_match, best_match])
        self.assertEqual(similarity["total_images"], 2)
        self.assertEqual(elo_sorted["total_images"], 2)
        self.assertNotIn(miss, [img["id"] for img in elo_sorted["images"]])

    async def test_mosaic_next_search_filters_candidates_and_counts_visibility(self):
        source = await self._source()
        visible_a = await self._image(source["id"], "landscape-a.jpg")
        hidden_match = await self._image(source["id"], "landscape-hidden.jpg")
        visible_b = await self._image(source["id"], "landscape-b.jpg")
        miss = await self._image(source["id"], "portrait-miss.jpg")
        for image_id in (visible_a, visible_b, miss):
            await self._cache_entry(image_id, "sm")

        self._stub_text_search(
            [visible_a, hidden_match, visible_b, miss],
            [0.95, 0.90, 0.80, 0.10],
        )

        result = await app_module.mosaic_next(n=5, strategy="random", q="landscapes")
        ids = {img["id"] for img in result["images"]}

        self.assertEqual(ids, {visible_a, visible_b})
        self.assertEqual(result["visible_images"], 2)
        self.assertEqual(result["total_images"], 3)
        self.assertEqual(result["hidden_pending_thumbnails"], 1)
        self.assertEqual(result["stats"]["filtered_pool_visible"], 2)
        self.assertEqual(result["stats"]["filtered_pool_total"], 3)

    async def test_compare_next_search_only_pairs_matching_candidates(self):
        source = await self._source()
        match_a = await self._image(source["id"], "landscape-a.jpg", elo=1500)
        match_b = await self._image(source["id"], "landscape-b.jpg", elo=1400)
        miss = await self._image(source["id"], "portrait-miss.jpg", elo=1300)
        for image_id in (match_a, match_b, miss):
            await self._cache_entry(image_id, "md")

        self._stub_text_search(
            [match_a, match_b, miss],
            [0.95, 0.80, 0.10],
        )

        result = await app_module.compare_next(n=2, mode="swiss", q="landscapes")
        pair_ids = {
            image["id"]
            for pair in result["pairs"]
            for image in (pair["left"], pair["right"])
        }

        self.assertEqual(pair_ids, {match_a, match_b})
        self.assertNotIn(miss, pair_ids)
        self.assertEqual(result["visible_images"], 2)
        self.assertEqual(result["total_images"], 2)

    async def test_metadata_fallback_constrains_compare_and_mosaic_pools(self):
        source = await self._source()
        match_a = await self._image(source["id"], "sunset-a.jpg", elo=1500)
        match_b = await self._image(source["id"], "sunset-b.jpg", elo=1400)
        miss = await self._image(source["id"], "portrait-miss.jpg", elo=1300)
        for image_id in (match_a, match_b, miss):
            await self._cache_entry(image_id, "sm")
            await self._cache_entry(image_id, "md")

        embedding_worker.encode_text = lambda _query: None

        mosaic = await app_module.mosaic_next(n=5, strategy="random", q="sunset")
        compare = await app_module.compare_next(n=2, mode="swiss", q="sunset")
        mosaic_ids = {img["id"] for img in mosaic["images"]}
        compare_ids = {
            image["id"]
            for pair in compare["pairs"]
            for image in (pair["left"], pair["right"])
        }

        self.assertEqual(mosaic_ids, {match_a, match_b})
        self.assertEqual(compare_ids, {match_a, match_b})
        self.assertEqual(mosaic["search_mode"], "metadata")
        self.assertEqual(compare["search_mode"], "metadata")
        self.assertTrue(mosaic["ai_unavailable"])
        self.assertTrue(compare["ai_unavailable"])

    async def test_search_endpoint_uses_metadata_fallback_when_ai_is_cold(self):
        source = await self._source()
        visible_match = await self._image(source["id"], "sunset-visible.jpg")
        hidden_match = await self._image(source["id"], "sunset-hidden.jpg")
        await self._cache_entry(visible_match, "sm")

        embedding_worker.encode_text = lambda _query: None

        result = await app_module.api_search(q="sunset", limit=10)

        self.assertEqual([img["id"] for img in result["images"]], [visible_match])
        self.assertIsNone(result["images"][0]["similarity"])
        self.assertEqual(result["search_mode"], "metadata")
        self.assertTrue(result["ai_unavailable"])
        self.assertEqual(result["visible_images"], 1)
        self.assertEqual(result["total_images"], 2)
        self.assertEqual(result["hidden_pending_thumbnails"], 1)
        self.assertNotIn(hidden_match, [img["id"] for img in result["images"]])

    async def test_similar_skips_uncached_sm_results_and_fills_later_visible_matches(self):
        source = await self._source()
        source_image = await self._image(source["id"], "source.jpg")
        hidden_best = await self._image(source["id"], "hidden-best.jpg")
        visible_first = await self._image(source["id"], "visible-first.jpg")
        hidden_next = await self._image(source["id"], "hidden-next.jpg")
        visible_second = await self._image(source["id"], "visible-second.jpg")
        await self._cache_entry(visible_first, "sm")
        await self._cache_entry(visible_second, "sm")

        image_ids = [source_image, hidden_best, visible_first, hidden_next, visible_second]
        matrix = np.array(
            [
                [1.00, 0.00],
                [0.99, 0.01],
                [0.90, 0.10],
                [0.80, 0.20],
                [0.70, 0.30],
            ],
            dtype=np.float32,
        )

        async def fake_get_matrix():
            return image_ids, matrix

        def fake_get_index():
            return {image_id: idx for idx, image_id in enumerate(image_ids)}

        def fake_get_vector(image_id):
            return matrix[fake_get_index()[image_id]]

        elo_propagation.embed_cache.get_matrix = fake_get_matrix
        elo_propagation.embed_cache.get_index = fake_get_index
        elo_propagation.embed_cache.get_vector = fake_get_vector

        result = await app_module.api_similar(source_image, limit=2)

        self.assertEqual([img["id"] for img in result["images"]], [visible_first, visible_second])
        self.assertEqual(result["visible_images"], 2)
        self.assertEqual(result["total_images"], 4)
        self.assertEqual(result["hidden_pending_thumbnails"], 2)

    async def test_warm_images_deduplicates_ids_and_ignores_invalid_values(self):
        source = await self._source()
        first = await self._image(source["id"], "first.jpg")
        second = await self._image(source["id"], "second.jpg")
        prefetch_calls = []
        full_calls = []

        async def fake_prefetch(rows, tier, limit=None, hot=False):
            prefetch_calls.append({
                "tier": tier,
                "ids": [row["id"] for row in rows],
                "limit": limit,
                "hot": hot,
            })
            return len(rows)

        async def fake_schedule_full(filepath, image_id, *, hot=True):
            full_calls.append({"id": image_id, "hot": hot, "filepath": filepath})

        app_module.thumbnails.prefetch_images = fake_prefetch
        app_module.thumbnails.schedule_full_image_cache = fake_schedule_full

        result = await app_module.warm_images(JsonRequest({
            "tiers": {
                "md": [first, str(first), -1, "bad", second, 999999],
                "full": [first, first, second, "nope"],
                "bogus": [first],
            }
        }))

        self.assertEqual(result["images"], 3)
        self.assertEqual(result["scheduled"], {"md": 2, "full": 2})
        self.assertEqual(prefetch_calls, [{
            "tier": "md",
            "ids": [first, second],
            "limit": 2,
            "hot": True,
        }])
        self.assertEqual([call["id"] for call in full_calls], [first, second])
        self.assertTrue(all(call["hot"] for call in full_calls))

    async def test_warm_images_skips_already_cached_thumbnail_ids(self):
        source = await self._source()
        cached = await self._image(source["id"], "cached.jpg")
        uncached = await self._image(source["id"], "uncached.jpg")
        await self._cache_entry(cached, "md")
        prefetch_calls = []

        async def fake_prefetch(rows, tier, limit=None, hot=False):
            prefetch_calls.append({
                "tier": tier,
                "ids": [row["id"] for row in rows],
                "limit": limit,
                "hot": hot,
            })
            return len(rows)

        app_module.thumbnails.prefetch_images = fake_prefetch

        result = await app_module.warm_images(JsonRequest({"tiers": {"md": [cached, uncached]}}))

        self.assertEqual(result["scheduled"], {"md": 1})
        self.assertEqual(prefetch_calls, [{
            "tier": "md",
            "ids": [uncached],
            "limit": 1,
            "hot": True,
        }])

    async def test_warm_images_is_best_effort_when_thumbnail_cache_is_locked(self):
        source = await self._source()
        image_id = await self._image(source["id"], "locked.jpg")

        async def locked_prefetch(*_args, **_kwargs):
            raise sqlite3.OperationalError("database is locked")

        app_module.thumbnails.prefetch_images = locked_prefetch

        result = await app_module.warm_images(JsonRequest({"tiers": {"md": [image_id]}}))

        self.assertEqual(result, {"scheduled": {"md": 0}, "images": 1})

    async def test_warm_images_is_best_effort_when_full_cache_is_locked(self):
        source = await self._source()
        image_id = await self._image(source["id"], "locked-full.jpg")

        async def locked_full(*_args, **_kwargs):
            raise sqlite3.OperationalError("database is locked")

        app_module.thumbnails.schedule_full_image_cache = locked_full

        result = await app_module.warm_images(JsonRequest({"tiers": {"full": [image_id]}}))

        self.assertEqual(result, {"scheduled": {"full": 0}, "images": 1})

    async def test_media_status_reports_cached_tiers_without_image_lookup(self):
        def fake_has_cached_fast(size, image_id):
            return image_id == 42 and size == "md"

        def fake_fast_disk_path_entry(size, image_id):
            if image_id == 42 and size == app_module.thumbnails.FULL_TIER:
                return ("sig", "/tmp/full.jpg")
            return None

        app_module.thumbnails.has_cached_fast = fake_has_cached_fast
        app_module.thumbnails.fast_disk_path_entry = fake_fast_disk_path_entry

        result = await app_module.image_media_status(42)

        self.assertEqual(set(result["tiers"].keys()), {"sm", "md", "lg", "full"})
        self.assertTrue(result["tiers"]["md"]["cached"])
        self.assertTrue(result["tiers"]["full"]["cached"])
        self.assertEqual(result["best_cached"], "full")
        self.assertEqual(result["tiers"]["md"]["cached_url"], "/api/thumb/md/42?cached=1")

    async def test_cache_status_reports_preview_and_original_progress_separately(self):
        source = await self._source()
        await self._image(source["id"], "browser-original.jpg")
        await self._image(source["id"], "raw-original.nef")

        def tier(count, bytes_used, budget):
            return {
                "count": count,
                "bytes": bytes_used,
                "current_count": count,
                "current_bytes": bytes_used,
                "stale_count": 0,
                "replacement_mode": False,
                "budget_bytes": budget,
            }

        cache_stats = {
            "memory": {"used_bytes": 0, "limit_bytes": 1, "tiers": {}},
            "disk": {
                "root": app_module.thumbnails.SSD_CACHE_DIR,
                "limit_bytes": 1000,
                "used_bytes": 460,
                "tiers": {
                    "sm": tier(2, 20, 100),
                    "md": tier(2, 80, 200),
                    "lg": tier(2, 160, 300),
                    "full": tier(0, 0, 400),
                },
            },
            "thumbnail_config": {"changed_at": 0, "replace_stale_thumbnails": False},
        }
        captured = {}

        def fake_pregen_status(target_total, stats=None, original_total=0):
            captured["target_total"] = target_total
            captured["original_total"] = original_total
            return {
                "state": "running",
                "manual_pause": False,
                "active_phase": "full",
                "phases": {
                    "sm": {"count": 2, "total": 2, "remaining": 0},
                    "md": {"count": 2, "total": 2, "remaining": 0},
                    "lg": {"count": 2, "total": 2, "remaining": 0},
                },
                "preview": {"count": 6, "total": 6, "remaining": 0, "progress_pct": 100.0},
                "originals": {"count": 0, "total": original_total, "remaining": original_total},
                "remaining": 0,
                "eta_seconds": None,
                "original_eta_seconds": None,
                "replacement_mode": False,
            }

        old_cache_stats = app_module.thumbnails.cache_stats
        old_pregen_status = app_module.thumbnails.get_pregen_status
        old_recommendations = app_module._cache_recommendations
        try:
            app_module.thumbnails.cache_stats = lambda: cache_stats
            app_module.thumbnails.get_pregen_status = fake_pregen_status
            app_module._cache_recommendations = (
                lambda _cache, eligible, total, browser: {
                    "eligible_images": eligible,
                    "total_images": total,
                    "browser_original_images": browser,
                    "tiers": {},
                }
            )

            result = await app_module.build_cache_status(ahead=0)
        finally:
            app_module.thumbnails.cache_stats = old_cache_stats
            app_module.thumbnails.get_pregen_status = old_pregen_status
            app_module._cache_recommendations = old_recommendations

        self.assertEqual(captured, {"target_total": 2, "original_total": 1})
        self.assertEqual(result["disk"]["tiers"]["sm"]["progress_total"], 2)
        self.assertEqual(result["disk"]["tiers"]["full"]["progress_total"], 1)
        self.assertEqual(result["pregen"]["preview"]["remaining"], 0)
        self.assertEqual(result["pregen"]["originals"]["remaining"], 1)

    async def test_ui_settings_returns_default_loupe_cache_status(self):
        result = await app_module.api_ui_settings()

        self.assertEqual(result, {"settings": {"show_loupe_cache_status": True}})

    async def test_loupe_cache_status_setting_persists(self):
        saved = app_module.settings.save_settings({"show_loupe_cache_status": False})
        self.assertFalse(saved["show_loupe_cache_status"])

        reloaded = app_module.settings.load_settings(force=True)
        result = await app_module.api_ui_settings()

        self.assertFalse(reloaded["show_loupe_cache_status"])
        self.assertEqual(result, {"settings": {"show_loupe_cache_status": False}})


if __name__ == "__main__":
    unittest.main()
