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

        db.DB_PATH = os.path.join(self.tempdir.name, "photoarchive-test.db")
        db.invalidate_stats_cache()
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
        db.DB_PATH = self.old_db_path
        db.invalidate_stats_cache()
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
    ):
        filepath = os.path.join(self.tempdir.name, f"{source_id}-{filename}")
        conn = await db.get_db()
        try:
            cursor = await conn.execute(
                "INSERT INTO images "
                "(source_id, filename, filepath, elo, comparisons, propagated_updates, status) "
                "VALUES (?, ?, ?, ?, ?, ?, 'kept')",
                (source_id, filename, filepath, elo, comparisons, propagated_updates),
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

        self.assertTrue(result["tiers"]["md"]["cached"])
        self.assertTrue(result["tiers"]["full"]["cached"])
        self.assertEqual(result["best_cached"], "full")
        self.assertEqual(result["tiers"]["md"]["cached_url"], "/api/thumb/md/42?cached=1")


if __name__ == "__main__":
    unittest.main()
