import os
import sys
import tempfile
import unittest

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
import app as app_module  # noqa: E402
import db  # noqa: E402
import elo_propagation  # noqa: E402


class JsonRequest:
    def __init__(self, payload):
        self.payload = payload

    async def json(self):
        return self.payload


class BackendRankingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.old_db_path = db.DB_PATH
        self.old_schedule_pairing_propagation = app_module._schedule_pairing_propagation
        self.old_get_matrix = elo_propagation.embed_cache.get_matrix
        self.old_get_index = elo_propagation.embed_cache.get_index

        db.DB_PATH = os.path.join(self.tempdir.name, "photoarchive-test.db")
        db.invalidate_stats_cache()
        await db.init_db()
        app_module._pairing_cache.update({"data": None, "by_id": None, "valid": False})
        app_module._matchups_cache.update({"data": None, "valid": False})

        def close_scheduled(coro):
            coro.close()

        app_module._schedule_pairing_propagation = close_scheduled

    async def asyncTearDown(self):
        app_module._schedule_pairing_propagation = self.old_schedule_pairing_propagation
        elo_propagation.embed_cache.get_matrix = self.old_get_matrix
        elo_propagation.embed_cache.get_index = self.old_get_index
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

    async def test_compare_payload_validation_rejects_invalid_and_inactive_images(self):
        source = await self._source()
        a = await self._image(source["id"], "a.jpg")
        b = await self._image(source["id"], "b.jpg")
        offline_source = await self._source("offline", online=False)
        offline = await self._image(offline_source["id"], "offline.jpg")

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


if __name__ == "__main__":
    unittest.main()
