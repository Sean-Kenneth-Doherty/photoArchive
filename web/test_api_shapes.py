import asyncio
import os
import sqlite3
import sys
import tempfile
import unittest

import numpy as np
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(__file__))
import app as app_module  # noqa: E402
import db  # noqa: E402
import embed_cache  # noqa: E402
import embedding_worker  # noqa: E402


CARD_KEYS = {
    "id",
    "filename",
    "elo",
    "comparisons",
    "propagated_updates",
    "status",
    "flag",
    "aspect_ratio",
    "date_taken",
    "camera_make",
    "camera_model",
    "lens",
    "file_ext",
    "file_size",
    "file_modified_at",
    "width",
    "height",
    "latitude",
    "longitude",
    "created_at",
    "thumb_url",
}


class ApiShapeTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.old_db_path = db.DB_PATH
        self.old_settings_path = app_module.settings.SETTINGS_PATH
        self.old_settings_state = app_module.settings._settings
        self.old_cache_dir = app_module.thumbnails.SSD_CACHE_DIR
        self.old_prefetch_images = app_module.thumbnails.prefetch_images
        self.old_encode_text = embedding_worker.encode_text
        self.old_get_matrix = embed_cache.get_matrix
        self.old_get_index = embed_cache.get_index
        self.old_get_vector = embed_cache.get_vector

        db.DB_PATH = os.path.join(self.tempdir.name, "api-shapes.db")
        app_module.settings.SETTINGS_PATH = os.path.join(self.tempdir.name, "settings.local.json")
        app_module.settings._settings = None
        app_module.thumbnails.SSD_CACHE_DIR = os.path.join(self.tempdir.name, "cache")
        db.invalidate_stats_cache()
        db.invalidate_cached_image_ids_cache()
        asyncio.run(db.init_db())
        app_module._pairing_cache.update({"data": None, "by_id": None, "valid": False})
        app_module._matchups_cache.update({"data": None, "valid": False})

        async def noop_prefetch(*_args, **_kwargs):
            return 0

        app_module.thumbnails.prefetch_images = noop_prefetch
        self.source_id = self._create_source()
        self.ids = self._create_images()
        self.client = TestClient(app_module.app)

    def tearDown(self):
        self.client.close()
        app_module.thumbnails.prefetch_images = self.old_prefetch_images
        embedding_worker.encode_text = self.old_encode_text
        embed_cache.get_matrix = self.old_get_matrix
        embed_cache.get_index = self.old_get_index
        embed_cache.get_vector = self.old_get_vector
        app_module.thumbnails.SSD_CACHE_DIR = self.old_cache_dir
        app_module.settings.SETTINGS_PATH = self.old_settings_path
        app_module.settings._settings = self.old_settings_state
        db.DB_PATH = self.old_db_path
        db.invalidate_stats_cache()
        db.invalidate_cached_image_ids_cache()
        app_module._pairing_cache.update({"data": None, "by_id": None, "valid": False})
        app_module._matchups_cache.update({"data": None, "valid": False})
        self.tempdir.cleanup()

    def _create_source(self):
        source_path = os.path.join(self.tempdir.name, "catalog")
        os.makedirs(source_path, exist_ok=True)
        source = asyncio.run(db.add_or_restore_source(source_path))
        return int(source["id"])

    def _create_images(self):
        rows = [
            (
                self.source_id,
                "sunset-alpha.jpg",
                os.path.join(self.tempdir.name, "catalog", "sunset-alpha.jpg"),
                1500.0,
                5,
                0,
                "kept",
                "picked",
                "landscape",
                1.6,
                "2024-05-10 12:00:00",
                "Fuji",
                "X-T5",
                "35mm",
                ".jpg",
                1000,
                1700000000.0,
                3200,
                2000,
                41.1,
                -87.1,
            ),
            (
                self.source_id,
                "portrait-beta.jpg",
                os.path.join(self.tempdir.name, "catalog", "portrait-beta.jpg"),
                1320.0,
                1,
                0,
                "kept",
                "unflagged",
                "portrait",
                0.75,
                "2024-06-02 08:00:00",
                "Canon",
                "R5",
                "50mm",
                ".jpg",
                2000,
                1700500000.0,
                2400,
                3200,
                41.2,
                -87.2,
            ),
            (
                self.source_id,
                "sunset-gamma.jpg",
                os.path.join(self.tempdir.name, "catalog", "sunset-gamma.jpg"),
                1210.0,
                0,
                1,
                "kept",
                "rejected",
                "landscape",
                1.5,
                None,
                "Fuji",
                "X-T5",
                "35mm",
                ".jpg",
                3000,
                1700600000.0,
                3000,
                2000,
                None,
                None,
            ),
            (
                self.source_id,
                "hidden-delta.jpg",
                os.path.join(self.tempdir.name, "catalog", "hidden-delta.jpg"),
                1250.0,
                0,
                0,
                "kept",
                "unflagged",
                "landscape",
                1.4,
                "2024-04-01 08:00:00",
                "Sony",
                "A7",
                "24mm",
                ".jpg",
                4000,
                1700700000.0,
                2800,
                2000,
                None,
                None,
            ),
        ]
        with sqlite3.connect(db.DB_PATH) as conn:
            ids = []
            for row in rows:
                cursor = conn.execute(
                    "INSERT INTO images "
                    "(source_id, filename, filepath, elo, comparisons, propagated_updates, "
                    "status, flag, orientation, aspect_ratio, date_taken, camera_make, camera_model, "
                    "lens, file_ext, file_size, file_modified_at, width, height, latitude, longitude) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    row,
                )
                ids.append(int(cursor.lastrowid))
            now = 12345.0
            for image_id in ids[:3]:
                for size in ("sm", "md"):
                    conn.execute(
                        "INSERT INTO cache_entries "
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
            conn.commit()
        db.invalidate_stats_cache()
        db.invalidate_cached_image_ids_cache()
        return ids

    def assertCardShape(self, card, thumb_size="sm", *, contextual=()):
        self.assertTrue(CARD_KEYS.issubset(card.keys()))
        self.assertEqual(card["thumb_url"], f"/api/thumb/{thumb_size}/{card['id']}")
        for key in ("similarity", "date_group"):
            if key in contextual:
                self.assertIn(key, card)
            else:
                self.assertNotIn(key, card)

    def test_rankings_cards_are_normalized(self):
        response = self.client.get("/api/rankings?limit=10&sort=elo")
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["visible_images"], 3)
        self.assertEqual(data["total_images"], 4)
        self.assertEqual(data["hidden_pending_thumbnails"], 1)
        for card in data["images"]:
            self.assertCardShape(card)

    def test_date_group_rankings_include_contextual_group_only(self):
        response = self.client.get("/api/rankings?limit=10&sort=date_taken")
        self.assertEqual(response.status_code, 200)
        cards = response.json()["images"]

        self.assertTrue(cards)
        for card in cards:
            self.assertCardShape(card, contextual=("date_group",))
        self.assertIn("2024-06", [card["date_group"] for card in cards])
        self.assertIn("", [card["date_group"] for card in cards])

    def test_search_cards_include_similarity_context(self):
        embedding_worker.encode_text = lambda _query: None

        response = self.client.get("/api/search?q=sunset&limit=10")
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["search_mode"], "metadata")
        self.assertEqual(data["visible_images"], 2)
        self.assertEqual(data["total_images"], 2)
        for card in data["images"]:
            self.assertCardShape(card, contextual=("similarity",))
            self.assertIsNone(card["similarity"])

    def test_similar_cards_include_similarity_context(self):
        image_ids = self.ids
        matrix = np.array(
            [
                [1.0, 0.0],
                [0.8, 0.2],
                [0.7, 0.3],
                [0.95, 0.05],
            ],
            dtype=np.float32,
        )

        async def fake_get_matrix():
            return image_ids, matrix

        def fake_get_index():
            return {image_id: index for index, image_id in enumerate(image_ids)}

        def fake_get_vector(image_id):
            return matrix[fake_get_index()[image_id]]

        embed_cache.get_matrix = fake_get_matrix
        embed_cache.get_index = fake_get_index
        embed_cache.get_vector = fake_get_vector

        response = self.client.get(f"/api/similar/{image_ids[0]}?limit=10")
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["visible_images"], 2)
        self.assertEqual(data["total_images"], 3)
        for card in data["images"]:
            self.assertCardShape(card, contextual=("similarity",))
            self.assertIsInstance(card["similarity"], float)

    def test_mosaic_cards_are_normalized(self):
        response = self.client.get("/api/mosaic/next?n=3&strategy=top")
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["visible_images"], 3)
        self.assertEqual(data["total_images"], 4)
        for card in data["images"]:
            self.assertCardShape(card)

    def test_compare_cards_are_normalized(self):
        response = self.client.get("/api/compare/next?n=2&mode=swiss")
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["visible_images"], 3)
        self.assertEqual(data["total_images"], 4)
        self.assertTrue(data["pairs"])
        for pair in data["pairs"]:
            self.assertCardShape(pair["left"], thumb_size="md")
            self.assertCardShape(pair["right"], thumb_size="md")


if __name__ == "__main__":
    unittest.main()
