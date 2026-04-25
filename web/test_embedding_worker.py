import asyncio
import os
import sys
import time
import unittest

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
import embedding_worker  # noqa: E402


class FakeImage:
    def __init__(self, image_id):
        self.image_id = image_id
        self.closed = False

    def close(self):
        self.closed = True


class FakeModel:
    def __init__(self, *, oom_above=None):
        self.oom_above = oom_above
        self.calls = []

    def encode(self, images, normalize_embeddings=True):
        ids = [image.image_id for image in images]
        self.calls.append(ids)
        if self.oom_above is not None and len(images) > self.oom_above:
            raise RuntimeError("CUDA out of memory while allocating test tensor")
        return np.array([[float(image_id), 0.0, 0.0, 0.0] for image_id in ids], dtype=np.float32)


class EmbeddingWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.old_preload_images = embedding_worker._preload_images
        self.old_store_embeddings_batch = embedding_worker.db.store_embeddings_batch
        self.old_get_embedding_count = embedding_worker.db.get_embedding_count
        self.old_add_vectors = embedding_worker.embed_cache.add_vectors
        self.old_get_settings = embedding_worker.settings.get_settings
        self.old_clear_cuda_cache = embedding_worker._clear_cuda_cache
        self.old_worker_status = dict(embedding_worker._worker_status)
        self.old_batch_control = dict(embedding_worker._batch_control)
        self.old_retry_after = dict(embedding_worker._embed_retry_after)
        self.old_manual_pause = embedding_worker._embedding_manual_pause
        self.old_history = list(embedding_worker._embedding_history)

        self.stored = []
        self.cached = []
        self.preload_calls = []
        self.missing_ids = set()

        def fake_preload(image_refs):
            self.preload_calls.append([image_id for image_id, _path in image_refs])
            valid = []
            valid_indices = []
            errors = [None] * len(image_refs)
            for index, (image_id, _path) in enumerate(image_refs):
                if image_id in self.missing_ids:
                    errors[index] = "md thumbnail not cached yet"
                    continue
                valid.append(FakeImage(image_id))
                valid_indices.append(index)
            return valid, valid_indices, errors

        async def fake_store(rows):
            self.stored.extend(rows)

        async def fake_count():
            return len(self.stored)

        def fake_add_vectors(rows):
            self.cached.extend(rows)

        embedding_worker._preload_images = fake_preload
        embedding_worker.db.store_embeddings_batch = fake_store
        embedding_worker.db.get_embedding_count = fake_count
        embedding_worker.embed_cache.add_vectors = fake_add_vectors
        embedding_worker.settings.get_settings = lambda: {"embed_batch_size": 8}
        embedding_worker._clear_cuda_cache = lambda: None

        embedding_worker._embedding_history.clear()
        embedding_worker._embed_retry_after.clear()
        embedding_worker._embedding_manual_pause = False
        embedding_worker._batch_control.update({
            "active_batch_size": 4,
            "successful_batches": 0,
            "oom_backoffs": 0,
            "last_oom_at": None,
            "growth_paused_until": None,
        })
        embedding_worker._worker_status.update({
            "last_batch_size": 0,
            "last_batch_seconds": 0.0,
            "last_embedded_at": None,
            "session_embedded": 0,
            "session_started_at": None,
            "session_embed_seconds": 0.0,
            "session_wall_seconds": 0.0,
            "recent_images_per_min": 0.0,
            "recent_wall_images_per_min": 0.0,
            "overall_images_per_min": 0.0,
            "overall_wall_images_per_min": 0.0,
            "last_batch_failures": 0,
            "last_batch_stage_seconds": {},
            "last_candidate_query_seconds": 0.012,
            "oom_backoffs": 0,
        })

    async def asyncTearDown(self):
        embedding_worker._preload_images = self.old_preload_images
        embedding_worker.db.store_embeddings_batch = self.old_store_embeddings_batch
        embedding_worker.db.get_embedding_count = self.old_get_embedding_count
        embedding_worker.embed_cache.add_vectors = self.old_add_vectors
        embedding_worker.settings.get_settings = self.old_get_settings
        embedding_worker._clear_cuda_cache = self.old_clear_cuda_cache
        embedding_worker._worker_status.clear()
        embedding_worker._worker_status.update(self.old_worker_status)
        embedding_worker._batch_control.clear()
        embedding_worker._batch_control.update(self.old_batch_control)
        embedding_worker._embed_retry_after.clear()
        embedding_worker._embed_retry_after.update(self.old_retry_after)
        embedding_worker._embedding_manual_pause = self.old_manual_pause
        embedding_worker._embedding_history.clear()
        embedding_worker._embedding_history.extend(self.old_history)

    def rows(self, count):
        return [
            {"id": image_id, "filepath": f"/test/{image_id}.jpg"}
            for image_id in range(1, count + 1)
        ]

    def stored_ids(self):
        return [image_id for image_id, _blob in self.stored]

    async def process(self, rows, model=None):
        loop = asyncio.get_running_loop()
        return await embedding_worker._process_embedding_candidates(
            loop,
            model or FakeModel(),
            rows,
            batch_pause_seconds=0.0,
        )

    async def test_candidate_window_splits_into_chunks_without_duplicate_stores(self):
        model = FakeModel()

        result = await self.process(self.rows(10), model)

        self.assertEqual(result["stored"], 10)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["chunks"], 3)
        self.assertEqual(self.stored_ids(), list(range(1, 11)))
        self.assertEqual(len(set(self.stored_ids())), 10)
        self.assertEqual([len(call) for call in model.calls], [4, 4, 2])

    async def test_preload_encode_pipeline_preserves_result_ordering(self):
        result = await self.process(self.rows(6))

        stored_values = [
            float(np.frombuffer(blob, dtype=np.float32)[0])
            for _image_id, blob in self.stored
        ]
        self.assertEqual(result["stored"], 6)
        self.assertEqual(self.stored_ids(), [1, 2, 3, 4, 5, 6])
        self.assertEqual(stored_values, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
        self.assertEqual(self.preload_calls[0], [1, 2, 3, 4])
        self.assertEqual(self.preload_calls[1], [5, 6])

    async def test_cuda_oom_retries_smaller_chunks_and_reduces_active_batch(self):
        model = FakeModel(oom_above=2)

        result = await self.process(self.rows(5), model)
        status = embedding_worker.get_worker_status()

        self.assertEqual(result["stored"], 5)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(self.stored_ids(), [1, 2, 3, 4, 5])
        self.assertEqual(model.calls[0], [1, 2, 3, 4])
        self.assertEqual([len(call) for call in model.calls[1:]], [2, 2, 1])
        self.assertEqual(status["active_batch_size"], 2)
        self.assertEqual(status["oom_backoffs"], 1)
        self.assertIsNotNone(status["batch_growth_paused_until"])

    async def test_failed_image_retry_cooldown_still_works(self):
        self.missing_ids = {2}

        result = await self.process(self.rows(3))

        self.assertEqual(result["stored"], 2)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(self.stored_ids(), [1, 3])
        self.assertNotIn(1, embedding_worker._embed_retry_after)
        self.assertGreater(embedding_worker._embed_retry_after[2], time.time())
        self.assertEqual(embedding_worker.get_worker_status()["last_batch_failures"], 1)

    async def test_timing_and_status_fields_are_populated(self):
        await self.process(self.rows(2))

        status = embedding_worker.get_worker_status()
        stages = status["last_batch_stage_seconds"]

        self.assertEqual(status["active_batch_size"], 4)
        self.assertEqual(status["target_batch_size"], 8)
        self.assertGreater(status["recent_wall_images_per_min"], 0)
        self.assertEqual(status["last_candidate_query_seconds"], 0.012)
        for key in ("candidate_query", "preload", "encode", "store", "pause", "wall"):
            self.assertIn(key, stages)
        self.assertIn("oom_backoffs", status)


if __name__ == "__main__":
    unittest.main()
