"""
Elo propagation via embedding similarity.

When a comparison is recorded, propagate scaled Elo adjustments to
visually similar images. This dramatically accelerates ranking for
archives with many similar shots (same scene, same shoot, etc.).

Direct comparisons are always source of truth — propagation only
nudges images that haven't been extensively compared yet.
"""

import asyncio
import logging
import numpy as np

import db
import embed_cache

log = logging.getLogger("elo_propagation")

# Tuning parameters
SIMILARITY_THRESHOLD = 0.75   # minimum cosine similarity to propagate
MAX_NEIGHBORS = 10            # max images to adjust per winner/loser
PROPAGATION_DECAY = 0.3       # scale factor (0.3 = propagated change is 30% of direct)
MAX_DIRECT_COMPARISONS = 8    # don't propagate to images with this many+ direct comparisons



def _find_similar(image_id, image_ids, matrix, id_to_idx, threshold, max_n):
    """Find the most similar images above threshold. Returns [(id, similarity), ...]."""
    idx = id_to_idx.get(image_id)
    if idx is None:
        return []

    similarities = matrix @ matrix[idx]  # cosine sim (already L2-normalized)
    candidate_count = min(len(image_ids), max_n + 1)
    if candidate_count <= 0:
        return []
    if len(image_ids) <= candidate_count:
        ranked = np.argsort(similarities)[::-1]
    else:
        candidates = np.argpartition(similarities, -candidate_count)[-candidate_count:]
        ranked = candidates[np.argsort(similarities[candidates])[::-1]]

    results = []
    for i in ranked:
        if image_ids[i] == image_id:
            continue
        sim = float(similarities[i])
        if sim < threshold:
            break
        results.append((image_ids[i], sim))
        if len(results) >= max_n:
            break
    return results


async def propagate_comparison(winner_id: int, loser_id: int, k: float):
    """
    After a direct comparison, propagate scaled Elo changes to similar images.
    Called as a fire-and-forget background task.
    """
    try:
        image_ids, matrix = await embed_cache.get_matrix()
        if image_ids is None:
            return  # no embeddings available yet
        id_to_idx = embed_cache.get_index()

        # Find similar images for winner and loser
        winner_neighbors = _find_similar(winner_id, image_ids, matrix, id_to_idx, SIMILARITY_THRESHOLD, MAX_NEIGHBORS)
        loser_neighbors = _find_similar(loser_id, image_ids, matrix, id_to_idx, SIMILARITY_THRESHOLD, MAX_NEIGHBORS)

        if not winner_neighbors and not loser_neighbors:
            return

        # Collect all neighbor IDs to fetch their current state
        all_neighbor_ids = list({nid for nid, _ in winner_neighbors + loser_neighbors})
        neighbors = await db.get_images_by_ids(all_neighbor_ids)

        conn = await db.get_db()
        try:
            deltas = {}

            # Boost images similar to the winner
            for neighbor_id, similarity in winner_neighbors:
                # Skip if this neighbor was directly involved in the comparison
                if neighbor_id == loser_id:
                    continue
                boost = k * similarity * PROPAGATION_DECAY
                deltas[neighbor_id] = deltas.get(neighbor_id, 0.0) + boost

            # Penalize images similar to the loser
            for neighbor_id, similarity in loser_neighbors:
                if neighbor_id == winner_id:
                    continue
                penalty = k * similarity * PROPAGATION_DECAY
                deltas[neighbor_id] = deltas.get(neighbor_id, 0.0) - penalty

            updates = []
            for neighbor_id, delta in deltas.items():
                neighbor = neighbors.get(neighbor_id)
                if not neighbor or neighbor["comparisons"] >= MAX_DIRECT_COMPARISONS:
                    continue
                updates.append((neighbor["elo"] + delta, neighbor_id))

            if updates:
                await conn.executemany(
                    "UPDATE images SET elo = ?, comparisons = comparisons + 1 WHERE id = ?",
                    updates,
                )
                await conn.commit()
                log.debug(f"Propagated Elo to {len(updates)} neighbors "
                         f"(winner={winner_id}, loser={loser_id})")
        finally:
            await conn.close()

    except Exception as e:
        log.warning(f"Elo propagation error: {e}")


async def propagate_mosaic(winner_id: int, loser_ids: list[int], k: float):
    """
    Propagate after a mosaic pick. The winner beat all losers, so boost
    images similar to the winner. For losers, apply a lighter penalty
    since they only lost to the best image, not to each other.
    """
    try:
        image_ids, matrix = await embed_cache.get_matrix()
        if image_ids is None:
            return
        id_to_idx = embed_cache.get_index()

        involved = {winner_id} | set(loser_ids)
        winner_neighbors = _find_similar(winner_id, image_ids, matrix, id_to_idx, SIMILARITY_THRESHOLD, MAX_NEIGHBORS)

        if not winner_neighbors:
            return

        all_neighbor_ids = list({nid for nid, _ in winner_neighbors})
        neighbors = await db.get_images_by_ids(all_neighbor_ids)

        conn = await db.get_db()
        try:
            updates = []
            for neighbor_id, similarity in winner_neighbors:
                neighbor = neighbors.get(neighbor_id)
                if not neighbor or neighbor["comparisons"] >= MAX_DIRECT_COMPARISONS:
                    continue
                if neighbor_id in involved:
                    continue
                # Boost proportional to similarity and K
                boost = k * similarity * PROPAGATION_DECAY
                new_elo = neighbor["elo"] + boost
                updates.append((new_elo, neighbor_id))

            if updates:
                await conn.executemany(
                    "UPDATE images SET elo = ?, comparisons = comparisons + 1 WHERE id = ?",
                    updates,
                )
                await conn.commit()
                log.debug(f"Propagated mosaic win to {len(updates)} neighbors of {winner_id}")
        finally:
            await conn.close()

    except Exception as e:
        log.warning(f"Mosaic propagation error: {e}")
