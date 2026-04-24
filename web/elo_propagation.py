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

# Last propagation result (read by /api/propagation/last)
last_propagation_count = 0

# Tuning parameters
SIMILARITY_THRESHOLD = 0.70   # minimum cosine similarity to propagate
MAX_NEIGHBORS = 100           # long tail — cubic scaling makes weak matches near-zero anyway
PROPAGATION_DECAY = 0.3       # scale factor (0.3 = propagated change is 30% of direct)
MAX_DIRECT_COMPARISONS = 50   # allow propagation to well-compared images (cubic scaling keeps it safe)


def _nonlinear_weight(similarity: float) -> float:
    """Remap similarity to a cubic curve so near-identical images (0.99)
    get strong propagation while barely-qualifying ones (0.75) get almost none.

    Linear:  0.75→0.75, 0.90→0.90, 0.99→0.99  (flat, everything gets a lot)
    Cubic:   0.75→0.00, 0.90→0.22, 0.99→0.89  (steep falloff for weak matches)
    """
    t = (similarity - SIMILARITY_THRESHOLD) / (1.0 - SIMILARITY_THRESHOLD)
    return t * t * t  # cubic



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


async def predict_propagation(grid_ids: list[int]) -> dict[int, int]:
    """Precompute how many images would be affected if each grid image were the winner.
    Returns {image_id: predicted_count} for each image in grid_ids."""
    try:
        image_ids, matrix = await embed_cache.get_matrix()
        if image_ids is None:
            return {gid: 0 for gid in grid_ids}
        id_to_idx = embed_cache.get_index()

        grid_set = set(grid_ids)
        # Precompute neighbors for every grid image
        neighbors_by_id = {}
        for gid in grid_ids:
            neighbors_by_id[gid] = _find_similar(gid, image_ids, matrix, id_to_idx, SIMILARITY_THRESHOLD, MAX_NEIGHBORS)

        # For filtering: fetch comparison counts for all potential neighbors
        all_neighbor_ids = set()
        for nlist in neighbors_by_id.values():
            for nid, _ in nlist:
                if nid not in grid_set:
                    all_neighbor_ids.add(nid)
        neighbor_data = await db.get_images_by_ids(list(all_neighbor_ids)) if all_neighbor_ids else {}

        result = {}
        for winner_id in grid_ids:
            affected = set()
            # Winner neighbors
            for nid, _ in neighbors_by_id[winner_id]:
                if nid in grid_set:
                    continue
                n = neighbor_data.get(nid)
                if n and n["comparisons"] < MAX_DIRECT_COMPARISONS:
                    affected.add(nid)
            # Loser neighbors (everyone else on the grid)
            for loser_id in grid_ids:
                if loser_id == winner_id:
                    continue
                for nid, _ in neighbors_by_id[loser_id]:
                    if nid in grid_set:
                        continue
                    n = neighbor_data.get(nid)
                    if n and n["comparisons"] < MAX_DIRECT_COMPARISONS:
                        affected.add(nid)
            result[winner_id] = len(affected)
        return result
    except Exception as e:
        log.warning(f"Propagation prediction error: {e}")
        return {gid: 0 for gid in grid_ids}


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
                if neighbor_id == loser_id:
                    continue
                weight = _nonlinear_weight(similarity)
                boost = k * weight * PROPAGATION_DECAY
                deltas[neighbor_id] = deltas.get(neighbor_id, 0.0) + boost

            # Penalize images similar to the loser
            for neighbor_id, similarity in loser_neighbors:
                if neighbor_id == winner_id:
                    continue
                weight = _nonlinear_weight(similarity)
                penalty = k * weight * PROPAGATION_DECAY
                deltas[neighbor_id] = deltas.get(neighbor_id, 0.0) - penalty

            updates = []
            for neighbor_id, delta in deltas.items():
                neighbor = neighbors.get(neighbor_id)
                if not neighbor or neighbor["comparisons"] >= MAX_DIRECT_COMPARISONS:
                    continue
                updates.append((neighbor["elo"] + delta, neighbor_id))

            global last_propagation_count
            if updates:
                await conn.executemany(
                    "UPDATE images SET elo = ?, comparisons = comparisons + 1 WHERE id = ?",
                    updates,
                )
                await conn.commit()
                last_propagation_count = len(updates)
                log.debug(f"Propagated Elo to {len(updates)} neighbors "
                         f"(winner={winner_id}, loser={loser_id})")
            else:
                last_propagation_count = 0
        finally:
            await conn.close()

    except Exception as e:
        log.warning(f"Elo propagation error: {e}")


async def propagate_mosaic(winner_id: int, loser_ids: list[int], k: float):
    """
    Propagate after a mosaic pick. Boost images similar to the winner,
    and penalize images similar to the losers. This makes each mosaic
    pick dramatically more powerful by also affecting look-alikes of
    every image on the grid.
    """
    try:
        image_ids, matrix = await embed_cache.get_matrix()
        if image_ids is None:
            return
        id_to_idx = embed_cache.get_index()

        involved = {winner_id} | set(loser_ids)

        # Find neighbors for winner AND all losers
        winner_neighbors = _find_similar(winner_id, image_ids, matrix, id_to_idx, SIMILARITY_THRESHOLD, MAX_NEIGHBORS)
        loser_neighbor_lists = []
        for lid in loser_ids:
            loser_neighbors = _find_similar(lid, image_ids, matrix, id_to_idx, SIMILARITY_THRESHOLD, MAX_NEIGHBORS)
            loser_neighbor_lists.append(loser_neighbors)

        all_neighbor_ids = set()
        for nid, _ in winner_neighbors:
            all_neighbor_ids.add(nid)
        for ln in loser_neighbor_lists:
            for nid, _ in ln:
                all_neighbor_ids.add(nid)

        if not all_neighbor_ids:
            return

        neighbors = await db.get_images_by_ids(list(all_neighbor_ids))

        conn = await db.get_db()
        try:
            deltas = {}

            # Boost images similar to the winner
            for neighbor_id, similarity in winner_neighbors:
                if neighbor_id in involved:
                    continue
                weight = _nonlinear_weight(similarity)
                boost = k * weight * PROPAGATION_DECAY
                deltas[neighbor_id] = deltas.get(neighbor_id, 0.0) + boost

            # Penalize images similar to losers (scaled down since each
            # loser only lost to the winner, not to each other)
            loser_scale = 1.0 / max(len(loser_ids), 1)
            for loser_neighbors in loser_neighbor_lists:
                for neighbor_id, similarity in loser_neighbors:
                    if neighbor_id in involved:
                        continue
                    weight = _nonlinear_weight(similarity)
                    penalty = k * weight * PROPAGATION_DECAY * loser_scale
                    deltas[neighbor_id] = deltas.get(neighbor_id, 0.0) - penalty

            updates = []
            for neighbor_id, delta in deltas.items():
                neighbor = neighbors.get(neighbor_id)
                if not neighbor or neighbor["comparisons"] >= MAX_DIRECT_COMPARISONS:
                    continue
                updates.append((neighbor["elo"] + delta, neighbor_id))

            global last_propagation_count
            if updates:
                await conn.executemany(
                    "UPDATE images SET elo = ?, comparisons = comparisons + 1 WHERE id = ?",
                    updates,
                )
                await conn.commit()
                last_propagation_count = len(updates)
                log.debug(f"Propagated mosaic to {len(updates)} neighbors "
                         f"(winner={winner_id}, {len(loser_ids)} losers)")
            else:
                last_propagation_count = 0
        finally:
            await conn.close()

    except Exception as e:
        log.warning(f"Mosaic propagation error: {e}")
