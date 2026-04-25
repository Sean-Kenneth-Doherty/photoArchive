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
_prediction_cache_key = None
_prediction_cache_counts = None

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
    return _rank_similar_from_scores(image_id, image_ids, similarities, threshold, max_n)


def _rank_similar_from_scores(image_id, image_ids, similarities, threshold, max_n):
    """Rank precomputed similarity scores. Returns [(id, similarity), ...]."""
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


def _find_similar_batch(image_ids_to_find, image_ids, matrix, id_to_idx, threshold, max_n):
    """Find similar images for many source IDs using one matrix multiply."""
    valid_ids = []
    valid_indices = []
    for image_id in image_ids_to_find:
        idx = id_to_idx.get(image_id)
        if idx is not None:
            valid_ids.append(image_id)
            valid_indices.append(idx)

    results_by_id = {image_id: [] for image_id in image_ids_to_find}
    if not valid_indices:
        return results_by_id

    grid_matrix = matrix[valid_indices]
    similarity_rows = grid_matrix @ matrix.T  # cosine sim (already L2-normalized)

    for image_id, similarities in zip(valid_ids, similarity_rows):
        results_by_id[image_id] = _rank_similar_from_scores(
            image_id, image_ids, similarities, threshold, max_n
        )
    return results_by_id


async def _apply_propagation_deltas(
    conn,
    neighbors: dict[int, dict],
    deltas: dict[int, float],
    *,
    action_id: str | None,
) -> int:
    updates = []
    history_rows = []
    for neighbor_id, delta in deltas.items():
        neighbor = neighbors.get(neighbor_id)
        if not neighbor or neighbor["comparisons"] >= MAX_DIRECT_COMPARISONS:
            continue
        before_elo = float(neighbor["elo"])
        before_count = int(neighbor.get("propagated_updates") or 0)
        after_elo = before_elo + float(delta)
        updates.append((after_elo, neighbor_id))
        if action_id:
            history_rows.append((
                action_id,
                neighbor_id,
                before_elo,
                before_count,
                after_elo,
                float(delta),
            ))

    if not updates:
        return 0

    if action_id:
        cursor = await conn.execute(
            "SELECT 1 FROM comparisons WHERE action_id = ? LIMIT 1",
            (action_id,),
        )
        if await cursor.fetchone() is None:
            return 0
        await conn.executemany(
            "INSERT INTO propagation_updates "
            "(action_id, image_id, elo_before, propagated_updates_before, elo_after, delta) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            history_rows,
        )

    await conn.executemany(
        "UPDATE images SET elo = ?, propagated_updates = COALESCE(propagated_updates, 0) + 1 WHERE id = ?",
        updates,
    )
    return len(updates)


async def predict_propagation(grid_ids: list[int]) -> dict[int, int]:
    """Precompute how many images would be affected if each grid image were the winner.
    Returns {image_id: predicted_count} for each image in grid_ids."""
    global _prediction_cache_key, _prediction_cache_counts
    cache_key = tuple(grid_ids)
    if _prediction_cache_key == cache_key and _prediction_cache_counts is not None:
        return dict(_prediction_cache_counts)

    try:
        image_ids, matrix = await embed_cache.get_matrix()
        if image_ids is None:
            return {gid: 0 for gid in grid_ids}
        id_to_idx = embed_cache.get_index()

        grid_set = set(grid_ids)
        # Precompute neighbors for every grid image
        neighbors_by_id = _find_similar_batch(
            grid_ids,
            image_ids,
            matrix,
            id_to_idx,
            SIMILARITY_THRESHOLD,
            MAX_NEIGHBORS,
        )

        # For filtering: fetch comparison counts for all potential neighbors
        all_neighbor_ids = set()
        for nlist in neighbors_by_id.values():
            for nid, _ in nlist:
                if nid not in grid_set:
                    all_neighbor_ids.add(nid)
        neighbor_data = await db.get_active_images_by_ids(list(all_neighbor_ids)) if all_neighbor_ids else {}

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
        _prediction_cache_key = cache_key
        _prediction_cache_counts = dict(result)
        return result
    except Exception as e:
        log.warning(f"Propagation prediction error: {e}")
        return {gid: 0 for gid in grid_ids}


async def propagate_comparison(winner_id: int, loser_id: int, k: float, action_id: str | None = None):
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
        neighbors = await db.get_active_images_by_ids(all_neighbor_ids)

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

            global last_propagation_count
            updated = await _apply_propagation_deltas(
                conn,
                neighbors,
                deltas,
                action_id=action_id,
            )
            if updated:
                await conn.commit()
                db.invalidate_stats_cache()
                last_propagation_count = updated
                log.debug(f"Propagated Elo to {updated} neighbors "
                         f"(winner={winner_id}, loser={loser_id})")
            else:
                last_propagation_count = 0
        finally:
            await conn.close()

    except Exception as e:
        log.warning(f"Elo propagation error: {e}")


async def propagate_mosaic(winner_id: int, loser_ids: list[int], k: float, action_id: str | None = None):
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

        neighbors = await db.get_active_images_by_ids(list(all_neighbor_ids))

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

            global last_propagation_count
            updated = await _apply_propagation_deltas(
                conn,
                neighbors,
                deltas,
                action_id=action_id,
            )
            if updated:
                await conn.commit()
                db.invalidate_stats_cache()
                last_propagation_count = updated
                log.debug(f"Propagated mosaic to {updated} neighbors "
                         f"(winner={winner_id}, {len(loser_ids)} losers)")
            else:
                last_propagation_count = 0
        finally:
            await conn.close()

    except Exception as e:
        log.warning(f"Mosaic propagation error: {e}")
