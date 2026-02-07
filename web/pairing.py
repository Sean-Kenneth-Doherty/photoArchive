import random


def update_elo(winner_elo: float, loser_elo: float, k: float) -> tuple[float, float]:
    """Standard Elo rating update."""
    expected_winner = 1.0 / (1.0 + 10.0 ** ((loser_elo - winner_elo) / 400.0))
    new_winner = winner_elo + k * (1.0 - expected_winner)
    new_loser = loser_elo + k * (expected_winner - 1.0)
    return new_winner, new_loser


def get_k_factor(comparisons: int, mode: str) -> float:
    """Dynamic K-factor based on comparison count and mode."""
    if mode == "topn":
        return 16.0
    if comparisons < 10:
        return 40.0
    return 20.0


def swiss_pair(images: list[dict], past_matchups: set[tuple[int, int]], max_pairs: int = 5) -> list[tuple[dict, dict]]:
    """
    Swiss-system pairing: sort by Elo, pair adjacent with slight randomization.

    images: list of dicts with at least 'id' and 'elo' keys
    past_matchups: set of (min_id, max_id) tuples to avoid repeats
    max_pairs: maximum number of pairs to return
    """
    if len(images) < 2:
        return []

    # Sort by Elo descending
    sorted_imgs = sorted(images, key=lambda x: x["elo"], reverse=True)

    # Add slight randomization: swap adjacent items with some probability
    randomized = list(sorted_imgs)
    for i in range(len(randomized) - 1):
        if random.random() < 0.3:  # 30% chance to swap adjacent
            randomized[i], randomized[i + 1] = randomized[i + 1], randomized[i]

    pairs = []
    used = set()

    for i in range(0, len(randomized) - 1):
        if len(pairs) >= max_pairs:
            break
        if randomized[i]["id"] in used:
            continue

        # Try to pair with the next unused image
        for j in range(i + 1, min(i + 6, len(randomized))):  # Look ahead up to 5 positions
            if randomized[j]["id"] in used:
                continue

            pair_key = (
                min(randomized[i]["id"], randomized[j]["id"]),
                max(randomized[i]["id"], randomized[j]["id"]),
            )

            if pair_key not in past_matchups:
                pairs.append((randomized[i], randomized[j]))
                used.add(randomized[i]["id"])
                used.add(randomized[j]["id"])
                break

    # If we couldn't find enough fresh pairs, allow repeats
    if len(pairs) < max_pairs:
        for i in range(0, len(randomized) - 1):
            if len(pairs) >= max_pairs:
                break
            if randomized[i]["id"] in used:
                continue
            for j in range(i + 1, min(i + 4, len(randomized))):
                if randomized[j]["id"] in used:
                    continue
                pairs.append((randomized[i], randomized[j]))
                used.add(randomized[i]["id"])
                used.add(randomized[j]["id"])
                break

    # Randomize left/right position
    return [(b, a) if random.random() < 0.5 else (a, b) for a, b in pairs]
