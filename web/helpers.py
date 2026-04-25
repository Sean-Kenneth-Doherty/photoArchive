import db


METADATA_FIELDS = (
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
)

_MISSING = object()


def _get(image, key: str, default=None):
    if hasattr(image, "get"):
        return image.get(key, default)
    try:
        return image[key]
    except (KeyError, IndexError, TypeError):
        return default


def _as_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def metadata_payload(image: dict) -> dict:
    return {field: _get(image, field) for field in METADATA_FIELDS}


def ranking_signal_count(image: dict) -> int:
    return _as_int(_get(image, "comparisons")) + _as_int(_get(image, "propagated_updates"))


def has_ranking_signal(image: dict) -> bool:
    return (
        ranking_signal_count(image) > 0
        or abs(_as_float(_get(image, "elo"), 1200.0) - 1200.0) > 0.0001
    )


def visibility_counts(total_images: int, visible_images: int) -> dict:
    total = max(0, _as_int(total_images))
    visible = max(0, _as_int(visible_images))
    return {
        "visible_images": visible,
        "total_images": total,
        "hidden_pending_thumbnails": max(total - visible, 0),
    }


def _rounded_elo(value) -> float:
    return round(_as_float(value, 1200.0), 1)


def image_card(
    image: dict,
    thumb_size: str = "sm",
    *,
    elo_value=_MISSING,
    similarity=_MISSING,
    date_group=_MISSING,
) -> dict:
    image_id = _as_int(_get(image, "id"))
    card = {
        "id": image_id,
        "filename": _get(image, "filename", ""),
        "elo": _rounded_elo(_get(image, "elo") if elo_value is _MISSING else elo_value),
        "comparisons": _as_int(_get(image, "comparisons")),
        "propagated_updates": _as_int(_get(image, "propagated_updates")),
        "status": _get(image, "status") or "kept",
        "flag": _get(image, "flag") or "unflagged",
        "aspect_ratio": _as_float(_get(image, "aspect_ratio"), 1.5) or 1.5,
        **metadata_payload(image),
        "thumb_url": f"/api/thumb/{thumb_size}/{image_id}",
    }
    if similarity is not _MISSING:
        card["similarity"] = None if similarity is None else round(_as_float(similarity), 4)
    if date_group is not _MISSING:
        card["date_group"] = date_group or ""
    return card


def date_group_for_image(image: dict) -> str:
    date_taken = str(_get(image, "date_taken") or "")
    return date_taken[:7] if len(date_taken) >= 7 else ""


def camera_label(image: dict) -> str:
    return " ".join(
        str(part).strip()
        for part in (_get(image, "camera_make"), _get(image, "camera_model"))
        if part
    ).strip()


def filter_by_metadata(
    images: list[dict],
    date_taken: str = "",
    file_type: str = "",
    camera: str = "",
    lens: str = "",
) -> list[dict]:
    if date_taken:
        if date_taken == "undated":
            images = [img for img in images if not img.get("date_taken")]
        elif date_taken.isdigit() and len(date_taken) == 4:
            prefix = f"{date_taken}-"
            images = [img for img in images if str(img.get("date_taken") or "").startswith(prefix)]

    if file_type:
        normalized_type = file_type.lower()
        if not normalized_type.startswith("."):
            normalized_type = f".{normalized_type}"
        images = [img for img in images if (img.get("file_ext") or "").lower() == normalized_type]

    if camera:
        images = [img for img in images if camera_label(img) == camera]

    if lens:
        images = [img for img in images if (img.get("lens") or "") == lens]

    return images


def filter_compare_mosaic_candidates(
    images,
    *,
    exclude_ids: set[int] | None = None,
    orientation: str = "",
    compared: str = "",
    min_stars: int = 0,
    folder: str = "",
    flag: str = "",
    date_taken: str = "",
    file_type: str = "",
    camera: str = "",
    lens: str = "",
) -> list[dict]:
    exclude_ids = exclude_ids or set()
    candidates = [dict(img) for img in images if _as_int(_get(img, "id")) not in exclude_ids]

    if orientation in ("landscape", "portrait"):
        candidates = [c for c in candidates if c.get("orientation") == orientation]
    if compared == "compared":
        candidates = [c for c in candidates if has_ranking_signal(c)]
    elif compared == "uncompared":
        candidates = [c for c in candidates if not has_ranking_signal(c)]
    elif compared == "confident":
        candidates = [c for c in candidates if _as_int(c.get("comparisons")) >= 10]
    if min_stars > 0:
        threshold = db.STAR_THRESHOLDS.get(min_stars, 0)
        candidates = [c for c in candidates if _as_float(c.get("elo"), 1200.0) >= threshold]
    if folder:
        candidates = [c for c in candidates if f"/{folder}/" in c.get("filepath", "")]
    if flag in ("picked", "unflagged", "rejected"):
        candidates = [c for c in candidates if (c.get("flag") or "unflagged") == flag]

    return filter_by_metadata(candidates, date_taken, file_type, camera, lens)


def _chunks(values: list[int], size: int = 900):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _unique_int_ids(values) -> list[int]:
    ids = []
    seen = set()
    for value in values:
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        ids.append(normalized)
    return ids


async def cached_image_ids(image_ids, size: str, cache_root: str) -> set[int]:
    ids = _unique_int_ids(image_ids)
    return await db.get_cached_image_ids(ids, size, cache_root)


async def filter_visible_candidates(candidates: list[dict], size: str, cache_root: str) -> list[dict]:
    if not candidates:
        return []
    cached_ids = await cached_image_ids([c.get("id") for c in candidates], size, cache_root)
    return [c for c in candidates if _as_int(c.get("id")) in cached_ids]


async def visible_ranked_images(
    ranked_ids: list[int],
    limit: int,
    size: str,
    cache_root: str,
) -> list[dict]:
    """Fetch active images in ranked order, skipping IDs without the displayed tier."""
    if limit <= 0 or not ranked_ids:
        return []

    results: list[dict] = []
    unique_ids = _unique_int_ids(ranked_ids)
    for chunk in _chunks(unique_ids):
        cached_ids = await cached_image_ids(chunk, size, cache_root)
        if not cached_ids:
            continue
        active_rows = await db.get_active_images_by_ids([image_id for image_id in chunk if image_id in cached_ids])
        for image_id in chunk:
            row = active_rows.get(image_id)
            if row is None:
                continue
            results.append(row)
            if len(results) >= limit:
                return results
    return results


async def count_visible_ranked_ids(ranked_ids: list[int], size: str, cache_root: str) -> int:
    if not ranked_ids:
        return 0
    visible = 0
    for chunk in _chunks(_unique_int_ids(ranked_ids)):
        cached_ids = await cached_image_ids(chunk, size, cache_root)
        if not cached_ids:
            continue
        active_rows = await db.get_active_images_by_ids([image_id for image_id in chunk if image_id in cached_ids])
        visible += len(active_rows)
    return visible
