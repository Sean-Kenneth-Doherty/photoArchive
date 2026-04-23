import asyncio
import csv
import io
import os

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import ai_models
import db
import elo_propagation
import pairing
import scanner
import settings
import thumbnails

app = FastAPI(title="photoArchive")
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Track cull history for undo (in-memory stack of {image_id, previous_status})
_cull_history: list[dict] = []
_BROWSER_IMAGE_EXTENSIONS = {".avif", ".bmp", ".gif", ".jpeg", ".jpg", ".png", ".webp"}
_IDLE_ACTIVITY_EXCLUDED_PATHS = {
    "/api/ai/status",
    "/api/cache/status",
    "/api/cache/pregen/status",
    "/api/scan/status",
    "/api/settings",
}


@app.middleware("http")
async def track_idle_activity(request: Request, call_next):
    path = request.url.path
    if not path.startswith("/static") and path not in _IDLE_ACTIVITY_EXCLUDED_PATHS:
        thumbnails.note_user_activity()
    return await call_next(request)


@app.on_event("startup")
async def startup():
    await db.init_db()
    thumbnails.configure(settings.load_settings())
    asyncio.create_task(thumbnails.run_prefetch_worker())
    asyncio.create_task(classify_orientations_background())
    try:
        import embedding_worker
        asyncio.create_task(embedding_worker.run_embedding_worker())
    except ImportError:
        pass  # AI features disabled — missing dependencies


async def classify_orientations_background():
    """Continuously classify unclassified images by reading just the image header."""
    from PIL import Image as PILImage
    loop = asyncio.get_event_loop()

    def _classify_batch(rows):
        results = []
        for row in rows:
            try:
                img = PILImage.open(row["filepath"])
                w, h = img.size
                img.close()
                orient = "landscape" if w >= h else "portrait"
                ar = round(w / h, 4) if h > 0 else 1.5
                results.append((orient, ar, row["id"]))
            except Exception:
                results.append(("landscape", 1.5, row["id"]))
        return results

    while True:
        try:
            rows = await db.get_unclassified_images(limit=200)
            if not rows:
                await asyncio.sleep(5)
                continue
            results = await loop.run_in_executor(None, _classify_batch, rows)
            if results:
                await db.batch_set_orientations(results)
            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"Orientation classifier error: {e}")
            await asyncio.sleep(5)


@app.on_event("shutdown")
async def shutdown():
    thumbnails.stop_prefetch()


# --- Pages ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    stats = await db.get_stats()
    folder = await db.get_scan_folder()
    return templates.TemplateResponse(request, "index.html", {"stats": stats, "folder": folder})


@app.get("/cull", response_class=HTMLResponse)
async def cull_page(request: Request):
    return templates.TemplateResponse(request, "cull.html")


@app.get("/compare", response_class=HTMLResponse)
async def compare_page(request: Request):
    return templates.TemplateResponse(request, "compare.html")


@app.get("/rankings", response_class=HTMLResponse)
async def rankings_page(request: Request):
    return templates.TemplateResponse(request, "library.html")


@app.get("/library", response_class=HTMLResponse)
async def library_page(request: Request):
    return templates.TemplateResponse(request, "library.html")


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    return templates.TemplateResponse(request, "settings.html")


# --- Scan API ---

@app.post("/api/scan")
async def start_scan(request: Request):
    body = await request.json()
    folder = body.get("folder", "")
    if not folder or not os.path.isdir(folder):
        return JSONResponse({"error": "Invalid folder path"}, status_code=400)

    if scanner.scan_state["scanning"]:
        return JSONResponse({"error": "Scan already in progress"}, status_code=409)

    async def on_batch(count):
        # Start prefetching thumbnails for early images
        if count <= 200:
            images = await db.get_unculled_images(limit=50)
            config = settings.get_settings()
            await thumbnails.prefetch_images(
                [dict(r) for r in images],
                "lg",
                limit=min(len(images), config["scan_prefetch_limit"]),
            )

    asyncio.create_task(scanner.scan_folder(folder, on_batch=on_batch))
    return {"status": "started", "folder": folder}


@app.get("/api/scan/status")
async def scan_status():
    return scanner.scan_state


# --- Thumbnail ---

@app.get("/api/thumb/{size}/{image_id}")
async def serve_thumbnail(request: Request, size: str, image_id: int):
    if size not in thumbnails.SIZES:
        return JSONResponse({"error": "Invalid size"}, status_code=400)

    # Fast path: check memory cache, then SSD disk cache — no DB lookup or HDD stat
    data = thumbnails._memory_get_fast(size, image_id)
    if data is None:
        data = await asyncio.get_event_loop().run_in_executor(
            None, thumbnails.fast_disk_read, size, image_id
        )
    if data:
        headers = {
            "Cache-Control": "public, max-age=86400, stale-while-revalidate=604800",
            "ETag": f'"{size}-{image_id}"',
        }
        if request.headers.get("if-none-match") == headers["ETag"]:
            return Response(status_code=304, headers=headers)
        return Response(content=data, media_type="image/jpeg", headers=headers)

    # Slow path: need to generate from source — requires DB lookup for filepath
    image = await db.get_image_by_id(image_id)
    if not image:
        return JSONResponse({"error": "Image not found"}, status_code=404)

    data = await thumbnails.get_thumbnail(image["filepath"], size, image_id)
    if not data:
        return JSONResponse({"error": "Thumbnail generation failed"}, status_code=500)

    headers = thumbnails.response_headers(image["filepath"], size, image_id)
    return Response(content=data, media_type="image/jpeg", headers=headers)


@app.get("/api/full/{image_id}")
async def serve_full_image(request: Request, image_id: int):
    image = await db.get_image_by_id(image_id)
    if not image:
        return JSONResponse({"error": "Image not found"}, status_code=404)

    ext = os.path.splitext(image["filepath"])[1].lower()
    if ext not in _BROWSER_IMAGE_EXTENSIONS:
        headers = thumbnails.response_headers(image["filepath"], "lg", image_id)
        if request.headers.get("if-none-match") == headers["ETag"]:
            return Response(status_code=304, headers=headers)

        data = await thumbnails.get_thumbnail(image["filepath"], "lg", image_id)
        if not data:
            return JSONResponse({"error": "Preview generation failed"}, status_code=500)
        return Response(content=data, media_type="image/jpeg", headers=headers)

    headers = thumbnails.response_headers(image["filepath"], thumbnails.FULL_TIER, image_id)
    if request.headers.get("if-none-match") == headers["ETag"]:
        return Response(status_code=304, headers=headers)

    path = await thumbnails.get_full_image_path(image["filepath"], image_id)
    if not path or not os.path.exists(path):
        return JSONResponse({"error": "Full image unavailable"}, status_code=404)

    return FileResponse(path, headers=headers)


# --- Cache Status ---

async def build_cache_status(ahead: int = 100):
    stats = await db.get_stats()
    target_total = stats["kept"] + stats["maybe"]
    cache = thumbnails.cache_stats()

    memory = cache["memory"]
    disk = cache["disk"]
    memory["utilization_pct"] = round(
        (memory["used_bytes"] / memory["limit_bytes"]) * 100,
        1,
    ) if memory["limit_bytes"] > 0 else 0.0
    disk["utilization_pct"] = round(
        (disk["used_bytes"] / disk["limit_bytes"]) * 100,
        1,
    ) if disk["limit_bytes"] > 0 else 0.0

    for tier_name, tier in disk["tiers"].items():
        progress_total = target_total if tier_name in thumbnails.THUMB_TIERS else stats["total_images"]
        tier["progress_total"] = progress_total
        tier["progress_pct"] = round((tier["count"] / progress_total) * 100, 1) if progress_total > 0 else 0.0
        tier["utilization_pct"] = round(
            (tier["bytes"] / tier["budget_bytes"]) * 100,
            1,
        ) if tier["budget_bytes"] > 0 else 0.0

    result = {
        **cache,
        "eligible_images": target_total,
        "pregen": thumbnails.get_pregen_status(target_total),
    }

    if ahead > 0:
        conn = await db.get_db()
        try:
            cursor = await conn.execute(
                "SELECT id, filepath FROM images WHERE status = 'unculled' ORDER BY id LIMIT ?",
                (ahead,),
            )
            rows = await cursor.fetchall()
        finally:
            await conn.close()

        result["total"] = len(rows)
        result["cached"] = sum(
            1 for row in rows if thumbnails.has_cached("lg", row["filepath"], row["id"])
        )
    else:
        result["total"] = 0
        result["cached"] = 0
    result["window"] = ahead
    return result


@app.get("/api/cache/status")
async def cache_status(ahead: int = 100):
    return await build_cache_status(ahead=ahead)


@app.post("/api/cache/pregen/start")
async def cache_pregen_start():
    thumbnails.start_pregeneration()
    return {"ok": True, "cache": await build_cache_status(ahead=0)}


@app.post("/api/cache/pregen/stop")
async def cache_pregen_stop():
    thumbnails.stop_pregeneration()
    return {"ok": True, "cache": await build_cache_status(ahead=0)}


@app.get("/api/cache/pregen/status")
async def cache_pregen_status():
    stats = await db.get_stats()
    return thumbnails.get_pregen_status(stats["kept"] + stats["maybe"])


# --- Settings API ---

@app.get("/api/settings")
async def api_settings():
    model_status = ai_models.get_model_status()
    ai_status = await build_ai_status()
    return {
        "settings": settings.get_settings(),
        "cache_stats": await build_cache_status(ahead=0),
        "model_status": model_status,
        "ai_status": ai_status,
        **settings.settings_metadata(),
    }


@app.post("/api/settings")
async def api_save_settings(request: Request):
    saved = settings.save_settings(await request.json())
    thumbnails.configure(saved)
    return {
        "ok": True,
        "settings": saved,
        "cache_stats": await build_cache_status(ahead=0),
        "model_status": ai_models.get_model_status(),
        "ai_status": await build_ai_status(),
    }


@app.post("/api/settings/reset")
async def api_reset_settings():
    saved = settings.reset_settings()
    thumbnails.configure(saved)
    return {
        "ok": True,
        "settings": saved,
        "cache_stats": await build_cache_status(ahead=0),
        "model_status": ai_models.get_model_status(),
        "ai_status": await build_ai_status(),
    }


@app.post("/api/cache/clear")
async def api_clear_thumbnail_cache():
    result = thumbnails.clear_cache()
    return {
        "ok": True,
        **result,
        "cache_stats": await build_cache_status(ahead=0),
        "ai_status": await build_ai_status(),
    }


@app.post("/api/ai/model/install")
async def api_install_ai_model():
    state = ai_models.start_model_install()
    return {
        "ok": True,
        "install": state,
        "model_status": ai_models.get_model_status(),
        "ai_status": await build_ai_status(),
    }


# --- Cull API ---

@app.get("/api/cull/next")
async def cull_next(n: int = 10, size: str = "lg", orientation: str = ""):
    if size not in thumbnails.SIZES:
        size = "lg"

    if orientation in ("landscape", "portrait"):
        images = await db.get_unculled_by_orientation(orientation, limit=n)
    else:
        images = await db.get_unculled_images(limit=n)

    result = []
    prefetch_rows = []

    for img in images:
        d = dict(img)
        prefetch_rows.append(d)
        result.append({"id": d["id"], "filename": d["filename"], "thumb_url": f"/api/thumb/{size}/{d['id']}"})

    if prefetch_rows:
        config = settings.get_settings()
        await thumbnails.prefetch_images(
            prefetch_rows,
            size,
            limit=min(len(prefetch_rows), config["cull_prefetch_limit"]),
        )

    stats = await db.get_stats()
    return {"images": result, "stats": stats}


@app.post("/api/cull")
async def submit_cull(request: Request):
    body = await request.json()
    image_id = body.get("image_id")
    status = body.get("status")

    if status not in ("kept", "maybe", "rejected"):
        return JSONResponse({"error": "Invalid status"}, status_code=400)

    image = await db.get_image_by_id(image_id)
    if not image:
        return JSONResponse({"error": "Image not found"}, status_code=404)

    # Elo adjustment: kept = win vs field (1200), rejected = loss vs field
    old_elo = image["elo"]
    if status == "kept":
        new_elo, _ = pairing.update_elo(old_elo, 1200.0, k=16.0)
    elif status == "rejected":
        _, new_elo = pairing.update_elo(1200.0, old_elo, k=16.0)
    else:  # maybe
        new_elo = old_elo

    _cull_history.append({"image_id": image_id, "previous_status": image["status"], "previous_elo": old_elo})
    await db.set_image_status_and_elo(image_id, status, new_elo)
    return {"ok": True}


@app.post("/api/cull/batch")
async def submit_cull_batch(request: Request):
    """Grid cull: receive a list of {image_id, status} decisions."""
    body = await request.json()
    decisions_in = body.get("decisions", [])
    if not decisions_in:
        return JSONResponse({"error": "No decisions"}, status_code=400)

    db_decisions = []
    history_batch = []

    for d in decisions_in:
        image = await db.get_image_by_id(d["image_id"])
        if not image:
            continue
        status = d["status"]
        old_elo = image["elo"]

        if status == "kept":
            new_elo, _ = pairing.update_elo(old_elo, 1200.0, k=16.0)
        elif status == "rejected":
            _, new_elo = pairing.update_elo(1200.0, old_elo, k=16.0)
        else:
            new_elo = old_elo

        db_decisions.append((d["image_id"], status, new_elo))
        history_batch.append({"image_id": d["image_id"], "previous_status": image["status"], "previous_elo": old_elo})

    await db.batch_cull(db_decisions)
    _cull_history.extend(history_batch)
    return {"ok": True, "count": len(db_decisions)}


@app.post("/api/cull/undo")
async def cull_undo():
    if not _cull_history:
        return JSONResponse({"error": "Nothing to undo"}, status_code=400)

    entry = _cull_history.pop()
    await db.set_image_status_and_elo(entry["image_id"], entry["previous_status"], entry["previous_elo"])
    return {"ok": True, "image_id": entry["image_id"], "restored_status": entry["previous_status"]}


# --- Mosaic Ranking API ---

async def _diverse_sample(candidates: list[dict], count: int) -> list[dict]:
    """Select images that maximize visual diversity using embedding distance."""
    import random
    if len(candidates) <= count:
        return candidates

    try:
        import numpy as np
        import embed_cache

        # Use shared embedding cache
        image_ids, matrix = await embed_cache.get_matrix()
        if image_ids is None:
            raise ImportError("No embeddings")
        embed_map = {image_ids[i]: matrix[i] for i in range(len(image_ids))}

        # Filter to candidates with embeddings
        with_emb = [(c, embed_map[c["id"]]) for c in candidates if c["id"] in embed_map]
        without_emb = [c for c in candidates if c["id"] not in embed_map]

        if len(with_emb) < count:
            # Not enough embeddings — fall back to random + explore
            sample = [c for c, _ in with_emb]
            remaining = count - len(sample)
            if without_emb and remaining > 0:
                sample.extend(random.sample(without_emb, min(remaining, len(without_emb))))
            return sample

        # Greedy diverse selection: pick first randomly (favor least-compared),
        # then iteratively pick the image most dissimilar from all selected
        explore_weights = [1.0 / (c["comparisons"] + 1) for c, _ in with_emb]
        first = random.choices(range(len(with_emb)), weights=explore_weights, k=1)[0]

        selected_indices = [first]
        vecs = np.array([v for _, v in with_emb])

        for _ in range(count - 1):
            # Compute min similarity to any already-selected image for each candidate
            selected_vecs = vecs[selected_indices]
            sims = vecs @ selected_vecs.T  # (N, len(selected))
            max_sim_to_selected = sims.max(axis=1)  # most similar selected image per candidate

            # Mask already-selected
            for si in selected_indices:
                max_sim_to_selected[si] = 999

            # Pick the one with lowest max similarity (most different from all selected)
            next_idx = int(np.argmin(max_sim_to_selected))
            selected_indices.append(next_idx)

        return [with_emb[i][0] for i in selected_indices]

    except Exception:
        # Fallback to random if embeddings unavailable
        import random
        return random.sample(candidates, min(count, len(candidates)))


@app.get("/api/mosaic/next")
async def mosaic_next(
    n: int = 12, exclude: str = "", strategy: str = "explore", grid_elo: float = 0,
    orientation: str = "", compared: str = "", min_stars: int = 0, folder: str = "",
):
    """Get kept images for mosaic ranking with configurable sampling strategy."""
    exclude_ids = set()
    if exclude:
        exclude_ids = {int(x) for x in exclude.split(",") if x.strip().isdigit()}

    if strategy == "top":
        images = await db.get_top_images(limit=50)
    else:
        images = await db.get_kept_images_for_pairing()

    if len(images) < 2:
        return {"images": [], "total_kept": len(images)}

    import random
    candidates = [dict(img) for img in images if img["id"] not in exclude_ids]

    # Apply filters
    if orientation in ("landscape", "portrait"):
        candidates = [c for c in candidates if c.get("orientation") == orientation]
    if compared == "compared":
        candidates = [c for c in candidates if c["comparisons"] > 0]
    elif compared == "uncompared":
        candidates = [c for c in candidates if c["comparisons"] == 0]
    elif compared == "confident":
        candidates = [c for c in candidates if c["comparisons"] >= 10]
    if min_stars > 0:
        from db import STAR_THRESHOLDS
        threshold = STAR_THRESHOLDS.get(min_stars, 0)
        candidates = [c for c in candidates if c["elo"] >= threshold]
    if folder:
        candidates = [c for c in candidates if f"/{folder}/" in c.get("filepath", "")]
    # Effective Elo: use direct if compared, predicted if not, 1200 as fallback
    for img in candidates:
        img["effective_elo"] = img["elo"]
    count = min(n, len(candidates))

    if strategy == "diverse":
        # Maximize visual diversity: pick images that are most dissimilar from each other
        sample = await _diverse_sample(candidates, count)
    else:
        if strategy == "explore":
            # Favor least-compared images
            weights = [1.0 / (img["comparisons"] + 1) for img in candidates]
        elif strategy == "compete" and grid_elo > 0:
            # Favor images with effective Elo close to the grid average
            weights = [1.0 / (abs(img["effective_elo"] - grid_elo) + 50) for img in candidates]
        elif strategy == "top":
            # Favor highest-rated within the top 50
            weights = [img["effective_elo"] for img in candidates]
        else:
            # Random — uniform
            weights = [1.0 for _ in candidates]

        sample = []
        indices = list(range(len(candidates)))
        for _ in range(count):
            if not indices:
                break
            chosen = random.choices(indices, weights=[weights[i] for i in indices], k=1)[0]
            sample.append(candidates[chosen])
            indices.remove(chosen)

    result = []
    for img in sample:
        result.append({
            "id": img["id"],
            "filename": img["filename"],
            "elo": round(img["effective_elo"], 1),
            "aspect_ratio": img.get("aspect_ratio") or 1.5,
            "thumb_url": f"/api/thumb/sm/{img['id']}",
        })

    if sample:
        config = settings.get_settings()
        await thumbnails.prefetch_images(
            sample,
            "sm",
            limit=min(len(sample), config["mosaic_prefetch_limit"]),
        )

    stats = await db.get_stats()
    return {"images": result, "total_kept": len(images), "stats": stats}


@app.post("/api/mosaic/pick")
async def mosaic_pick(request: Request):
    """
    User picked the best image from the visible mosaic.
    Body: { "winner_id": int, "loser_ids": [int, ...] }
    K=12 per pair.
    """
    body = await request.json()
    picked_id = body.get("winner_id")
    other_ids = body.get("loser_ids", [])

    if not picked_id or not other_ids:
        return JSONResponse({"error": "Need winner_id and loser_ids"}, status_code=400)

    # Single batch query instead of N+1 individual queries
    all_ids = [picked_id] + list(other_ids)
    images = await db.get_images_by_ids(all_ids)

    if picked_id not in images:
        return JSONResponse({"error": "Picked image not found"}, status_code=404)

    picked = images[picked_id]
    picked_elo = picked["elo"]

    conn = await db.get_db()
    try:
        for oid in other_ids:
            other = images.get(oid)
            if not other:
                continue
            new_picked, new_other = pairing.update_elo(picked_elo, other["elo"], k=12.0)

            await conn.execute(
                "INSERT INTO comparisons (winner_id, loser_id, mode, elo_before_winner, elo_before_loser) VALUES (?, ?, 'mosaic', ?, ?)",
                (picked_id, oid, picked_elo, other["elo"]),
            )
            await conn.execute(
                "UPDATE images SET elo = ?, comparisons = comparisons + 1 WHERE id = ?",
                (new_other, oid),
            )
            picked_elo = new_picked

        await conn.execute(
            "UPDATE images SET elo = ?, comparisons = comparisons + ? WHERE id = ?",
            (picked_elo, len(other_ids), picked_id),
        )
        await conn.commit()
    finally:
        await conn.close()

    # Fire-and-forget: propagate Elo to similar images via embeddings
    asyncio.create_task(elo_propagation.propagate_mosaic(picked_id, other_ids, k=12.0))

    return {"ok": True, "new_elo": round(picked_elo, 1), "pairs_recorded": len(other_ids)}


# --- Compare API ---

@app.get("/api/compare/next")
async def compare_next(n: int = 5, mode: str = "swiss"):
    if mode == "topn":
        images = await db.get_top_images(limit=50)
    else:
        images = await db.get_kept_images_for_pairing()

    if len(images) < 2:
        return {"pairs": [], "total_kept": len(images)}

    image_dicts = [dict(img) for img in images]
    past = await db.get_past_matchups()
    pairs = pairing.swiss_pair(image_dicts, past, max_pairs=n)

    result = []
    prefetch_rows = []
    for left, right in pairs:
        prefetch_rows.append(left)
        prefetch_rows.append(right)
        result.append({
            "left": {"id": left["id"], "filename": left["filename"], "elo": round(left["elo"], 1), "thumb_url": f"/api/thumb/md/{left['id']}"},
            "right": {"id": right["id"], "filename": right["filename"], "elo": round(right["elo"], 1), "thumb_url": f"/api/thumb/md/{right['id']}"},
        })

    if prefetch_rows:
        config = settings.get_settings()
        await thumbnails.prefetch_images(
            prefetch_rows,
            "md",
            limit=min(len(prefetch_rows), config["compare_prefetch_limit"]),
        )

    stats = await db.get_stats()
    return {"pairs": result, "total_kept": len(image_dicts), "stats": stats}


@app.post("/api/compare")
async def submit_comparison(request: Request):
    body = await request.json()
    winner_id = body.get("winner_id")
    loser_id = body.get("loser_id")
    mode = body.get("mode", "swiss")

    winner = await db.get_image_by_id(winner_id)
    loser = await db.get_image_by_id(loser_id)

    if not winner or not loser:
        return JSONResponse({"error": "Image not found"}, status_code=404)

    k = pairing.get_k_factor(min(winner["comparisons"], loser["comparisons"]), mode)
    new_winner_elo, new_loser_elo = pairing.update_elo(winner["elo"], loser["elo"], k)

    await db.record_comparison(
        winner_id, loser_id, mode,
        winner["elo"], loser["elo"],
        new_winner_elo, new_loser_elo,
    )

    # Fire-and-forget: propagate Elo to similar images via embeddings
    asyncio.create_task(elo_propagation.propagate_comparison(winner_id, loser_id, k))

    return {
        "ok": True,
        "winner_elo": round(new_winner_elo, 1),
        "loser_elo": round(new_loser_elo, 1),
    }


@app.post("/api/compare/undo")
async def compare_undo():
    result = await db.undo_last_comparison()
    if result:
        return {"ok": True, **result}
    return JSONResponse({"error": "Nothing to undo"}, status_code=400)


# --- Rankings API ---

@app.get("/api/rankings")
async def api_rankings(
    limit: int = 100, offset: int = 0, sort: str = "elo",
    orientation: str = "", compared: str = "", min_stars: int = 0,
    folder: str = "",
):
    images = await db.get_rankings(
        limit=limit, offset=offset, sort=sort,
        orientation=orientation, compared=compared, min_stars=min_stars,
        folder=folder,
    )
    if images:
        await thumbnails.prefetch_images(
            [dict(img) for img in images],
            "sm",
            limit=min(len(images), 48),
        )
    result = []
    for img in images:
        d = dict(img)
        result.append({
            "id": d["id"],
            "filename": d["filename"],
            "elo": round(d["elo"], 1),
            "comparisons": d["comparisons"],
            "status": d["status"],
            "aspect_ratio": d.get("aspect_ratio") or 1.5,
            "thumb_url": f"/api/thumb/sm/{d['id']}",
        })
    return {"images": result}


@app.get("/api/export")
async def export_rankings(format: str = "json", ids: str = ""):
    if ids:
        id_list = [int(x) for x in ids.split(",") if x.strip().isdigit()]
        images_dict = await db.get_images_by_ids(id_list)
        images = [images_dict[i] for i in id_list if i in images_dict]
    else:
        images = await db.get_rankings(limit=10000)
    data = [
        {
            "rank": i + 1,
            "filename": dict(img)["filename"],
            "filepath": dict(img)["filepath"],
            "elo": round(dict(img)["elo"], 1),
            "comparisons": dict(img)["comparisons"],
            "status": dict(img)["status"],
        }
        for i, img in enumerate(images)
    ]

    if format == "csv":
        output = io.StringIO()
        if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=rankings.csv"},
        )

    return data


@app.get("/api/search")
async def api_search(q: str = "", limit: int = 50):
    """Search images by text query using embedding similarity."""
    if not q.strip():
        return {"images": [], "query": q}

    try:
        import embedding_worker
        import numpy as np
        import embed_cache
    except ImportError:
        return JSONResponse({"error": "Embeddings not available"}, status_code=503)

    text_vec = await asyncio.get_event_loop().run_in_executor(None, embedding_worker.encode_text, q)
    if text_vec is None:
        return JSONResponse({"error": "Model still loading"}, status_code=503)

    image_ids, matrix = await embed_cache.get_matrix()
    if image_ids is None:
        return {"images": [], "query": q}

    similarities = matrix @ text_vec
    top_indices = np.argsort(similarities)[::-1][:limit]
    top_ids = [image_ids[i] for i in top_indices]
    top_scores = [float(similarities[i]) for i in top_indices]

    images = await db.get_images_by_ids(top_ids)
    result = []
    for img_id, score in zip(top_ids, top_scores):
        img = images.get(img_id)
        if not img:
            continue
        result.append({
            "id": img_id,
            "filename": img["filename"],
            "elo": round(img["elo"], 1),
            "comparisons": img["comparisons"],
            "similarity": round(score, 4),
            "aspect_ratio": img.get("aspect_ratio") or 1.5,
            "thumb_url": f"/api/thumb/sm/{img_id}",
        })

    return {"images": result, "query": q}


@app.get("/api/similar/{image_id}")
async def api_similar(image_id: int, limit: int = 50):
    """Find visually similar images using embedding cosine similarity."""
    try:
        import numpy as np
        import embed_cache
    except ImportError:
        return JSONResponse({"error": "Embeddings not available"}, status_code=503)

    image_ids, matrix = await embed_cache.get_matrix()
    if image_ids is None:
        return {"images": [], "source_id": image_id}

    source_vec = embed_cache.get_vector(image_id)
    if source_vec is None:
        return JSONResponse({"error": "Image not embedded yet"}, status_code=404)

    similarities = matrix @ source_vec
    top_indices = np.argsort(similarities)[::-1]
    results = []
    images_data = await db.get_images_by_ids(image_ids)
    for idx in top_indices:
        img_id = image_ids[idx]
        if img_id == image_id:
            continue
        img = images_data.get(img_id)
        if not img:
            continue
        results.append({
            "id": img_id,
            "filename": img["filename"],
            "elo": round(img["elo"], 1),
            "comparisons": img["comparisons"],
            "similarity": round(float(similarities[idx]), 4),
            "aspect_ratio": img.get("aspect_ratio") or 1.5,
            "thumb_url": f"/api/thumb/sm/{img_id}",
        })
        if len(results) >= limit:
            break

    return {"images": results, "source_id": image_id}


@app.get("/api/duplicates")
async def api_duplicates(threshold: float = 0.95, limit: int = 100):
    """Find near-duplicate image pairs using embedding similarity."""
    try:
        import numpy as np
        import embed_cache
    except ImportError:
        return JSONResponse({"error": "Embeddings not available"}, status_code=503)

    image_ids, matrix = await embed_cache.get_matrix()
    if image_ids is None or len(image_ids) < 2:
        return {"pairs": []}

    sim_matrix = matrix @ matrix.T
    pairs = []
    for i in range(len(image_ids)):
        for j in range(i + 1, len(image_ids)):
            if sim_matrix[i, j] >= threshold:
                pairs.append((image_ids[i], image_ids[j], float(sim_matrix[i, j])))
        if len(pairs) >= limit:
            break

    # Fetch image details
    all_ids = list({p[0] for p in pairs} | {p[1] for p in pairs})
    images = await db.get_images_by_ids(all_ids) if all_ids else {}

    result = []
    for id_a, id_b, sim in pairs[:limit]:
        a, b = images.get(id_a), images.get(id_b)
        if not a or not b:
            continue
        result.append({
            "similarity": round(sim, 4),
            "a": {"id": id_a, "filename": a["filename"], "elo": round(a["elo"], 1), "thumb_url": f"/api/thumb/sm/{id_a}"},
            "b": {"id": id_b, "filename": b["filename"], "elo": round(b["elo"], 1), "thumb_url": f"/api/thumb/sm/{id_b}"},
        })

    return {"pairs": result}


@app.get("/api/image/{image_id}/exif")
async def api_exif(image_id: int):
    """Extract EXIF metadata from an image."""
    image = await db.get_image_by_id(image_id)
    if not image:
        return JSONResponse({"error": "Image not found"}, status_code=404)

    from PIL import Image as PILImage
    from PIL.ExifTags import TAGS, IFD
    try:
        img = PILImage.open(image["filepath"])
        exif_raw = img.getexif()
        # Also read EXIF IFD (where most camera data lives)
        exif_ifd = {}
        try:
            exif_ifd = exif_raw.get_ifd(IFD.Exif)
        except Exception:
            pass
        img.close()
    except Exception:
        return {"exif": {}}

    # Merge base and IFD tags
    all_tags = {}
    for tag_id, value in exif_raw.items():
        tag_name = TAGS.get(tag_id, "")
        if tag_name:
            all_tags[tag_name] = value
    for tag_id, value in exif_ifd.items():
        tag_name = TAGS.get(tag_id, "")
        if tag_name:
            all_tags[tag_name] = value

    def fmt_rational(val):
        if hasattr(val, 'numerator'):
            return float(val)
        return val

    exif = {}
    # Camera
    make = str(all_tags.get("Make", "")).strip()
    model = str(all_tags.get("Model", "")).strip()
    # Remove make from model if duplicated (e.g., "Canon Canon EOS R5")
    if make and model.startswith(make):
        model = model[len(make):].strip()
    if make:
        exif["camera_make"] = make
    if model:
        exif["camera_model"] = model

    # Lens
    lens = str(all_tags.get("LensModel", "")).strip()
    if lens:
        exif["lens"] = lens

    # Focal length
    fl = all_tags.get("FocalLength")
    if fl is not None:
        exif["focal_length"] = f"{fmt_rational(fl):.0f}mm"
    fl35 = all_tags.get("FocalLengthIn35mmFilm")
    if fl35 is not None:
        exif["focal_length_35mm"] = f"{int(fl35)}mm"

    # Aperture
    fnum = all_tags.get("FNumber")
    if fnum is not None:
        exif["aperture"] = f"f/{fmt_rational(fnum):.1f}"

    # Shutter speed
    exp = all_tags.get("ExposureTime")
    if exp is not None:
        ev = fmt_rational(exp)
        if ev > 0:
            exif["shutter_speed"] = f"1/{int(1/ev)}" if ev < 1 else f"{ev:.1f}"

    # ISO
    iso = all_tags.get("ISOSpeedRatings") or all_tags.get("PhotographicSensitivity")
    if iso is not None:
        exif["iso"] = str(int(iso) if isinstance(iso, (int, float)) else iso)

    # Exposure program
    exp_prog_map = {1: "Manual", 2: "Program", 3: "Aperture Priority", 4: "Shutter Priority"}
    exp_prog = all_tags.get("ExposureProgram")
    if exp_prog in exp_prog_map:
        exif["exposure_program"] = exp_prog_map[exp_prog]

    # White balance
    wb = all_tags.get("WhiteBalance")
    if wb is not None:
        exif["white_balance"] = "Auto" if wb == 0 else "Manual"

    # Flash
    flash = all_tags.get("Flash")
    if flash is not None:
        exif["flash"] = "Fired" if (flash & 1) else "No flash"

    # Dimensions
    w = all_tags.get("ExifImageWidth") or all_tags.get("ImageWidth")
    h = all_tags.get("ExifImageHeight") or all_tags.get("ImageLength")
    if w and h:
        exif["dimensions"] = f"{w} x {h}"

    # Date
    date = all_tags.get("DateTimeOriginal") or all_tags.get("DateTime")
    if date:
        exif["date"] = str(date)

    # File info
    exif["filepath"] = image["filepath"]
    try:
        exif["filesize"] = f"{os.path.getsize(image['filepath']) / (1024*1024):.1f} MB"
    except Exception:
        pass

    return {"exif": exif}


@app.get("/api/collections")
async def api_collections(n_clusters: int = 20):
    """Auto-group images into collections using embedding clustering."""
    try:
        import embedding_worker
        import numpy as np
        import embed_cache
        from sklearn.cluster import KMeans
    except ImportError:
        return JSONResponse({"error": "Dependencies not available"}, status_code=503)

    image_ids, matrix = await embed_cache.get_matrix()
    if image_ids is None or len(image_ids) < n_clusters:
        return {"collections": []}

    kmeans = KMeans(n_clusters=n_clusters, n_init=3, random_state=42)
    labels = kmeans.fit_predict(matrix)

    # Group images by cluster and pick a representative (closest to centroid)
    images_data = await db.get_images_by_ids(image_ids)
    collections = []
    for c in range(n_clusters):
        cluster_indices = [i for i, l in enumerate(labels) if l == c]
        if not cluster_indices:
            continue

        # Find representative: closest to centroid
        centroid = kmeans.cluster_centers_[c]
        cluster_vecs = matrix[cluster_indices]
        dists = np.linalg.norm(cluster_vecs - centroid, axis=1)
        rep_idx = cluster_indices[np.argmin(dists)]
        rep_id = image_ids[rep_idx]
        rep_img = images_data.get(rep_id, {})

        member_ids = [image_ids[i] for i in cluster_indices]
        collections.append({
            "id": c,
            "count": len(cluster_indices),
            "representative": {
                "id": rep_id,
                "filename": rep_img.get("filename", ""),
                "thumb_url": f"/api/thumb/sm/{rep_id}",
            },
            "image_ids": member_ids[:50],  # first 50 for preview
        })

    # Sort by size descending
    collections.sort(key=lambda c: c["count"], reverse=True)
    return {"collections": collections}


@app.get("/api/folders")
async def api_folders():
    """Get folder tree with image counts."""
    conn = await db.get_db()
    try:
        cursor = await conn.execute(
            "SELECT filepath FROM images WHERE status IN ('kept', 'maybe')"
        )
        rows = await cursor.fetchall()
    finally:
        await conn.close()

    # Find common root and build relative folder tree
    if not rows:
        return {"folders": []}

    paths = [os.path.dirname(row["filepath"]) for row in rows]
    root = os.path.commonpath(paths)

    folder_counts = {}
    for p in paths:
        rel = os.path.relpath(p, root)
        # Build hierarchy: each level gets counted
        parts = rel.split(os.sep)
        for depth in range(1, len(parts) + 1):
            key = os.sep.join(parts[:depth])
            folder_counts[key] = folder_counts.get(key, 0) + 1

    # Sort by path and return
    folders = [{"path": k, "count": v, "depth": k.count(os.sep)}
               for k, v in sorted(folder_counts.items())]
    return {"folders": folders, "root": root}


@app.get("/api/stats")
async def api_stats():
    return await db.get_stats()


async def build_ai_status():
    """Embedding worker + model install status for UI surfaces."""
    embedded = await db.get_embedding_count()
    stats = await db.get_stats()
    total_kept = stats["kept"] + stats["maybe"]
    remaining = max(total_kept - embedded, 0)

    worker_status = {}
    try:
        import embedding_worker
        worker_status = embedding_worker.get_worker_status()
    except Exception:
        worker_status = {
            "state": "unavailable",
            "message": "AI worker unavailable",
            "ready": False,
            "model_id": "",
            "model_dir": "",
            "last_error": "",
            "last_batch_size": 0,
            "last_batch_seconds": 0.0,
            "last_embedded_at": None,
            "session_embedded": 0,
            "session_started_at": None,
            "session_embed_seconds": 0.0,
            "recent_images_per_min": 0.0,
            "overall_images_per_min": 0.0,
        }

    model_status = ai_models.get_model_status()

    conn = await db.get_db()
    try:
        cursor = await conn.execute(
            "SELECT COUNT(*) as c FROM images WHERE comparisons > 0"
        )
        compared = (await cursor.fetchone())["c"]
    finally:
        await conn.close()

    recent_rate = float(worker_status.get("recent_images_per_min") or 0.0)
    overall_rate = float(worker_status.get("overall_images_per_min") or 0.0)
    effective_rate = recent_rate if recent_rate > 0 else overall_rate
    eta_seconds = int((remaining / effective_rate) * 60) if remaining > 0 and effective_rate > 0 else None
    progress_pct = round((embedded / total_kept) * 100, 1) if total_kept > 0 else 0.0

    return {
        "embedded": embedded,
        "total_kept": total_kept,
        "remaining": remaining,
        "progress_pct": progress_pct,
        "compared": compared,
        "model_installed": model_status["installed"],
        "installing": model_status["install"]["running"],
        "install_status": model_status["install"]["status"],
        "install_message": model_status["install"]["message"],
        "model_id": model_status["model_id"],
        "model_dir": model_status["model_dir"],
        "worker_state": worker_status["state"],
        "worker_message": worker_status["message"],
        "worker_ready": worker_status["ready"],
        "worker_error": worker_status["last_error"],
        "last_batch_size": worker_status.get("last_batch_size", 0),
        "last_batch_seconds": worker_status.get("last_batch_seconds", 0.0),
        "last_embedded_at": worker_status.get("last_embedded_at"),
        "session_embedded": worker_status.get("session_embedded", 0),
        "session_started_at": worker_status.get("session_started_at"),
        "session_embed_seconds": worker_status.get("session_embed_seconds", 0.0),
        "recent_images_per_min": recent_rate,
        "overall_images_per_min": overall_rate,
        "eta_seconds": eta_seconds,
    }


@app.get("/api/ai/status")
async def ai_status():
    """CLIP embedding and taste model status for the bottom bar."""
    return await build_ai_status()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
