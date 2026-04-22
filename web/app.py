import asyncio
import csv
import io
import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import db
import scanner
import thumbnails
import pairing

app = FastAPI(title="photoArchive")
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Track cull history for undo (in-memory stack of {image_id, previous_status})
_cull_history: list[dict] = []


@app.on_event("startup")
async def startup():
    await db.init_db()
    asyncio.create_task(thumbnails.run_prefetch_worker())
    asyncio.create_task(classify_orientations_background())
    try:
        import clip_worker
        asyncio.create_task(clip_worker.run_clip_worker())
    except ImportError:
        pass  # CLIP features disabled — missing open-clip-torch or scikit-learn


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
            await thumbnails.prefetch_images(
                [dict(r) for r in images], "lg"
            )

    asyncio.create_task(scanner.scan_folder(folder, on_batch=on_batch))
    return {"status": "started", "folder": folder}


@app.get("/api/scan/status")
async def scan_status():
    return scanner.scan_state


# --- Thumbnail ---

@app.get("/api/thumb/{size}/{image_id}")
async def serve_thumbnail(size: str, image_id: int):
    if size not in thumbnails.SIZES:
        return JSONResponse({"error": "Invalid size"}, status_code=400)

    image = await db.get_image_by_id(image_id)
    if not image:
        return JSONResponse({"error": "Image not found"}, status_code=404)

    data = await thumbnails.get_thumbnail(image["filepath"], size, image_id)
    if not data:
        return JSONResponse({"error": "Thumbnail generation failed"}, status_code=500)

    return Response(content=data, media_type="image/jpeg")


# --- Cache Status ---

@app.get("/api/cache/status")
async def cache_status(ahead: int = 100):
    """How many of the next N unculled images have thumbnails ready."""
    conn = await db.get_db()
    try:
        cursor = await conn.execute(
            "SELECT id FROM images WHERE status = 'unculled' ORDER BY id LIMIT ?",
            (ahead,),
        )
        rows = await cursor.fetchall()
    finally:
        await conn.close()

    total = len(rows)
    cached = sum(1 for r in rows if thumbnails.has_cached("lg", r["id"]))
    return {"total": total, "cached": cached, "window": ahead}


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
    prefetch_tasks = []

    for img in images:
        d = dict(img)
        prefetch_tasks.append(thumbnails.get_thumbnail(d["filepath"], size, d["id"]))
        result.append({"id": d["id"], "filename": d["filename"], "thumb_url": f"/api/thumb/{size}/{d['id']}"})

    if prefetch_tasks:
        await asyncio.gather(*prefetch_tasks)

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

@app.get("/api/mosaic/next")
async def mosaic_next(n: int = 12, exclude: str = "", strategy: str = "explore", grid_elo: float = 0):
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
    # Effective Elo: use direct if compared, predicted if not, 1200 as fallback
    for img in candidates:
        img["effective_elo"] = img["elo"] if img["comparisons"] > 0 else (img.get("predicted_elo") or img["elo"])
    count = min(n, len(candidates))

    if strategy == "learn":
        # AI-guided: favor images the taste model is most uncertain about
        weights = []
        for img in candidates:
            u = img.get("uncertainty")
            if u is not None:
                weights.append(u + 0.01)  # small floor to avoid zero
            else:
                weights.append(1.0 / (img["comparisons"] + 1))  # fallback to explore
    elif strategy == "explore":
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
            "thumb_url": f"/api/thumb/sm/{img['id']}",
        })

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
    prefetch_tasks = []
    for left, right in pairs:
        prefetch_tasks.append(thumbnails.get_thumbnail(left["filepath"], "md", left["id"]))
        prefetch_tasks.append(thumbnails.get_thumbnail(right["filepath"], "md", right["id"]))
        result.append({
            "left": {"id": left["id"], "filename": left["filename"], "elo": round(left["elo"], 1), "thumb_url": f"/api/thumb/md/{left['id']}"},
            "right": {"id": right["id"], "filename": right["filename"], "elo": round(right["elo"], 1), "thumb_url": f"/api/thumb/md/{right['id']}"},
        })

    # Ensure all thumbnails are ready before responding
    if prefetch_tasks:
        await asyncio.gather(*prefetch_tasks)

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
):
    images = await db.get_rankings(
        limit=limit, offset=offset, sort=sort,
        orientation=orientation, compared=compared, min_stars=min_stars,
    )
    result = []
    for img in images:
        d = dict(img)
        predicted = d.get("predicted_elo")
        result.append({
            "id": d["id"],
            "filename": d["filename"],
            "elo": round(d["elo"], 1),
            "predicted_elo": round(predicted, 1) if predicted is not None else None,
            "comparisons": d["comparisons"],
            "status": d["status"],
            "aspect_ratio": d.get("aspect_ratio") or 1.5,
            "thumb_url": f"/api/thumb/sm/{d['id']}",
        })
    return {"images": result}


@app.get("/api/export")
async def export_rankings(format: str = "json"):
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
    """Search images by text query using CLIP text-image similarity."""
    if not q.strip():
        return {"images": [], "query": q}

    try:
        import clip_worker
        import numpy as np
    except ImportError:
        return JSONResponse({"error": "CLIP not available"}, status_code=503)

    text_vec = await asyncio.get_event_loop().run_in_executor(None, clip_worker.encode_text, q)
    if text_vec is None:
        return JSONResponse({"error": "CLIP model still loading"}, status_code=503)

    all_embeddings = await db.get_all_embeddings()
    if not all_embeddings:
        return {"images": [], "query": q}

    # Compute cosine similarity against all image embeddings
    image_ids = [row["image_id"] for row in all_embeddings]
    matrix = np.array([clip_worker.blob_to_vec(row["embedding"]) for row in all_embeddings])
    similarities = matrix @ text_vec  # already L2-normalized, so dot product = cosine sim

    # Get top results
    top_indices = np.argsort(similarities)[::-1][:limit]
    top_ids = [image_ids[i] for i in top_indices]
    top_scores = [float(similarities[i]) for i in top_indices]

    # Fetch image details
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
        import clip_worker
        import numpy as np
    except ImportError:
        return JSONResponse({"error": "Embeddings not available"}, status_code=503)

    # Get the source image's embedding
    conn = await db.get_db()
    try:
        cursor = await conn.execute("SELECT embedding FROM embeddings WHERE image_id = ?", (image_id,))
        row = await cursor.fetchone()
    finally:
        await conn.close()

    if not row:
        return JSONResponse({"error": "Image not embedded yet"}, status_code=404)

    source_vec = clip_worker.blob_to_vec(row["embedding"])

    all_embeddings = await db.get_all_embeddings()
    if not all_embeddings:
        return {"images": [], "source_id": image_id}

    # Compute cosine similarity against all other images
    image_ids = [r["image_id"] for r in all_embeddings]
    import numpy as np
    matrix = np.array([clip_worker.blob_to_vec(r["embedding"]) for r in all_embeddings])
    similarities = matrix @ source_vec

    # Exclude the source image and get top results
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


@app.get("/api/stats")
async def api_stats():
    return await db.get_stats()


@app.get("/api/ai/status")
async def ai_status():
    """CLIP embedding and taste model status for the bottom bar."""
    embedded = await db.get_embedding_count()
    stats = await db.get_stats()
    total_kept = stats["kept"] + stats["maybe"]
    conn = await db.get_db()
    try:
        cursor = await conn.execute(
            "SELECT COUNT(*) as c FROM images WHERE predicted_elo IS NOT NULL"
        )
        predicted = (await cursor.fetchone())["c"]
        cursor = await conn.execute(
            "SELECT COUNT(*) as c FROM images WHERE comparisons > 0"
        )
        compared = (await cursor.fetchone())["c"]
    finally:
        await conn.close()
    return {
        "embedded": embedded,
        "total_kept": total_kept,
        "model_trained": predicted > 0,
        "predicted": predicted,
        "compared": compared,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
