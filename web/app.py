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

app = FastAPI(title="PhotoRanker")
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Track cull history for undo (in-memory stack of {image_id, previous_status})
_cull_history: list[dict] = []


@app.on_event("startup")
async def startup():
    await db.init_db()
    asyncio.create_task(thumbnails.run_prefetch_worker())
    asyncio.create_task(classify_orientations_background())


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
                results.append((orient, row["id"]))
            except Exception:
                results.append(("landscape", row["id"]))
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
    return templates.TemplateResponse("index.html", {"request": request, "stats": stats, "folder": folder})


@app.get("/cull", response_class=HTMLResponse)
async def cull_page(request: Request):
    return templates.TemplateResponse("cull.html", {"request": request})


@app.get("/compare", response_class=HTMLResponse)
async def compare_page(request: Request):
    return templates.TemplateResponse("compare.html", {"request": request})


@app.get("/rankings", response_class=HTMLResponse)
async def rankings_page(request: Request):
    return templates.TemplateResponse("rankings.html", {"request": request})


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
async def mosaic_next(n: int = 12, exclude: str = ""):
    """Get kept images for mosaic ranking, excluding IDs already on screen."""
    exclude_ids = set()
    if exclude:
        exclude_ids = {int(x) for x in exclude.split(",") if x.strip().isdigit()}

    images = await db.get_kept_images_for_pairing()
    if len(images) < 2:
        return {"images": [], "total_kept": len(images)}

    import random
    image_dicts = [dict(img) for img in images if img["id"] not in exclude_ids]
    sample = random.sample(image_dicts, min(n, len(image_dicts)))

    result = []
    for img in sample:
        result.append({
            "id": img["id"],
            "filename": img["filename"],
            "elo": round(img["elo"], 1),
            "thumb_url": f"/api/thumb/sm/{img['id']}",
        })

    stats = await db.get_stats()
    return {"images": result, "total_kept": len(images), "stats": stats}


@app.post("/api/mosaic/pick")
async def mosaic_pick(request: Request):
    """
    User picked the worst or best image from the visible mosaic.
    Worst mode body: { "loser_id": int, "winner_ids": [int, ...] }
    Best mode body:  { "winner_id": int, "loser_ids": [int, ...] }
    K=12 per pair.
    """
    body = await request.json()

    # Determine direction
    if "loser_id" in body:
        picked_id = body["loser_id"]
        other_ids = body.get("winner_ids", [])
        picked_is_loser = True
    elif "winner_id" in body:
        picked_id = body["winner_id"]
        other_ids = body.get("loser_ids", [])
        picked_is_loser = False
    else:
        return JSONResponse({"error": "Need loser_id+winner_ids or winner_id+loser_ids"}, status_code=400)

    if not picked_id or not other_ids:
        return JSONResponse({"error": "Need picked image and other images"}, status_code=400)

    picked = await db.get_image_by_id(picked_id)
    if not picked:
        return JSONResponse({"error": "Picked image not found"}, status_code=404)
    picked = dict(picked)

    others = {}
    for oid in other_ids:
        img = await db.get_image_by_id(oid)
        if img:
            others[oid] = dict(img)

    picked_elo = picked["elo"]
    conn = await db.get_db()
    try:
        for oid, other in others.items():
            if picked_is_loser:
                # Other beats picked
                new_other, new_picked = pairing.update_elo(other["elo"], picked_elo, k=12.0)
                winner_id, loser_id = oid, picked_id
                elo_before_w, elo_before_l = other["elo"], picked_elo
            else:
                # Picked beats other
                new_picked, new_other = pairing.update_elo(picked_elo, other["elo"], k=12.0)
                winner_id, loser_id = picked_id, oid
                elo_before_w, elo_before_l = picked_elo, other["elo"]

            await conn.execute(
                "INSERT INTO comparisons (winner_id, loser_id, mode, elo_before_winner, elo_before_loser) VALUES (?, ?, 'mosaic', ?, ?)",
                (winner_id, loser_id, elo_before_w, elo_before_l),
            )
            await conn.execute(
                "UPDATE images SET elo = ?, comparisons = comparisons + 1 WHERE id = ?",
                (new_other, oid),
            )
            picked_elo = new_picked

        await conn.execute(
            "UPDATE images SET elo = ?, comparisons = comparisons + ? WHERE id = ?",
            (picked_elo, len(others), picked_id),
        )
        await conn.commit()
    finally:
        await conn.close()

    return {"ok": True, "new_elo": round(picked_elo, 1), "pairs_recorded": len(others)}


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
async def api_rankings(limit: int = 100, offset: int = 0):
    images = await db.get_rankings(limit=limit, offset=offset)
    result = []
    for img in images:
        d = dict(img)
        result.append({
            "id": d["id"],
            "filename": d["filename"],
            "elo": round(d["elo"], 1),
            "comparisons": d["comparisons"],
            "status": d["status"],
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


@app.get("/api/stats")
async def api_stats():
    return await db.get_stats()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
