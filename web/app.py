import asyncio
import csv
import io
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import ai_models
import db
import elo_propagation
import pairing
import photo_metadata
import resource_governor
import scanner
import settings
import thumbnails

from starlette.middleware.gzip import GZipMiddleware


class SelectiveGZipMiddleware:
    """Compress text/JSON responses without spending CPU on JPEG/full images."""

    def __init__(self, app, minimum_size: int = 1000):
        self.app = app
        self.gzip = GZipMiddleware(app, minimum_size=minimum_size)

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http":
            path = scope.get("path") or ""
            if path.startswith("/api/thumb/") or path.startswith("/api/full/"):
                await self.app(scope, receive, send)
                return
        await self.gzip(scope, receive, send)


app = FastAPI(title="photoArchive")
app.add_middleware(SelectiveGZipMiddleware, minimum_size=1000)
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

_BROWSER_IMAGE_EXTENSIONS = {".avif", ".bmp", ".gif", ".jpeg", ".jpg", ".png", ".webp"}
_IDLE_ACTIVITY_EXCLUDED_PATHS = {
    "/api/ai/status",
    "/api/cache/status",
    "/api/cache/pregen/status",
    "/api/dev/status",
    "/api/scan/status",
    "/api/settings",
    "/api/ui/settings",
}
_STARTED_AT = time.time()


def _positive_int(value) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _clamp_int(value, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


async def _json_object(request: Request):
    try:
        body = await request.json()
    except Exception:
        return None, JSONResponse({"error": "Malformed JSON body"}, status_code=400)
    if not isinstance(body, dict):
        return None, JSONResponse({"error": "JSON body must be an object"}, status_code=400)
    return body, None


def _ranking_signal_count(image: dict) -> int:
    return int(image.get("comparisons") or 0) + int(image.get("propagated_updates") or 0)


def _has_ranking_signal(image: dict) -> bool:
    return (
        _ranking_signal_count(image) > 0
        or abs(float(image.get("elo") or 1200.0) - 1200.0) > 0.0001
    )


def _git_commit() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(os.path.dirname(__file__)),
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=1,
        ).strip()
    except Exception:
        return None


_GIT_COMMIT = _git_commit()


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
    asyncio.create_task(scan_metadata_background())
    try:
        import embedding_worker
        asyncio.create_task(embedding_worker.run_embedding_worker())
    except ImportError:
        pass  # AI features disabled — missing dependencies

    # Pre-warm embed cache in background so first search/similar is fast
    async def _warm_embed_cache():
        await asyncio.sleep(0)  # yield once then warm immediately
        try:
            import embed_cache
            await embed_cache.get_matrix()
        except Exception:
            pass
    asyncio.create_task(_warm_embed_cache())

    async def _warm_interaction_caches():
        await asyncio.sleep(0.25)
        await asyncio.gather(
            _get_pairing_images(),
            _get_past_matchups(),
            return_exceptions=True,
        )
    asyncio.create_task(_warm_interaction_caches())


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
        except Exception as e:
            print(f"Orientation classifier error: {e}")
            await asyncio.sleep(5)


def _metadata_update_tuple(image_id: int, metadata: dict):
    width = metadata.get("width")
    height = metadata.get("height")
    orientation = None
    aspect_ratio = None
    if width and height:
        try:
            width_num = int(width)
            height_num = int(height)
            if height_num > 0:
                orientation = "landscape" if width_num >= height_num else "portrait"
                aspect_ratio = round(width_num / height_num, 4)
        except Exception:
            pass

    return (
        metadata.get("date_taken") or None,
        metadata.get("camera_make") or None,
        metadata.get("camera_model") or None,
        metadata.get("lens") or None,
        metadata.get("file_ext") or None,
        metadata.get("file_size"),
        metadata.get("file_modified_at"),
        width,
        height,
        time.time(),
        photo_metadata.METADATA_EXTRACTOR_VERSION,
        orientation,
        aspect_ratio,
        metadata.get("latitude"),
        metadata.get("longitude"),
        image_id,
    )


async def scan_metadata_background():
    """Backfill EXIF/file metadata used for library filters and sorts."""
    loop = asyncio.get_event_loop()

    def _extract_batch(rows):
        updates = []
        for row in rows:
            metadata = photo_metadata.extract_image_metadata(row["filepath"])
            updates.append(_metadata_update_tuple(row["id"], metadata))
        return updates

    while True:
        try:
            decision = resource_governor.get_background_decision(thumbnails.get_idle_seconds())
            if decision.pause:
                await asyncio.sleep(decision.sleep_seconds)
                continue

            batch_limit = max(10, min(100, int(100 * max(decision.intensity, 0.1))))
            rows = await db.get_images_needing_metadata(
                limit=batch_limit,
                metadata_version=photo_metadata.METADATA_EXTRACTOR_VERSION,
            )
            if not rows:
                await asyncio.sleep(10)
                continue
            updates = await loop.run_in_executor(None, _extract_batch, rows)
            await db.batch_update_metadata(updates)
            _invalidate_pairing_cache()
            await asyncio.sleep(max(0.05, decision.embedding_pause_seconds))
        except Exception as e:
            print(f"Metadata scanner error: {e}")
            await asyncio.sleep(10)


@app.on_event("shutdown")
async def shutdown():
    thumbnails.stop_prefetch()


# --- Pages ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "settings.html")


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


@app.get("/catalog", response_class=HTMLResponse)
async def catalog_page(request: Request):
    return templates.TemplateResponse(request, "settings.html")


@app.get("/api/dev/status")
async def dev_status():
    """Lightweight process/version probe for local server management."""
    return {
        "pid": os.getpid(),
        "started_at": _STARTED_AT,
        "uptime_seconds": round(time.time() - _STARTED_AT, 3),
        "git_commit": _GIT_COMMIT,
        "cwd": os.getcwd(),
    }


# --- Scan API ---

@app.post("/api/scan")
async def start_scan(request: Request):
    body, error = await _json_object(request)
    if error:
        return error
    folder = body.get("folder", "")
    if not folder or not os.path.isdir(folder):
        return JSONResponse({"error": "Invalid folder path"}, status_code=400)

    if scanner.scan_state["scanning"]:
        return JSONResponse({"error": "Scan already in progress"}, status_code=409)

    async def on_batch(count):
        # Start prefetching thumbnails for early images.
        if count <= 200:
            images = await db.get_recent_active_images(limit=50)
            config = settings.get_settings()
            await thumbnails.prefetch_images(
                [dict(r) for r in images],
                "lg",
                limit=min(len(images), config["scan_prefetch_limit"]),
            )

    source = await db.add_or_restore_source(folder)
    asyncio.create_task(scanner.scan_folder(source["path"], source_id=source["id"], on_batch=on_batch))
    _invalidate_pairing_cache(matchups=True)
    _invalidate_folders_cache()
    try:
        import embed_cache
        embed_cache.invalidate()
    except Exception:
        pass
    return {"status": "started", "folder": source["path"], "source_id": source["id"]}


@app.get("/api/scan/status")
async def scan_status():
    return scanner.scan_state


@app.get("/api/scan/folder")
async def scan_folder():
    folder = await db.get_scan_folder()
    return {"folder": folder or ""}


async def _scan_prefetch_on_batch(count):
    # Start prefetching thumbnails for early images.
    if count <= 200:
        images = await db.get_recent_active_images(limit=50)
        config = settings.get_settings()
        await thumbnails.prefetch_images(
            [dict(r) for r in images],
            "lg",
            limit=min(len(images), config["scan_prefetch_limit"]),
        )


def _quick_browse_roots() -> list[dict]:
    home = os.path.expanduser("~")
    candidates = [
        ("Home", home),
        ("Pictures", os.path.join(home, "Pictures")),
        ("Media", "/media"),
        ("Mounts", "/mnt"),
        ("Run Media", os.path.join("/run/media", os.getenv("USER", ""))),
        ("Volumes", "/Volumes"),
    ]
    roots = []
    seen = set()
    for label, path in candidates:
        normalized = db.normalize_source_path(path)
        if normalized in seen or not os.path.isdir(normalized):
            continue
        seen.add(normalized)
        roots.append({"label": label, "path": normalized})
    return roots


def _folder_picker_start(path: str = "") -> str:
    candidate = db.normalize_source_path(path or os.path.expanduser("~"))
    if os.path.isdir(candidate):
        return candidate
    parent = os.path.dirname(candidate)
    while parent and parent != candidate:
        if os.path.isdir(parent):
            return parent
        candidate = parent
        parent = os.path.dirname(candidate)
    return os.path.expanduser("~")


def _folder_picker_commands(initial: str) -> list[tuple[str, list[str]]]:
    commands: list[tuple[str, list[str]]] = []
    if shutil.which("zenity"):
        commands.append((
            "zenity",
            [
                "zenity",
                "--file-selection",
                "--directory",
                "--title=Select Catalog Folder",
                f"--filename={initial.rstrip(os.sep) + os.sep}",
            ],
        ))
    if shutil.which("kdialog"):
        commands.append((
            "kdialog",
            ["kdialog", "--title", "Select Catalog Folder", "--getexistingdirectory", initial],
        ))
    if shutil.which("yad"):
        commands.append((
            "yad",
            [
                "yad",
                "--file-selection",
                "--directory",
                "--title=Select Catalog Folder",
                f"--filename={initial.rstrip(os.sep) + os.sep}",
            ],
        ))
    if shutil.which("qarma"):
        commands.append((
            "qarma",
            [
                "qarma",
                "--file-selection",
                "--directory",
                "--title=Select Catalog Folder",
                f"--filename={initial.rstrip(os.sep) + os.sep}",
            ],
        ))
    commands.append((
        "tkinter",
        [
            sys.executable,
            "-c",
            (
                "import os, sys\n"
                "try:\n"
                "    import tkinter as tk\n"
                "    from tkinter import filedialog\n"
                "    root = tk.Tk()\n"
                "    root.withdraw()\n"
                "    try:\n"
                "        root.attributes('-topmost', True)\n"
                "    except Exception:\n"
                "        pass\n"
                "    path = filedialog.askdirectory(title='Select Catalog Folder', initialdir=sys.argv[1], mustexist=True)\n"
                "    root.destroy()\n"
                "    if path:\n"
                "        print(path)\n"
                "        raise SystemExit(0)\n"
                "    raise SystemExit(1)\n"
                "except SystemExit:\n"
                "    raise\n"
                "except Exception as exc:\n"
                "    print(str(exc), file=sys.stderr)\n"
                "    raise SystemExit(2)\n"
            ),
            initial,
        ],
    ))
    return commands


def _native_folder_picker_available() -> bool:
    if any(shutil.which(cmd) for cmd in ("zenity", "kdialog", "yad", "qarma")):
        return True
    try:
        import tkinter  # noqa: F401
        return True
    except Exception:
        return False


def _run_native_folder_picker(initial: str) -> dict:
    if not _native_folder_picker_available():
        return {"ok": False, "cancelled": False, "error": "No native folder picker is available"}

    last_error = ""
    for tool_name, command in _folder_picker_commands(initial):
        try:
            proc = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=3600,
            )
        except FileNotFoundError:
            continue
        except subprocess.TimeoutExpired:
            return {"ok": False, "cancelled": False, "error": "Folder picker timed out"}
        except Exception as exc:
            last_error = str(exc)
            continue

        selected = proc.stdout.strip().splitlines()[0] if proc.stdout.strip() else ""
        stderr = proc.stderr.strip()
        if proc.returncode == 0 and selected:
            selected_path = db.normalize_source_path(selected)
            if os.path.isdir(selected_path):
                return {"ok": True, "path": selected_path, "tool": tool_name}
            last_error = "Selected path is not a folder"
            continue
        if proc.returncode in (1, 5) and not selected and not stderr:
            return {"ok": False, "cancelled": True, "tool": tool_name}
        last_error = stderr or f"{tool_name} exited with status {proc.returncode}"

    return {"ok": False, "cancelled": False, "error": last_error or "Folder picker failed"}


@app.get("/api/catalog/folder-picker")
async def api_catalog_folder_picker_status():
    tkinter_available = False
    try:
        import tkinter  # noqa: F401
        tkinter_available = True
    except Exception:
        pass
    return {
        "available": _native_folder_picker_available(),
        "tools": [
            name
            for name, command in _folder_picker_commands(os.path.expanduser("~"))
            if shutil.which(command[0]) and (name != "tkinter" or tkinter_available)
        ],
    }


@app.post("/api/catalog/select-folder")
async def api_catalog_select_folder(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    initial = _folder_picker_start(body.get("path") or "")
    result = await asyncio.to_thread(_run_native_folder_picker, initial)
    if not result.get("ok") and not result.get("cancelled"):
        return JSONResponse(result, status_code=503)
    return result


@app.get("/api/catalog/browse")
async def api_catalog_browse(path: str = ""):
    current = db.normalize_source_path(path or os.path.expanduser("~"))
    roots = _quick_browse_roots()
    result = {
        "path": current,
        "parent": os.path.dirname(current.rstrip(os.sep)) or current,
        "exists": os.path.exists(current),
        "is_dir": os.path.isdir(current),
        "readable": os.access(current, os.R_OK | os.X_OK) if os.path.isdir(current) else False,
        "roots": roots,
        "entries": [],
        "error": "",
    }
    if not result["exists"]:
        result["error"] = "Path does not exist"
        return result
    if not result["is_dir"]:
        result["error"] = "Path is not a directory"
        return result
    if not result["readable"]:
        result["error"] = "Directory is not readable"
        return result

    try:
        entries = []
        with os.scandir(current) as scan:
            for entry in scan:
                try:
                    if not entry.is_dir(follow_symlinks=True):
                        continue
                    entry_path = db.normalize_source_path(entry.path)
                    entries.append({
                        "name": entry.name,
                        "path": entry_path,
                        "readable": os.access(entry_path, os.R_OK | os.X_OK),
                    })
                except OSError:
                    continue
        result["entries"] = sorted(entries, key=lambda item: item["name"].lower())
    except PermissionError:
        result["readable"] = False
        result["error"] = "Directory is not readable"
    except OSError as exc:
        result["error"] = str(exc)
    return result


@app.get("/api/catalog")
async def api_catalog_summary():
    return await db.get_catalog_summary()


@app.post("/api/catalog/sources")
async def api_add_catalog_source(request: Request):
    body, error = await _json_object(request)
    if error:
        return error
    folder = body.get("path") or body.get("folder") or ""
    scan = body.get("scan", True)
    if not folder or not os.path.isdir(db.normalize_source_path(folder)):
        return JSONResponse({"error": "Invalid folder path"}, status_code=400)
    if scan and scanner.scan_state["scanning"]:
        return JSONResponse({"error": "Scan already in progress"}, status_code=409)

    source = await db.add_or_restore_source(folder)
    if scan:
        asyncio.create_task(scanner.scan_folder(source["path"], source_id=source["id"], on_batch=_scan_prefetch_on_batch))
    _invalidate_pairing_cache(matchups=True)
    _invalidate_folders_cache()
    try:
        import embed_cache
        embed_cache.invalidate()
    except Exception:
        pass
    return {
        "ok": True,
        "source": dict(source),
        "scan_started": bool(scan),
        "catalog": await db.get_catalog_summary(),
    }


@app.post("/api/catalog/sources/{source_id}/rescan")
async def api_rescan_catalog_source(source_id: int):
    source = await db.get_source(source_id)
    if not source:
        return JSONResponse({"error": "Source not found"}, status_code=404)
    if not os.path.isdir(source["path"]):
        return JSONResponse({"error": "Source folder is offline"}, status_code=400)
    if scanner.scan_state["scanning"]:
        return JSONResponse({"error": "Scan already in progress"}, status_code=409)

    restored = await db.add_or_restore_source(source["path"])
    asyncio.create_task(scanner.scan_folder(restored["path"], source_id=restored["id"], on_batch=_scan_prefetch_on_batch))
    _invalidate_pairing_cache(matchups=True)
    _invalidate_folders_cache()
    try:
        import embed_cache
        embed_cache.invalidate()
    except Exception:
        pass
    return {"ok": True, "source": dict(restored), "scan_started": True}


@app.post("/api/catalog/sources/{source_id}/remove")
async def api_remove_catalog_source(source_id: int, request: Request):
    body, error = await _json_object(request)
    if error:
        return error
    mode = body.get("mode", "keep")
    source = await db.get_source(source_id)
    if not source:
        return JSONResponse({"error": "Source not found"}, status_code=404)
    if scanner.scan_state["scanning"] and scanner.scan_state.get("source_id") == source_id:
        return JSONResponse({"error": "Cannot remove a source while it is scanning"}, status_code=409)

    if mode == "keep":
        await db.remove_source_keep_data(source_id)
        action = {"kept_data": True, "images_deleted": 0, "comparisons_deleted": 0}
        try:
            import embed_cache
            embed_cache.invalidate()
        except Exception:
            pass
    elif mode in ("delete", "purge"):
        image_ids = await db.get_source_image_ids(source_id)
        cache_result = thumbnails.purge_image_cache(image_ids)
        purge_result = await db.purge_source_catalog_data(source_id)
        action = {"kept_data": False, **purge_result, "cache": cache_result}
        try:
            import embed_cache
            embed_cache.invalidate()
        except Exception:
            pass
    else:
        return JSONResponse({"error": "Invalid removal mode"}, status_code=400)

    _invalidate_pairing_cache(matchups=True)
    _invalidate_folders_cache()
    return {"ok": True, "source_id": source_id, **action, "catalog": await db.get_catalog_summary()}


# --- Thumbnail ---

@app.get("/api/thumb/{size}/{image_id}")
async def serve_thumbnail(request: Request, size: str, image_id: int, cached: bool = False):
    if size not in thumbnails.SIZES:
        return JSONResponse({"error": "Invalid size"}, status_code=400)

    # Fast path: check memory cache, then SSD disk cache — no DB lookup or HDD stat.
    # The cached signature is strong enough for browser revalidation and avoids
    # the old "size-id" ETag that could mask regenerated thumbnails.
    entry = thumbnails._memory_get_entry_fast(size, image_id)
    if entry is None:
        entry = await asyncio.get_event_loop().run_in_executor(
            None,
            thumbnails.fast_disk_read_entry,
            size,
            image_id,
            None,
        )
        if entry is not None:
            signature, data = entry
            thumbnails._memory_put(size, image_id, signature, data)
    if entry is not None:
        signature, data = entry
        headers = {
            "Cache-Control": (
                f"public, max-age={thumbnails.BROWSER_CACHE_MAX_AGE}, "
                f"stale-while-revalidate={thumbnails.BROWSER_CACHE_STALE_WHILE_REVALIDATE}"
            ),
            "ETag": f'"{signature}"',
        }
        if request.headers.get("if-none-match") == headers["ETag"]:
            return Response(status_code=304, headers=headers)
        return Response(content=data, media_type="image/jpeg", headers=headers)
    if cached:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})

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
async def serve_full_image(request: Request, image_id: int, background_tasks: BackgroundTasks, cached: bool = False):
    if cached:
        entry = thumbnails.fast_disk_path_entry(thumbnails.FULL_TIER, image_id)
        if entry is None:
            return Response(status_code=204, headers={"Cache-Control": "no-store"})
        signature, path = entry
        headers = {
            "Cache-Control": (
                f"public, max-age={thumbnails.BROWSER_CACHE_MAX_AGE}, "
                f"stale-while-revalidate={thumbnails.BROWSER_CACHE_STALE_WHILE_REVALIDATE}"
            ),
            "ETag": f'"{signature}"',
        }
        if request.headers.get("if-none-match") == headers["ETag"]:
            return Response(status_code=304, headers=headers)
        return FileResponse(path, headers=headers)

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

    path = thumbnails.get_cached_full_image_path(image["filepath"], image_id)
    if path is None:
        path = image["filepath"]
        background_tasks.add_task(thumbnails.schedule_full_image_cache, image["filepath"], image_id)

    if not path or not os.path.exists(path):
        return JSONResponse({"error": "Full image unavailable"}, status_code=404)

    return FileResponse(path, headers=headers)


@app.get("/api/image/{image_id}/media-status")
async def image_media_status(image_id: int):
    tiers = {}
    for size in thumbnails.THUMB_TIERS:
        cached = thumbnails.has_cached_fast(size, image_id)
        tiers[size] = {
            "cached": cached,
            "url": f"/api/thumb/{size}/{image_id}",
            "cached_url": f"/api/thumb/{size}/{image_id}?cached=1",
        }

    full_cached = thumbnails.fast_disk_path_entry(thumbnails.FULL_TIER, image_id) is not None
    tiers[thumbnails.FULL_TIER] = {
        "cached": full_cached,
        "url": f"/api/full/{image_id}",
        "cached_url": f"/api/full/{image_id}?cached=1",
    }
    best_cached = next(
        (tier for tier in (thumbnails.FULL_TIER, "lg", "md", "sm") if tiers.get(tier, {}).get("cached")),
        None,
    )
    return {"id": image_id, "tiers": tiers, "best_cached": best_cached}


@app.post("/api/images/warm")
async def warm_images(request: Request):
    """Mark current/nearby images as hot and schedule SSD cache warming."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    tier_requests = body.get("tiers") or {}
    requested: dict[str, list[int]] = {}
    all_ids: set[int] = set()

    for tier, values in tier_requests.items():
        if tier not in thumbnails.ALL_TIERS:
            continue
        ids = []
        seen_for_tier = set()
        values_iter = values if isinstance(values, (list, tuple, set)) else [values]
        for value in values_iter or []:
            try:
                image_id = int(value)
            except (TypeError, ValueError):
                continue
            if image_id <= 0 or image_id in seen_for_tier:
                continue
            seen_for_tier.add(image_id)
            ids.append(image_id)
            all_ids.add(image_id)
        if ids:
            requested[tier] = ids[:96]

    if not requested or not all_ids:
        return {"scheduled": {}, "images": 0}

    try:
        rows_by_id = await db.get_active_images_by_ids(list(all_ids))
    except (sqlite3.OperationalError, OSError) as exc:
        print(f"Warm image lookup skipped: {exc}")
        return {"scheduled": {tier: 0 for tier in requested}, "images": len(all_ids)}
    except Exception as exc:
        print(f"Warm image lookup skipped: {exc}")
        return {"scheduled": {tier: 0 for tier in requested}, "images": len(all_ids)}

    scheduled = {}
    for tier, ids in requested.items():
        rows = [rows_by_id[image_id] for image_id in ids if image_id in rows_by_id]
        if not rows:
            scheduled[tier] = 0
            continue
        if tier in thumbnails.THUMB_TIERS:
            try:
                scheduled[tier] = await thumbnails.prefetch_images(
                    rows,
                    tier,
                    limit=len(rows),
                    hot=True,
                )
            except (sqlite3.OperationalError, OSError) as exc:
                print(f"Warm {tier} skipped: {exc}")
                scheduled[tier] = 0
            except Exception as exc:
                print(f"Warm {tier} skipped: {exc}")
                scheduled[tier] = 0
        elif tier == thumbnails.FULL_TIER:
            count = 0
            for row in rows[:12]:
                ext = os.path.splitext(row["filepath"])[1].lower()
                if ext not in _BROWSER_IMAGE_EXTENSIONS:
                    continue
                try:
                    await thumbnails.schedule_full_image_cache(row["filepath"], row["id"], hot=True)
                    count += 1
                except (sqlite3.OperationalError, OSError) as exc:
                    print(f"Warm full image {row['id']} skipped: {exc}")
                except Exception as exc:
                    print(f"Warm full image {row['id']} skipped: {exc}")
            scheduled[tier] = count

    return {"scheduled": scheduled, "images": len(all_ids)}


# --- Cache Status ---

def _cache_recommendations(cache: dict, eligible_images: int, total_images: int) -> dict:
    estimates = thumbnails.cache_archive_estimates()
    tiers = {}
    for tier_name in thumbnails.ALL_TIERS:
        avg_bytes = int(estimates.get("avg_bytes", {}).get(tier_name) or thumbnails.estimated_tier_bytes(tier_name))
        target_count = total_images if tier_name == thumbnails.FULL_TIER else eligible_images
        full_archive_bytes = avg_bytes * max(0, int(target_count))
        budget_bytes = int(cache.get("disk", {}).get("tiers", {}).get(tier_name, {}).get("budget_bytes") or 0)
        estimated_cached = int(budget_bytes / avg_bytes) if avg_bytes > 0 else 0
        tiers[tier_name] = {
            "avg_bytes": avg_bytes,
            "sample_count": int(estimates.get("sample_count", {}).get(tier_name) or 0),
            "full_archive_bytes": full_archive_bytes,
            "budget_bytes": budget_bytes,
            "estimated_cached": min(target_count, estimated_cached),
            "coverage_pct": round((budget_bytes / full_archive_bytes) * 100, 1) if full_archive_bytes > 0 else 0.0,
        }

    return {
        "eligible_images": eligible_images,
        "total_images": total_images,
        "budget": thumbnails.cache_budget_config(),
        "tiers": tiers,
    }


async def build_cache_status(ahead: int = 100):
    stats = await db.get_stats()
    active_total = stats["kept"] + stats["maybe"]
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
        progress_total = active_total if tier_name in thumbnails.THUMB_TIERS else stats["total_images"]
        progress_count = (
            tier.get("current_count", 0)
            if tier.get("replacement_mode")
            else tier.get("count", 0)
        )
        tier["progress_total"] = progress_total
        tier["progress_count"] = progress_count
        tier["progress_pct"] = round((progress_count / progress_total) * 100, 1) if progress_total > 0 else 0.0
        tier["utilization_pct"] = round(
            (tier["bytes"] / tier["budget_bytes"]) * 100,
            1,
        ) if tier["budget_bytes"] > 0 else 0.0

    result = {
        **cache,
        "eligible_images": active_total,
        "recommendations": _cache_recommendations(cache, active_total, stats["total_images"]),
        "pregen": thumbnails.get_pregen_status(active_total, cache),
        "governor": resource_governor.get_background_decision(
            thumbnails.get_idle_seconds()
        ).to_dict(),
    }

    if ahead > 0:
        conn = await db.get_db()
        try:
            cursor = await conn.execute(
                "SELECT i.id, i.filepath FROM images i "
                "JOIN catalog_sources s ON s.id = i.source_id "
                "WHERE s.included = 1 AND s.online = 1 AND i.missing_at IS NULL "
                "ORDER BY i.id LIMIT ?",
                (ahead,),
            )
            rows = await cursor.fetchall()
        finally:
            await conn.close()

        result["total"] = len(rows)
        result["cached"] = sum(
            1 for row in rows if thumbnails.has_cached_fast("lg", row["id"])
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


@app.post("/api/ai/embeddings/pause")
async def api_pause_embeddings():
    try:
        import embedding_worker
    except ImportError:
        return JSONResponse({"error": "Embeddings not available"}, status_code=503)
    embedding_worker.pause_embedding_worker()
    return {"ok": True, "ai_status": await build_ai_status()}


@app.post("/api/ai/embeddings/resume")
async def api_resume_embeddings():
    try:
        import embedding_worker
    except ImportError:
        return JSONResponse({"error": "Embeddings not available"}, status_code=503)
    embedding_worker.resume_embedding_worker()
    return {"ok": True, "ai_status": await build_ai_status()}


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
        "catalog": await db.get_catalog_summary(),
        **settings.settings_metadata(),
    }


@app.get("/api/ui/settings")
async def api_ui_settings():
    config = settings.get_settings()
    return {
        "settings": {
            "show_loupe_cache_status": bool(config.get("show_loupe_cache_status", True)),
        }
    }


@app.post("/api/image/{image_id}/flag")
async def api_set_image_flag(image_id: int, request: Request):
    body, error = await _json_object(request)
    if error:
        return error
    flag = body.get("flag", "unflagged")
    if flag not in ("picked", "unflagged", "rejected"):
        return JSONResponse({"error": "Invalid flag"}, status_code=400)

    image = await db.get_image_by_id(image_id)
    if not image:
        return JSONResponse({"error": "Image not found"}, status_code=404)

    await db.set_image_flag(image_id, flag)
    _invalidate_pairing_cache()
    return {"ok": True, "id": image_id, "flag": flag}


@app.post("/api/images/flag")
async def api_batch_set_flag(request: Request):
    body, error = await _json_object(request)
    if error:
        return error
    flag = body.get("flag", "unflagged")
    image_ids = body.get("image_ids", [])
    if flag not in ("picked", "unflagged", "rejected"):
        return JSONResponse({"error": "Invalid flag"}, status_code=400)
    if not image_ids or not isinstance(image_ids, list):
        return JSONResponse({"error": "image_ids must be a non-empty list"}, status_code=400)

    normalized_ids = []
    seen_ids = set()
    for value in image_ids:
        try:
            image_id = int(value)
        except (TypeError, ValueError):
            continue
        if image_id <= 0 or image_id in seen_ids:
            continue
        seen_ids.add(image_id)
        normalized_ids.append(image_id)

    if not normalized_ids:
        return JSONResponse({"error": "No valid image ids"}, status_code=400)

    count = await db.batch_set_image_flags(normalized_ids, flag)
    _invalidate_pairing_cache()
    return {"ok": True, "count": count, "flag": flag}


@app.post("/api/settings")
async def api_save_settings(request: Request):
    body, error = await _json_object(request)
    if error:
        return error
    current = settings.get_settings()
    saved = settings.save_settings(body)
    thumbnail_changed = any(
        int(current.get(field, 0)) != int(saved.get(field, 0))
        for field in ("thumb_size_sm", "thumb_size_md", "thumb_size_lg", "thumb_quality")
    )
    replace_thumbnail_cache = (
        thumbnail_changed
        and str(body.get("thumbnail_cache_policy", "keep")).strip().lower() == "replace"
    )
    thumbnails.configure({**saved, "_replace_thumbnail_cache": replace_thumbnail_cache})
    return {
        "ok": True,
        "settings": saved,
        "cache_stats": await build_cache_status(ahead=0),
        "model_status": ai_models.get_model_status(),
        "ai_status": await build_ai_status(),
        "catalog": await db.get_catalog_summary(),
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
        "catalog": await db.get_catalog_summary(),
    }


@app.post("/api/cache/clear")
async def api_clear_thumbnail_cache():
    result = thumbnails.clear_cache()
    if result.get("refused"):
        return JSONResponse({"ok": False, **result}, status_code=400)
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


# --- Mosaic Ranking API ---

# Cache for active pairing images — patched on direct comparisons and
# refreshed after background Elo propagation touches wider neighborhoods.
_pairing_cache = {"data": None, "by_id": None, "valid": False}
_matchups_cache = {"data": None, "valid": False}

async def _get_pairing_images():
    """Cached wrapper — invalidated by mosaic_pick and submit_comparison."""
    if _pairing_cache["valid"] and _pairing_cache["data"] is not None:
        return _pairing_cache["data"]
    rows = await db.get_active_images_for_pairing()
    images = [dict(row) for row in rows]
    _pairing_cache["data"] = images
    _pairing_cache["by_id"] = {img["id"]: img for img in images}
    _pairing_cache["valid"] = True
    return images

def _invalidate_pairing_cache(*, matchups: bool = False):
    _pairing_cache["valid"] = False
    if matchups:
        _matchups_cache["valid"] = False


async def _get_past_matchups():
    if _matchups_cache["valid"] and _matchups_cache["data"] is not None:
        return _matchups_cache["data"]
    matchups = await db.get_past_matchups()
    _matchups_cache["data"] = matchups
    _matchups_cache["valid"] = True
    return matchups


def _add_past_matchups(pairs: list[tuple[int, int]]):
    if not _matchups_cache["valid"] or _matchups_cache["data"] is None:
        return
    for a, b in pairs:
        _matchups_cache["data"].add((min(a, b), max(a, b)))


def _patch_pairing_cache(updates: list[tuple[int, float, int]]):
    if not _pairing_cache["valid"] or _pairing_cache["by_id"] is None:
        return
    by_id = _pairing_cache["by_id"]
    for image_id, new_elo, comparison_delta in updates:
        img = by_id.get(image_id)
        if img is None:
            continue
        img["elo"] = new_elo
        img["comparisons"] = max(0, int(img.get("comparisons") or 0) + comparison_delta)


def _schedule_pairing_propagation(coro):
    async def _runner():
        try:
            await coro
        except Exception as exc:
            print(f"Elo propagation error: {exc}")
        finally:
            _invalidate_pairing_cache()

    asyncio.create_task(_runner())


def _top_indices_desc(values, limit: int, exclude_index: int | None = None):
    import numpy as np

    if limit <= 0 or len(values) == 0:
        return []
    if exclude_index is not None:
        values = values.copy()
        values[exclude_index] = -np.inf

    limit = min(limit, len(values))
    if len(values) <= limit:
        return np.argsort(values)[::-1]

    candidates = np.argpartition(values, -limit)[-limit:]
    return candidates[np.argsort(values[candidates])[::-1]]


def _camera_label(image: dict) -> str:
    return " ".join(
        str(part).strip()
        for part in (image.get("camera_make"), image.get("camera_model"))
        if part
    ).strip()


def _metadata_payload(image: dict) -> dict:
    return {
        "date_taken": image.get("date_taken"),
        "camera_make": image.get("camera_make"),
        "camera_model": image.get("camera_model"),
        "lens": image.get("lens"),
        "file_ext": image.get("file_ext"),
        "file_size": image.get("file_size"),
        "file_modified_at": image.get("file_modified_at"),
        "width": image.get("width"),
        "height": image.get("height"),
        "latitude": image.get("latitude"),
        "longitude": image.get("longitude"),
        "created_at": image.get("created_at"),
    }


def _visibility_counts(total_images: int, visible_images: int) -> dict:
    total = max(0, int(total_images or 0))
    visible = max(0, int(visible_images or 0))
    return {
        "visible_images": visible,
        "total_images": total,
        "hidden_pending_thumbnails": max(total - visible, 0),
    }


def _cache_root() -> str:
    return thumbnails.SSD_CACHE_DIR


def _chunks(values: list[int], size: int = 900):
    for start in range(0, len(values), size):
        yield values[start:start + size]


async def _cached_image_ids(image_ids, size: str) -> set[int]:
    ids = [int(image_id) for image_id in image_ids if image_id is not None]
    return await db.get_cached_image_ids(ids, size, _cache_root())


async def _filter_visible_candidates(candidates: list[dict], size: str) -> list[dict]:
    if not candidates:
        return []
    cached_ids = await _cached_image_ids([c.get("id") for c in candidates], size)
    return [c for c in candidates if int(c.get("id") or 0) in cached_ids]


async def _visible_ranked_images(ranked_ids: list[int], limit: int, size: str = "sm") -> list[dict]:
    """Fetch active images in ranked order, skipping IDs without the displayed tier."""
    if limit <= 0 or not ranked_ids:
        return []
    results: list[dict] = []
    seen: set[int] = set()
    unique_ids = []
    for image_id in ranked_ids:
        try:
            normalized = int(image_id)
        except (TypeError, ValueError):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_ids.append(normalized)

    for chunk in _chunks(unique_ids):
        cached_ids = await _cached_image_ids(chunk, size)
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


async def _count_visible_ranked_ids(ranked_ids: list[int], size: str = "sm") -> int:
    if not ranked_ids:
        return 0
    visible = 0
    seen: set[int] = set()
    unique_ids = []
    for image_id in ranked_ids:
        try:
            normalized = int(image_id)
        except (TypeError, ValueError):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_ids.append(normalized)
    for chunk in _chunks(unique_ids):
        cached_ids = await _cached_image_ids(chunk, size)
        if not cached_ids:
            continue
        active_rows = await db.get_active_images_by_ids([image_id for image_id in chunk if image_id in cached_ids])
        visible += len(active_rows)
    return visible


def _filter_by_metadata(
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
        images = [img for img in images if _camera_label(img) == camera]

    if lens:
        images = [img for img in images if (img.get("lens") or "") == lens]

    return images


async def _diverse_sample(candidates: list[dict], count: int) -> list[dict]:
    """Select images that maximize visual diversity using embedding distance."""
    import random
    if len(candidates) <= count:
        return candidates

    try:
        import numpy as np
        import embed_cache

        image_ids, matrix = await embed_cache.get_matrix()
        if image_ids is None:
            raise ImportError("No embeddings")

        # Use id_to_idx for O(1) lookups instead of building a full dict copy
        id_to_idx = embed_cache._cache.get("id_to_idx") or {}

        # Bound the expensive per-candidate work for large libraries. The final
        # diversity pool is only 500 images, so a 5k search window keeps the same
        # broad random/exploratory behavior without building arrays over 100k+ rows.
        SEARCH_POOL = max(5000, count * 40)
        search_candidates = candidates
        if len(candidates) > SEARCH_POOL:
            search_candidates = random.sample(candidates, SEARCH_POOL)

        # Filter candidates to those with embeddings, get their matrix indices
        cand_indices = []  # index into matrix
        cand_items = []    # corresponding candidate dicts
        without_emb = []
        for c in search_candidates:
            idx = id_to_idx.get(c["id"])
            if idx is not None:
                cand_indices.append(idx)
                cand_items.append(c)
            else:
                without_emb.append(c)

        # If embeddings are sparse, fall back to scanning the full candidate set
        # so the function still returns enough images instead of letting the cap
        # change behavior for partially embedded libraries.
        if len(cand_items) < count and search_candidates is not candidates:
            seen = {c["id"] for c in search_candidates}
            for c in candidates:
                if c["id"] in seen:
                    continue
                idx = id_to_idx.get(c["id"])
                if idx is not None:
                    cand_indices.append(idx)
                    cand_items.append(c)
                else:
                    without_emb.append(c)
                if len(cand_items) >= count:
                    break

        if len(cand_items) < count:
            sample = list(cand_items)
            remaining = count - len(sample)
            if without_emb and remaining > 0:
                sample.extend(random.sample(without_emb, min(remaining, len(without_emb))))
            return sample

        # Subsample a random pool — skip building the full candidate matrix.
        # Use comparison-count strata instead of np.random.choice(..., p=weights):
        # it keeps the least-compared bias, but avoids normalizing/probability
        # sampling across every candidate.
        POOL = min(500, len(cand_items))
        comp_counts = np.fromiter(
            (c["comparisons"] for c in cand_items),
            dtype=np.float32,
            count=len(cand_items),
        )

        if len(cand_items) > POOL:
            bucket_defs = (
                comp_counts == 0,
                (comp_counts > 0) & (comp_counts <= 2),
                (comp_counts > 2) & (comp_counts <= 5),
                (comp_counts > 5) & (comp_counts <= 10),
                comp_counts > 10,
            )
            bucket_indices = [np.flatnonzero(mask) for mask in bucket_defs]
            bucket_weights = np.array(
                [
                    float((1.0 / (comp_counts[idx] + 1.0)).sum()) if len(idx) else 0.0
                    for idx in bucket_indices
                ],
                dtype=np.float64,
            )

            if bucket_weights.sum() > 0:
                raw_quotas = bucket_weights / bucket_weights.sum() * POOL
                quotas = np.minimum(
                    np.floor(raw_quotas).astype(int),
                    [len(idx) for idx in bucket_indices],
                )
                remaining = POOL - int(quotas.sum())
                fractions = raw_quotas - np.floor(raw_quotas)
                for bucket in np.argsort(fractions)[::-1]:
                    if remaining <= 0:
                        break
                    capacity = len(bucket_indices[bucket]) - quotas[bucket]
                    if capacity <= 0:
                        continue
                    take = min(remaining, capacity)
                    quotas[bucket] += take
                    remaining -= take

                selected_idx = []
                for idx, quota in zip(bucket_indices, quotas):
                    if quota <= 0:
                        continue
                    selected_idx.extend(random.sample(idx.tolist(), int(quota)))

                if len(selected_idx) < POOL:
                    selected_set = set(selected_idx)
                    remaining_idx = [i for i in range(len(cand_items)) if i not in selected_set]
                    selected_idx.extend(random.sample(remaining_idx, POOL - len(selected_idx)))
                pool_idx = np.array(selected_idx, dtype=np.intp)
                np.random.shuffle(pool_idx)
            else:
                pool_idx = np.array(random.sample(range(len(cand_items)), POOL), dtype=np.intp)
        else:
            pool_idx = np.arange(len(cand_items))

        # Build matrix only for the pool (500 x 2048 instead of 20k x 2048)
        pool_matrix_idx = np.fromiter(
            (cand_indices[int(i)] for i in pool_idx),
            dtype=np.intp,
            count=len(pool_idx),
        )
        pool_matrix = matrix[pool_matrix_idx]
        pool_items = [cand_items[int(i)] for i in pool_idx]
        pool_bias = 1.0 / (comp_counts[pool_idx] + 1.0)

        # Random seed for variety
        first = random.randrange(len(pool_items))
        selected = [first]

        # Greedy farthest-point with comparison-count bias
        max_sim = pool_matrix @ pool_matrix[first]

        for _ in range(count - 1):
            max_sim[selected[-1]] = 999.0
            score = max_sim - pool_bias * 0.15
            next_pick = int(np.argmin(score))
            selected.append(next_pick)
            new_sims = pool_matrix @ pool_matrix[next_pick]
            np.maximum(max_sim, new_sims, out=max_sim)

        return [pool_items[i] for i in selected]

    except Exception:
        # Fallback to random if embeddings unavailable
        import random
        return random.sample(candidates, min(count, len(candidates)))


@app.get("/api/mosaic/next")
async def mosaic_next(
    n: int = 12, exclude: str = "", strategy: str = "explore", grid_elo: float = 0,
    orientation: str = "", compared: str = "", min_stars: int = 0, folder: str = "",
    flag: str = "", date_taken: str = "", file_type: str = "", camera: str = "", lens: str = "",
):
    """Get active images for mosaic ranking with configurable sampling strategy."""
    exclude_ids = set()
    if exclude:
        exclude_ids = {int(x) for x in exclude.split(",") if x.strip().isdigit()}

    if strategy == "top":
        images = await db.get_top_images(limit=50)
    else:
        images = await _get_pairing_images()

    if len(images) < 2:
        visible = await _filter_visible_candidates([dict(img) for img in images], "sm")
        counts = _visibility_counts(len(images), len(visible))
        stats = dict(await db.get_stats())
        stats["filtered_pool"] = len(visible)
        stats["filtered_pool_visible"] = len(visible)
        stats["filtered_pool_total"] = len(images)
        return {"images": [], **counts, "total_kept": len(images), "stats": stats}

    import random
    candidates = [dict(img) for img in images if img["id"] not in exclude_ids]

    # Apply filters
    if orientation in ("landscape", "portrait"):
        candidates = [c for c in candidates if c.get("orientation") == orientation]
    if compared == "compared":
        candidates = [c for c in candidates if _has_ranking_signal(c)]
    elif compared == "uncompared":
        candidates = [c for c in candidates if not _has_ranking_signal(c)]
    elif compared == "confident":
        candidates = [c for c in candidates if c["comparisons"] >= 10]
    if min_stars > 0:
        from db import STAR_THRESHOLDS
        threshold = STAR_THRESHOLDS.get(min_stars, 0)
        candidates = [c for c in candidates if c["elo"] >= threshold]
    if folder:
        candidates = [c for c in candidates if f"/{folder}/" in c.get("filepath", "")]
    if flag in ("picked", "unflagged", "rejected"):
        candidates = [c for c in candidates if (c.get("flag") or "unflagged") == flag]
    candidates = _filter_by_metadata(candidates, date_taken, file_type, camera, lens)
    filtered_total = len(candidates)
    candidates = await _filter_visible_candidates(candidates, "sm")
    visible_count = len(candidates)

    # Effective Elo: use direct if compared, predicted if not, 1200 as fallback
    for img in candidates:
        img["effective_elo"] = img["elo"]
    count = min(n, visible_count)

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
            "comparisons": img["comparisons"],
            "propagated_updates": img.get("propagated_updates") or 0,
            "flag": img.get("flag") or "unflagged",
            "aspect_ratio": img.get("aspect_ratio") or 1.5,
            "thumb_url": f"/api/thumb/sm/{img['id']}",
        })

    if sample:
        config = settings.get_settings()
        await thumbnails.prefetch_images(
            sample,
            "md",
            limit=min(len(sample), config["mosaic_prefetch_limit"]),
        )

    stats = dict(await db.get_stats())
    stats["filtered_pool"] = visible_count
    stats["filtered_pool_visible"] = visible_count
    stats["filtered_pool_total"] = filtered_total
    return {
        "images": result,
        **_visibility_counts(filtered_total, visible_count),
        "total_kept": filtered_total,
        "stats": stats,
    }


@app.post("/api/mosaic/pick")
async def mosaic_pick(request: Request):
    """
    User picked the best image from the visible mosaic.
    Body: { "winner_id": int, "loser_ids": [int, ...] }
    K=12 per pair.
    """
    body, error = await _json_object(request)
    if error:
        return error
    picked_id = _positive_int(body.get("winner_id"))
    raw_other_ids = body.get("loser_ids", [])

    if picked_id is None or not isinstance(raw_other_ids, list) or not raw_other_ids:
        return JSONResponse({"error": "Need winner_id and loser_ids"}, status_code=400)

    other_ids = []
    seen_losers = set()
    for value in raw_other_ids:
        loser_id = _positive_int(value)
        if loser_id is None:
            return JSONResponse({"error": "loser_ids must contain valid image ids"}, status_code=400)
        if loser_id == picked_id:
            return JSONResponse({"error": "Winner cannot also be a loser"}, status_code=400)
        if loser_id in seen_losers:
            return JSONResponse({"error": "Duplicate loser_ids are not allowed"}, status_code=400)
        seen_losers.add(loser_id)
        other_ids.append(loser_id)

    # Single batch query instead of N+1 individual queries
    all_ids = [picked_id] + other_ids
    images = await db.get_active_images_by_ids(all_ids)

    missing_ids = [image_id for image_id in all_ids if image_id not in images]
    if missing_ids:
        return JSONResponse(
            {"error": "Images must exist in an active online catalog", "image_ids": missing_ids},
            status_code=400,
        )

    picked = images[picked_id]
    picked_elo = picked["elo"]
    comparison_rows = []
    loser_updates = []
    action_id = uuid.uuid4().hex

    conn = await db.get_db()
    try:
        for oid in other_ids:
            other = images.get(oid)
            new_picked, new_other = pairing.update_elo(picked_elo, other["elo"], k=12.0)

            comparison_rows.append((picked_id, oid, picked_elo, other["elo"], action_id))
            loser_updates.append((new_other, oid))
            picked_elo = new_picked

        if comparison_rows:
            await conn.executemany(
                "INSERT INTO comparisons "
                "(winner_id, loser_id, mode, elo_before_winner, elo_before_loser, action_id) "
                "VALUES (?, ?, 'mosaic', ?, ?, ?)",
                comparison_rows,
            )
            await conn.executemany(
                "UPDATE images SET elo = ?, comparisons = COALESCE(comparisons, 0) + 1 WHERE id = ?",
                loser_updates,
            )

            await conn.execute(
                "UPDATE images SET elo = ?, comparisons = COALESCE(comparisons, 0) + ? WHERE id = ?",
                (picked_elo, len(comparison_rows), picked_id),
            )
        await conn.commit()
        db.invalidate_stats_cache()
    finally:
        await conn.close()

    if comparison_rows:
        _patch_pairing_cache(
            [(picked_id, picked_elo, len(comparison_rows))]
            + [(image_id, new_elo, 1) for new_elo, image_id in loser_updates]
        )
    _add_past_matchups([(picked_id, row[1]) for row in comparison_rows])

    # Fire-and-forget: propagate Elo to similar images via embeddings
    valid_loser_ids = [row[1] for row in comparison_rows]
    _schedule_pairing_propagation(
        elo_propagation.propagate_mosaic(picked_id, valid_loser_ids, k=12.0, action_id=action_id)
    )

    return {
        "ok": True,
        "new_elo": round(picked_elo, 1),
        "pairs_recorded": len(comparison_rows),
        "action_id": action_id,
    }


@app.get("/api/propagation/last")
async def propagation_last():
    """Return the number of images affected by the last Elo propagation."""
    return {"count": elo_propagation.last_propagation_count}


@app.post("/api/propagation/predict")
async def propagation_predict(request: Request):
    """Precompute propagation counts for each possible winner in a grid."""
    body, error = await _json_object(request)
    if error:
        return error
    grid_ids = body.get("grid_ids", [])
    if not grid_ids:
        return {"counts": {}}
    counts = await elo_propagation.predict_propagation(grid_ids)
    return {"counts": {str(k): v for k, v in counts.items()}}


# --- Compare API ---

@app.get("/api/compare/next")
async def compare_next(
    n: int = 5, mode: str = "swiss",
    orientation: str = "", compared: str = "", min_stars: int = 0, folder: str = "",
    flag: str = "", date_taken: str = "", file_type: str = "", camera: str = "", lens: str = "",
):
    if mode == "topn":
        images = await db.get_top_images(limit=50)
    else:
        images = await _get_pairing_images()

    if len(images) < 2:
        visible = await _filter_visible_candidates([dict(img) for img in images], "md")
        stats = dict(await db.get_stats())
        stats["filtered_pool"] = len(visible)
        stats["filtered_pool_visible"] = len(visible)
        stats["filtered_pool_total"] = len(images)
        return {
            "pairs": [],
            **_visibility_counts(len(images), len(visible)),
            "total_kept": len(images),
            "stats": stats,
        }

    image_dicts = [dict(img) for img in images]

    # Apply filters
    if orientation in ("landscape", "portrait"):
        image_dicts = [c for c in image_dicts if c.get("orientation") == orientation]
    if compared == "compared":
        image_dicts = [c for c in image_dicts if _has_ranking_signal(c)]
    elif compared == "uncompared":
        image_dicts = [c for c in image_dicts if not _has_ranking_signal(c)]
    elif compared == "confident":
        image_dicts = [c for c in image_dicts if c["comparisons"] >= 10]
    if min_stars > 0:
        from db import STAR_THRESHOLDS
        threshold = STAR_THRESHOLDS.get(min_stars, 0)
        image_dicts = [c for c in image_dicts if c["elo"] >= threshold]
    if folder:
        image_dicts = [c for c in image_dicts if f"/{folder}/" in c.get("filepath", "")]
    if flag in ("picked", "unflagged", "rejected"):
        image_dicts = [c for c in image_dicts if (c.get("flag") or "unflagged") == flag]
    image_dicts = _filter_by_metadata(image_dicts, date_taken, file_type, camera, lens)
    filtered_total = len(image_dicts)
    image_dicts = await _filter_visible_candidates(image_dicts, "md")
    visible_count = len(image_dicts)

    if len(image_dicts) < 2:
        stats = dict(await db.get_stats())
        stats["filtered_pool"] = visible_count
        stats["filtered_pool_visible"] = visible_count
        stats["filtered_pool_total"] = filtered_total
        return {
            "pairs": [],
            **_visibility_counts(filtered_total, visible_count),
            "total_kept": filtered_total,
            "stats": stats,
        }
    past = await _get_past_matchups()
    pairs = pairing.swiss_pair(image_dicts, past, max_pairs=n)

    result = []
    prefetch_rows = []
    for left, right in pairs:
        prefetch_rows.append(left)
        prefetch_rows.append(right)
        result.append({
            "left": {
                "id": left["id"],
                "filename": left["filename"],
                "elo": round(left["elo"], 1),
                "comparisons": left["comparisons"],
                "propagated_updates": left.get("propagated_updates") or 0,
                "flag": left.get("flag") or "unflagged",
                "thumb_url": f"/api/thumb/md/{left['id']}",
            },
            "right": {
                "id": right["id"],
                "filename": right["filename"],
                "elo": round(right["elo"], 1),
                "comparisons": right["comparisons"],
                "propagated_updates": right.get("propagated_updates") or 0,
                "flag": right.get("flag") or "unflagged",
                "thumb_url": f"/api/thumb/md/{right['id']}",
            },
        })

    if prefetch_rows:
        config = settings.get_settings()
        await thumbnails.prefetch_images(
            prefetch_rows,
            "md",
            limit=min(len(prefetch_rows), config["compare_prefetch_limit"]),
        )

    stats = dict(await db.get_stats())
    stats["filtered_pool"] = visible_count
    stats["filtered_pool_visible"] = visible_count
    stats["filtered_pool_total"] = filtered_total
    return {
        "pairs": result,
        **_visibility_counts(filtered_total, visible_count),
        "total_kept": filtered_total,
        "stats": stats,
    }


@app.post("/api/compare")
async def submit_comparison(request: Request):
    body, error = await _json_object(request)
    if error:
        return error
    winner_id = _positive_int(body.get("winner_id"))
    loser_id = _positive_int(body.get("loser_id"))
    mode = body.get("mode", "swiss")

    if winner_id is None or loser_id is None:
        return JSONResponse({"error": "winner_id and loser_id are required"}, status_code=400)
    if winner_id == loser_id:
        return JSONResponse({"error": "Winner and loser must be different images"}, status_code=400)

    both = await db.get_active_images_by_ids([winner_id, loser_id])
    winner, loser = both.get(winner_id), both.get(loser_id)

    if not winner or not loser:
        return JSONResponse(
            {"error": "Images must exist in an active online catalog"},
            status_code=400,
        )

    k = pairing.get_k_factor(min(winner["comparisons"], loser["comparisons"]), mode)
    new_winner_elo, new_loser_elo = pairing.update_elo(winner["elo"], loser["elo"], k)
    action_id = uuid.uuid4().hex

    await db.record_comparison(
        winner_id, loser_id, mode,
        winner["elo"], loser["elo"],
        new_winner_elo, new_loser_elo,
        action_id=action_id,
    )

    _patch_pairing_cache([(winner_id, new_winner_elo, 1), (loser_id, new_loser_elo, 1)])
    _add_past_matchups([(winner_id, loser_id)])
    # Fire-and-forget: propagate Elo to similar images via embeddings.
    _schedule_pairing_propagation(
        elo_propagation.propagate_comparison(winner_id, loser_id, k, action_id=action_id)
    )

    return {
        "ok": True,
        "winner_elo": round(new_winner_elo, 1),
        "loser_elo": round(new_loser_elo, 1),
        "action_id": action_id,
    }


@app.post("/api/compare/undo")
async def compare_undo():
    result = await db.undo_last_comparison()
    if result:
        _invalidate_pairing_cache(matchups=True)
        return {"ok": True, **result}
    return JSONResponse({"error": "Nothing to undo"}, status_code=400)


# --- Rankings API ---

@app.get("/api/rankings")
async def api_rankings(
    limit: int = 100, offset: int = 0, sort: str = "elo",
    orientation: str = "", compared: str = "", min_stars: int = 0,
    folder: str = "", flag: str = "", date_taken: str = "", file_type: str = "",
    camera: str = "", lens: str = "", q: str = "",
):
    limit = _clamp_int(limit, 100, 1, 500)
    offset = _clamp_int(offset, 0, 0, 1_000_000)
    # When a search query is present, pre-filter to images above similarity threshold
    search_ids = None
    search_scores = {}
    search_mode = ""
    text_query = ""
    search_active = bool(q.strip())
    if search_active:
        normalized_query = q.strip()
        try:
            import embedding_worker
            import embed_cache
            text_vec = await asyncio.get_event_loop().run_in_executor(
                None,
                embedding_worker.encode_text,
                normalized_query,
            )
            if text_vec is not None:
                image_ids, matrix = await embed_cache.get_matrix()
                if image_ids is not None:
                    config = settings.get_settings()
                    threshold = config.get("search_similarity_threshold", 0.35)
                    similarities = matrix @ text_vec
                    mask = similarities >= threshold
                    search_ids = set()
                    for i in range(len(image_ids)):
                        if mask[i]:
                            search_ids.add(image_ids[i])
                            search_scores[image_ids[i]] = float(similarities[i])
        except Exception:
            pass
        if search_ids is None:
            text_query = normalized_query
            search_mode = "metadata"
        else:
            search_mode = "embedding"

    db_sort = "elo" if sort == "similarity" and not search_scores else sort

    # Similarity sort: fetch all matches, sort in Python, then paginate
    if sort == "similarity" and search_scores:
        total_images, visible_images = await asyncio.gather(
            db.count_rankings(
                orientation=orientation, compared=compared, min_stars=min_stars,
                folder=folder, flag=flag, date_taken=date_taken, file_type=file_type,
                camera=camera, lens=lens,
                id_filter=search_ids,
                text_query=text_query,
            ),
            db.count_rankings(
                orientation=orientation, compared=compared, min_stars=min_stars,
                folder=folder, flag=flag, date_taken=date_taken, file_type=file_type,
                camera=camera, lens=lens,
                id_filter=search_ids,
                visible_thumb_size="sm", cache_root=_cache_root(),
                text_query=text_query,
            ),
        )
        images = await db.get_rankings(
            limit=visible_images, offset=0, sort="elo",
            orientation=orientation, compared=compared, min_stars=min_stars,
            folder=folder, flag=flag, date_taken=date_taken, file_type=file_type,
            camera=camera, lens=lens,
            id_filter=search_ids,
            visible_thumb_size="sm", cache_root=_cache_root(),
            text_query=text_query,
        )
        all_results = []
        for img in images:
            d = dict(img)
            all_results.append({
                "id": d["id"], "filename": d["filename"],
                "elo": round(d["elo"], 1), "comparisons": d["comparisons"],
                "propagated_updates": d.get("propagated_updates") or 0,
                "status": d["status"], "flag": d.get("flag") or "unflagged",
                "aspect_ratio": d.get("aspect_ratio") or 1.5,
                **_metadata_payload(d),
                "thumb_url": f"/api/thumb/sm/{d['id']}",
                "similarity": round(search_scores.get(d["id"], 0), 4),
            })
        all_results.sort(key=lambda x: x["similarity"], reverse=(sort == "similarity"))
        page = all_results[offset:offset + limit]
        if page:
            await thumbnails.prefetch_images(
                [{"id": r["id"], "filepath": ""} for r in page], "sm",
                limit=min(len(page), 48),
            )
        return {
            "images": page,
            **_visibility_counts(total_images, visible_images),
            "total_kept": total_images,
            "search_mode": search_mode,
            "ai_unavailable": False,
        }

    images, visible_images, total_images = await asyncio.gather(
        db.get_rankings(
            limit=limit, offset=offset, sort=db_sort,
            orientation=orientation, compared=compared, min_stars=min_stars,
            folder=folder, flag=flag, date_taken=date_taken, file_type=file_type,
            camera=camera, lens=lens,
            id_filter=search_ids,
            visible_thumb_size="sm", cache_root=_cache_root(),
            text_query=text_query,
        ),
        db.count_rankings(
            orientation=orientation, compared=compared, min_stars=min_stars,
            folder=folder, flag=flag, date_taken=date_taken, file_type=file_type,
            camera=camera, lens=lens,
            id_filter=search_ids,
            visible_thumb_size="sm", cache_root=_cache_root(),
            text_query=text_query,
        ),
        db.count_rankings(
            orientation=orientation, compared=compared, min_stars=min_stars,
            folder=folder, flag=flag, date_taken=date_taken, file_type=file_type,
            camera=camera, lens=lens,
            id_filter=search_ids,
            text_query=text_query,
        ),
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
        entry = {
            "id": d["id"],
            "filename": d["filename"],
            "elo": round(d["elo"], 1),
            "comparisons": d["comparisons"],
            "propagated_updates": d.get("propagated_updates") or 0,
            "status": d["status"],
            "flag": d.get("flag") or "unflagged",
            "aspect_ratio": d.get("aspect_ratio") or 1.5,
            **_metadata_payload(d),
            "thumb_url": f"/api/thumb/sm/{d['id']}",
        }
        if search_scores:
            entry["similarity"] = round(search_scores.get(d["id"], 0), 4)
        if sort in ("date_taken", "date_taken_asc"):
            dt = d.get("date_taken") or ""
            entry["date_group"] = dt[:7] if len(dt) >= 7 else ""
        result.append(entry)
    return {
        "images": result,
        **_visibility_counts(total_images, visible_images),
        "total_kept": total_images,
        "search_mode": search_mode,
        "ai_unavailable": bool(search_active and text_query),
    }


@app.get("/api/date-groups")
async def api_date_groups(
    orientation: str = "", compared: str = "", min_stars: int = 0,
    folder: str = "", flag: str = "", date_taken: str = "", file_type: str = "",
    camera: str = "", lens: str = "",
):
    """Return date groups with counts for the scrubber, respecting active filters."""
    groups = await db.get_date_groups(
        orientation=orientation, compared=compared, min_stars=min_stars,
        folder=folder, flag=flag, date_taken=date_taken, file_type=file_type,
        camera=camera, lens=lens,
        visible_thumb_size="sm", cache_root=_cache_root(),
    )
    return {"groups": groups}


@app.get("/api/map/markers")
async def api_map_markers(
    orientation: str = "", compared: str = "", min_stars: int = 0,
    folder: str = "", flag: str = "", date_taken: str = "", file_type: str = "",
    camera: str = "", lens: str = "",
):
    """Return images with GPS data for map display."""
    return await db.get_map_markers(
        orientation=orientation, compared=compared, min_stars=min_stars,
        folder=folder, flag=flag, date_taken=date_taken, file_type=file_type,
        camera=camera, lens=lens,
        visible_thumb_size="sm", cache_root=_cache_root(),
    )


@app.get("/api/export")
async def export_rankings(format: str = "json", ids: str = ""):
    if ids:
        id_list = [int(x) for x in ids.split(",") if x.strip().isdigit()]
        images_dict = await db.get_images_by_ids(id_list)
        images = [images_dict[i] for i in id_list if i in images_dict]
    else:
        images = await db.get_rankings(limit=10000)
    image_dicts = [dict(img) for img in images]
    data = [
        {
            "rank": i + 1,
            "filename": img["filename"],
            "filepath": img["filepath"],
            "elo": round(img["elo"], 1),
            "comparisons": img["comparisons"],
            "propagated_updates": img.get("propagated_updates") or 0,
            "status": img["status"],
            "flag": img.get("flag") or "unflagged",
            "date_taken": img.get("date_taken"),
            "camera_make": img.get("camera_make"),
            "camera_model": img.get("camera_model"),
            "lens": img.get("lens"),
            "file_ext": img.get("file_ext"),
            "file_size": img.get("file_size"),
            "file_modified_at": img.get("file_modified_at"),
            "width": img.get("width"),
            "height": img.get("height"),
            "latitude": img.get("latitude"),
            "longitude": img.get("longitude"),
        }
        for i, img in enumerate(image_dicts)
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
    query = q.strip()
    if not query:
        return {"images": [], "query": q, **_visibility_counts(0, 0)}
    limit = _clamp_int(limit, 50, 1, 500)

    async def metadata_fallback(reason: str):
        rows, visible_images, total_images = await asyncio.gather(
            db.get_rankings(
                limit=limit,
                offset=0,
                sort="elo",
                visible_thumb_size="sm",
                cache_root=_cache_root(),
                text_query=query,
            ),
            db.count_rankings(
                visible_thumb_size="sm",
                cache_root=_cache_root(),
                text_query=query,
            ),
            db.count_rankings(text_query=query),
        )
        result = []
        for img in rows:
            d = dict(img)
            result.append({
                "id": d["id"],
                "filename": d["filename"],
                "elo": round(d["elo"], 1),
                "comparisons": d["comparisons"],
                "propagated_updates": d.get("propagated_updates") or 0,
                "flag": d.get("flag") or "unflagged",
                "similarity": None,
                "aspect_ratio": d.get("aspect_ratio") or 1.5,
                **_metadata_payload(d),
                "thumb_url": f"/api/thumb/sm/{d['id']}",
            })
        if rows:
            await thumbnails.prefetch_images(
                [dict(img) for img in rows],
                "sm",
                limit=min(len(rows), 48),
            )
        return {
            "images": result,
            "query": q,
            "search_mode": "metadata",
            "ai_unavailable": True,
            "fallback_reason": reason,
            **_visibility_counts(total_images, visible_images),
        }

    try:
        import embedding_worker
        import embed_cache
    except ImportError:
        return await metadata_fallback("embeddings_unavailable")

    text_vec = await asyncio.get_event_loop().run_in_executor(None, embedding_worker.encode_text, query)
    if text_vec is None:
        return await metadata_fallback("model_loading")

    image_ids, matrix = await embed_cache.get_matrix()
    if image_ids is None:
        return await metadata_fallback("embeddings_not_indexed")

    similarities = matrix @ text_vec
    ranked_indices = _top_indices_desc(similarities, len(image_ids))
    ranked_ids = [int(image_ids[int(i)]) for i in ranked_indices]
    visible_images = await _count_visible_ranked_ids(ranked_ids, "sm")
    visible_rows = await _visible_ranked_images(ranked_ids, limit, "sm")
    score_by_id = {int(image_ids[int(i)]): float(similarities[int(i)]) for i in ranked_indices}

    result = []
    for img in visible_rows:
        img_id = int(img["id"])
        score = score_by_id.get(img_id, 0.0)
        result.append({
            "id": img_id,
            "filename": img["filename"],
            "elo": round(img["elo"], 1),
            "comparisons": img["comparisons"],
            "propagated_updates": img.get("propagated_updates") or 0,
            "flag": img.get("flag") or "unflagged",
            "similarity": round(score, 4),
            "aspect_ratio": img.get("aspect_ratio") or 1.5,
            **_metadata_payload(img),
            "thumb_url": f"/api/thumb/sm/{img_id}",
        })

    return {
        "images": result,
        "query": q,
        "search_mode": "embedding",
        "ai_unavailable": False,
        **_visibility_counts(len(ranked_ids), visible_images),
    }


@app.get("/api/similar/{image_id}")
async def api_similar(image_id: int, limit: int = 50):
    """Find visually similar images using embedding cosine similarity."""
    limit = max(1, min(int(limit), 500))
    try:
        import embed_cache
    except ImportError:
        return JSONResponse({"error": "Embeddings not available"}, status_code=503)

    image_ids, matrix = await embed_cache.get_matrix()
    if image_ids is None:
        return {"images": [], "source_id": image_id, **_visibility_counts(0, 0)}

    source_vec = embed_cache.get_vector(image_id)
    if source_vec is None:
        return JSONResponse({"error": "Image not embedded yet"}, status_code=404)

    similarities = matrix @ source_vec
    source_index = embed_cache.get_index().get(image_id)
    ranked_indices = [
        int(idx)
        for idx in _top_indices_desc(similarities, len(image_ids), exclude_index=source_index)
        if int(image_ids[int(idx)]) != image_id
    ]
    ranked_ids = [int(image_ids[idx]) for idx in ranked_indices]
    visible_images = await _count_visible_ranked_ids(ranked_ids, "sm")
    visible_rows = await _visible_ranked_images(ranked_ids, limit, "sm")
    score_by_id = {int(image_ids[idx]): float(similarities[idx]) for idx in ranked_indices}
    results = []
    for img in visible_rows:
        img_id = int(img["id"])
        results.append({
            "id": img_id,
            "filename": img["filename"],
            "elo": round(img["elo"], 1),
            "comparisons": img["comparisons"],
            "propagated_updates": img.get("propagated_updates") or 0,
            "flag": img.get("flag") or "unflagged",
            "similarity": round(score_by_id.get(img_id, 0.0), 4),
            "aspect_ratio": img.get("aspect_ratio") or 1.5,
            **_metadata_payload(img),
            "thumb_url": f"/api/thumb/sm/{img_id}",
        })

    return {
        "images": results,
        "source_id": image_id,
        **_visibility_counts(len(ranked_ids), visible_images),
    }


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
        return {"pairs": [], "visible_pairs": 0, "total_pairs": 0, "hidden_pending_thumbnails": 0}

    # Process in batches to avoid allocating a full n×n matrix (~850MB for 20k images).
    # Each batch computes similarities for a chunk of rows against all columns.
    BATCH = 500
    n = len(image_ids)
    cached_sm_ids = await _cached_image_ids([int(image_id) for image_id in image_ids], "sm")
    pairs = []
    total_pairs = 0
    hidden_pairs = 0
    for start in range(0, n, BATCH):
        end = min(start + BATCH, n)
        chunk_sims = matrix[start:end] @ matrix.T  # (BATCH, n) — manageable
        for i_local in range(end - start):
            i = start + i_local
            # Only check upper triangle (j > i)
            j_start = max(i + 1, 0)
            row = chunk_sims[i_local, j_start:]
            above = (row >= threshold).nonzero()[0]
            for offset in above:
                j = j_start + int(offset)
                id_a = int(image_ids[i])
                id_b = int(image_ids[j])
                total_pairs += 1
                if id_a in cached_sm_ids and id_b in cached_sm_ids:
                    pairs.append((id_a, id_b, float(row[int(offset)])))
                else:
                    hidden_pairs += 1
                if len(pairs) >= limit:
                    break
            if len(pairs) >= limit:
                break
        if len(pairs) >= limit:
            break

    # Fetch image details
    all_ids = list({p[0] for p in pairs} | {p[1] for p in pairs})
    images = await db.get_active_images_by_ids(all_ids) if all_ids else {}

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

    return {
        "pairs": result,
        "visible_pairs": len(result),
        "total_pairs": total_pairs,
        "hidden_pending_thumbnails": hidden_pairs,
    }


_exif_cache: dict[int, dict] = {}
_EXIF_CACHE_MAX = 2000
_collections_cache = {"key": None, "data": None}

@app.get("/api/image/{image_id}/exif")
async def api_exif(image_id: int):
    """Extract EXIF metadata from an image (cached per image)."""
    if image_id in _exif_cache:
        return _exif_cache[image_id]
    image = await db.get_image_by_id(image_id)
    if not image:
        return JSONResponse({"error": "Image not found"}, status_code=404)

    try:
        exif = photo_metadata.extract_image_metadata(image["filepath"])
    except Exception:
        exif = {}

    row = dict(image)
    for key in (
        "date_taken", "camera_make", "camera_model", "lens", "file_ext",
        "file_size", "file_modified_at", "latitude", "longitude",
    ):
        if not exif.get(key) and row.get(key) is not None:
            exif[key] = row.get(key)
    if not exif.get("dimensions") and row.get("width") and row.get("height"):
        exif["dimensions"] = f"{row['width']} x {row['height']}"
    if not exif.get("filepath"):
        exif["filepath"] = image["filepath"]

    try:
        await db.batch_update_metadata([_metadata_update_tuple(image_id, exif)])
        _invalidate_pairing_cache()
    except Exception:
        pass

    result = {"exif": exif}
    _exif_cache[image_id] = result
    if len(_exif_cache) > _EXIF_CACHE_MAX:
        # Evict oldest entries
        to_remove = list(_exif_cache.keys())[:_EXIF_CACHE_MAX // 2]
        for k in to_remove:
            del _exif_cache[k]
    return result


@app.get("/api/collections")
async def api_collections(n_clusters: int = 20):
    """Auto-group images into collections using embedding clustering."""
    try:
        import embedding_worker
        import numpy as np
        import embed_cache
        from sklearn.cluster import KMeans, MiniBatchKMeans
    except ImportError:
        return JSONResponse({"error": "Dependencies not available"}, status_code=503)

    n_clusters = max(2, min(int(n_clusters), 100))
    image_ids, matrix = await embed_cache.get_matrix()
    if image_ids is None or len(image_ids) < n_clusters:
        return {"collections": []}

    cached_sm_ids = await _cached_image_ids([int(image_id) for image_id in image_ids], "sm")
    if not cached_sm_ids:
        return {"collections": []}

    cache_key = (int(n_clusters), len(image_ids), len(cached_sm_ids))
    if _collections_cache["key"] == cache_key and _collections_cache["data"] is not None:
        return _collections_cache["data"]

    loop = asyncio.get_running_loop()
    if len(image_ids) > 5000:
        kmeans = MiniBatchKMeans(
            n_clusters=n_clusters,
            batch_size=4096,
            n_init=3,
            random_state=42,
        )
    else:
        kmeans = KMeans(n_clusters=n_clusters, n_init=3, random_state=42)
    labels = await loop.run_in_executor(None, kmeans.fit_predict, matrix)

    # Group images by cluster and pick a representative (closest to centroid).
    # Only representative rows need DB metadata; fetching every embedded image
    # used to dominate this endpoint after clustering was cached/warm.
    collection_drafts = []
    representative_ids = []
    for c in range(n_clusters):
        cluster_indices = np.flatnonzero(labels == c)
        if cluster_indices.size == 0:
            continue

        centroid = kmeans.cluster_centers_[c]
        cluster_vecs = matrix[cluster_indices]
        dists = np.linalg.norm(cluster_vecs - centroid, axis=1)
        rep_idx = None
        for local_idx in np.argsort(dists):
            candidate_idx = int(cluster_indices[int(local_idx)])
            candidate_id = int(image_ids[candidate_idx])
            if candidate_id in cached_sm_ids:
                rep_idx = candidate_idx
                break
        if rep_idx is None:
            continue
        rep_id = int(image_ids[rep_idx])
        representative_ids.append(rep_id)
        member_ids = [int(image_ids[int(i)]) for i in cluster_indices[:50] if int(image_ids[int(i)]) in cached_sm_ids]
        if rep_id not in member_ids:
            member_ids.insert(0, rep_id)
        collection_drafts.append((c, int(cluster_indices.size), rep_id, member_ids))

    images_data = await db.get_active_images_by_ids(representative_ids)
    collections = []
    for c, count, rep_id, member_ids in collection_drafts:
        rep_img = images_data.get(rep_id, {})
        collections.append({
            "id": c,
            "count": count,
            "representative": {
                "id": rep_id,
                "filename": rep_img.get("filename", ""),
                "thumb_url": f"/api/thumb/sm/{rep_id}",
            },
            "image_ids": member_ids,
        })

    # Sort by size descending
    collections.sort(key=lambda c: c["count"], reverse=True)
    result = {"collections": collections}
    _collections_cache["key"] = cache_key
    _collections_cache["data"] = result
    return result


_folders_cache = {"data": None, "expires": 0}


def _invalidate_folders_cache():
    _folders_cache["data"] = None
    _folders_cache["expires"] = 0

@app.get("/api/folders")
async def api_folders():
    """Get folder tree with image counts (cached 60s)."""
    import time as _time
    if _folders_cache["data"] and _time.time() < _folders_cache["expires"]:
        return _folders_cache["data"]

    conn = await db.get_db()
    try:
        cursor = await conn.execute(
            "SELECT i.filepath FROM images i "
            "JOIN catalog_sources s ON s.id = i.source_id "
            "WHERE s.included = 1 AND s.online = 1 AND i.missing_at IS NULL"
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
    result = {"folders": folders, "root": root}
    _folders_cache["data"] = result
    _folders_cache["expires"] = _time.time() + 60
    return result


@app.get("/api/filter-options")
async def api_filter_options():
    """Return metadata-backed filter choices for the bottom bar."""
    return await db.get_filter_options()


@app.get("/api/stats")
async def api_stats():
    return await db.get_stats()


async def build_ai_status():
    """Embedding worker + model install status for UI surfaces."""
    embedded = await db.get_embedding_count()
    stats = await db.get_stats()
    total_images = stats["kept"] + stats["maybe"]
    remaining = max(total_images - embedded, 0)

    worker_status = {}
    try:
        import embedding_worker
        worker_status = embedding_worker.get_worker_status()
    except Exception:
        worker_status = {
            "state": "unavailable",
            "message": "AI worker unavailable",
            "ready": False,
            "manual_pause": False,
            "model_id": "",
            "model_dir": "",
            "last_error": "",
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
            "active_batch_size": 0,
            "target_batch_size": 0,
            "successful_batches_at_size": 0,
            "last_batch_failures": 0,
            "last_batch_stage_seconds": {},
            "last_candidate_query_seconds": 0.0,
            "last_candidate_count": 0,
            "last_candidate_window_size": 0,
            "last_ready_count": 0,
            "last_cooled_down_count": 0,
            "next_retry_at": None,
            "oom_backoffs": 0,
            "last_oom_at": None,
            "batch_growth_paused_until": None,
            "governor": resource_governor.get_background_decision(
                thumbnails.get_idle_seconds()
            ).to_dict(),
        }

    model_status = ai_models.get_model_status()
    compared = int(stats.get("rated_images") or 0)

    recent_rate = float(worker_status.get("recent_images_per_min") or 0.0)
    overall_rate = float(worker_status.get("overall_images_per_min") or 0.0)
    effective_rate = recent_rate if recent_rate > 0 else overall_rate
    eta_seconds = int((remaining / effective_rate) * 60) if remaining > 0 and effective_rate > 0 else None
    progress_pct = round((embedded / total_images) * 100, 1) if total_images > 0 else 0.0

    return {
        "embedded": embedded,
        "total_images": total_images,
        "total_kept": total_images,
        "remaining": remaining,
        "progress_pct": progress_pct,
        "compared": compared,
        "rated_images": compared,
        "direct_comparison_rows": int(stats.get("direct_comparison_rows") or 0),
        "ranking_signal_count": int(stats.get("ranking_signal_count") or stats.get("total_comparisons") or 0),
        "imported_ranking_without_history": int(stats.get("imported_ranking_without_history") or 0),
        "model_installed": model_status["installed"],
        "installing": model_status["install"]["running"],
        "install_status": model_status["install"]["status"],
        "install_message": model_status["install"]["message"],
        "model_id": model_status["model_id"],
        "model_dir": model_status["model_dir"],
        "worker_state": worker_status["state"],
        "worker_message": worker_status["message"],
        "worker_ready": worker_status["ready"],
        "embedding_manual_pause": bool(worker_status.get("manual_pause")),
        "worker_error": worker_status["last_error"],
        "last_batch_size": worker_status.get("last_batch_size", 0),
        "last_batch_seconds": worker_status.get("last_batch_seconds", 0.0),
        "last_embedded_at": worker_status.get("last_embedded_at"),
        "session_embedded": worker_status.get("session_embedded", 0),
        "session_started_at": worker_status.get("session_started_at"),
        "session_embed_seconds": worker_status.get("session_embed_seconds", 0.0),
        "session_wall_seconds": worker_status.get("session_wall_seconds", 0.0),
        "recent_images_per_min": recent_rate,
        "recent_wall_images_per_min": float(worker_status.get("recent_wall_images_per_min") or 0.0),
        "overall_images_per_min": overall_rate,
        "overall_wall_images_per_min": float(worker_status.get("overall_wall_images_per_min") or 0.0),
        "active_batch_size": worker_status.get("active_batch_size", 0),
        "target_batch_size": worker_status.get("target_batch_size", 0),
        "successful_batches_at_size": worker_status.get("successful_batches_at_size", 0),
        "last_batch_failures": worker_status.get("last_batch_failures", 0),
        "last_batch_stage_seconds": worker_status.get("last_batch_stage_seconds") or {},
        "last_candidate_query_seconds": worker_status.get("last_candidate_query_seconds", 0.0),
        "last_candidate_count": worker_status.get("last_candidate_count", 0),
        "last_candidate_window_size": worker_status.get("last_candidate_window_size", 0),
        "last_ready_count": worker_status.get("last_ready_count", 0),
        "last_cooled_down_count": worker_status.get("last_cooled_down_count", 0),
        "next_retry_at": worker_status.get("next_retry_at"),
        "oom_backoffs": worker_status.get("oom_backoffs", 0),
        "last_oom_at": worker_status.get("last_oom_at"),
        "batch_growth_paused_until": worker_status.get("batch_growth_paused_until"),
        "eta_seconds": eta_seconds,
        "governor": worker_status.get("governor") or resource_governor.get_background_decision(
            thumbnails.get_idle_seconds()
        ).to_dict(),
    }


@app.get("/api/ai/status")
async def ai_status():
    """Embedding worker and taste model status for the bottom bar."""
    return await build_ai_status()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
