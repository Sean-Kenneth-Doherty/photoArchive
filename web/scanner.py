import os
import asyncio
import db

SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".dng", ".cr3", ".tif", ".tiff", ".webp"}

# Global scan state
scan_state = {
    "scanning": False,
    "total_found": 0,
    "total_inserted": 0,
    "folder": None,
    "source_id": None,
    "done": False,
    "error": "",
}


def walk_images(folder: str):
    """Yield image rows with cheap filesystem metadata."""
    for root, _dirs, files in os.walk(folder):
        for f in files:
            file_ext = os.path.splitext(f)[1].lower()
            if file_ext in SUPPORTED_EXTENSIONS:
                filepath = os.path.join(root, f)
                file_size = None
                file_modified_at = None
                try:
                    stat = os.stat(filepath)
                    file_size = int(stat.st_size)
                    file_modified_at = float(stat.st_mtime)
                except Exception:
                    pass
                yield f, filepath, file_ext, file_size, file_modified_at


async def scan_folder(folder: str, source_id: int | None = None, on_batch=None):
    """Scan a folder for images and insert them into the database in batches."""
    scan_state["scanning"] = True
    scan_state["total_found"] = 0
    scan_state["total_inserted"] = 0
    scan_state["folder"] = folder
    scan_state["source_id"] = source_id
    scan_state["done"] = False
    scan_state["error"] = ""

    batch = []
    batch_size = 100

    try:
        if source_id is not None:
            await db.mark_source_scan_started(source_id)

        for row in walk_images(folder):
            batch.append(row)
            scan_state["total_found"] += 1

            if len(batch) >= batch_size:
                await db.insert_images_batch(batch, source_id=source_id)
                scan_state["total_inserted"] += len(batch)
                if on_batch:
                    await on_batch(scan_state["total_inserted"])
                batch = []
                # Yield control so other tasks can run
                await asyncio.sleep(0)

        if batch:
            await db.insert_images_batch(batch, source_id=source_id)
            scan_state["total_inserted"] += len(batch)
            if on_batch:
                await on_batch(scan_state["total_inserted"])

        if source_id is not None:
            await db.mark_source_scan_finished(source_id)
    except Exception as exc:
        scan_state["error"] = str(exc)
    finally:
        scan_state["scanning"] = False
        scan_state["done"] = True
