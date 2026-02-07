import os
import asyncio
from db import insert_images_batch

SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".dng"}

# Global scan state
scan_state = {
    "scanning": False,
    "total_found": 0,
    "total_inserted": 0,
    "folder": None,
    "done": False,
}


def walk_images(folder: str):
    """Yield (filename, filepath) tuples for supported image files."""
    for root, _dirs, files in os.walk(folder):
        for f in files:
            if os.path.splitext(f)[1].lower() in SUPPORTED_EXTENSIONS:
                yield f, os.path.join(root, f)


async def scan_folder(folder: str, on_batch=None):
    """Scan a folder for images and insert them into the database in batches."""
    scan_state["scanning"] = True
    scan_state["total_found"] = 0
    scan_state["total_inserted"] = 0
    scan_state["folder"] = folder
    scan_state["done"] = False

    batch = []
    batch_size = 100

    for filename, filepath in walk_images(folder):
        batch.append((filename, filepath))
        scan_state["total_found"] += 1

        if len(batch) >= batch_size:
            await insert_images_batch(batch)
            scan_state["total_inserted"] += len(batch)
            if on_batch:
                await on_batch(scan_state["total_inserted"])
            batch = []
            # Yield control so other tasks can run
            await asyncio.sleep(0)

    if batch:
        await insert_images_batch(batch)
        scan_state["total_inserted"] += len(batch)
        if on_batch:
            await on_batch(scan_state["total_inserted"])

    scan_state["scanning"] = False
    scan_state["done"] = True
