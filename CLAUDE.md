# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PhotoRanker is a single-file Python/Tkinter desktop app that ranks photos using an Elo rating system. Users select a folder of images, then repeatedly pick a winner from side-by-side pairs. Ratings persist in `elo_ratings.json` and blacklisted images in `blacklist.json`.

## Running

```bash
python Main.py
```

On launch, a folder picker dialog opens. Select a folder containing images (JPG, PNG, JPEG, DNG supported — subfolders are scanned recursively).

## Dependencies

- Python 3 with tkinter
- `Pillow` — image loading and resizing
- `rawpy` — DNG (raw photo) support
- `inputs` — Xbox controller support (optional; app continues without it)

## Architecture

Everything lives in `Main.py` — no modules, no classes. The code follows a procedural/global-state pattern:

- **Elo system**: `update_elo_rank()` implements standard Elo with dynamic K-factor (32 for close ratings, 16 otherwise). Starting rating is 1200.
- **Image selection**: `get_least_compared_images()` sorts non-blacklisted images by comparison count; pairs are randomly drawn from the 20 least-compared.
- **Preloading**: A background thread (`preload_images`) fills a `Queue(maxsize=5)` with pre-resized image pairs for instant display.
- **Controller thread**: A second daemon thread polls for Xbox gamepad input via the `inputs` library. Controller events are bridged to Tkinter via `event_generate`.
- **Persistence**: Ratings and blacklist are saved as JSON on quit. Ratings are loaded with migration support for an older format (plain number → dict with rating/compared/confidence).

## Controls

| Input | Action |
|---|---|
| Left arrow / A button | Pick left image |
| Right arrow / B button | Pick right image |
| Z / X button | Blacklist left image |
| X / Y button | Blacklist right image |
| Escape | Save and quit |

## Data Files

- `elo_ratings.json` — per-filename rating/compared/confidence/path (gitignored, ~2.5MB with many images)
- `blacklist.json` — array of blacklisted filenames (tracked in git)
