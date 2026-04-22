# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

photoArchive is a FastAPI web app that ranks photos using Elo ratings via a three-phase workflow: Cull → Compare → Rankings.

## Running

```bash
cd web && .venv/bin/uvicorn app:app --reload
```

To set up from scratch:

```bash
cd web && python -m venv .venv && .venv/bin/pip install -r requirements.txt
```

Arch Linux enforces PEP 668 — never use system pip directly.

## Web App Architecture

### Backend (FastAPI + aiosqlite)

| File | Purpose |
|---|---|
| `app.py` | Routes, startup tasks (prefetch worker, orientation classifier) |
| `db.py` | SQLite schema, queries, WAL mode. Tables: `images`, `comparisons` |
| `scanner.py` | Recursive folder scan, batch inserts (100 at a time) |
| `pairing.py` | Elo calculation, Swiss-system pairing with 30% random swap |
| `thumbnails.py` | Three-size thumbnail generation (sm/md/lg), in-memory LRU cache, background prefetch |

### Frontend (vanilla HTML/CSS/JS)

- `static/app.js` — IIFE module pattern returning `photoArchive` object. Manages all UI state.
- `static/style.css` — Dark theme, responsive grids
- `templates/` — Jinja2 templates extending `base.html`

### Database Schema

**images**: id, filename, filepath (UNIQUE), elo (default 1200), comparisons, status (unculled|kept|maybe|rejected), orientation (landscape|portrait|null)

**comparisons**: winner_id, loser_id, mode (swiss|topn|mosaic), elo_before_winner, elo_before_loser (for undo support)

### Three-Phase Workflow

1. **Cull** — Filter bad images quickly. Single-image (arrow keys) or grid mode (batch click). Status transitions: unculled → kept/maybe/rejected.
2. **Compare** — Rank kept/maybe images via:
   - **Mosaic mode**: Pick best from 4x3 grid → records 11 comparisons per click (K=12)
   - **Swiss mode**: A/B pairs sorted by Elo (K=20 swiss, K=16 top-N refinement)
3. **Rankings** — View sorted results, export JSON/CSV, lightbox viewer

### Thumbnail System

Three sizes: sm (400px, grids), md (1920px, A/B compare), lg (3840px, cull single view). All cached in-memory via LRU (OrderedDict with locks). Background prefetch worker generates sm thumbnails for kept images continuously with 50ms yields to avoid starving user requests. Photos are expected to be on a slow HDD, so prefetching is critical.

### Key Patterns

- **Fire-and-forget**: Cull and mosaic picks POST without awaiting response for instant feel
- **Undo**: Cull uses in-memory stack; Compare uses DB-backed elo_before values
- **Prefetch**: Separate ThreadPoolExecutor (2 workers) for background work vs 4 workers for user requests
- **Swiss pairing**: Sort by Elo, 30% chance to swap adjacent, look ahead 5 positions to avoid repeat matchups

