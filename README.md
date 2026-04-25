# photoArchive

A local-first photo archive manager with AI-powered ranking, semantic search, and a Lightroom-inspired library interface. Built for photographers who need to efficiently review, rank, and organize large photo collections.

Everything runs locally on your machine — no cloud, no subscriptions, no uploads. Your photos stay yours.

## Quick Start

```bash
git clone https://github.com/SeanKennyDoherty/photoArchive.git
cd photoArchive/web
python -m venv .venv
source .venv/bin/activate    # Linux/Mac
# .venv\Scripts\activate     # Windows
pip install -r requirements.txt

cd ..
./scripts/photoarchive-server restart
```

Open **http://localhost:8000**. Click "Scan Folder" and point it at your photo directory. That's it.

### Requirements

- Python 3.11+
- NVIDIA GPU with ~3.5GB VRAM (for AI features — the app works without a GPU but semantic search, similarity, and taste learning require it)
- A folder of photos (JPG, JPEG, PNG, TIFF, DNG, CR3, WebP)

### Alternative: Run Directly

```bash
cd web
.venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000
```

The port and host are configurable via `PHOTOARCHIVE_PORT` and `PHOTOARCHIVE_HOST` environment variables (defaults: `127.0.0.1:8000`).

## What It Does

### The Workflow: Browse → Flag → Compare

1. **Browse** — Review your archive in a justified Library grid and Lightroom-style loupe.

2. **Flag** — Mark images as Picked, Unflagged, or Rejected with lightweight Lightroom-style flags. Flags are just filters; they do not change ranking behavior.

3. **Compare** — Rank images using Elo ratings. Three modes:
   - **Mosaic**: Pick the best from a justified grid of 12 images. One click = 11 comparisons. Fast.
   - **Swiss**: Side-by-side A/B comparisons with Swiss-system pairing (30% random swap to avoid echo chambers).
   - **Top 50**: Refine rankings among your highest-rated images.
   - Five selection strategies: Learn (AI-guided uncertainty), Explore, Compete, Top Cut, Random.

4. **Library** — Browse your ranked archive. Justified grid, Lightroom-style loupe view with filmstrip, text search, find similar, filter by flag/orientation/stars/folder/ranked status, export rankings as JSON or CSV.

### AI Features

The AI system is optional but powerful. Install the model from the Settings page (or it auto-downloads on first use, ~3.4GB).

- **Semantic search**: Type "red car" or "portrait with bokeh" and find matching photos via 2048-dim multimodal embeddings
- **Find similar**: Click any photo and find visually similar ones across your archive
- **Taste learning**: A Ridge regression taste model learns your preferences from comparisons and predicts where uncompared images would rank
- **Uncertainty-driven selection**: The "Learn" strategy surfaces the most informative images to compare next, using per-image uncertainty via leverage scores
- **Elo propagation**: Compare one photo from a set of 30 similar shots, and all 30 get nudged toward the right ranking automatically (cosine similarity > 0.75, 30% K-factor, max 10 neighbors)
- **Auto-collections**: K-means clustering groups your photos into visual themes
- **Duplicate detection**: Find near-duplicates via embedding cosine similarity

### How Ranking Works

Images start at 1200 Elo. In mosaic mode, picking one image as "best" records it as the winner against all others visible (K=12 per pair). The Elo system naturally surfaces the best photos over time.

The taste model accelerates this: after enough comparisons, it learns patterns in your preferences via the image embeddings and predicts where uncompared images would rank. This "predicted Elo" is used for pairing uncompared images so they get matched against appropriate opponents instead of all starting at 1200.

Elo propagation means you don't have to compare every photo individually. Rank one shot from a burst of 30 similar photos, and all 30 get adjusted. The propagation only affects images with fewer than 8 direct comparisons, so it never overrides confident rankings.

## Architecture

```
web/
  app.py               FastAPI routes, middleware, all API endpoints
  db.py                 SQLite schema, queries, migrations (WAL mode)
  embedding_worker.py   Background AI worker (Qwen3-VL-Embedding-2B, int4)
  embed_cache.py        Shared in-memory embedding matrix for search/similar/duplicates
  ai_models.py          Model installation and download management
  settings.py           Runtime settings with JSON persistence and hot-reload
  scanner.py            Recursive folder scanning with batch inserts
  pairing.py            Elo calculation and Swiss-system pairing
  elo_propagation.py    Propagate Elo to similar images via embedding cosine similarity
  thumbnails.py         Progressive sm/md/lg/original cache, RAM/SSD budgets, prefetch
  static/
    app.js              Frontend module (vanilla JS IIFE, no framework)
    style.css           Dark theme, justified grids, loupe view
  templates/
    base.html           Base layout with navbar/bottom_bar blocks
    _filters.html       Shared filter partial (orientation, stars, folder, ranked status)
    index.html          Landing page with folder scan
    compare.html        Mosaic/Swiss/Top50 compare modes with AI panel
    library.html        Justified grid, loupe view, filmstrip, search
    settings.html       Thumbnail, cache, and AI model configuration
```

### Stack

- **Backend**: Python, FastAPI, aiosqlite (SQLite in WAL mode)
- **Frontend**: Vanilla HTML/CSS/JS — no build step, no node_modules, no framework
- **AI**: Qwen3-VL-Embedding-2B via sentence-transformers + bitsandbytes (int4 quantized)
- **ML**: scikit-learn Ridge regression for taste model

### Design Decisions

- **Local-first**: Everything runs on your machine. SQLite database, local thumbnails, local embeddings. No network calls except model download.
- **Slow storage friendly**: Designed for photos on external HDDs. RAM + SSD cache budgets are user-configurable, with automatic tier allocation and progressive image upgrades from `sm` to `md` to `lg` to cached originals.
- **In-place preview refreshes**: Changing thumbnail size or JPEG quality does not have to wipe the cache. Old previews can remain usable while photoArchive replaces each file in the background.
- **SSD-first AI workflow**: The embedding worker reads cached `md` previews from SSD, so search indexing does not compete with thumbnail generation for HDD reads.
- **No framework frontend**: The entire UI is one vanilla JS module. No build step, no dependencies. Just open and edit.
- **Elo over stars**: Star ratings require absolute judgments. Elo only requires relative ones ("which is better?") and converges to a true ranking automatically.
- **Selective GZip**: Custom middleware skips compression for binary image payloads to avoid wasting CPU on already-compressed data.

## Configuration

Settings are available at **http://localhost:8000/settings** after launch:

- Thumbnail sizes (small, medium, large) and quality
- RAM/SSD cache budgets, cache profile, tier allocation, and prefetch settings
- AI model installation and status
- Embedding model selection
- Search similarity threshold

When thumbnail size or JPEG quality changes, Settings lets you either keep existing previews as-is or refresh them in the background. Refreshing is progressive: the old preview remains available until the new one is generated and atomically written into place.

Settings persist to `web/settings.local.json` (gitignored). Legacy settings are auto-migrated.

## Data

All app data stays in `web/` by default:
- `photoarchive.db` — SQLite database (image metadata, Elo ratings, comparisons, embeddings)
- `.thumbcache/` — Generated thumbnails plus selective SSD copies of hot originals
- `.embedcache/` — Cached embedding vectors
- `.models/` — Downloaded AI models (~3.4GB)

All of these are gitignored. Your source photo files are never modified. Browser-readable originals may be copied into the SSD hot cache when there is room, but the archive files themselves remain untouched.

## License

MIT
