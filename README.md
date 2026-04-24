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

### The Workflow: Cull → Compare → Browse

1. **Cull** — Quickly review and reject bad images. Arrow keys in single mode, or batch-click in grid mode. Undo supported.

2. **Compare** — Rank the keepers using Elo ratings. Three modes:
   - **Mosaic**: Pick the best from a justified grid of 12 images. One click = 11 comparisons. Fast.
   - **Swiss**: Side-by-side A/B comparisons with Swiss-system pairing (30% random swap to avoid echo chambers).
   - **Top 50**: Refine rankings among your highest-rated images.
   - Five selection strategies: Learn (AI-guided uncertainty), Explore, Compete, Top Cut, Random.

3. **Library** — Browse your ranked archive. Justified grid, Lightroom-style loupe view with filmstrip, text search, find similar, filter by orientation/stars/folder/ranked status, export rankings as JSON or CSV.

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
  thumbnails.py         Three-tier thumbnail generation (sm/md/lg), LRU cache, prefetch
  static/
    app.js              Frontend module (vanilla JS IIFE, no framework)
    style.css           Dark theme, justified grids, loupe view
  templates/
    base.html           Base layout with navbar/bottom_bar blocks
    _filters.html       Shared filter partial (orientation, stars, folder, ranked status)
    index.html          Landing page with folder scan
    cull.html            Single and grid cull modes
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
- **Slow storage friendly**: Designed for photos on external HDDs. Three-tier thumbnail cache on fast local storage, aggressive prefetching, background processing.
- **No framework frontend**: The entire UI is one vanilla JS module. No build step, no dependencies. Just open and edit.
- **Elo over stars**: Star ratings require absolute judgments. Elo only requires relative ones ("which is better?") and converges to a true ranking automatically.
- **Selective GZip**: Custom middleware skips compression for binary image payloads to avoid wasting CPU on already-compressed data.

## Configuration

Settings are available at **http://localhost:8000/settings** after launch:

- Thumbnail sizes (small, medium, large) and quality
- Cache limits and prefetch settings
- AI model installation and status
- Embedding model selection
- Search similarity threshold

Settings persist to `web/settings.local.json` (gitignored). Legacy settings are auto-migrated.

## Data

All data stays in `web/`:
- `photoarchive.db` — SQLite database (image metadata, Elo ratings, comparisons, embeddings)
- `.thumbcache/` — Generated thumbnails
- `.embedcache/` — Cached embedding vectors
- `.models/` — Downloaded AI models (~3.4GB)

All of these are gitignored. Your photo files are never copied or modified — the app only reads them to generate thumbnails and embeddings.

## License

MIT
