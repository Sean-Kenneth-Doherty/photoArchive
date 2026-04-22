# photoArchive

A local-first photo archive manager with AI-powered ranking, semantic search, and a Lightroom-inspired library interface. Built for photographers who need to efficiently review, rank, and organize large photo collections.

## Features

### Library
- Justified flex grid that adapts to any window size with no dead space
- Lightroom-style loupe view with filmstrip navigation
- Progressive image loading (sm -> md -> lg)
- Text-to-image semantic search powered by Qwen3-VL-Embedding
- Find visually similar images from any photo
- Filters: orientation, ranked/unranked, star rating, folder
- Adjustable thumbnail size, infinite scroll
- Batch selection and export (JSON/CSV)

### Compare
- **Mosaic mode**: Pick the best from a justified grid of 12 images (11 Elo comparisons per click)
- **Swiss mode**: Side-by-side A/B comparisons with Swiss-system pairing
- **Top 50 mode**: Refine rankings among the highest-rated images
- Five selection strategies: Learn (AI-guided), Explore, Compete, Top Cut, Random
- Same filter controls as Library (orientation, ranked status, stars, folder)

### AI System
- **Qwen3-VL-Embedding-2B** (int4 quantized) for state-of-the-art multimodal embeddings
- **Active learning**: Taste model (Ridge regression) learns your preferences from comparisons and predicts ratings for uncompared images
- **Smart pairing**: Uses predicted Elo so uncompared images get matched against appropriate opponents instead of all starting at 1200
- **Uncertainty-driven selection**: Learn strategy surfaces the most informative images to compare
- **Elo propagation**: Each comparison ripples scaled Elo adjustments to visually similar images — comparing one shot from a set of 30 similar photos effectively ranks them all
- Embeddings power search, similarity, duplicate detection, and auto-collections

### Additional Features
- EXIF metadata display (camera, lens, focal length, aperture, shutter, ISO)
- Near-duplicate detection via embedding similarity
- Auto-collections via k-means clustering
- Folder browsing with image counts
- Configurable thumbnail sizes, cache limits, and prefetch settings
- Color-coded Elo tier indicators (gold/silver/bronze)

## Stack

- **Backend**: Python, FastAPI, aiosqlite (SQLite WAL mode)
- **Frontend**: Vanilla HTML/CSS/JS (no framework)
- **AI**: Qwen3-VL-Embedding-2B via sentence-transformers + bitsandbytes (int4)
- **ML**: scikit-learn (Ridge regression for taste model)

## Requirements

- Python 3.11+
- NVIDIA GPU with ~3.5GB VRAM (for AI features)
- Photos on any mounted drive (designed for slow HDD with fast SSD caching)

## Setup

```bash
cd web
python -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Install the AI model from the Settings page after first launch, or it will download automatically.

## Running

```bash
cd web
.venv/bin/uvicorn app:app --reload
```

Open http://localhost:8000. Scan a folder to import photos, then start comparing.

## Architecture

```
web/
  app.py           # FastAPI routes and API endpoints
  db.py            # SQLite schema, queries, migrations
  clip_worker.py   # Background AI worker (embedding, taste model, search)
  ai_models.py     # Model installation and management
  settings.py      # Runtime settings with JSON persistence
  scanner.py       # Recursive folder scanning
  pairing.py       # Elo math and Swiss-system pairing
  elo_propagation.py # Propagate Elo to similar images via embeddings
  thumbnails.py    # Multi-tier thumbnail generation and caching
  static/
    app.js         # Frontend IIFE module (PhotoArchive)
    style.css      # Dark theme, justified grids, loupe view
  templates/
    base.html      # Base layout with navbar/bottom_bar blocks
    _filters.html  # Shared filter partial (both pages)
    library.html   # Library grid + loupe view + filmstrip
    compare.html   # Mosaic/Swiss/Top50 compare modes
    cull.html      # Single and grid cull modes
    settings.html  # Configuration page
    index.html     # Landing page with folder scan
```

## How Ranking Works

Images start at 1200 Elo. In mosaic mode, picking one image as "best" records it as the winner against all other visible images (K=12 per pair). The Elo system naturally surfaces the best photos over time.

The AI taste model accelerates this: after enough comparisons, it learns patterns in your preferences via the image embeddings and predicts where uncompared images would likely rank. The Learn strategy then shows you the images the model is most uncertain about, making each click maximally informative.

Additionally, each comparison propagates scaled Elo adjustments to visually similar images via embedding cosine similarity. If you rank one photo from a shoot of 30 similar shots, all 30 get nudged toward the right position automatically. The propagation is gentle (30% of the direct K-factor, scaled by similarity) and only affects images with fewer than 8 direct comparisons, so it never overrides confident rankings.

## License

MIT
