# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

photoArchive is a FastAPI web app for managing, ranking, and searching a large photo archive using AI-powered embeddings and Elo-based ranking. The workflow: Cull bad images, Compare/rank keepers via mosaic grid, Browse/search in the Library.

## Running

```bash
./scripts/photoarchive-server restart
./scripts/photoarchive-server status
./scripts/photoarchive-server logs
```

Use the helper for performance work: it runs a single non-reload uvicorn process,
writes logs/PID state to `web/.run/`, and `/api/dev/status` reports the live PID
and git commit. Avoid `--reload` while benchmarking because the reloader creates
extra processes and noisy timings.

To set up from scratch:

```bash
cd web && python -m venv .venv && .venv/bin/pip install -r requirements.txt
```

Arch Linux enforces PEP 668 — never use system pip directly.

Photos are on an external HDD at `/run/media/sean/Expansion/Photos/Exported Edits`. The HDD is slow, so prefetching and caching are critical throughout.

## Architecture

### Backend (FastAPI + aiosqlite)

| File | Purpose |
|---|---|
| `app.py` | Routes, startup tasks, API endpoints for all features |
| `db.py` | SQLite schema, queries, WAL mode. Tables: `images`, `comparisons`, `embeddings` |
| `clip_worker.py` | Background AI worker: Qwen3-VL-Embedding-2B (int4), taste model training, text search encoding |
| `ai_models.py` | Model installation/management, download state tracking |
| `settings.py` | Runtime settings with JSON persistence, hot-reload support |
| `scanner.py` | Recursive folder scan, batch inserts (100 at a time) |
| `pairing.py` | Elo calculation, Swiss-system pairing with 30% random swap |
| `elo_propagation.py` | Propagate Elo changes to similar images via embedding cosine similarity |
| `thumbnails.py` | Three-size thumbnail generation (sm/md/lg), LRU cache, disk cache, background prefetch |

### Frontend (vanilla HTML/CSS/JS)

| File | Purpose |
|---|---|
| `static/app.js` | IIFE module (`PhotoArchive`). All UI state for cull, compare, library, settings |
| `static/style.css` | Dark theme, justified flex grids, bottom bar, lightbox, filters |
| `templates/base.html` | Base layout with navbar/bottom_bar blocks |
| `templates/_filters.html` | Shared filter partial (orientation, ranked status, stars, folder, thumb slider) |
| `templates/compare.html` | Mosaic/Swiss/Top50 compare modes with AI panel |
| `templates/library.html` | Justified photo grid with search, filters, lightbox, batch select |
| `templates/cull.html` | Single-image and grid cull modes |
| `templates/settings.html` | Thumbnail, cache, and AI model configuration |
| `templates/index.html` | Landing page with folder scan |

### Database Schema

**images**: id, filename, filepath (UNIQUE), elo (default 1200), comparisons, status (unculled|kept|maybe|rejected), orientation, predicted_elo, uncertainty, aspect_ratio

**comparisons**: winner_id, loser_id, mode (swiss|topn|mosaic), elo_before_winner, elo_before_loser (for undo)

**embeddings**: image_id (PK), embedding (BLOB, 2048-dim float32), created_at

### Pages

1. **/** — Landing page, folder scan
2. **/cull** — Filter bad images. Single (arrow keys) or grid mode (batch click)
3. **/compare** — Rank via mosaic (pick best from justified grid) or Swiss A/B pairs
4. **/library** — Browse, search, filter, export. Justified flex grid with infinite scroll
5. **/settings** — Thumbnail sizes, cache config, AI model install/status

### AI System (`clip_worker.py`)

- **Model**: Qwen3-VL-Embedding-2B, int4 quantized via bitsandbytes, ~3.4GB VRAM
- **Embeddings**: 2048-dim (Matryoshka truncated from 4096), stored as BLOB in `embeddings` table
- **Taste model**: Ridge regression trained on (embedding, elo) pairs. Predicts elo for uncompared images. Computes per-image uncertainty via leverage for active learning.
- **Effective Elo**: `elo if comparisons > 0 else predicted_elo`. Used for mosaic pairing, never overwrites direct elo.
- **Text search**: Encodes query text via same model, cosine similarity against all image embeddings
- **Find Similar**: Cosine similarity from a source image to all others
- **Auto-collections**: K-means clustering on embeddings
- **Duplicate detection**: Pairwise cosine similarity above threshold
- **Elo propagation** (`elo_propagation.py`): After each comparison, propagates scaled Elo adjustments to visually similar images (cosine sim > 0.75, max 10 neighbors, decay 0.3). Only affects images with < 8 direct comparisons. Fire-and-forget background task.
- **Background loop**: Embeds in batches of 4, trains taste model every 5 batches, reports speed/ETA metrics

### UI Patterns

- **Bottom bar**: Fixed bar on compare/library/settings pages. Contains mode toggles, filters, stats, AI status, nav links. Wraps on narrow viewports.
- **Justified flex grid**: Both library and mosaic use `flex-wrap` with `flex-grow: aspectRatio` per card. Library scrolls, mosaic fits viewport via binary-search row height.
- **Shared filters** (`_filters.html`): Orientation icons, ranked status icons, star rating (hover preview), folder dropdown, thumb size slider. Used on both compare and library pages.
- **Loupe view**: Lightroom-style full-screen image view with filmstrip navigation, progressive loading (sm→md→lg), EXIF metadata, Find Similar
- **Fire-and-forget**: Cull and mosaic picks POST without awaiting response
- **Infinite scroll**: Library loads more images when within 600px of bottom

### Key API Endpoints

| Endpoint | Purpose |
|---|---|
| **Scan** | |
| `POST /api/scan` | Start recursive folder scan |
| `GET /api/scan/status` | Scan progress |
| **Cull** | |
| `GET /api/cull/next` | Get unculled images for review |
| `POST /api/cull` | Submit single cull decision |
| `POST /api/cull/batch` | Submit batch cull decisions (grid mode) |
| `POST /api/cull/undo` | Undo last cull |
| **Compare** | |
| `GET /api/mosaic/next` | Get images for mosaic grid with strategy/filter params |
| `POST /api/mosaic/pick` | Record mosaic comparison (winner vs all losers) + propagate |
| `GET /api/compare/next` | Get Swiss/Top50 A/B pairs |
| `POST /api/compare` | Submit A/B comparison + propagate |
| `POST /api/compare/undo` | Undo last comparison |
| **Library** | |
| `GET /api/rankings` | Paginated ranked images with sort/filter params |
| `GET /api/search?q=` | Text-to-image search via embedding similarity |
| `GET /api/similar/{id}` | Find visually similar images |
| `GET /api/duplicates` | Near-duplicate detection |
| `GET /api/collections` | Auto-grouped collections via k-means |
| `GET /api/folders` | Folder tree with image counts |
| `GET /api/export` | Export rankings as JSON/CSV (supports id filtering) |
| **Images** | |
| `GET /api/thumb/{size}/{id}` | Serve thumbnail (sm/md/lg) with ETag caching |
| `GET /api/image/{id}/exif` | EXIF metadata from image file |
| **AI** | |
| `GET /api/ai/status` | Embedding progress, model state, speed metrics, ETA |
| `POST /api/ai/model/install` | Trigger model download |
| **Settings & Cache** | |
| `GET /api/settings` | Current settings + cache stats + model status |
| `POST /api/settings` | Save settings (hot-reload) |
| `POST /api/settings/reset` | Reset to defaults |
| `GET /api/cache/status` | Thumbnail cache fill stats |
| `POST /api/cache/clear` | Clear thumbnail caches |
| `GET /api/stats` | Overall image/comparison counts |

## Skill routing

When the user's request matches an available skill, invoke it via the Skill tool. The
skill has multi-step workflows, checklists, and quality gates that produce better
results than an ad-hoc answer. When in doubt, invoke the skill. A false positive is
cheaper than a false negative.

Key routing rules:
- Product ideas, "is this worth building", brainstorming → invoke /office-hours
- Strategy, scope, "think bigger", "what should we build" → invoke /plan-ceo-review
- Architecture, "does this design make sense" → invoke /plan-eng-review
- Design system, brand, "how should this look" → invoke /design-consultation
- Design review of a plan → invoke /plan-design-review
- Developer experience of a plan → invoke /plan-devex-review
- "Review everything", full review pipeline → invoke /autoplan
- Bugs, errors, "why is this broken", "wtf", "this doesn't work" → invoke /investigate
- Test the site, find bugs, "does this work" → invoke /qa (or /qa-only for report only)
- Code review, check the diff, "look at my changes" → invoke /review
- Visual polish, design audit, "this looks off" → invoke /design-review
- Developer experience audit, try onboarding → invoke /devex-review
- Ship, deploy, create a PR, "send it" → invoke /ship
- Merge + deploy + verify → invoke /land-and-deploy
- Configure deployment → invoke /setup-deploy
- Post-deploy monitoring → invoke /canary
- Update docs after shipping → invoke /document-release
- Weekly retro, "how'd we do" → invoke /retro
- Second opinion, codex review → invoke /codex
- Safety mode, careful mode, lock it down → invoke /careful or /guard
- Restrict edits to a directory → invoke /freeze or /unfreeze
- Upgrade gstack → invoke /gstack-upgrade
- Save progress, "save my work" → invoke /context-save
- Resume, restore, "where was I" → invoke /context-restore
- Security audit, OWASP, "is this secure" → invoke /cso
- Make a PDF, document, publication → invoke /make-pdf
- Launch real browser for QA → invoke /open-gstack-browser
- Import cookies for authenticated testing → invoke /setup-browser-cookies
- Performance regression, page speed, benchmarks → invoke /benchmark
- Review what gstack has learned → invoke /learn
- Tune question sensitivity → invoke /plan-tune
- Code quality dashboard → invoke /health
