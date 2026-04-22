# Multi-Tier Thumbnail & Cache System — Implementation Spec

## Overview

Replace the current in-memory-only LRU cache with a three-layer system:
Memory → SSD disk → HDD source. One user-facing setting (`ssd_cache_gb`)
controls the budget. The system adaptively allocates space across thumbnail
tiers and original file caching.

## Thumbnail Sizes

| Tier | Resolution | ~Per Image | 20k Total | Purpose |
|------|-----------|------------|-----------|---------|
| sm | 400px long side | ~15KB | 300MB | Grid browsing, mosaic cells |
| md | 1920px long side | ~200KB | 4GB | Lightbox initial view, AI embeddings, compare A/B |
| lg | 3840px long side | ~600KB | 12GB | Cull single view, 4K display |
| full | Original file | 5-30MB | N/A | Lightbox zoom, export — LRU cached from HDD |

## Cache Layers

### Layer 1: Memory LRU
- Same as current OrderedDict approach
- Budget: `memory_cache_mb` setting (default 512)
- Stores decoded JPEG bytes keyed by `(size, image_id)`
- Evicts LRU when budget exceeded
- Cleared on restart

### Layer 2: SSD Disk Cache
- Directory: `ssd_cache_dir` setting (default `~/.cache/photoArchive/thumbs`)
- File layout: `{ssd_cache_dir}/{size}/{image_id}.jpg` for thumbnails
- File layout: `{ssd_cache_dir}/full/{image_id}.{ext}` for originals (byte-copy from HDD)
- Budget: `ssd_cache_gb` setting (default 10)
- Persistent across restarts
- Track total size in a lightweight SQLite table or just scan on startup

### Layer 3: HDD Source (generate)
- Read original file, resize, encode as JPEG
- Write result back to SSD cache + memory cache
- Only happens on double cache miss
- This is the slow path (200-500ms per image)

## Lookup Chain

```
get_thumbnail(image_id, size):
    1. Check memory LRU → hit? return bytes
    2. Check SSD: {cache_dir}/{size}/{image_id}.jpg exists?
       → hit? read file, add to memory LRU, return bytes
    3. Cache miss: read original from HDD, resize to {size},
       save JPEG to SSD cache, add to memory LRU, return bytes

get_full_image(image_id):
    1. Check SSD: {cache_dir}/full/{image_id}.{ext} exists?
       → hit? return path or stream
    2. Cache miss: copy original from HDD to SSD cache, return
    3. If SSD budget exceeded: evict oldest full-size files
```

## Adaptive Budget Allocation

The user sets ONE number: `ssd_cache_gb`. The system allocates:

```
budget < 1GB:    sm all (300MB)
budget 1-5GB:    sm all + md LRU
budget 5-15GB:   sm all + md all + lg LRU
budget 15-50GB:  sm all + md all + lg all + full originals LRU
budget 50GB+:    all thumbnails + aggressive original caching
```

Allocation logic:
1. Reserve 300MB for sm (always pre-generate all)
2. If remaining >= 4GB, reserve 4GB for md (pre-generate all)
3. If remaining >= 12GB, reserve 12GB for lg (pre-generate all)
4. Remaining budget → full originals LRU cache

If a tier doesn't get full reservation, it operates as LRU within whatever
fraction of the budget it gets.

## Pre-Generation Background Worker

Runs during idle (no user requests for 5+ seconds). Priority order:

```
Phase 1: sm for all kept/maybe images, ordered by Elo DESC
Phase 2: md for all kept/maybe images, ordered by Elo DESC
Phase 3: lg for all kept/maybe images, ordered by Elo DESC (if budget allows)
```

Each phase checks the SSD cache before generating. Yields between images
to avoid starving user requests. Reports progress via `/api/cache/status`.

## Progressive Lightbox Loading

When user opens an image in the lightbox:

```javascript
// Frame 0: show sm instantly (already visible from grid)
lbImg.src = `/api/thumb/sm/${id}`;

// Frame 1: upgrade to md from SSD
const md = new Image();
md.onload = () => lbImg.src = md.src;
md.src = `/api/thumb/md/${id}`;

// Frame 2: load full original for zoom capability
const full = new Image();
full.onload = () => lbImg.src = full.src;
full.src = `/api/full/${id}`;
```

Also prefetch md + full for the next/prev images in the lightbox sequence.

## Lightbox Zoom

Once the full original is loaded, enable pinch/scroll zoom to inspect at
100% pixel level. Before the original arrives, zooming into the md
thumbnail shows a slightly soft preview — still better than nothing.

## AI Embedding Integration

The embedding worker (`clip_worker.py`) should prefer md thumbnails from
SSD over reading originals from HDD:

```
Embedding source priority:
  1. md thumbnail from SSD cache (1920px, fast, full model resolution)
  2. Original from HDD (fallback if md not cached yet)
```

This means: if pre-generation Phase 2 (md thumbnails) runs before or
alongside the embedding worker, embedding speed improves dramatically
since the I/O bottleneck shifts from HDD reads to GPU inference.

## Thumbnail Generation Optimizations

### 1. Generate all sizes from one read

When any size is requested and others are missing, generate all missing
sizes from a single file read. Cascade the resize (original→lg→md→sm)
so each step works on a smaller image:

```python
def generate_all_sizes(filepath, image_id, needed_sizes):
    img = open_image(filepath)  # one HDD read
    for size in ['lg', 'md', 'sm']:
        if size in needed_sizes:
            img = img.resize(SIZES[size])  # cascade: each input is the previous output
            save_to_ssd(size, image_id, img)
```

### 2. JPEG draft mode for small thumbnails

For JPEGs, decode at reduced resolution using Pillow's draft mode.
A 6000x4000 JPEG decoded at 1/8 = 750x500, then resize to 400px.
Skips decompressing 95% of pixels — 3-5x faster for sm generation:

```python
img = Image.open(path)
img.draft('RGB', (target_width * 2, target_height * 2))
img.load()
img = img.resize((target_width, target_height), Image.LANCZOS)
```

### 3. Extract embedded JPEG from RAW files

RAW files (CR3, DNG) contain embedded JPEG previews at ~1920px+.
Extract the preview instead of decoding the RAW data — turns a
2-5 second RAW decode into a ~50ms JPEG extraction:

```python
raw = rawpy.imread(path)
thumb = raw.extract_thumb()
if thumb.format == rawpy.ThumbFormat.JPEG:
    img = Image.open(io.BytesIO(thumb.data))  # instant
```

Fall back to full RAW decode only if the embedded preview is too small
for the requested thumbnail size.

### 4. Pipelined I/O and compute

Overlap HDD reads with CPU resize/encode. While image N is being
resized, start reading image N+1 from disk. Use a two-stage pipeline
with a prefetch buffer:

```
HDD thread:  [---read A---][---read B---][---read C---]
CPU thread:            [resize+encode A][resize+encode B][resize+encode C]
```

### 5. libjpeg-turbo

Ensure Pillow uses libjpeg-turbo (2-6x faster JPEG encode/decode via
SIMD). On Arch: `pacman -S libjpeg-turbo`. Pillow picks it up
automatically, no code changes needed.

### 6. Filesystem-ordered bulk pre-generation

When pre-generating all thumbnails, process images sorted by filepath
(not by Elo). Spinning HDDs are dramatically faster with sequential
reads vs random seeks. The photos are organized by folder/date, so
filepath order = sequential disk access:

```python
# Bulk pre-generation: ORDER BY filepath ASC (sequential HDD reads)
# Priority pre-generation (top-rated first): ORDER BY elo DESC (random, small subset)
```

### 7. Aspect ratio detection during generation

Already implemented: the thumbnail generator detects orientation and
aspect_ratio from image dimensions during the first resize. Store these
in the images table so the frontend can lay out grids before thumbnails
finish loading (no layout shift).

### Expected speedups (combined)

| Optimization | Speedup | Applies to |
|---|---|---|
| Single read → all sizes | 3x fewer HDD reads | Pre-generation |
| JPEG draft mode | 3-5x for sm generation | sm thumbnails |
| RAW embedded JPEG | 10-50x for RAW files | Any RAW thumbnail |
| Pipelined I/O | ~1.5x throughput | Pre-generation |
| libjpeg-turbo | 2-6x encode/decode | All JPEG operations |
| Sequential disk order | 2-5x for bulk reads | Pre-generation |

Combined, bulk pre-generation should go from ~2-3 images/sec to
~15-30 images/sec for JPEGs, completing 20k images in ~15-20 minutes
instead of hours.

## New API Endpoints

```
GET  /api/full/{image_id}       — serve full original (from SSD cache or HDD)
GET  /api/cache/status          — live stats per tier (count, size, % complete)
POST /api/cache/pregen/start    — trigger pre-generation
POST /api/cache/pregen/stop     — pause pre-generation
GET  /api/cache/pregen/status   — progress of pre-generation phases
```

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `ssd_cache_dir` | `~/.cache/photoArchive/thumbs` | SSD cache root |
| `ssd_cache_gb` | 10 | Total SSD budget — system auto-allocates across tiers |
| `memory_cache_mb` | 512 | RAM budget for in-memory thumbnail LRU |
| `pregenerate_on_idle` | true | Auto-fill SSD cache during idle periods |
| `thumb_quality` | 92 | JPEG quality for generated thumbnails |

## Settings UI

Show a visual breakdown of cache usage:

```
SSD Cache: 7.2 / 10.0 GB
  ████████░░ sm:  287 MB — 20,142 / 20,603 (98%)
  ██████░░░░ md:  3.8 GB — 19,012 / 20,603 (92%)
  ███░░░░░░░ lg:  2.1 GB — 3,500 / 20,603 (17%)
  █░░░░░░░░░ full: 1.0 GB — 142 / 20,603 (1%)

Memory: 412 / 512 MB
Pre-generation: Phase 2 — md thumbnails (92% complete)
```

## SSD Eviction Strategy

When a tier exceeds its allocated budget:
- Evict by last access time (simple LRU)
- Track access times in a small SQLite table: `cache_meta(path, size_bytes, last_accessed)`
- Or simpler: use file mtime, touch on read

## File Layout on Disk

```
~/.cache/photoArchive/thumbs/
  sm/
    1234.jpg
    5678.jpg
    ...
  md/
    1234.jpg
    ...
  lg/
    1234.jpg
    ...
  full/
    1234.jpg      (byte-copy of original)
    5678.cr3      (preserves original format)
```
