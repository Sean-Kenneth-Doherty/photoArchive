const PhotoArchive = (() => {
    // --- Cull Mode State ---
    let cullQueue = [];
    let cullIndex = 0;
    let cullHistory = [];
    let cullBusy = false;
    let cullStats = {};
    // Track browser-loaded images: url -> Promise that resolves when loaded
    const preloaded = new Map();

    // --- Compare Mode State ---
    let comparePairs = [];
    let compareIndex = 0;
    let compareMode = 'swiss';
    let compareBusy = false;
    let compareStats = {};

    // --- Rankings State ---
    let rankingsOffset = 0;
    const RANKINGS_PAGE_SIZE = 100;

    // ==================== CULL MODE ====================

    let cachePoller = null;

    async function initCull() {
        document.addEventListener('keydown', handleCullKey);
        await fetchCullBatch();
        showCullImage();
        // Start polling cache status
        pollCacheStatus();
        cachePoller = setInterval(pollCacheStatus, 1000);
    }

    async function pollCacheStatus() {
        try {
            const res = await fetch('/api/cache/status');
            const data = await res.json();
            const fill = document.getElementById('cache-fill');
            const text = document.getElementById('cache-text');
            if (!fill || !text) return;

            const pct = data.total > 0 ? (data.cached / data.total * 100) : 0;
            fill.style.width = pct + '%';
            fill.className = 'cache-fill' + (pct < 20 ? ' low' : '');
            text.textContent = `${data.cached}/${data.total}`;
        } catch {}
    }

    let cullFetching = false;

    async function fetchCullBatch() {
        if (cullFetching) return;
        cullFetching = true;
        try {
            const res = await fetch('/api/cull/next?n=30');
            const data = await res.json();
            cullStats = data.stats || {};

            if (data.images.length === 0 && cullQueue.length <= cullIndex) {
                showCullDone();
                return;
            }

            // Append new images, avoiding duplicates
            const existingIds = new Set(cullQueue.map(i => i.id));
            for (const img of data.images) {
                if (!existingIds.has(img.id)) {
                    cullQueue.push(img);
                    preloadImage(img.thumb_url);
                }
            }
        } finally {
            cullFetching = false;
        }
    }

    async function showCullImage() {
        // If we've run out, fetch more before giving up
        if (cullIndex >= cullQueue.length) {
            await fetchCullBatch();
            if (cullIndex >= cullQueue.length) {
                showCullDone();
                return;
            }
        }

        const img = cullQueue[cullIndex];
        const el = document.getElementById('cull-image');
        const filenameEl = document.getElementById('cull-filename');

        // Wait for this image to be preloaded in the browser before showing
        if (preloaded.has(img.thumb_url)) {
            await preloaded.get(img.thumb_url);
        }

        el.src = img.thumb_url;
        filenameEl.textContent = img.filename;

        updateCullProgress();

        // Prefetch more when running low — stay 15+ images ahead
        if (cullQueue.length - cullIndex < 15) {
            fetchCullBatch();
        }
    }

    function updateCullProgress() {
        const culled = (cullStats.kept || 0) + (cullStats.maybe || 0) + (cullStats.rejected || 0) + cullHistory.length;
        const total = cullStats.total_images || 1;
        const pct = Math.min(100, (culled / total) * 100);

        const fill = document.getElementById('cull-progress-fill');
        const text = document.getElementById('cull-progress-text');
        if (fill) fill.style.width = pct + '%';
        if (text) text.textContent = `${culled} / ${total} images culled`;
    }

    function submitCull(status) {
        if (cullBusy || cullIndex >= cullQueue.length) return;

        const img = cullQueue[cullIndex];
        // Fire and forget — don't wait for server response
        fetch('/api/cull', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ image_id: img.id, status }),
        });
        cullHistory.push({ index: cullIndex, image: img, status });
        cullIndex++;
        showCullImage();
    }

    async function undoCull() {
        if (cullBusy || cullHistory.length === 0) return;
        cullBusy = true;

        try {
            const res = await fetch('/api/cull/undo', { method: 'POST' });
            if (res.ok) {
                const entry = cullHistory.pop();
                cullIndex = entry.index;
                showCullImage();
            }
        } finally {
            cullBusy = false;
        }
    }

    function handleCullKey(e) {
        switch (e.key) {
            case 'ArrowRight': submitCull('kept'); break;
            case 'ArrowLeft': submitCull('rejected'); break;
            case 'ArrowDown': submitCull('maybe'); break;
            case 'ArrowUp': undoCull(); break;
        }
    }

    function showCullDone() {
        const single = document.getElementById('cull-single');
        const grid = document.getElementById('cull-grid');
        const done = document.getElementById('cull-done');
        if (single) single.classList.add('hidden');
        if (grid) grid.classList.add('hidden');
        if (done) done.classList.remove('hidden');
    }

    // ==================== GRID CULL MODE ====================

    let cullMode = 'single';
    const GRID_BATCH_SIZE = 12;
    let gridBatch = [];
    let gridSelected = new Set();
    let gridOrientation = 'landscape'; // alternate between landscape/portrait batches

    function setCullMode(mode) {
        cullMode = mode;
        document.querySelectorAll('.cull-mode-toggle .mode-btn').forEach(b => b.classList.remove('active'));
        document.getElementById('mode-' + mode).classList.add('active');

        const single = document.getElementById('cull-single');
        const grid = document.getElementById('cull-grid');

        if (mode === 'single') {
            single.classList.remove('hidden');
            grid.classList.add('hidden');
        } else {
            single.classList.add('hidden');
            grid.classList.remove('hidden');
            loadGridBatch();
        }
    }

    async function loadGridBatch() {
        // Wait for orientation classification to catch up (retry a few times)
        let data = null;
        for (let attempt = 0; attempt < 5; attempt++) {
            let res = await fetch(`/api/cull/next?n=${GRID_BATCH_SIZE}&size=sm&orientation=${gridOrientation}`);
            data = await res.json();

            if (data.images.length > 0) break;

            // Try the other orientation
            gridOrientation = gridOrientation === 'landscape' ? 'portrait' : 'landscape';
            res = await fetch(`/api/cull/next?n=${GRID_BATCH_SIZE}&size=sm&orientation=${gridOrientation}`);
            data = await res.json();

            if (data.images.length > 0) break;

            // Still nothing — maybe images haven't been classified yet, wait a moment
            if (data.stats && data.stats.unculled > 0) {
                await new Promise(r => setTimeout(r, 1000));
            } else {
                break;
            }
        }

        cullStats = data.stats || {};
        updateCullProgress();

        if (!data || data.images.length === 0) {
            showCullDone();
            return;
        }

        gridBatch = data.images;
        gridSelected = new Set();
        renderGrid();
    }

    function renderGrid() {
        const container = document.getElementById('cull-grid-images');
        container.innerHTML = '';

        const section = document.createElement('div');
        section.className = gridOrientation === 'portrait' ? 'grid-section grid-portrait' : 'grid-section grid-landscape';

        for (const img of gridBatch) {
            const cell = document.createElement('div');
            cell.className = 'grid-cell';
            cell.dataset.id = img.id;
            cell.onclick = () => toggleGridCell(cell, img.id);
            cell.innerHTML = `
                <img src="${img.thumb_url}" alt="${img.filename}">
                <div class="grid-check">✓</div>
                <div class="grid-filename">${img.filename}</div>
            `;
            section.appendChild(cell);
        }
        container.appendChild(section);
    }

    function toggleGridCell(cell, id) {
        if (gridSelected.has(id)) {
            gridSelected.delete(id);
            cell.classList.remove('selected');
        } else {
            gridSelected.add(id);
            cell.classList.add('selected');
        }
    }

    function gridSelectAll() {
        gridSelected = new Set(gridBatch.map(i => i.id));
        document.querySelectorAll('.grid-cell').forEach(c => c.classList.add('selected'));
    }

    function gridSelectNone() {
        gridSelected.clear();
        document.querySelectorAll('.grid-cell').forEach(c => c.classList.remove('selected'));
    }

    async function gridSubmit() {
        const decisions = gridBatch.map(img => ({
            image_id: img.id,
            status: gridSelected.has(img.id) ? 'kept' : 'rejected',
        }));

        // Alternate orientation for next batch
        gridOrientation = gridOrientation === 'landscape' ? 'portrait' : 'landscape';

        fetch('/api/cull/batch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ decisions }),
        });

        loadGridBatch();
    }

    // ==================== MOSAIC RANKING MODE ====================

    const MOSAIC_SIZE = 12;
    let mosaicImages = []; // currently visible images [{id, filename, elo, thumb_url}, ...]
    let mosaicAge = []; // how many clicks each image has survived on the board
    let mosaicPickCount = 0;
    let mosaicStrategy = 'explore';

    function mosaicGridElo() {
        if (mosaicImages.length === 0) return 0;
        return mosaicImages.reduce((s, img) => s + img.elo, 0) / mosaicImages.length;
    }

    async function loadMosaicBatch() {
        const res = await fetch(`/api/mosaic/next?n=${MOSAIC_SIZE}&strategy=${mosaicStrategy}&grid_elo=${mosaicGridElo()}`);
        const data = await res.json();
        compareStats = data.stats || {};
        updateCompareProgress();

        if (data.images.length < 2) {
            showCompareEmpty();
            return;
        }

        mosaicImages = data.images;
        mosaicAge = new Array(data.images.length).fill(0);
        mosaicPickCount = 0;
        mosaicReplacements = [];
        mosaicFilling = false;
        mosaicBusy = false;
        renderMosaic();
        mosaicFillReplacements();
    }

    function renderMosaic() {
        const grid = document.getElementById('mosaic-grid');
        grid.innerHTML = '';

        // Calculate best cols/rows to minimize dead space for the viewport
        const n = mosaicImages.length;
        const barHeight = 40;
        const vw = window.innerWidth;
        const vh = window.innerHeight - barHeight;
        const aspect = vw / vh;

        // Try different column counts and pick the one whose cell aspect ratio
        // is closest to 3:2 (typical photo aspect ratio)
        const targetAspect = 3 / 2;
        let bestCols = 4;
        let bestScore = Infinity;
        for (let c = 2; c <= 6; c++) {
            const r = Math.ceil(n / c);
            const cellW = vw / c;
            const cellH = vh / r;
            const cellAspect = cellW / cellH;
            const score = Math.abs(cellAspect - targetAspect);
            if (score < bestScore) {
                bestScore = score;
                bestCols = c;
            }
        }
        const cols = bestCols;
        const rows = Math.ceil(n / cols);
        grid.style.setProperty('--mosaic-cols', cols);
        grid.style.setProperty('--mosaic-rows', rows);

        for (const img of mosaicImages) {
            const cell = document.createElement('div');
            cell.className = 'mosaic-cell';
            cell.dataset.id = img.id;
            cell.onclick = () => mosaicClick(img.id);
            cell.innerHTML = `<img src="${img.thumb_url}" alt="${img.filename}">`;
            preloadImage(img.thumb_url);
            grid.appendChild(cell);
        }
    }

    // Pre-fetched replacement images ready to swap in instantly
    let mosaicReplacements = [];
    let mosaicFilling = false;

    async function mosaicFillReplacements() {
        if (mosaicFilling || mosaicReplacements.length >= 10) return;
        mosaicFilling = true;
        try {
            const excludeIds = [
                ...mosaicImages.map(img => img.id),
                ...mosaicReplacements.map(img => img.id),
            ].join(',');
            const res = await fetch(`/api/mosaic/next?n=10&exclude=${excludeIds}&strategy=${mosaicStrategy}&grid_elo=${mosaicGridElo()}`);
            const data = await res.json();
            if (data.stats) {
                compareStats = data.stats;
                updateCompareProgress();
            }
            // Deduplicate against current grid and existing replacements
            const onGrid = new Set(mosaicImages.map(img => img.id));
            const inBuffer = new Set(mosaicReplacements.map(img => img.id));
            for (const img of data.images) {
                if (!onGrid.has(img.id) && !inBuffer.has(img.id)) {
                    mosaicReplacements.push(img);
                    inBuffer.add(img.id);
                    preloadImage(img.thumb_url);
                }
            }
        } catch {} finally {
            mosaicFilling = false;
        }
    }

    let mosaicBusy = false;

    function mosaicClick(id) {
        if (mosaicBusy) return;
        const idx = mosaicImages.findIndex(img => img.id === id);
        if (idx === -1) return;
        mosaicBusy = true;

        const otherIds = mosaicImages.filter(img => img.id !== id).map(img => img.id);

        // Fire and forget with error handling
        fetch('/api/mosaic/pick', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ winner_id: id, loser_ids: otherIds }),
        }).catch(() => {
            showToast('Failed to save pick — check connection');
        });

        // Update stats immediately (N-1 comparisons per click)
        compareStats.total_comparisons = (compareStats.total_comparisons || 0) + otherIds.length;
        updateCompareProgress();

        // Green flash + scale pulse on the picked cell
        const cells = document.querySelectorAll('.mosaic-cell');
        const pickedCell = cells[idx];
        if (pickedCell) pickedCell.classList.add('mosaic-picked');

        // Age all non-clicked images
        for (let i = 0; i < mosaicAge.length; i++) {
            if (i !== idx) mosaicAge[i]++;
        }

        // Find the oldest survivor
        let oldestIdx = -1;
        let oldestAge = -1;
        for (let i = 0; i < mosaicAge.length; i++) {
            if (i !== idx && mosaicAge[i] > oldestAge) {
                oldestAge = mosaicAge[i];
                oldestIdx = i;
            }
        }

        const replaceIndices = [idx];
        if (oldestIdx >= 0 && oldestAge >= 10) replaceIndices.push(oldestIdx);

        // Swap after animation completes
        setTimeout(() => {
            for (const ri of replaceIndices) {
                const targetCell = cells[ri];
                if (!targetCell || mosaicReplacements.length === 0) continue;
                const newImg = mosaicReplacements.shift();
                mosaicImages[ri] = newImg;
                mosaicAge[ri] = 0;
                targetCell.dataset.id = newImg.id;
                targetCell.onclick = () => mosaicClick(newImg.id);
                targetCell.querySelector('img').src = newImg.thumb_url;
                targetCell.querySelector('img').alt = newImg.filename;
                targetCell.classList.remove('mosaic-picked');
            }

            mosaicBusy = false;

            // Refill replacement buffer in the background
            mosaicFillReplacements();

            if (mosaicImages.length < 2) {
                showCompareEmpty();
            }
        }, 150);
    }

    function showToast(msg) {
        let toast = document.getElementById('mosaic-toast');
        if (!toast) {
            toast = document.createElement('div');
            toast.id = 'mosaic-toast';
            toast.className = 'toast';
            document.body.appendChild(toast);
        }
        toast.textContent = msg;
        toast.classList.add('visible');
        setTimeout(() => toast.classList.remove('visible'), 3000);
    }

    function setMosaicStrategy(strategy) {
        mosaicStrategy = strategy;
        const btn = document.getElementById('strategy-' + strategy);
        if (btn) {
            btn.parentElement.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
        }
        loadMosaicBatch();
    }

    function mosaicShuffle() {
        loadMosaicBatch();
    }

    // ==================== COMPARE MODE ====================

    async function initCompare() {
        document.addEventListener('keydown', handleCompareKey);
        document.getElementById('compare-left').addEventListener('click', () => submitComparison('left'));
        document.getElementById('compare-right').addEventListener('click', () => submitComparison('right'));
        // Start in mosaic mode by default
        setCompareMode('mosaic');
        // Poll AI status for the bottom bar
        pollAIStatus();
        setInterval(pollAIStatus, 5000);
    }

    async function pollAIStatus() {
        try {
            const res = await fetch('/api/ai/status');
            const data = await res.json();
            const countEl = document.getElementById('ai-embed-count');
            const totalEl = document.getElementById('ai-embed-total');
            const stateEl = document.getElementById('ai-model-state');
            if (countEl) countEl.textContent = data.embedded.toLocaleString();
            if (totalEl) totalEl.textContent = data.total_kept.toLocaleString();
            if (stateEl) {
                if (data.embedded < data.total_kept) {
                    stateEl.textContent = 'Embedding';
                    stateEl.className = 'bar-ai-state embedding';
                } else if (data.model_trained) {
                    stateEl.textContent = 'Trained';
                    stateEl.className = 'bar-ai-state trained';
                } else {
                    stateEl.textContent = '';
                    stateEl.className = 'bar-ai-state';
                }
            }
            // Update panel if open
            const embedFill = document.getElementById('ai-panel-embed-fill');
            const embedText = document.getElementById('ai-panel-embed-text');
            const modelText = document.getElementById('ai-panel-model-text');
            const predText = document.getElementById('ai-panel-predictions-text');
            if (embedFill) {
                const pct = data.total_kept > 0 ? (data.embedded / data.total_kept * 100) : 0;
                embedFill.style.width = pct + '%';
            }
            if (embedText) embedText.textContent = `${data.embedded.toLocaleString()} / ${data.total_kept.toLocaleString()} images`;
            if (modelText) {
                if (data.model_trained) {
                    modelText.textContent = `Trained on ${data.compared.toLocaleString()} compared images`;
                    modelText.className = 'ai-panel-value trained';
                } else if (data.embedded > 0) {
                    modelText.textContent = 'Waiting for embeddings to complete';
                    modelText.className = 'ai-panel-value';
                } else {
                    modelText.textContent = 'Not started';
                    modelText.className = 'ai-panel-value';
                }
            }
            if (predText) {
                if (data.predicted > 0) {
                    predText.textContent = `${data.predicted.toLocaleString()} images have predicted ratings`;
                } else {
                    predText.textContent = 'None yet';
                }
            }
        } catch {
            const aiSection = document.getElementById('bar-ai');
            if (aiSection) aiSection.style.display = 'none';
        }
    }

    function toggleAIPanel() {
        const panel = document.getElementById('ai-panel');
        if (panel) panel.classList.toggle('hidden');
    }

    async function fetchComparePairs() {
        const res = await fetch(`/api/compare/next?n=8&mode=${compareMode}`);
        const data = await res.json();
        compareStats = data.stats || {};

        if (data.pairs.length === 0 && comparePairs.length === 0) {
            showCompareEmpty();
            return;
        }

        // Append new pairs
        for (const pair of data.pairs) {
            comparePairs.push(pair);
            preloadImage(pair.left.thumb_url);
            preloadImage(pair.right.thumb_url);
        }
    }

    function showComparePair() {
        if (compareIndex >= comparePairs.length) {
            // Fetch more pairs
            compareIndex = 0;
            comparePairs = [];
            fetchComparePairs().then(() => {
                if (comparePairs.length > 0) showComparePair();
            });
            return;
        }

        const pair = comparePairs[compareIndex];
        const leftImg = document.getElementById('compare-left-img');
        const rightImg = document.getElementById('compare-right-img');
        const leftInfo = document.getElementById('compare-left-info');
        const rightInfo = document.getElementById('compare-right-info');

        leftImg.classList.add('fading');
        rightImg.classList.add('fading');

        setTimeout(() => {
            leftImg.src = pair.left.thumb_url;
            rightImg.src = pair.right.thumb_url;
            leftImg.onload = () => leftImg.classList.remove('fading');
            rightImg.onload = () => rightImg.classList.remove('fading');
            leftInfo.textContent = `${pair.left.filename} — ${pair.left.elo}`;
            rightInfo.textContent = `${pair.right.filename} — ${pair.right.elo}`;
        }, 50);

        updateCompareProgress();

        // Prefetch if running low
        if (comparePairs.length - compareIndex < 4) {
            fetchComparePairs();
        }
    }

    function updateCompareProgress() {
        const total = compareStats.total_comparisons || 0;
        const kept = compareStats.kept || 0;
        const compEl = document.getElementById('compare-stat-comparisons');
        const poolEl = document.getElementById('compare-stat-pool');
        if (compEl) compEl.textContent = total.toLocaleString();
        if (poolEl) poolEl.textContent = kept.toLocaleString();
    }

    async function submitComparison(side) {
        if (compareBusy || compareIndex >= comparePairs.length) return;
        compareBusy = true;

        const pair = comparePairs[compareIndex];
        const winnerId = side === 'left' ? pair.left.id : pair.right.id;
        const loserId = side === 'left' ? pair.right.id : pair.left.id;

        try {
            const res = await fetch('/api/compare', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ winner_id: winnerId, loser_id: loserId, mode: compareMode }),
            });
            if (res.ok) {
                const result = await res.json();
                // Update local elo for display
                if (side === 'left') {
                    pair.left.elo = result.winner_elo;
                    pair.right.elo = result.loser_elo;
                } else {
                    pair.right.elo = result.winner_elo;
                    pair.left.elo = result.loser_elo;
                }
                compareStats.total_comparisons = (compareStats.total_comparisons || 0) + 1;
                compareIndex++;
                showComparePair();
            }
        } finally {
            compareBusy = false;
        }
    }

    async function undoComparison() {
        if (compareBusy) return;
        compareBusy = true;
        try {
            const res = await fetch('/api/compare/undo', { method: 'POST' });
            if (res.ok && compareIndex > 0) {
                compareIndex--;
                compareStats.total_comparisons = Math.max(0, (compareStats.total_comparisons || 1) - 1);
                showComparePair();
            }
        } finally {
            compareBusy = false;
        }
    }

    function handleCompareKey(e) {
        switch (e.key) {
            case 'ArrowLeft': submitComparison('left'); break;
            case 'ArrowRight': submitComparison('right'); break;
            case 'ArrowUp': undoComparison(); break;
        }
    }

    function setCompareMode(mode) {
        compareMode = mode;
        const btn = document.getElementById('mode-' + mode);
        if (btn) {
            btn.parentElement.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
        }

        const abContainer = document.getElementById('compare-images');
        const mosaicContainer = document.getElementById('mosaic-container');
        const strategies = document.getElementById('bar-strategies');
        const hints = document.getElementById('bar-hints');

        if (mode === 'mosaic') {
            // Show mosaic, hide A/B
            if (abContainer) abContainer.classList.add('hidden');
            if (mosaicContainer) mosaicContainer.classList.remove('hidden');
            if (strategies) strategies.classList.remove('hidden');
            if (hints) hints.classList.add('hidden');
            loadMosaicBatch();
        } else {
            // Show A/B, hide mosaic
            if (abContainer) abContainer.classList.remove('hidden');
            if (mosaicContainer) mosaicContainer.classList.add('hidden');
            if (strategies) strategies.classList.add('hidden');
            if (hints) hints.classList.remove('hidden');
            // Reset and fetch new pairs
            comparePairs = [];
            compareIndex = 0;
            fetchComparePairs().then(() => showComparePair());
        }
    }

    function showCompareEmpty() {
        const images = document.getElementById('compare-images');
        const empty = document.getElementById('compare-empty');
        const hints = document.getElementById('compare-hints');
        if (images) images.classList.add('hidden');
        if (hints) hints.classList.add('hidden');
        if (empty) empty.classList.remove('hidden');
    }

    // ==================== LIBRARY ====================

    let rankingsSort = 'elo';
    let searchQuery = '';
    let searchDebounce = null;
    let rankingsLoading = false;
    let rankingsExhausted = false;
    let thumbHeight = 220;
    let libraryImages = []; // all loaded images for lightbox navigation
    let lightboxIndex = -1;
    let filters = { orientation: '', compared: '', rating: '', folder: '' };

    async function initLibrary() {
        rankingsOffset = 0;
        rankingsExhausted = false;
        await loadRankings();

        // Load stats and AI status for the bottom bar
        try {
            const stats = await (await fetch('/api/stats')).json();
            compareStats = stats;
            updateCompareProgress();
        } catch {}
        pollAIStatus();
        setInterval(pollAIStatus, 5000);

        // Load folder list
        fetch('/api/folders').then(r => r.json()).then(data => {
            const sel = document.getElementById('filter-folder');
            if (!sel || !data.folders) return;
            // Show only top-level folders (depth 0) for cleanliness
            const topFolders = data.folders.filter(f => f.depth <= 1);
            for (const f of topFolders) {
                const opt = document.createElement('option');
                opt.value = f.path;
                const indent = f.depth > 0 ? '  ' : '';
                opt.textContent = `${indent}${f.path} (${f.count})`;
                sel.appendChild(opt);
            }
        }).catch(() => {});

        // Infinite scroll
        window.addEventListener('scroll', () => {
            if (rankingsLoading || rankingsExhausted || searchQuery) return;
            if (window.innerHeight + window.scrollY >= document.body.offsetHeight - 600) {
                loadRankings();
            }
        });

        // Lightbox keyboard navigation
        document.addEventListener('keydown', (e) => {
            const lb = document.getElementById('lightbox');
            if (!lb || lb.classList.contains('hidden')) return;
            if (e.key === 'ArrowRight') { e.preventDefault(); lightboxNext(); }
            else if (e.key === 'ArrowLeft') { e.preventDefault(); lightboxPrev(); }
            else if (e.key === 'Escape') { closeLightbox(); }
        });

        // Set up search input
        const input = document.getElementById('search-input');
        if (input) {
            input.addEventListener('input', (e) => {
                clearTimeout(searchDebounce);
                searchDebounce = setTimeout(() => {
                    searchQuery = e.target.value.trim();
                    const clearBtn = document.getElementById('search-clear');
                    if (clearBtn) clearBtn.classList.toggle('hidden', !searchQuery);
                    // Switch to search mode or back to sort mode
                    rankingsOffset = 0;
                    document.getElementById('rankings-grid').innerHTML = '';
                    const sortToggles = document.getElementById('sort-toggles');
                    if (sortToggles) sortToggles.style.opacity = searchQuery ? '0.3' : '1';
                    if (searchQuery) {
                        loadSearchResults();
                    } else {
                        loadRankings();
                    }
                }, 300);
            });
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') clearSearch();
            });
        }
    }

    function initRankings() { initLibrary(); }

    function clearSearch() {
        searchQuery = '';
        const input = document.getElementById('search-input');
        const clearBtn = document.getElementById('search-clear');
        const sortToggles = document.getElementById('sort-toggles');
        if (input) input.value = '';
        if (clearBtn) clearBtn.classList.add('hidden');
        if (sortToggles) sortToggles.style.opacity = '1';
        rankingsOffset = 0;
        rankingsExhausted = false;
        libraryImages = [];
        document.getElementById('rankings-grid').innerHTML = '';
        loadRankings();
    }

    async function loadSearchResults() {
        libraryImages = [];
        const res = await fetch(`/api/search?q=${encodeURIComponent(searchQuery)}&limit=100`);
        const data = await res.json();
        const grid = document.getElementById('rankings-grid');

        for (let i = 0; i < data.images.length; i++) {
            const img = data.images[i];
            const ar = img.aspect_ratio || 1.5;
            const card = document.createElement('div');
            card.className = 'rank-card';
            card.dataset.ar = ar;
            card.style.height = thumbHeight + 'px';
            card.style.flexGrow = ar;
            card.style.flexBasis = (thumbHeight * ar) + 'px';
            card.onclick = () => openLightbox(img);

            const simPct = (img.similarity * 100).toFixed(0);

            card.innerHTML = `
                <img src="${img.thumb_url}" alt="${img.filename}" loading="lazy" onload="this.classList.add('loaded')">
                <div class="rank-card-info">
                    <span class="rank-elo">${img.elo} Elo</span>
                    <span class="rank-similarity">${simPct}% match</span>
                </div>
            `;
            grid.appendChild(card);
            libraryImages.push(img);
        }

        const loadMoreWrap = document.getElementById('load-more-wrap');
        if (loadMoreWrap) loadMoreWrap.classList.add('hidden');
    }

    function setRankingsSort(sort) {
        if (searchQuery) return; // ignore sort changes during search
        rankingsSort = sort;
        rankingsOffset = 0;
        rankingsExhausted = false;
        libraryImages = [];
        document.getElementById('rankings-grid').innerHTML = '';
        const btn = document.getElementById('sort-' + sort);
        if (btn) {
            btn.parentElement.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
        }
        loadRankings();
    }

    async function loadRankings() {
        if (rankingsLoading) return;
        rankingsLoading = true;
        let url = `/api/rankings?limit=${RANKINGS_PAGE_SIZE}&offset=${rankingsOffset}&sort=${rankingsSort}`;
        if (filters.orientation) url += `&orientation=${filters.orientation}`;
        if (filters.compared) url += `&compared=${filters.compared}`;
        if (filters.rating) url += `&min_stars=${filters.rating}`;
        if (filters.folder) url += `&folder=${encodeURIComponent(filters.folder)}`;
        const res = await fetch(url);
        const data = await res.json();
        const grid = document.getElementById('rankings-grid');
        const showRank = (rankingsSort === 'elo' || rankingsSort === 'elo_asc' || rankingsSort === 'ai');
        const rowH = thumbHeight;

        for (let i = 0; i < data.images.length; i++) {
            const img = data.images[i];
            const rank = rankingsOffset + i + 1;
            const ar = img.aspect_ratio || 1.5;
            const tier = getTierClass(img.elo, img.comparisons);
            const conf = img.comparisons > 0 ? getConfidenceClass(img.comparisons) : '';

            const card = document.createElement('div');
            card.className = 'rank-card' + (tier ? ' ' + tier : '');
            card.dataset.ar = ar;
            card.style.height = rowH + 'px';
            card.style.flexGrow = ar;
            card.style.flexBasis = (rowH * ar) + 'px';
            card.onclick = () => {
                if (batchMode) { toggleBatchSelect(img.id, card); }
                else { openLightbox(img); }
            };

            const eloLabel = img.comparisons > 0 ? `${img.elo}` : (img.predicted_elo ? `~${img.predicted_elo}` : `${img.elo}`);
            const sourceTag = img.comparisons === 0 && img.predicted_elo ? '<span class="rank-ai-tag">AI</span>' : '';
            const confDot = conf ? `<div class="rank-confidence ${conf}"></div>` : '';

            const infoLine = showRank
                ? `<span class="rank-number">#${rank}</span><span class="rank-elo">${eloLabel} ${sourceTag}</span>`
                : `<span class="rank-elo">${eloLabel} ${sourceTag}</span><span class="rank-comparisons">${img.comparisons} cmp</span>`;

            card.innerHTML = `
                <img src="${img.thumb_url}" alt="${img.filename}" loading="lazy" onload="this.classList.add('loaded')">
                <div class="select-check">✓</div>
                ${confDot}
                <div class="rank-card-info">${infoLine}</div>
            `;
            grid.appendChild(card);
            libraryImages.push(img);
        }

        rankingsOffset += data.images.length;
        rankingsLoading = false;
        if (data.images.length < RANKINGS_PAGE_SIZE) {
            rankingsExhausted = true;
        }
    }

    function openLightbox(img) {
        lightboxIndex = libraryImages.findIndex(i => i.id === img.id);
        showLightboxImage(img);
    }

    function showLightboxImage(img) {
        const lb = document.getElementById('lightbox');
        const lbImg = document.getElementById('lightbox-img');
        const lbInfo = document.getElementById('lightbox-info');
        const lbExif = document.getElementById('lightbox-exif');
        lbImg.src = `/api/thumb/md/${img.id}`;
        const stars = eloToStars(img.elo, img.comparisons);
        const starStr = '★'.repeat(stars) + '☆'.repeat(5 - stars);
        lbInfo.textContent = `${img.filename}  ·  ${img.elo} Elo  ·  ${starStr}  ·  ${img.comparisons} comparisons`;
        if (lbExif) lbExif.textContent = '';
        lb.classList.remove('hidden');

        // Load EXIF asynchronously
        fetch(`/api/image/${img.id}/exif`).then(r => r.json()).then(data => {
            if (!lbExif || !data.exif) return;
            const e = data.exif;
            const parts = [];
            if (e.camera_model) parts.push(e.camera_model);
            if (e.lens) parts.push(e.lens);
            if (e.focal_length) parts.push(e.focal_length);
            if (e.aperture) parts.push(e.aperture);
            if (e.shutter_speed) parts.push(e.shutter_speed + 's');
            if (e.iso) parts.push('ISO ' + e.iso);
            if (e.date) parts.push(e.date);
            lbExif.textContent = parts.join('  ·  ');
        }).catch(() => {});
    }

    function lightboxNext() {
        if (lightboxIndex < 0 || lightboxIndex >= libraryImages.length - 1) return;
        lightboxIndex++;
        showLightboxImage(libraryImages[lightboxIndex]);
    }

    function lightboxPrev() {
        if (lightboxIndex <= 0) return;
        lightboxIndex--;
        showLightboxImage(libraryImages[lightboxIndex]);
    }

    function closeLightbox() {
        document.getElementById('lightbox').classList.add('hidden');
        lightboxIndex = -1;
    }

    function setThumbSize(value) {
        thumbHeight = parseInt(value);
        // Update all existing cards
        document.querySelectorAll('.rank-card').forEach(card => {
            const ar = parseFloat(card.dataset.ar) || 1.5;
            card.style.height = thumbHeight + 'px';
            card.style.flexBasis = (thumbHeight * ar) + 'px';
        });
    }

    function setFilter(key, value) {
        filters[key] = value;
        rankingsOffset = 0;
        rankingsExhausted = false;
        libraryImages = [];
        document.getElementById('rankings-grid').innerHTML = '';
        loadRankings();
    }

    function eloToStars(elo, comparisons) {
        if (comparisons === 0) return 0;
        if (elo >= 1500) return 5;
        if (elo >= 1350) return 4;
        if (elo >= 1250) return 3;
        if (elo >= 1150) return 2;
        return 1;
    }

    function getConfidenceClass(comparisons) {
        if (comparisons >= 10) return 'high';
        if (comparisons >= 3) return 'medium';
        return 'low';
    }

    function getTierClass(elo, comparisons) {
        if (comparisons < 5) return '';
        if (elo >= 1500) return 'tier-gold';
        if (elo >= 1350) return 'tier-silver';
        if (elo >= 1250) return 'tier-bronze';
        return '';
    }

    async function findSimilar() {
        if (lightboxIndex < 0 || lightboxIndex >= libraryImages.length) return;
        const img = libraryImages[lightboxIndex];
        closeLightbox();

        // Clear grid and show similar images
        searchQuery = '__similar__';
        const input = document.getElementById('search-input');
        if (input) input.value = `Similar to: ${img.filename}`;
        const clearBtn = document.getElementById('search-clear');
        if (clearBtn) clearBtn.classList.remove('hidden');
        const sortToggles = document.getElementById('sort-toggles');
        if (sortToggles) sortToggles.style.opacity = '0.3';

        libraryImages = [];
        const grid = document.getElementById('rankings-grid');
        grid.innerHTML = '';

        const res = await fetch(`/api/similar/${img.id}?limit=100`);
        const data = await res.json();

        for (let i = 0; i < data.images.length; i++) {
            const simg = data.images[i];
            const ar = simg.aspect_ratio || 1.5;
            const simPct = (simg.similarity * 100).toFixed(0);

            const card = document.createElement('div');
            card.className = 'rank-card';
            card.dataset.ar = ar;
            card.style.height = thumbHeight + 'px';
            card.style.flexGrow = ar;
            card.style.flexBasis = (thumbHeight * ar) + 'px';
            card.onclick = () => openLightbox(simg);

            card.innerHTML = `
                <img src="${simg.thumb_url}" alt="${simg.filename}" loading="lazy" onload="this.classList.add('loaded')">
                <div class="rank-card-info">
                    <span class="rank-elo">${simg.elo} Elo</span>
                    <span class="rank-similarity">${simPct}% match</span>
                </div>
            `;
            grid.appendChild(card);
            libraryImages.push(simg);
        }
    }

    // ==================== BATCH SELECTION ====================

    let batchMode = false;
    let batchSelected = new Set();

    function toggleBatchMode() {
        batchMode = !batchMode;
        batchSelected.clear();
        document.querySelectorAll('.rank-card').forEach(c => {
            c.classList.toggle('selectable', batchMode);
            c.classList.remove('selected');
        });
        updateBatchBar();
    }

    function toggleBatchSelect(imgId, card) {
        if (batchSelected.has(imgId)) {
            batchSelected.delete(imgId);
            card.classList.remove('selected');
        } else {
            batchSelected.add(imgId);
            card.classList.add('selected');
        }
        updateBatchBar();
    }

    function updateBatchBar() {
        let bar = document.getElementById('batch-bar');
        if (batchMode) {
            if (!bar) {
                bar = document.createElement('div');
                bar.id = 'batch-bar';
                bar.className = 'batch-bar';
                document.body.appendChild(bar);
            }
            bar.innerHTML = `
                <span>${batchSelected.size} selected</span>
                <button onclick="PhotoArchive.batchExport('json')">Export JSON</button>
                <button onclick="PhotoArchive.batchExport('csv')">Export CSV</button>
                <button class="batch-cancel" onclick="PhotoArchive.toggleBatchMode()">Cancel</button>
            `;
        } else if (bar) {
            bar.remove();
        }
    }

    function batchExport(format) {
        const ids = Array.from(batchSelected).join(',');
        window.open(`/api/export?format=${format}&ids=${ids}`, '_blank');
    }

    function exportRankings(format) {
        window.open(`/api/export?format=${format}`, '_blank');
    }

    // ==================== UTILITIES ====================

    const PRELOAD_LIMIT = 200;

    function preloadImage(url) {
        if (preloaded.has(url)) return;
        // Evict oldest entries when limit reached
        if (preloaded.size >= PRELOAD_LIMIT) {
            const first = preloaded.keys().next().value;
            preloaded.delete(first);
        }
        const promise = new Promise((resolve) => {
            const img = new Image();
            img.onload = resolve;
            img.onerror = resolve;
            img.src = url;
        });
        preloaded.set(url, promise);
    }

    // ==================== PUBLIC API ====================

    return {
        initCull,
        initCompare,
        initLibrary,
        initRankings,
        clearSearch,
        setCullMode,
        setCompareMode,
        setRankingsSort,
        exportRankings,
        closeLightbox,
        lightboxNext,
        lightboxPrev,
        setThumbSize,
        setFilter,
        findSimilar,
        toggleBatchMode,
        batchExport,
        gridSelectAll,
        gridSelectNone,
        gridSubmit,
        mosaicShuffle,
        setMosaicStrategy,
        toggleAIPanel,
    };
})();
