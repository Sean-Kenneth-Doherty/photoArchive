const PhotoRanker = (() => {
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
    let mosaicLastPick = null;
    let mosaicPickMode = 'best';

    async function loadMosaicBatch() {
        const res = await fetch(`/api/mosaic/next?n=${MOSAIC_SIZE}`);
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
        mosaicLastPick = null;
        mosaicReplacements = [];
        renderMosaic();
        mosaicFillReplacements();
    }

    function renderMosaic() {
        const grid = document.getElementById('mosaic-grid');
        grid.innerHTML = '';

        for (const img of mosaicImages) {
            const cell = document.createElement('div');
            cell.className = 'mosaic-cell';
            cell.dataset.id = img.id;
            cell.onclick = () => mosaicClick(img.id);
            cell.innerHTML = `
                <img src="${img.thumb_url}" alt="${img.filename}">
                <div class="mosaic-rank"></div>
                <div class="mosaic-filename">${img.filename}</div>
            `;
            preloadImage(img.thumb_url);
            grid.appendChild(cell);
        }
    }

    // Pre-fetched replacement images ready to swap in instantly
    let mosaicReplacements = [];

    async function mosaicFillReplacements() {
        if (mosaicReplacements.length >= 10) return;
        const excludeIds = [
            ...mosaicImages.map(img => img.id),
            ...mosaicReplacements.map(img => img.id),
        ].join(',');
        try {
            const res = await fetch(`/api/mosaic/next?n=10&exclude=${excludeIds}`);
            const data = await res.json();
            if (data.stats) {
                compareStats = data.stats;
                updateCompareProgress();
            }
            for (const img of data.images) {
                mosaicReplacements.push(img);
                preloadImage(img.thumb_url);
            }
        } catch {}
    }

    function mosaicClick(id) {
        const idx = mosaicImages.findIndex(img => img.id === id);
        if (idx === -1) return;

        const otherIds = mosaicImages.filter(img => img.id !== id).map(img => img.id);

        // Fire and forget — clicked image beats all others
        fetch('/api/mosaic/pick', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ winner_id: id, loser_ids: otherIds }),
        });

        // Update stats immediately (N-1 comparisons per click)
        compareStats.total_comparisons = (compareStats.total_comparisons || 0) + otherIds.length;
        updateCompareProgress();

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

        // Swap cells instantly from pre-fetched replacements
        const cells = document.querySelectorAll('.mosaic-cell');
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
            targetCell.querySelector('.mosaic-rank').textContent = '';
            targetCell.querySelector('.mosaic-filename').textContent = newImg.filename;
        }

        // Refill replacement buffer in the background
        mosaicFillReplacements();

        if (mosaicImages.length < 2) {
            showCompareEmpty();
        }
    }

    function mosaicUndo() {
        // Undo not practical in continuous mode — would need to undo N-1 comparisons
        // For now, just undo the last DB comparison entry
        if (!mosaicLastPick) return;
        fetch('/api/compare/undo', { method: 'POST' });
        mosaicLastPick = null;
    }

    function mosaicSkipRest() {
        // Shuffle the current grid with fresh images
        loadMosaicBatch();
    }

    function mosaicNext() {
        loadMosaicBatch();
    }

    // ==================== COMPARE MODE ====================

    async function initCompare() {
        document.addEventListener('keydown', handleCompareKey);
        document.getElementById('compare-left').addEventListener('click', () => submitComparison('left'));
        document.getElementById('compare-right').addEventListener('click', () => submitComparison('right'));
        // Start in mosaic mode by default
        setCompareMode('mosaic');
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
        const text = document.getElementById('compare-progress-text');
        const total = compareStats.total_comparisons || 0;
        const kept = compareStats.kept || 0;
        if (text) text.textContent = `${total} comparisons — ${kept} images in pool — ${compareMode} mode`;
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
        document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('active'));
        document.getElementById('mode-' + mode).classList.add('active');

        const abContainer = document.getElementById('compare-images');
        const abControls = document.querySelector('.compare-controls');
        const mosaicContainer = document.getElementById('mosaic-container');

        if (mode === 'mosaic') {
            // Show mosaic, hide A/B
            if (abContainer) abContainer.classList.add('hidden');
            if (abControls) abControls.classList.add('hidden');
            if (mosaicContainer) mosaicContainer.classList.remove('hidden');
            loadMosaicBatch();
        } else {
            // Show A/B, hide mosaic
            if (abContainer) abContainer.classList.remove('hidden');
            if (abControls) abControls.classList.remove('hidden');
            if (mosaicContainer) mosaicContainer.classList.add('hidden');
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

    // ==================== RANKINGS ====================

    async function initRankings() {
        rankingsOffset = 0;
        await loadRankings();
    }

    async function loadRankings() {
        const res = await fetch(`/api/rankings?limit=${RANKINGS_PAGE_SIZE}&offset=${rankingsOffset}`);
        const data = await res.json();
        const grid = document.getElementById('rankings-grid');

        for (let i = 0; i < data.images.length; i++) {
            const img = data.images[i];
            const rank = rankingsOffset + i + 1;

            const card = document.createElement('div');
            card.className = 'rank-card';
            card.onclick = () => openLightbox(img);
            card.innerHTML = `
                <img src="${img.thumb_url}" alt="${img.filename}" loading="lazy">
                <div class="rank-card-info">
                    <span class="rank-number">#${rank}</span>
                    <span class="rank-elo">${img.elo} Elo</span>
                </div>
                <div class="rank-filename">${img.filename}</div>
            `;
            grid.appendChild(card);
        }

        rankingsOffset += data.images.length;

        const loadMoreWrap = document.getElementById('load-more-wrap');
        if (data.images.length >= RANKINGS_PAGE_SIZE) {
            loadMoreWrap.classList.remove('hidden');
        } else {
            loadMoreWrap.classList.add('hidden');
        }
    }

    function loadMoreRankings() {
        loadRankings();
    }

    function openLightbox(img) {
        const lb = document.getElementById('lightbox');
        const lbImg = document.getElementById('lightbox-img');
        const lbInfo = document.getElementById('lightbox-info');
        // Use md size for lightbox
        lbImg.src = `/api/thumb/md/${img.id}`;
        lbInfo.textContent = `${img.filename} — ${img.elo} Elo — ${img.comparisons} comparisons`;
        lb.classList.remove('hidden');
    }

    function closeLightbox() {
        document.getElementById('lightbox').classList.add('hidden');
    }

    function exportRankings(format) {
        window.open(`/api/export?format=${format}`, '_blank');
    }

    // ==================== UTILITIES ====================

    function preloadImage(url) {
        if (preloaded.has(url)) return;
        const promise = new Promise((resolve) => {
            const img = new Image();
            img.onload = resolve;
            img.onerror = resolve; // resolve anyway so we don't block
            img.src = url;
        });
        preloaded.set(url, promise);
    }

    // ==================== PUBLIC API ====================

    return {
        initCull,
        initCompare,
        initRankings,
        setCullMode,
        setCompareMode,
        loadMoreRankings,
        exportRankings,
        closeLightbox,
        gridSelectAll,
        gridSelectNone,
        gridSubmit,
        mosaicUndo,
        mosaicSkipRest,
        mosaicNext,
    };
})();
