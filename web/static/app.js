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
    const INITIAL_RANKINGS_PAGE_SIZE = 48;
    const RANKINGS_PAGE_SIZE = 100;
    const BACKGROUND_WARM_DELAY_MS = 250;
    const LIBRARY_NEIGHBOR_LIMIT = 24;
    const MOSAIC_NEIGHBOR_LIMIT = 8;
    const COMPARE_NEIGHBOR_PAIRS = 4;
    const FILMSTRIP_WINDOW_RADIUS = 55;
    const LOUPE_FULL_LOAD_DELAY_MS = 750;
    const backgroundWarmTimers = new Map();
    const backgroundWarmTokens = new Map();
    const WARM_CACHE_PREFIX = 'photoarchive:warm:';
    const WARM_CACHE_MAX_AGE_MS = 30000;

    function scheduleBackgroundWarm(key, task, delay = BACKGROUND_WARM_DELAY_MS) {
        const token = (backgroundWarmTokens.get(key) || 0) + 1;
        backgroundWarmTokens.set(key, token);

        const existingTimer = backgroundWarmTimers.get(key);
        if (existingTimer) clearTimeout(existingTimer);

        const timer = setTimeout(() => {
            backgroundWarmTimers.delete(key);
            const run = () => Promise.resolve(task(token)).catch(() => {});
            if (typeof window !== 'undefined' && 'requestIdleCallback' in window) {
                window.requestIdleCallback(run, { timeout: 1500 });
            } else {
                run();
            }
        }, delay);

        backgroundWarmTimers.set(key, timer);
    }

    function isWarmTokenCurrent(key, token) {
        return backgroundWarmTokens.get(key) === token;
    }

    async function fetchWarmJson(url) {
        try {
            const res = await fetch(url);
            if (!res.ok) return null;
            return await res.json();
        } catch {
            return null;
        }
    }

    function warmCacheKey(key) {
        return `${WARM_CACHE_PREFIX}${key}`;
    }

    function saveWarmCache(key, data) {
        if (typeof sessionStorage === 'undefined' || !data) return;
        try {
            sessionStorage.setItem(
                warmCacheKey(key),
                JSON.stringify({ savedAt: Date.now(), data }),
            );
        } catch {}
    }

    function takeWarmCache(key) {
        if (typeof sessionStorage === 'undefined') return null;
        try {
            const storageKey = warmCacheKey(key);
            const raw = sessionStorage.getItem(storageKey);
            if (!raw) return null;
            sessionStorage.removeItem(storageKey);
            const parsed = JSON.parse(raw);
            if (!parsed?.data) return null;
            if (Date.now() - Number(parsed.savedAt || 0) > WARM_CACHE_MAX_AGE_MS) return null;
            return parsed.data;
        } catch {
            return null;
        }
    }

    function warmImageUrls(urls) {
        for (const url of urls || []) {
            preloadImage(url, 'low');
        }
    }

    async function warmRequests(key, token, requests) {
        for (const request of requests) {
            if (!isWarmTokenCurrent(key, token)) return;
            const data = await fetchWarmJson(request.url);
            if (!data || !isWarmTokenCurrent(key, token)) return;
            if (request.cacheKey) saveWarmCache(request.cacheKey, data);
            warmImageUrls(request.extract(data));
        }
    }

    function imageThumbUrls(data) {
        return (data.images || []).map((img) => img.thumb_url).filter(Boolean);
    }

    function compareThumbUrls(data) {
        const urls = [];
        for (const pair of data.pairs || []) {
            if (pair.left?.thumb_url) urls.push(pair.left.thumb_url);
            if (pair.right?.thumb_url) urls.push(pair.right.thumb_url);
        }
        return urls;
    }

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
        const url = buildMosaicUrl({ n: MOSAIC_SIZE });
        const data = takeWarmCache(`compare:${url}`) || await fetchWarmJson(url);
        if (!data) return;
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
        scheduleCompareNeighborWarmup('mosaic');
        scheduleCrossViewWarmup('compare');
    }

    function renderMosaic() {
        const grid = document.getElementById('mosaic-grid');
        grid.innerHTML = '';

        // Calculate row height to fit all images in the viewport
        // using the same justified flex layout as the library
        const gap = 3;
        const containerW = grid.clientWidth || window.innerWidth;
        const containerH = grid.clientHeight || (window.innerHeight - 60);
        const n = mosaicImages.length;

        // Simulate row packing to find the right row height
        // Binary search for the height that fits all images in the container
        let lo = 60, hi = containerH;
        for (let iter = 0; iter < 20; iter++) {
            const mid = (lo + hi) / 2;
            let rows = 1, rowW = 0;
            for (const img of mosaicImages) {
                const ar = img.aspect_ratio || 1.5;
                const w = mid * ar + gap;
                if (rowW + w > containerW + gap && rowW > 0) {
                    rows++;
                    rowW = w;
                } else {
                    rowW += w;
                }
            }
            const totalH = rows * (mid + gap);
            if (totalH > containerH) hi = mid;
            else lo = mid;
        }
        const rowH = Math.floor(lo);

        for (const img of mosaicImages) {
            const ar = img.aspect_ratio || 1.5;
            const cell = document.createElement('div');
            cell.className = 'mosaic-cell';
            cell.dataset.id = img.id;
            cell.style.height = rowH + 'px';
            cell.style.flexGrow = ar;
            cell.style.flexBasis = (rowH * ar) + 'px';
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
            const res = await fetch(`/api/mosaic/next?n=10&exclude=${excludeIds}&strategy=${mosaicStrategy}&grid_elo=${mosaicGridElo()}${filterParams()}`);
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
        setCompareMode('mosaic');
        pollAIStatus();
        setInterval(pollAIStatus, 5000);
        // Load folder list for filter dropdown
        loadFolderList();
        // Star hover preview
        initStarHover();
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
                if (data.installing) {
                    stateEl.textContent = 'Installing';
                    stateEl.className = 'bar-ai-state embedding';
                } else if (!data.model_installed) {
                    stateEl.textContent = 'Install';
                    stateEl.className = 'bar-ai-state';
                } else if (!data.worker_ready && data.worker_state === 'loading_model') {
                    stateEl.textContent = 'Loading';
                    stateEl.className = 'bar-ai-state embedding';
                } else if (data.embedded < data.total_kept) {
                    stateEl.textContent = 'Embedding';
                    stateEl.className = 'bar-ai-state embedding';
                } else if (data.embedded > 0) {
                    stateEl.textContent = 'Ready';
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
                if (data.installing) {
                    modelText.textContent = data.install_message || `Installing ${data.model_id}`;
                    modelText.className = 'ai-panel-value';
                } else if (!data.model_installed) {
                    modelText.textContent = `Model not installed. Open Settings to install ${data.model_id}.`;
                    modelText.className = 'ai-panel-value';
                } else if (data.worker_state === 'loading_model') {
                    modelText.textContent = data.worker_message || `Loading ${data.model_id}`;
                    modelText.className = 'ai-panel-value';
                } else if (data.embedded > 0) {
                    modelText.textContent = `${data.compared.toLocaleString()} images compared · Elo propagation active`;
                    modelText.className = 'ai-panel-value trained';
                } else {
                    modelText.textContent = 'Not started';
                    modelText.className = 'ai-panel-value';
                }
            }
            if (predText) {
                if (data.compared > 0) {
                    predText.textContent = `${data.compared.toLocaleString()} images ranked via comparisons + propagation`;
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
        const url = buildCompareUrl(compareMode, 8);
        const useCache = compareIndex === 0 && comparePairs.length === 0;
        const data = (useCache ? takeWarmCache(`compare:${url}`) : null) || await fetchWarmJson(url);
        if (!data) return;
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

        if (compareIndex === 0) {
            scheduleCompareNeighborWarmup(compareMode);
        }
        scheduleCrossViewWarmup('compare');
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
    let sortField = 'elo';
    let sortDesc = true;
    let searchQuery = '';
    let searchDebounce = null;
    let rankingsLoading = false;
    let rankingsExhausted = false;
    let thumbHeight = 220;
    let libraryImages = [];
    let lightboxIndex = -1;
    let filters = { orientation: '', compared: '', rating: '', folder: '' };

    function filterParams(state = filters) {
        let p = '';
        if (state.orientation) p += `&orientation=${state.orientation}`;
        if (state.compared) p += `&compared=${state.compared}`;
        if (state.rating) p += `&min_stars=${state.rating}`;
        if (state.folder) p += `&folder=${encodeURIComponent(state.folder)}`;
        return p;
    }

    function currentFilterState() {
        return {
            orientation: filters.orientation || '',
            compared: filters.compared || '',
            rating: filters.rating || '',
            folder: filters.folder || '',
        };
    }

    function buildFilterNeighborStates(baseState = currentFilterState()) {
        const states = [];
        const seen = new Set();
        const currentKey = JSON.stringify(baseState);

        function pushState(patch) {
            const state = { ...baseState, ...patch };
            const key = JSON.stringify(state);
            if (key === currentKey || seen.has(key)) return;
            seen.add(key);
            states.push(state);
        }

        if (!baseState.orientation) {
            pushState({ orientation: 'landscape' });
            pushState({ orientation: 'portrait' });
        } else {
            pushState({ orientation: '' });
        }

        if (!baseState.compared) {
            pushState({ compared: 'compared' });
            pushState({ compared: 'uncompared' });
        } else {
            pushState({ compared: '' });
        }

        if (baseState.folder) {
            pushState({ folder: '' });
        }

        const rating = Number(baseState.rating || 0);
        if (rating > 0) {
            pushState({ rating: rating > 1 ? String(rating - 1) : '' });
            if (rating < 5) pushState({ rating: String(rating + 1) });
        }

        return states;
    }

    function buildRankingsUrl({ sort = rankingsSort, filterState = currentFilterState(), limit = LIBRARY_NEIGHBOR_LIMIT, offset = 0 } = {}) {
        return `/api/rankings?limit=${limit}&offset=${offset}&sort=${sort}${filterParams(filterState)}`;
    }

    function buildMosaicUrl({
        strategy = mosaicStrategy,
        filterState = currentFilterState(),
        gridElo = mosaicGridElo(),
        n = MOSAIC_NEIGHBOR_LIMIT,
        exclude = '',
    } = {}) {
        let url = `/api/mosaic/next?n=${n}&strategy=${strategy}&grid_elo=${gridElo}${filterParams(filterState)}`;
        if (exclude) url += `&exclude=${exclude}`;
        return url;
    }

    function buildCompareUrl(mode, n = COMPARE_NEIGHBOR_PAIRS) {
        return `/api/compare/next?n=${n}&mode=${mode}`;
    }

    function currentLibraryPageSize() {
        return rankingsOffset === 0 ? INITIAL_RANKINGS_PAGE_SIZE : RANKINGS_PAGE_SIZE;
    }

    function scheduleCrossViewWarmup(fromView) {
        if (fromView === 'compare') {
            const libraryUrl = buildRankingsUrl({
                sort: 'elo',
                filterState: { orientation: '', compared: '', rating: '', folder: '' },
                limit: INITIAL_RANKINGS_PAGE_SIZE,
                offset: 0,
            });
            const requests = [{
                url: libraryUrl,
                cacheKey: `library:${libraryUrl}`,
                extract: imageThumbUrls,
            }];
            scheduleBackgroundWarm(
                'crossview-library',
                (token) => warmRequests('crossview-library', token, requests),
                200,
            );
        } else if (fromView === 'library') {
            const compareUrl = buildMosaicUrl({
                strategy: 'explore',
                filterState: { orientation: '', compared: '', rating: '', folder: '' },
                gridElo: 0,
                n: MOSAIC_SIZE,
            });
            const requests = [{
                url: compareUrl,
                cacheKey: `compare:${compareUrl}`,
                extract: imageThumbUrls,
            }];
            scheduleBackgroundWarm(
                'crossview-compare',
                (token) => warmRequests('crossview-compare', token, requests),
                200,
            );
        }
    }

    function scheduleLibraryNeighborWarmup() {
        if (searchQuery) return;

        const requests = [];
        const seen = new Set();
        const addRequest = (url) => {
            if (seen.has(url)) return;
            seen.add(url);
            requests.push({ url, cacheKey: `library:${url}`, extract: imageThumbUrls });
        };

        const sortKey = SORT_KEYS[sortField];
        if (sortKey) {
            addRequest(buildRankingsUrl({ sort: sortDesc ? sortKey.asc : sortKey.desc }));
        }

        addRequest(buildRankingsUrl({ sort: rankingsSort === 'elo' ? 'comparisons' : 'elo' }));

        for (const neighborState of buildFilterNeighborStates().slice(0, 3)) {
            addRequest(buildRankingsUrl({ filterState: neighborState }));
        }

        if (!requests.length) return;
        scheduleBackgroundWarm('library-neighbors', (token) => warmRequests('library-neighbors', token, requests));
    }

    function scheduleCompareNeighborWarmup(mode = compareMode) {
        const requests = [];

        if (mode === 'mosaic') {
            requests.push({ url: buildCompareUrl('swiss'), extract: compareThumbUrls });
            requests.push({ url: buildCompareUrl('topn'), extract: compareThumbUrls });

            for (const neighborState of buildFilterNeighborStates().slice(0, 2)) {
                requests.push({
                    url: buildMosaicUrl({ filterState: neighborState, gridElo: 0 }),
                    extract: imageThumbUrls,
                });
            }
        } else {
            requests.push({
                url: buildCompareUrl(mode === 'topn' ? 'swiss' : 'topn'),
                extract: compareThumbUrls,
            });
            requests.push({
                url: buildMosaicUrl({ gridElo: 0 }),
                extract: imageThumbUrls,
            });
        }

        scheduleBackgroundWarm('compare-neighbors', (token) => warmRequests('compare-neighbors', token, requests), 350);
    }

    const SORT_KEYS = {
        'elo':         { desc: 'elo',           asc: 'elo_asc' },
        'comparisons': { desc: 'comparisons',   asc: 'least_compared' },
        'newest':      { desc: 'newest',        asc: 'oldest' },
        'filename':    { desc: 'filename_desc', asc: 'filename' },
    };

    async function initLibrary() {
        rankingsOffset = 0;
        rankingsExhausted = false;

        // Fire all init requests in parallel — don't block on rankings
        const rankingsPromise = loadRankings();
        const statsPromise = fetch('/api/stats').then(r => r.json()).then(stats => {
            compareStats = stats;
            updateCompareProgress();
        }).catch(() => {});

        loadFolderList();
        initStarHover();
        pollAIStatus();
        setInterval(pollAIStatus, 5000);

        await rankingsPromise;
        await statsPromise;

        // Infinite scroll via IntersectionObserver (avoids continuous scroll events)
        const sentinel = document.createElement('div');
        sentinel.style.height = '1px';
        document.querySelector('.rankings-grid')?.after(sentinel);
        const scrollObserver = new IntersectionObserver((entries) => {
            if (entries[0].isIntersecting && !rankingsLoading && !rankingsExhausted && !searchQuery) {
                loadRankings();
            }
        }, { rootMargin: '600px' });
        scrollObserver.observe(sentinel);

        // Loupe keyboard navigation
        document.addEventListener('keydown', (e) => {
            const loupe = document.getElementById('loupe');
            if (!loupe || loupe.classList.contains('hidden')) return;
            if (e.key === 'ArrowRight') { e.preventDefault(); lightboxNext(); }
            else if (e.key === 'ArrowLeft') { e.preventDefault(); lightboxPrev(); }
            else if (e.key === 'Escape') { closeLightbox(); }
        });

        // Loupe zoom/pan interaction
        initLoupeInteraction();

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
        rankingsLoading = false;
        loadRankings(true);
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
        if (searchQuery) return;
        rankingsSort = sort;
        rankingsOffset = 0;
        rankingsExhausted = false;
        libraryImages = [];
        // Clear grid only after new data arrives to avoid blank flash
        rankingsLoading = false; // allow loadRankings to proceed
        loadRankings(true);  // true = clear grid before appending
    }

    function setSortField(field) {
        if (searchQuery) return;
        sortField = field;
        sortDesc = true; // reset to desc when changing field
        updateSortDirIcon();
        const key = SORT_KEYS[field];
        setRankingsSort(key ? key.desc : field);
    }

    function toggleSortDir() {
        if (searchQuery) return;
        sortDesc = !sortDesc;
        updateSortDirIcon();
        const key = SORT_KEYS[sortField];
        if (key) {
            setRankingsSort(sortDesc ? key.desc : key.asc);
        }
    }

    function updateSortDirIcon() {
        const btn = document.getElementById('sort-dir-btn');
        if (btn) btn.classList.toggle('active', !sortDesc);
    }

    async function loadRankings(clearFirst = false) {
        if (rankingsLoading) return;
        rankingsLoading = true;
        const requestOffset = rankingsOffset;
        const limit = currentLibraryPageSize();
        let url = `/api/rankings?limit=${limit}&offset=${requestOffset}&sort=${rankingsSort}${filterParams()}`;
        const data = (requestOffset === 0 ? takeWarmCache(`library:${url}`) : null) || await fetchWarmJson(url);
        if (!data) {
            rankingsLoading = false;
            return;
        }
        const grid = document.getElementById('rankings-grid');
        if (clearFirst) grid.innerHTML = '';
        const showRank = (rankingsSort === 'elo' || rankingsSort === 'elo_asc');
        const rowH = thumbHeight;

        // Batch DOM writes with DocumentFragment to avoid per-card reflows
        const frag = document.createDocumentFragment();
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

            const confDot = conf ? `<div class="rank-confidence ${conf}"></div>` : '';
            const infoLine = showRank
                ? `<span class="rank-number">#${rank}</span><span class="rank-elo">${img.elo}</span>`
                : `<span class="rank-elo">${img.elo}</span><span class="rank-comparisons">${img.comparisons} cmp</span>`;

            card.innerHTML = `
                <img src="${img.thumb_url}" alt="${img.filename}" loading="lazy" onload="this.classList.add('loaded')">
                <div class="select-check">✓</div>
                ${confDot}
                <div class="rank-card-info">${infoLine}</div>
            `;
            frag.appendChild(card);
            libraryImages.push(img);
        }
        grid.appendChild(frag);

        rankingsOffset += data.images.length;
        rankingsLoading = false;
        if (data.images.length < limit) {
            rankingsExhausted = true;
        }
        if (data.images.length > 0) {
            // Always warm neighbors — not just on first load
            scheduleLibraryNeighborWarmup();
            if (requestOffset === 0) scheduleCrossViewWarmup('library');
        }
    }

    function openLightbox(img) {
        lightboxIndex = libraryImages.findIndex(i => i.id === img.id);
        if (lightboxIndex < 0) lightboxIndex = 0;
        buildFilmstrip();
        showLoupeImage(libraryImages[lightboxIndex]);
    }

    let _filmstripBuiltFor = null;  // track which image set the filmstrip was built for
    let _filmstripWindowStart = 0;
    let _filmstripWindowEnd = 0;

    function buildFilmstrip() {
        const scroll = document.getElementById('filmstrip-scroll');
        if (!scroll) return;
        const start = Math.max(0, lightboxIndex - FILMSTRIP_WINDOW_RADIUS);
        const end = Math.min(libraryImages.length, lightboxIndex + FILMSTRIP_WINDOW_RADIUS + 1);
        const windowStillUseful = (
            _filmstripBuiltFor === libraryImages &&
            lightboxIndex >= _filmstripWindowStart &&
            lightboxIndex < _filmstripWindowEnd &&
            lightboxIndex - _filmstripWindowStart > 10 &&
            _filmstripWindowEnd - lightboxIndex > 10
        );

        if (windowStillUseful) {
            updateFilmstripActive();
            return;
        }

        _filmstripBuiltFor = libraryImages;
        _filmstripWindowStart = start;
        _filmstripWindowEnd = end;
        scroll.innerHTML = '';
        for (let i = start; i < end; i++) {
            const img = libraryImages[i];
            const thumb = document.createElement('div');
            thumb.className = 'filmstrip-thumb' + (i === lightboxIndex ? ' active' : '');
            thumb.dataset.idx = i;
            thumb.onclick = () => {
                lightboxIndex = i;
                showLoupeImage(libraryImages[i]);
            };
            thumb.innerHTML = `<img src="${img.thumb_url}" alt="${img.filename}" loading="lazy">`;
            scroll.appendChild(thumb);
        }
        centerFilmstripActive(scroll);
    }

    function updateFilmstripActive() {
        const scroll = document.getElementById('filmstrip-scroll');
        if (!scroll) return;
        if (lightboxIndex < _filmstripWindowStart || lightboxIndex >= _filmstripWindowEnd) {
            buildFilmstrip();
            return;
        }
        scroll.querySelectorAll('.filmstrip-thumb').forEach((el) => {
            el.classList.toggle('active', Number(el.dataset.idx) === lightboxIndex);
        });
        centerFilmstripActive(scroll);
    }

    function centerFilmstripActive(scroll) {
        const active = scroll.querySelector('.filmstrip-thumb.active');
        if (!active) return;

        requestAnimationFrame(() => {
            const maxScroll = Math.max(0, scroll.scrollWidth - scroll.clientWidth);
            const centeredLeft = active.offsetLeft + (active.offsetWidth / 2) - (scroll.clientWidth / 2);
            const targetLeft = Math.max(0, Math.min(maxScroll, centeredLeft));
            scroll.scrollTo({ left: targetLeft, behavior: 'auto' });
        });
    }

    // Loupe zoom/pan state
    let loupeScale = 1;
    let loupeFitScale = 1;
    let loupePanX = 0;
    let loupePanY = 0;
    let loupeNatW = 0;
    let loupeNatH = 0;
    let loupeIsFit = true;
    let _loupeDragMoved = false;
    let _loupeDragging = false;
    let loupeZoomMode = 'fit';
    let loupeDisplayedTierRank = -1;
    let loupeImageToken = 0;
    let loupeCurrentImage = null;
    let loupeFullLoadTimer = null;
    let loupeFullLoadToken = 0;
    const loupeRefLong = 3840; // lg thumbnail long side used before original dimensions are known
    const LOUPE_PRELOAD_RADIUS = 3;

    function showLoupeImage(img) {
        const loupe = document.getElementById('loupe');
        const loupeImg = document.getElementById('loupe-img');
        if (!loupe || !loupeImg) return;

        // Pre-calculate fit dimensions from aspect ratio so all progressive
        // loads (sm/md/lg) display at the same screen size — no size jumps
        const token = ++loupeImageToken;
        loupeCurrentImage = img;
        loupeFullLoadToken = 0;
        if (loupeFullLoadTimer) {
            clearTimeout(loupeFullLoadTimer);
            loupeFullLoadTimer = null;
        }
        loupeDisplayedTierRank = -1;
        loupeIsFit = true;
        loupeZoomMode = 'fit';
        const ar = img.aspect_ratio || 1.5;
        loupeNatW = ar >= 1 ? loupeRefLong : Math.round(loupeRefLong * ar);
        loupeNatH = ar >= 1 ? Math.round(loupeRefLong / ar) : loupeRefLong;
        loupeImg.style.transition = 'opacity 0.15s';
        loupeApplyImageSize();

        document.body.classList.add('loupe-open');
        loupe.classList.remove('hidden');
        loupeCenterFit({ animate: false });

        // Progressive loading: sm -> md -> lg -> original. Loads can finish
        // out of order, so rank checks prevent a late small image from
        // replacing a sharper one.
        loupeImg.alt = img.filename || '';
        loupeImg.src = img.thumb_url;
        loupeDisplayedTierRank = 0;
        loadLoupeTier(img, `/api/thumb/md/${img.id}`, 1, token, { adoptDimensions: false });
        loadLoupeTier(img, `/api/thumb/lg/${img.id}`, 2, token, { adoptDimensions: false });
        scheduleLoupeFullLoad(img, token, LOUPE_FULL_LOAD_DELAY_MS);

        // Populate metadata overlay
        const filenameEl = document.getElementById('loupe-overlay-filename');
        const exifEl = document.getElementById('loupe-overlay-exif');
        const statsEl = document.getElementById('loupe-overlay-stats');

        if (filenameEl) filenameEl.textContent = img.filename;
        if (exifEl) exifEl.textContent = '';

        const stars = eloToStars(img.elo, img.comparisons);
        const starStr = stars > 0 ? '★'.repeat(stars) + '☆'.repeat(5 - stars) + '  ' : '';
        if (statsEl) statsEl.textContent = `${starStr}${img.elo} Elo · ${img.comparisons} comparisons`;

        updateFilmstripActive();

        preloadLoupeNeighbors();

        // Load EXIF
        fetch(`/api/image/${img.id}/exif`).then(r => r.json()).then(data => {
            if (!data.exif || libraryImages[lightboxIndex]?.id !== img.id) return;
            const e = data.exif;
            const parts = [];
            const camera = [e.camera_make, e.camera_model].filter(Boolean).join(' ');
            if (camera) parts.push(camera);
            const settings = [];
            if (e.focal_length) settings.push(e.focal_length);
            if (e.aperture) settings.push(e.aperture);
            if (e.shutter_speed) settings.push(e.shutter_speed + 's');
            if (e.iso) settings.push('ISO ' + e.iso);
            if (settings.length) parts.push(settings.join('  '));
            if (e.lens) parts.push(e.lens);
            if (exifEl) exifEl.textContent = parts.join('\n');
            if (e.dimensions && statsEl) {
                statsEl.textContent += ' · ' + e.dimensions;
            }
        }).catch(() => {});
    }

    function isCurrentLoupeImage(img, token) {
        return token === loupeImageToken && libraryImages[lightboxIndex]?.id === img.id;
    }

    function loupeApplyImageSize() {
        const img = document.getElementById('loupe-img');
        if (!img || !loupeNatW || !loupeNatH) return;
        img.style.width = `${loupeNatW}px`;
        img.style.height = `${loupeNatH}px`;
    }

    function loadLoupeTier(img, url, rank, token, { adoptDimensions = false } = {}) {
        if (!url) return;
        const probe = new Image();
        probe.decoding = 'async';
        if ('fetchPriority' in probe) probe.fetchPriority = rank >= 2 ? 'high' : 'auto';
        probe.onload = () => {
            if (!isCurrentLoupeImage(img, token) || rank <= loupeDisplayedTierRank) return;

            if (adoptDimensions && probe.naturalWidth > 0 && probe.naturalHeight > 0) {
                loupeAdoptSourceDimensions(probe.naturalWidth, probe.naturalHeight);
            }

            const loupeImg = document.getElementById('loupe-img');
            if (!loupeImg) return;
            loupeDisplayedTierRank = rank;
            loupeImg.src = probe.src;
        };
        probe.onerror = () => {};
        probe.src = url;
    }

    function scheduleLoupeFullLoad(img, token, delayMs) {
        if (loupeFullLoadTimer) clearTimeout(loupeFullLoadTimer);
        loupeFullLoadTimer = setTimeout(() => {
            loupeFullLoadTimer = null;
            requestLoupeFullImage(img, token);
        }, delayMs);
    }

    function requestLoupeFullImage(img = loupeCurrentImage, token = loupeImageToken) {
        if (!img || !isCurrentLoupeImage(img, token) || loupeFullLoadToken === token) return;
        loupeFullLoadToken = token;
        if (loupeFullLoadTimer) {
            clearTimeout(loupeFullLoadTimer);
            loupeFullLoadTimer = null;
        }
        loadLoupeTier(img, `/api/full/${img.id}`, 3, token, { adoptDimensions: true });
    }

    function loupeAdoptSourceDimensions(width, height) {
        const wrap = document.getElementById('loupe-image-wrap');
        const oldW = loupeNatW;
        const oldH = loupeNatH;
        if (!wrap || !oldW || !oldH || width <= 0 || height <= 0) {
            loupeNatW = width;
            loupeNatH = height;
            loupeApplyImageSize();
            return;
        }

        if (Math.abs(oldW - width) < 1 && Math.abs(oldH - height) < 1) return;

        const focusX = Math.max(0, Math.min(1, ((wrap.clientWidth / 2) - loupePanX) / loupeScale / oldW));
        const focusY = Math.max(0, Math.min(1, ((wrap.clientHeight / 2) - loupePanY) / loupeScale / oldH));
        const oldScale = loupeScale;
        const wasFit = loupeZoomMode === 'fit' || loupeIsFit;
        const wasOneToOne = loupeZoomMode === 'one-to-one';

        loupeNatW = width;
        loupeNatH = height;
        loupeApplyImageSize();
        loupeFitScale = loupeComputeFitScale();

        if (wasFit) {
            loupeScale = loupeFitScale;
            loupePanX = (wrap.clientWidth - loupeNatW * loupeScale) / 2;
            loupePanY = (wrap.clientHeight - loupeNatH * loupeScale) / 2;
            loupeIsFit = true;
            loupeZoomMode = 'fit';
        } else {
            loupeScale = wasOneToOne ? 1 : oldScale * (oldW / loupeNatW);
            loupePanX = (wrap.clientWidth / 2) - (focusX * loupeNatW * loupeScale);
            loupePanY = (wrap.clientHeight / 2) - (focusY * loupeNatH * loupeScale);
            loupeIsFit = false;
            loupeZoomMode = wasOneToOne ? 'one-to-one' : 'custom';
            loupeClampPan();
        }

        loupeApplyTransform();
        wrap.style.cursor = loupeIsFit ? 'zoom-in' : 'grab';
    }

    function preloadLoupeNeighbors() {
        for (let distance = 1; distance <= LOUPE_PRELOAD_RADIUS; distance++) {
            for (const offset of [-distance, distance]) {
                const ni = lightboxIndex + offset;
                if (ni < 0 || ni >= libraryImages.length) continue;
                const neighbor = libraryImages[ni];
                preloadImage(neighbor.thumb_url, 'low');
                preloadImage(`/api/thumb/md/${neighbor.id}`, 'low');
                preloadImage(`/api/thumb/lg/${neighbor.id}`, 'low');
            }
        }
    }

    // ==================== LOUPE ZOOM/PAN ====================

    function loupeComputeFitScale() {
        const wrap = document.getElementById('loupe-image-wrap');
        if (!wrap || !loupeNatW || !loupeNatH) return 1;
        return Math.min(wrap.clientWidth / loupeNatW, wrap.clientHeight / loupeNatH);
    }

    function loupeApplyTransform() {
        const img = document.getElementById('loupe-img');
        if (!img) return;
        img.style.transform = `translate(${loupePanX}px, ${loupePanY}px) scale(${loupeScale})`;
    }

    function loupeCenterFit({ animate = true } = {}) {
        const wrap = document.getElementById('loupe-image-wrap');
        const img = document.getElementById('loupe-img');
        if (!wrap || !img) return;
        loupeApplyImageSize();
        loupeFitScale = loupeComputeFitScale();
        loupeScale = loupeFitScale;
        loupePanX = (wrap.clientWidth - loupeNatW * loupeScale) / 2;
        loupePanY = (wrap.clientHeight - loupeNatH * loupeScale) / 2;
        loupeIsFit = true;
        loupeZoomMode = 'fit';
        if (animate) img.style.transition = 'transform 0.2s ease-out, opacity 0.15s';
        loupeApplyTransform();
        wrap.style.cursor = 'zoom-in';
        if (animate) setTimeout(() => { if (img) img.style.transition = 'opacity 0.15s'; }, 200);
    }

    function loupeZoomTo(newScale, pivotX, pivotY, mode = 'custom') {
        const wrap = document.getElementById('loupe-image-wrap');
        if (!wrap) return;

        const rect = wrap.getBoundingClientRect();
        const imgX = (pivotX - rect.left - loupePanX) / loupeScale;
        const imgY = (pivotY - rect.top - loupePanY) / loupeScale;

        const minScale = Math.max(0.01, Math.min(loupeFitScale, 1) * 0.5);
        const maxScale = Math.max(4, loupeFitScale * 4);
        loupeScale = Math.max(minScale, Math.min(maxScale, newScale));

        loupePanX = pivotX - rect.left - imgX * loupeScale;
        loupePanY = pivotY - rect.top - imgY * loupeScale;

        loupeIsFit = Math.abs(loupeScale - loupeFitScale) < 0.001;
        loupeZoomMode = loupeIsFit ? 'fit' : mode;
        loupeClampPan();
        loupeApplyTransform();
        wrap.style.cursor = loupeIsFit ? 'zoom-in' : 'grab';
    }

    function loupeClampPan() {
        const wrap = document.getElementById('loupe-image-wrap');
        if (!wrap) return;
        const cw = wrap.clientWidth;
        const ch = wrap.clientHeight;
        const iw = loupeNatW * loupeScale;
        const ih = loupeNatH * loupeScale;

        if (iw <= cw) {
            loupePanX = (cw - iw) / 2;
        } else {
            loupePanX = Math.min(0, Math.max(cw - iw, loupePanX));
        }

        if (ih <= ch) {
            loupePanY = (ch - ih) / 2;
        } else {
            loupePanY = Math.min(0, Math.max(ch - ih, loupePanY));
        }
    }

    function initLoupeInteraction() {
        const wrap = document.getElementById('loupe-image-wrap');
        const img = document.getElementById('loupe-img');
        if (!wrap || !img) return;

        // Drag to pan
        let dragStartX = 0, dragStartY = 0;
        let dragPanStartX = 0, dragPanStartY = 0;

        wrap.addEventListener('mousedown', (e) => {
            if (e.button !== 0) return;
            const img = document.getElementById('loupe-img');
            if (img) img.style.transition = 'opacity 0.15s';
            _loupeDragMoved = false;
            dragStartX = e.clientX;
            dragStartY = e.clientY;
            dragPanStartX = loupePanX;
            dragPanStartY = loupePanY;

            if (!loupeIsFit) {
                _loupeDragging = true;
                wrap.style.cursor = 'grabbing';
                e.preventDefault();
            }
        });

        window.addEventListener('mousemove', (e) => {
            if (!_loupeDragging) return;
            const dx = e.clientX - dragStartX;
            const dy = e.clientY - dragStartY;
            if (Math.abs(dx) > 3 || Math.abs(dy) > 3) _loupeDragMoved = true;
            loupePanX = dragPanStartX + dx;
            loupePanY = dragPanStartY + dy;
            loupeClampPan();
            loupeApplyTransform();
        });

        window.addEventListener('mouseup', () => {
            if (_loupeDragging) {
                _loupeDragging = false;
                wrap.style.cursor = loupeIsFit ? 'zoom-in' : 'grab';
            }
        });

        // Click to toggle between fit and 1:1
        wrap.addEventListener('click', (e) => {
            if (_loupeDragMoved) { _loupeDragMoved = false; return; }

            if (loupeIsFit) {
                // 1 image pixel == 1 CSS pixel against the current highest-res basis.
                const targetScale = 1;
                const img = document.getElementById('loupe-img');
                requestLoupeFullImage();
                if (img) img.style.transition = 'transform 0.2s ease-out, opacity 0.15s';
                loupeZoomTo(targetScale, e.clientX, e.clientY, 'one-to-one');
                setTimeout(() => { if (img) img.style.transition = 'opacity 0.15s'; }, 200);
            } else {
                loupeCenterFit();
            }
        });

        // Mouse wheel zoom (centered on cursor)
        wrap.addEventListener('wheel', (e) => {
            e.preventDefault();
            const img = document.getElementById('loupe-img');
            requestLoupeFullImage();
            if (img) img.style.transition = 'opacity 0.15s';
            const factor = e.deltaY < 0 ? 1.15 : 1 / 1.15;
            loupeZoomTo(loupeScale * factor, e.clientX, e.clientY, 'custom');
        }, { passive: false });

        // Recalculate fit on window resize
        window.addEventListener('resize', () => {
            if (!loupeNatW) return;
            loupeFitScale = loupeComputeFitScale();
            if (loupeIsFit) {
                loupeCenterFit({ animate: false });
            } else {
                loupeClampPan();
                loupeApplyTransform();
            }
        });
    }

    function lightboxNext() {
        if (lightboxIndex < 0 || lightboxIndex >= libraryImages.length - 1) return;
        lightboxIndex++;
        showLoupeImage(libraryImages[lightboxIndex]);
    }

    function lightboxPrev() {
        if (lightboxIndex <= 0) return;
        lightboxIndex--;
        showLoupeImage(libraryImages[lightboxIndex]);
    }

    function closeLightbox() {
        const loupe = document.getElementById('loupe');
        if (loupe) loupe.classList.add('hidden');
        document.body.classList.remove('loupe-open');
        loupeImageToken++;
        if (loupeFullLoadTimer) {
            clearTimeout(loupeFullLoadTimer);
            loupeFullLoadTimer = null;
        }
        loupeCurrentImage = null;
        loupeFullLoadToken = 0;
        loupeDisplayedTierRank = -1;
        loupeIsFit = true;
        loupeZoomMode = 'fit';
        loupeNatW = 0;
        loupeNatH = 0;
        lightboxIndex = -1;
    }

    function setThumbSize(value) {
        thumbHeight = parseInt(value);
        // Use CSS custom property to avoid per-card layout thrashing
        document.documentElement.style.setProperty('--thumb-height', thumbHeight + 'px');
        // Cards that need explicit flex-basis still need individual updates,
        // but batch reads first, then writes to avoid interleaved reflows.
        const cards = document.querySelectorAll('.rank-card');
        const updates = [];
        for (const card of cards) {
            updates.push({ el: card, basis: thumbHeight * (parseFloat(card.dataset.ar) || 1.5) });
        }
        for (const { el, basis } of updates) {
            el.style.height = thumbHeight + 'px';
            el.style.flexBasis = basis + 'px';
        }
    }

    function reloadForFilters() {
        // Reload the appropriate view based on which page we're on
        const grid = document.getElementById('rankings-grid');
        if (grid) {
            rankingsOffset = 0;
            rankingsExhausted = false;
            libraryImages = [];
            rankingsLoading = false;
            loadRankings(true);
        } else {
            // Compare page — reload mosaic
            loadMosaicBatch();
        }
    }

    function setFilter(key, value) {
        filters[key] = value;
        reloadForFilters();
    }

    function toggleFilter(key, value, btn) {
        if (filters[key] === value) {
            filters[key] = '';
            btn.classList.remove('active');
        } else {
            btn.parentElement.querySelectorAll('.filter-icon').forEach(b => b.classList.remove('active'));
            filters[key] = value;
            btn.classList.add('active');
        }
        reloadForFilters();
    }

    function toggleStar(level) {
        const newValue = filters.rating == level ? '' : level;
        filters.rating = newValue;
        document.querySelectorAll('.filter-star').forEach(s => {
            const star = parseInt(s.dataset.star);
            const lit = newValue && star <= newValue;
            s.classList.toggle('lit', lit);
            s.textContent = lit ? '★' : '☆';
        });
        reloadForFilters();
    }

    function loadFolderList() {
        fetch('/api/folders').then(r => r.json()).then(data => {
            const sel = document.getElementById('filter-folder');
            if (!sel || !data.folders) return;
            const topFolders = data.folders.filter(f => f.depth <= 1);
            for (const f of topFolders) {
                const opt = document.createElement('option');
                opt.value = f.path;
                const indent = f.depth > 0 ? '  ' : '';
                opt.textContent = `${indent}${f.path} (${f.count})`;
                sel.appendChild(opt);
            }
        }).catch(() => {});
    }

    function initStarHover() {
        document.querySelectorAll('.filter-star').forEach(star => {
            star.addEventListener('mouseenter', () => {
                const level = parseInt(star.dataset.star);
                document.querySelectorAll('.filter-star').forEach(s => {
                    s.classList.toggle('hovered', parseInt(s.dataset.star) <= level);
                    if (!s.classList.contains('lit')) {
                        s.textContent = parseInt(s.dataset.star) <= level ? '★' : '☆';
                    }
                });
            });
            star.addEventListener('mouseleave', () => {
                document.querySelectorAll('.filter-star').forEach(s => {
                    s.classList.remove('hovered');
                    if (!s.classList.contains('lit')) s.textContent = '☆';
                });
            });
        });
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

    // ==================== SETTINGS ====================

    const SETTINGS_FIELDS = [
        'embed_model_id',
        'embed_model_revision',
        'embed_model_dir',
        'thumb_size_sm',
        'thumb_size_md',
        'thumb_size_lg',
        'thumb_quality',
        'memory_cache_gb',
        'ssd_cache_dir',
        'ssd_cache_gb',
        'pregenerate_on_idle',
    ];
    let settingsPoller = null;

    function setSettingsStatus(message, tone = '') {
        const el = document.getElementById('settings-status');
        if (!el) return;
        el.textContent = message;
        el.className = 'settings-status' + (tone ? ' ' + tone : '');
    }

    function formatBytes(bytes) {
        const value = Number(bytes || 0);
        if (value <= 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let size = value;
        let unit = 0;
        while (size >= 1024 && unit < units.length - 1) {
            size /= 1024;
            unit += 1;
        }
        const digits = size >= 100 || unit === 0 ? 0 : size >= 10 ? 1 : 2;
        return `${size.toFixed(digits)} ${units[unit]}`;
    }

    function formatCacheTier(label, tier) {
        if (!tier) return `${label}: —`;
        const budget = Number(tier.budget_bytes || 0);
        const progressTotal = Number(tier.progress_total || 0);
        const progress = progressTotal > 0
            ? `${Number(tier.count || 0).toLocaleString()} / ${progressTotal.toLocaleString()} (${Number(tier.progress_pct || 0).toFixed(1)}%)`
            : `${Number(tier.count || 0).toLocaleString()} cached`;
        const budgetText = budget > 0
            ? `${formatBytes(tier.bytes)} / ${formatBytes(budget)}`
            : `${formatBytes(tier.bytes)} / off`;
        return `${label}: ${budgetText} · ${progress}`;
    }

    function renderCacheSettingsStatus(cacheStatus) {
        if (!cacheStatus) return;

        const memory = cacheStatus.memory || {};
        const disk = cacheStatus.disk || {};
        const tiers = disk.tiers || {};
        const pregen = cacheStatus.pregen || {};

        const cacheEl = document.getElementById('cache-stats-inline');
        const ramEl = document.getElementById('cache-ram-usage');
        const ssdEl = document.getElementById('cache-ssd-usage');
        const smEl = document.getElementById('cache-tier-sm');
        const mdEl = document.getElementById('cache-tier-md');
        const lgEl = document.getElementById('cache-tier-lg');
        const fullEl = document.getElementById('cache-tier-full');
        const pregenEl = document.getElementById('cache-pregen-summary');

        if (cacheEl) {
            cacheEl.textContent = `${formatBytes(memory.used_bytes)} / ${formatBytes(memory.limit_bytes)}`;
        }
        if (ramEl) {
            const memoryTiers = memory.tiers || {};
            const parts = ['sm', 'md', 'lg'].map((size) => {
                const tier = memoryTiers[size] || {};
                return `${size} ${Number(tier.count || 0)} · ${formatBytes(tier.bytes)}`;
            });
            ramEl.textContent = parts.join('   ');
        }
        if (ssdEl) {
            const pct = Number(disk.utilization_pct || 0);
            ssdEl.textContent = `${formatBytes(disk.used_bytes)} / ${formatBytes(disk.limit_bytes)} (${pct.toFixed(1)}%)`;
        }
        if (smEl) smEl.textContent = formatCacheTier('sm', tiers.sm);
        if (mdEl) mdEl.textContent = formatCacheTier('md', tiers.md);
        if (lgEl) lgEl.textContent = formatCacheTier('lg', tiers.lg);
        if (fullEl) fullEl.textContent = formatCacheTier('full', tiers.full);
        if (pregenEl) {
            const state = pregen.state || 'idle';
            const phase = pregen.active_phase ? ` · ${pregen.active_phase}` : '';
            const message = pregen.message ? ` · ${pregen.message}` : '';
            pregenEl.textContent = `${state}${phase}${message}`;
        }
    }

    function renderAutoTuningStatus(settings) {
        if (!settings) return;

        const workersEl = document.getElementById('cache-auto-workers');
        const prefetchEl = document.getElementById('cache-auto-prefetch');
        const browserEl = document.getElementById('cache-browser-policy');

        if (workersEl) {
            const cpu = Number(settings.cpu_count || 0);
            const ram = settings.system_memory_gb ? `${settings.system_memory_gb} GB system RAM` : 'system RAM unknown';
            workersEl.textContent =
                `${Number(settings.user_workers || 0)} request · ${Number(settings.prefetch_workers || 0)} background` +
                (cpu > 0 ? ` on ${cpu} CPU threads` : '') +
                ` · ${ram}`;
        }

        if (prefetchEl) {
            prefetchEl.textContent =
                `scan ${Number(settings.scan_prefetch_limit || 0)} · ` +
                `cull ${Number(settings.cull_prefetch_limit || 0)} · ` +
                `compare ${Number(settings.compare_prefetch_limit || 0)} · ` +
                `mosaic ${Number(settings.mosaic_prefetch_limit || 0)}`;
        }

        if (browserEl) {
            const maxAge = Number(settings.browser_cache_max_age || 0);
            const stale = Number(settings.browser_cache_stale_while_revalidate || 0);
            browserEl.textContent = `${maxAge.toLocaleString()}s max-age · ${stale.toLocaleString()}s stale-while-revalidate`;
        }
    }

    function renderSettingsMeta(data) {
        const pathEl = document.getElementById('settings-path');
        renderCacheSettingsStatus(data.cache_stats);
        if (pathEl && data.settings_path) {
            pathEl.textContent = data.settings_path;
        }
        renderAutoTuningStatus(data.settings);
        renderModelStatus(data.model_status);
        renderAISettingsStatus(data.ai_status);
    }

    function renderModelStatus(modelStatus) {
        const statusEl = document.getElementById('model-install-status');
        const messageEl = document.getElementById('model-install-message');
        const buttonEl = document.getElementById('install-model-btn');
        if (!statusEl || !messageEl || !buttonEl || !modelStatus) return;

        const install = modelStatus.install || {};
        if (install.running) {
            statusEl.textContent = 'Downloading';
            messageEl.textContent = install.message || `Downloading ${modelStatus.model_id}…`;
            buttonEl.disabled = true;
            buttonEl.textContent = 'Installing…';
        } else if (modelStatus.installed) {
            statusEl.textContent = 'Installed';
            messageEl.textContent = `${modelStatus.model_id} is available locally at ${modelStatus.model_dir}`;
            buttonEl.disabled = false;
            buttonEl.textContent = 'Reinstall Model';
        } else if (install.status === 'error') {
            statusEl.textContent = 'Error';
            messageEl.textContent = install.message || 'Model install failed.';
            buttonEl.disabled = false;
            buttonEl.textContent = 'Retry Install';
        } else {
            statusEl.textContent = 'Not installed';
            messageEl.textContent = `Install ${modelStatus.model_id} to enable offline embeddings.`;
            buttonEl.disabled = false;
            buttonEl.textContent = 'Save + Install Model';
        }
    }

    function formatRatePerMinute(rate) {
        const value = Number(rate || 0);
        if (value <= 0) return 'Waiting for data';
        return `${value >= 100 ? value.toFixed(0) : value.toFixed(1)} images/min`;
    }

    function formatEta(seconds) {
        const totalSeconds = Math.max(0, Math.round(Number(seconds || 0)));
        if (!totalSeconds) return 'Calculating…';
        const hours = Math.floor(totalSeconds / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        if (hours > 0) return `~${hours}h ${minutes}m`;
        if (minutes > 0) return `~${minutes}m`;
        return `~${totalSeconds}s`;
    }

    function renderAISettingsStatus(aiStatus) {
        const progressEl = document.getElementById('ai-settings-embed-progress');
        const remainingEl = document.getElementById('ai-settings-embed-remaining');
        const speedEl = document.getElementById('ai-settings-embed-speed');
        const etaEl = document.getElementById('ai-settings-embed-eta');
        const predictionsEl = document.getElementById('ai-settings-predictions');
        const workerEl = document.getElementById('ai-settings-worker-message');
        if (!progressEl || !remainingEl || !speedEl || !etaEl || !predictionsEl || !workerEl || !aiStatus) return;

        const embedded = Number(aiStatus.embedded || 0);
        const total = Number(aiStatus.total_kept || 0);
        const remaining = Number(aiStatus.remaining || 0);
        const progressPct = Number(aiStatus.progress_pct || 0);
        const recentRate = Number(aiStatus.recent_images_per_min || 0);
        const overallRate = Number(aiStatus.overall_images_per_min || 0);

        progressEl.textContent = `${embedded.toLocaleString()} / ${total.toLocaleString()} (${progressPct.toFixed(1)}%)`;
        remainingEl.textContent = remaining.toLocaleString();

        if (recentRate > 0 && overallRate > 0) {
            speedEl.textContent = `${formatRatePerMinute(recentRate)} recent · ${formatRatePerMinute(overallRate)} avg`;
        } else if (recentRate > 0) {
            speedEl.textContent = `${formatRatePerMinute(recentRate)} recent`;
        } else if (overallRate > 0) {
            speedEl.textContent = `${formatRatePerMinute(overallRate)} avg`;
        } else if (embedded >= total && total > 0) {
            speedEl.textContent = 'Complete';
        } else {
            speedEl.textContent = 'Waiting for embedding activity';
        }

        if (remaining <= 0 && total > 0) {
            etaEl.textContent = 'Done';
        } else if (aiStatus.eta_seconds) {
            etaEl.textContent = formatEta(aiStatus.eta_seconds);
        } else if (aiStatus.worker_state === 'embedding') {
            etaEl.textContent = 'Measuring…';
        } else {
            etaEl.textContent = 'Waiting for speed data';
        }

        predictionsEl.textContent = `${Number(aiStatus.predicted || 0).toLocaleString()} predicted · ${Number(aiStatus.compared || 0).toLocaleString()} compared`;
        workerEl.textContent = aiStatus.worker_message || 'Waiting for worker activity';
    }

    function populateSettingsForm(settings) {
        for (const field of SETTINGS_FIELDS) {
            const input = document.getElementById(field);
            if (!input || settings[field] === undefined || settings[field] === null) continue;
            if (input.type === 'checkbox') input.checked = Boolean(settings[field]);
            else input.value = settings[field];
        }
    }

    function collectSettingsForm() {
        const payload = {};
        for (const field of SETTINGS_FIELDS) {
            const input = document.getElementById(field);
            if (!input) continue;
            if (input.type === 'checkbox') payload[field] = input.checked;
            else if (input.type === 'number') payload[field] = Number(input.value || '0');
            else payload[field] = input.value;
        }
        return payload;
    }

    async function loadSettingsPage(showStatus = true) {
        const res = await fetch('/api/settings');
        const data = await res.json();
        populateSettingsForm(data.settings || {});
        renderSettingsMeta(data);
        if (showStatus) {
            setSettingsStatus('Loaded current settings.', 'muted');
        }
        return data;
    }

    async function refreshSettingsMeta() {
        const res = await fetch('/api/settings');
        const data = await res.json();
        renderSettingsMeta(data);
        return data;
    }

    async function initSettings() {
        const form = document.getElementById('settings-form');
        if (form) {
            form.addEventListener('submit', (e) => {
                e.preventDefault();
                saveSettings();
            });
        }

        try {
            await loadSettingsPage(false);
            setSettingsStatus('Ready. Save to apply changes immediately.', 'muted');
            if (settingsPoller) clearInterval(settingsPoller);
            settingsPoller = setInterval(() => refreshSettingsMeta().catch(() => {}), 5000);
        } catch (err) {
            setSettingsStatus(`Could not load settings: ${err.message}`, 'error');
        }
    }

    async function saveSettings() {
        setSettingsStatus('Saving settings…', 'muted');
        try {
            const res = await fetch('/api/settings', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(collectSettingsForm()),
            });
            const data = await res.json();
            if (!res.ok || !data.ok) {
                throw new Error(data.error || 'Save failed');
            }
            populateSettingsForm(data.settings || {});
            renderSettingsMeta(data);
            setSettingsStatus('Saved. New requests are using the updated runtime settings.', 'success');
        } catch (err) {
            setSettingsStatus(`Save failed: ${err.message}`, 'error');
        }
    }

    async function resetSettings() {
        setSettingsStatus('Resetting to defaults…', 'muted');
        try {
            const res = await fetch('/api/settings/reset', { method: 'POST' });
            const data = await res.json();
            if (!res.ok || !data.ok) {
                throw new Error(data.error || 'Reset failed');
            }
            populateSettingsForm(data.settings || {});
            renderSettingsMeta(data);
            setSettingsStatus('Defaults restored. Runtime settings were updated.', 'success');
        } catch (err) {
            setSettingsStatus(`Reset failed: ${err.message}`, 'error');
        }
    }

    async function clearThumbnailCache() {
        setSettingsStatus('Clearing in-memory and disk thumbnail cache…', 'muted');
        try {
            const res = await fetch('/api/cache/clear', { method: 'POST' });
            const data = await res.json();
            if (!res.ok || !data.ok) {
                throw new Error(data.error || 'Cache clear failed');
            }
            renderSettingsMeta(data);
            setSettingsStatus(
                `Cleared ${data.memory_entries_cleared || 0} RAM entries (${formatBytes(data.memory_bytes_cleared || 0)}) and ${data.disk_files_removed || 0} disk files.`,
                'success'
            );
        } catch (err) {
            setSettingsStatus(`Cache clear failed: ${err.message}`, 'error');
        }
    }

    async function startCachePregeneration() {
        setSettingsStatus('Starting cache pre-generation…', 'muted');
        try {
            const res = await fetch('/api/cache/pregen/start', { method: 'POST' });
            const data = await res.json();
            if (!res.ok || !data.ok) {
                throw new Error(data.error || 'Could not start pre-generation');
            }
            renderCacheSettingsStatus(data.cache);
            setSettingsStatus('Pre-generation is running.', 'success');
        } catch (err) {
            setSettingsStatus(`Could not start pre-generation: ${err.message}`, 'error');
        }
    }

    async function stopCachePregeneration() {
        setSettingsStatus('Pausing cache pre-generation…', 'muted');
        try {
            const res = await fetch('/api/cache/pregen/stop', { method: 'POST' });
            const data = await res.json();
            if (!res.ok || !data.ok) {
                throw new Error(data.error || 'Could not pause pre-generation');
            }
            renderCacheSettingsStatus(data.cache);
            setSettingsStatus('Pre-generation paused.', 'success');
        } catch (err) {
            setSettingsStatus(`Could not pause pre-generation: ${err.message}`, 'error');
        }
    }

    async function installAIModel() {
        setSettingsStatus('Saving settings and starting model install…', 'muted');
        try {
            const saveRes = await fetch('/api/settings', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(collectSettingsForm()),
            });
            const saveData = await saveRes.json();
            if (!saveRes.ok || !saveData.ok) {
                throw new Error(saveData.error || 'Could not save settings');
            }
            populateSettingsForm(saveData.settings || {});
            renderSettingsMeta(saveData);

            const installRes = await fetch('/api/ai/model/install', { method: 'POST' });
            const installData = await installRes.json();
            if (!installRes.ok || !installData.ok) {
                throw new Error(installData.error || 'Install could not be started');
            }
            renderModelStatus(installData.model_status);
            setSettingsStatus('Model install started. The AI worker will pick it up automatically when the download finishes.', 'success');
        } catch (err) {
            setSettingsStatus(`Model install failed to start: ${err.message}`, 'error');
        }
    }

    // ==================== UTILITIES ====================

    const PRELOAD_LIMIT = 240;
    const PRELOAD_CONCURRENCY = 8;
    const preloadQueue = [];
    let preloadActive = 0;

    function pumpPreloadQueue() {
        while (preloadActive < PRELOAD_CONCURRENCY && preloadQueue.length > 0) {
            const item = preloadQueue.shift();
            preloadActive++;
            const img = new Image();
            img.decoding = 'async';
            if ('fetchPriority' in img) img.fetchPriority = item.priority;
            const finish = () => {
                preloadActive = Math.max(0, preloadActive - 1);
                item.resolve();
                pumpPreloadQueue();
            };
            img.onload = finish;
            img.onerror = finish;
            img.src = item.url;
        }
    }

    function preloadImage(url, priority = 'auto') {
        if (preloaded.has(url)) return preloaded.get(url);
        // Evict oldest entries when limit reached
        if (preloaded.size >= PRELOAD_LIMIT) {
            const first = preloaded.keys().next().value;
            preloaded.delete(first);
        }
        const normalizedPriority = priority === 'high' || priority === 'low' ? priority : 'auto';
        const promise = new Promise((resolve) => {
            const item = { url, priority: normalizedPriority, resolve };
            if (normalizedPriority === 'high') {
                preloadQueue.unshift(item);
            } else {
                preloadQueue.push(item);
            }
            pumpPreloadQueue();
        });
        preloaded.set(url, promise);
        return promise;
    }

    // ==================== PUBLIC API ====================

    return {
        initCull,
        initCompare,
        initLibrary,
        initRankings,
        initSettings,
        clearSearch,
        setCullMode,
        setCompareMode,
        setRankingsSort,
        setSortField,
        toggleSortDir,
        exportRankings,
        closeLightbox,
        lightboxNext,
        lightboxPrev,
        setThumbSize,
        setFilter,
        toggleFilter,
        toggleStar,
        findSimilar,
        toggleBatchMode,
        batchExport,
        gridSelectAll,
        gridSelectNone,
        gridSubmit,
        mosaicShuffle,
        setMosaicStrategy,
        toggleAIPanel,
        saveSettings,
        resetSettings,
        clearThumbnailCache,
        startCachePregeneration,
        stopCachePregeneration,
        installAIModel,
    };
})();
