const PhotoArchive = (() => {
    // Track browser-loaded images: url -> Promise that resolves when loaded
    const preloaded = new Map();

    // --- Compare Mode State ---
    let comparePairs = [];
    let compareIndex = 0;
    let compareMode = 'swiss';
    let compareBusy = false;
    let compareStats = {};
    let compareImageToken = 0;
    const compareDisplayedTier = { left: -1, right: -1 };

    // --- Rankings State ---
    let rankingsOffset = 0;
    const INITIAL_RANKINGS_PAGE_SIZE = 48;
    const RANKINGS_PAGE_SIZE = 100;
    const BACKGROUND_WARM_DELAY_MS = 250;
    const LIBRARY_NEIGHBOR_LIMIT = 24;
    const MOSAIC_NEIGHBOR_LIMIT = 8;
    const COMPARE_NEIGHBOR_PAIRS = 4;
    const FILMSTRIP_WINDOW_RADIUS = 55;
    const LOUPE_TIER_LABELS = ['Thumbnail (sm)', 'Medium (md)', 'Large (lg)', 'Original'];
    const LOUPE_TIER_TIMEOUTS = { md: 1800, lg: 2600, full: 5000 };
    const LOUPE_TIER_RANKS = { sm: 0, md: 1, lg: 2, full: 3 };
    const MEDIA_STATUS_MAX_AGE_MS = 15000;
    const mediaStatusCache = new Map();
    let selectedLibraryIndex = -1;
    let selectedMosaicIndex = -1;
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

    function warmImageTiers(tiers) {
        const payload = {};
        for (const [tier, ids] of Object.entries(tiers || {})) {
            const unique = [...new Set((ids || []).map((id) => Number(id)).filter((id) => id > 0))];
            if (unique.length) payload[tier] = unique;
        }
        if (!Object.keys(payload).length) return;
        fetch('/api/images/warm', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tiers: payload }),
            keepalive: true,
        }).catch(() => {});
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
            if (pair.left?.id) urls.push(`/api/thumb/sm/${pair.left.id}`);
            if (pair.left?.thumb_url) urls.push(pair.left.thumb_url);
            if (pair.right?.id) urls.push(`/api/thumb/sm/${pair.right.id}`);
            if (pair.right?.thumb_url) urls.push(pair.right.thumb_url);
        }
        return urls;
    }

    function loadImageProbe(url, { priority = 'auto', timeoutMs = 0 } = {}) {
        if (!url) return Promise.resolve({ ok: false });
        return new Promise((resolve) => {
            const img = new Image();
            let settled = false;
            let timer = null;
            const finish = (ok) => {
                if (settled) return;
                settled = true;
                if (timer) clearTimeout(timer);
                resolve({
                    ok,
                    url: img.src,
                    width: img.naturalWidth || 0,
                    height: img.naturalHeight || 0,
                });
            };
            img.decoding = 'async';
            if ('fetchPriority' in img) img.fetchPriority = priority;
            img.onload = () => finish(Boolean(img.naturalWidth && img.naturalHeight));
            img.onerror = () => finish(false);
            if (timeoutMs > 0) timer = setTimeout(() => finish(false), timeoutMs);
            img.src = url;
        });
    }

    // ==================== MOSAIC RANKING MODE ====================

    let mosaicSize = 12;
    let mosaicImages = []; // currently visible images [{id, filename, elo, thumb_url}, ...]
    let mosaicAge = []; // how many clicks each image has survived on the board
    let mosaicPickCount = 0;
    let mosaicStrategy = 'diverse';
    let mosaicPropagationCounts = {}; // precomputed: {imageId: predictedCount}
    let mosaicRenderToken = 0;

    function mosaicGridElo() {
        if (mosaicImages.length === 0) return 0;
        return mosaicImages.reduce((s, img) => s + img.elo, 0) / mosaicImages.length;
    }

    async function loadMosaicBatch() {
        const url = buildMosaicUrl({ n: mosaicSize });
        // Never use warm cache for diverse strategy — each load should be fresh
        const data = (mosaicStrategy !== 'diverse' ? takeWarmCache(`compare:${url}`) : null) || await fetchWarmJson(url);
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
        warmImageTiers({
            md: mosaicImages.map((img) => img.id),
            lg: mosaicImages.map((img) => img.id),
        });
        mosaicFillReplacements();
        precomputePropagation();
        scheduleCompareNeighborWarmup('mosaic');
        scheduleCrossViewWarmup('compare');
    }

    function renderMosaic() {
        const grid = document.getElementById('mosaic-grid');
        grid.innerHTML = '';
        selectedMosaicIndex = -1;
        const token = ++mosaicRenderToken;

        // Calculate row height to fit all images in the viewport
        // using the same justified flex layout as the library
        const gap = 3;
        const containerW = grid.clientWidth || window.innerWidth;
        const barH = document.querySelector('.bottom-bar')?.offsetHeight || 60;
        const containerH = window.innerHeight - barH - 4;
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

        for (let index = 0; index < mosaicImages.length; index++) {
            const img = mosaicImages[index];
            const ar = img.aspect_ratio || 1.5;
            const cell = document.createElement('div');
            cell.className = 'mosaic-cell';
            cell.dataset.id = img.id;
            cell.style.height = rowH + 'px';
            cell.style.flexGrow = ar;
            cell.style.flexBasis = (rowH * ar) + 'px';
            cell.onclick = () => mosaicClick(img.id);
            cell.innerHTML = `<img src="${img.thumb_url}" alt="${img.filename}" data-tier-rank="0">`;
            preloadImage(img.thumb_url);
            grid.appendChild(cell);
            scheduleMosaicImageUpgrade(cell, img, rowH, token, index);
        }
    }

    function scheduleMosaicImageUpgrade(cell, img, rowH, token, index = 0) {
        const delay = Math.min(900, index * 80);
        setTimeout(() => {
            upgradeMosaicCellImage(cell, img, rowH, token).catch(() => {});
        }, delay);
    }

    async function upgradeMosaicCellImage(cell, img, rowH, token) {
        if (token !== mosaicRenderToken || !cell?.isConnected) return;
        const imgEl = cell.querySelector('img');
        if (!imgEl) return;

        const status = await getMediaStatus(img.id);
        if (token !== mosaicRenderToken || !cell.isConnected || cell.dataset.id !== String(img.id)) return;

        const tiers = status?.tiers || {};
        const cachedBest = tiers.lg?.cached ? 'lg' : tiers.md?.cached ? 'md' : null;
        if (cachedBest) {
            await adoptMosaicTier(cell, img, cachedBest, true, token, 900);
        }

        const targetTier = rowH >= 360 ? 'lg' : 'md';
        await adoptMosaicTier(cell, img, 'md', false, token, LOUPE_TIER_TIMEOUTS.md);
        if (targetTier === 'lg') {
            await adoptMosaicTier(cell, img, 'lg', false, token, LOUPE_TIER_TIMEOUTS.lg);
        }
    }

    async function adoptMosaicTier(cell, img, tier, cachedOnly, token, timeoutMs) {
        const imgEl = cell?.querySelector('img');
        if (!imgEl) return false;
        const rank = LOUPE_TIER_RANKS[tier] || 0;
        if (rank <= Number(imgEl.dataset.tierRank || 0)) return false;

        const result = await loadImageProbe(loupeTierUrl(tier, img.id, cachedOnly), {
            priority: 'low',
            timeoutMs,
        });
        if (!result.ok || token !== mosaicRenderToken || !cell.isConnected || cell.dataset.id !== String(img.id)) {
            return false;
        }
        if (rank <= Number(imgEl.dataset.tierRank || 0)) return false;
        imgEl.dataset.tierRank = String(rank);
        imgEl.src = result.url;
        return true;
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

        // Update stats using precomputed propagation count if available
        const propagated = mosaicPropagationCounts[id] || 0;
        compareStats.total_comparisons = (compareStats.total_comparisons || 0) + otherIds.length + propagated;
        updateCompareProgress();
        if (propagated > 0) {
            showPropagationBadge(propagated);
        } else {
            // Precompute not ready — poll immediately
            fetchPropagationCount(0);
        }

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
                const imgEl = targetCell.querySelector('img');
                if (imgEl) {
                    imgEl.dataset.tierRank = '0';
                    imgEl.src = newImg.thumb_url;
                    imgEl.alt = newImg.filename;
                }
                targetCell.classList.remove('mosaic-picked');
                scheduleMosaicImageUpgrade(targetCell, newImg, targetCell.clientHeight || 220, mosaicRenderToken, ri);
            }

            mosaicBusy = false;

            // Refill replacement buffer and recompute propagation for new grid
            mosaicFillReplacements();
            precomputePropagation();

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
        // Set slider to match default mosaic size (12 images → slider ~168)
        const slider = document.getElementById('thumb-size');
        if (slider) {
            const t = (24 - mosaicSize) / 20; // invert: size→t
            slider.value = Math.round(120 + t * 280);
        }
        restoreFilters();
        setCompareMode('mosaic');
        pollAIStatus();
        setInterval(pollAIStatus, 5000);
        loadFolderList();
        initStarHover();
    }

    async function pollAIStatus() {
        try {
            const res = await fetch('/api/ai/status');
            const data = await res.json();
            const countEl = document.getElementById('ai-embed-count');
            const totalEl = document.getElementById('ai-embed-total');
            const stateEl = document.getElementById('ai-model-state');
            const totalImages = Number(data.total_images ?? data.total_kept ?? 0);
            if (countEl) countEl.textContent = data.embedded.toLocaleString();
            if (totalEl) totalEl.textContent = totalImages.toLocaleString();
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
                } else if (data.embedded < totalImages) {
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
                const pct = totalImages > 0 ? (data.embedded / totalImages * 100) : 0;
                embedFill.style.width = pct + '%';
            }
            if (embedText) embedText.textContent = `${data.embedded.toLocaleString()} / ${totalImages.toLocaleString()} images`;
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
        const token = ++compareImageToken;
        const leftImg = document.getElementById('compare-left-img');
        const rightImg = document.getElementById('compare-right-img');
        const leftInfo = document.getElementById('compare-left-info');
        const rightInfo = document.getElementById('compare-right-info');

        if (leftInfo) leftInfo.textContent = `${pair.left.filename} — ${pair.left.elo}`;
        if (rightInfo) rightInfo.textContent = `${pair.right.filename} — ${pair.right.elo}`;
        renderCompareImage(pair.left, leftImg, 'left', token);
        renderCompareImage(pair.right, rightImg, 'right', token);
        warmImageTiers({
            lg: [pair.left.id, pair.right.id],
            full: [pair.left.id, pair.right.id],
        });

        updateCompareProgress();

        // Prefetch if running low
        if (comparePairs.length - compareIndex < 4) {
            fetchComparePairs();
        }
    }

    function isCurrentCompareImage(token) {
        return token === compareImageToken && compareIndex < comparePairs.length;
    }

    function renderCompareImage(img, imgEl, side, token) {
        if (!imgEl || !img) return;
        compareDisplayedTier[side] = -1;
        imgEl.alt = img.filename || '';
        imgEl.classList.add('fading');
        imgEl.onload = () => {
            if (isCurrentCompareImage(token)) imgEl.classList.remove('fading');
        };
        imgEl.onerror = () => {
            if (isCurrentCompareImage(token)) imgEl.classList.remove('fading');
        };
        imgEl.src = `/api/thumb/sm/${img.id}`;
        compareDisplayedTier[side] = 0;
        upgradeCompareImage(img, imgEl, side, token).catch(() => {});
    }

    async function upgradeCompareImage(img, imgEl, side, token) {
        const status = await getMediaStatus(img.id);
        if (!isCurrentCompareImage(token)) return;

        const tiers = status?.tiers || {};
        const cachedBest = tiers.full?.cached ? 'full' : tiers.lg?.cached ? 'lg' : tiers.md?.cached ? 'md' : null;
        if (cachedBest) {
            await adoptCompareTier(img, imgEl, side, cachedBest, true, token, 1000);
        }

        for (const tier of ['md', 'lg']) {
            await adoptCompareTier(img, imgEl, side, tier, false, token, LOUPE_TIER_TIMEOUTS[tier]);
            if (!isCurrentCompareImage(token)) return;
        }

        if (tiers.full?.cached) {
            await adoptCompareTier(img, imgEl, side, 'full', true, token, 1200);
        }
    }

    async function adoptCompareTier(img, imgEl, side, tier, cachedOnly, token, timeoutMs) {
        const rank = LOUPE_TIER_RANKS[tier] || 0;
        if (rank <= compareDisplayedTier[side]) return false;
        const result = await loadImageProbe(loupeTierUrl(tier, img.id, cachedOnly), {
            priority: rank >= 2 ? 'high' : 'auto',
            timeoutMs,
        });
        if (!result.ok || !isCurrentCompareImage(token) || rank <= compareDisplayedTier[side]) {
            return false;
        }
        compareDisplayedTier[side] = rank;
        imgEl.src = result.url;
        imgEl.classList.remove('fading');
        return true;
    }

    let _propagationBadgeTimer = null;
    let _rollupAnim = null;
    let _displayedComparisons = -1;

    function updateCompareProgress() {
        const total = compareStats.total_comparisons || 0;
        const poolCount = compareStats['ke' + 'pt'] || 0;
        const compEl = document.getElementById('compare-stat-comparisons');
        const poolEl = document.getElementById('compare-stat-pool');
        if (poolEl) poolEl.textContent = poolCount.toLocaleString();
        if (!compEl) return;

        if (_displayedComparisons < 0) {
            // First load — no animation
            _displayedComparisons = total;
            compEl.textContent = total.toLocaleString();
            return;
        }
        if (total === _displayedComparisons) return;
        rollUpCounter(compEl, _displayedComparisons, total);
        _displayedComparisons = total;
    }

    function rollUpCounter(el, from, to) {
        if (_rollupAnim) cancelAnimationFrame(_rollupAnim);
        const diff = to - from;
        const duration = Math.min(2000, Math.max(600, Math.abs(diff) * 10));
        const start = performance.now();

        function tick(now) {
            const t = Math.min((now - start) / duration, 1);
            const eased = 1 - (1 - t) * (1 - t); // ease-out quad
            const current = Math.round(from + diff * eased);
            el.textContent = current.toLocaleString();
            if (t < 1) {
                _rollupAnim = requestAnimationFrame(tick);
            } else {
                _rollupAnim = null;
            }
        }
        _rollupAnim = requestAnimationFrame(tick);
    }

    function precomputePropagation() {
        const gridIds = mosaicImages.map(img => img.id);
        if (!gridIds.length) return;
        fetch('/api/propagation/predict', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ grid_ids: gridIds }),
        }).then(r => r.json()).then(data => {
            mosaicPropagationCounts = {};
            for (const [id, count] of Object.entries(data.counts || {})) {
                mosaicPropagationCounts[parseInt(id)] = count;
            }
        }).catch(() => {});
    }

    function fetchPropagationCount(directCount = 0) {
        fetch('/api/propagation/last').then(r => r.json()).then(data => {
            const total = directCount + (data.count || 0);
            if (total > 0) {
                compareStats.total_comparisons = (compareStats.total_comparisons || 0) + total;
                updateCompareProgress();
                if (data.count > 0) showPropagationBadge(data.count);
            }
        }).catch(() => {
            // Propagation fetch failed — still apply direct count
            if (directCount > 0) {
                compareStats.total_comparisons = (compareStats.total_comparisons || 0) + directCount;
                updateCompareProgress();
            }
        });
    }

    function showPropagationBadge(count) {
        const badge = document.getElementById('propagation-badge');
        if (!badge) return;
        badge.textContent = ` +${count} similar`;
        badge.classList.add('visible');
        clearTimeout(_propagationBadgeTimer);
        _propagationBadgeTimer = setTimeout(() => badge.classList.remove('visible'), 3000);
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
                compareIndex++;
                showComparePair();
                fetchPropagationCount(1);
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
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

        if (e.key === 'Tab') {
            e.preventDefault();
            window.location.href = '/library';
            return;
        }

        if (compareMode === 'mosaic') {
            const cells = document.querySelectorAll('.mosaic-cell');
            if (!cells.length) return;

            if (e.key === 'ArrowRight' || e.key === 'ArrowLeft' || e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                e.preventDefault();
                if (selectedMosaicIndex < 0) {
                    selectMosaicCell(0, cells);
                    return;
                }
                if (e.key === 'ArrowRight') {
                    selectMosaicCell(Math.min(selectedMosaicIndex + 1, cells.length - 1), cells);
                } else if (e.key === 'ArrowLeft') {
                    selectMosaicCell(Math.max(selectedMosaicIndex - 1, 0), cells);
                } else if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                    const target = findMosaicCellInDirection(cells, selectedMosaicIndex, e.key === 'ArrowDown' ? 1 : -1);
                    selectMosaicCell(target, cells);
                }
            } else if (e.key === 'Enter' && selectedMosaicIndex >= 0 && selectedMosaicIndex < mosaicImages.length) {
                e.preventDefault();
                const keepIdx = selectedMosaicIndex;
                mosaicClick(mosaicImages[selectedMosaicIndex].id);
                // Re-select after swap animation so the cursor stays in place
                setTimeout(() => {
                    const cells = document.querySelectorAll('.mosaic-cell');
                    if (keepIdx < cells.length) selectMosaicCell(keepIdx, cells);
                }, 200);
            } else if (e.key === 'Escape' && selectedMosaicIndex >= 0) {
                e.preventDefault();
                deselectMosaicCell(cells);
            }
            if (e.key === 'ArrowUp' && selectedMosaicIndex < 0) {
                e.preventDefault();
                undoComparison();
            }
            return;
        }

        // Swiss/A-B mode
        switch (e.key) {
            case 'ArrowLeft': submitComparison('left'); break;
            case 'ArrowRight': submitComparison('right'); break;
            case 'ArrowUp': undoComparison(); break;
        }
    }

    function selectMosaicCell(index, cells) {
        if (!cells) cells = document.querySelectorAll('.mosaic-cell');
        if (index < 0 || index >= cells.length) return;
        cells.forEach(c => c.classList.remove('kb-selected'));
        selectedMosaicIndex = index;
        cells[index].classList.add('kb-selected');
    }

    function deselectMosaicCell(cells) {
        if (!cells) cells = document.querySelectorAll('.mosaic-cell');
        cells.forEach(c => c.classList.remove('kb-selected'));
        selectedMosaicIndex = -1;
    }

    function findMosaicCellInDirection(cells, currentIdx, direction) {
        const current = cells[currentIdx];
        if (!current) return currentIdx;
        const rect = current.getBoundingClientRect();
        const centerX = rect.left + rect.width / 2;
        let best = currentIdx;
        let bestDist = Infinity;

        for (let i = 0; i < cells.length; i++) {
            if (i === currentIdx) continue;
            const r = cells[i].getBoundingClientRect();
            const isTarget = direction > 0 ? r.top > rect.bottom - 5 : r.bottom < rect.top + 5;
            if (!isTarget) continue;
            const dx = (r.left + r.width / 2) - centerX;
            const dy = (r.top + r.height / 2) - (rect.top + rect.height / 2);
            const dist = Math.abs(dx) + Math.abs(dy) * 0.1;
            if (dist < bestDist) { bestDist = dist; best = i; }
        }
        return best;
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
            compareImageToken++;
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
    let filters = { orientation: '', compared: '', rating: '', folder: '', flag: '' };

    function saveFilters() {
        try { sessionStorage.setItem('pa_filters', JSON.stringify(filters)); } catch {}
    }

    function restoreFilters() {
        try {
            const saved = sessionStorage.getItem('pa_filters');
            if (!saved) return;
            const parsed = JSON.parse(saved);
            Object.assign(filters, parsed);

            // Restore UI state for filter icons
            if (filters.orientation) {
                document.querySelectorAll('.filter-icon').forEach(btn => {
                    if (btn.title?.toLowerCase() === filters.orientation) btn.classList.add('active');
                });
            }
            if (filters.compared) {
                const titles = { compared: 'Ranked', uncompared: 'Unranked', confident: 'High confidence (10+)' };
                document.querySelectorAll('.filter-icon').forEach(btn => {
                    if (btn.title === titles[filters.compared]) btn.classList.add('active');
                });
            }
            if (filters.rating) {
                document.querySelectorAll('.filter-star').forEach(s => {
                    s.classList.toggle('lit', Number(s.dataset.star) <= filters.rating);
                });
            }
            if (filters.folder) {
                const sel = document.getElementById('filter-folder');
                if (sel) sel.value = filters.folder;
            }
            if (filters.flag) {
                document.querySelectorAll('.filter-flag').forEach(btn => {
                    btn.classList.toggle('active', btn.dataset.flag === filters.flag);
                });
            }
        } catch {}
    }

    function filterParams(state = filters) {
        let p = '';
        if (state.orientation) p += `&orientation=${state.orientation}`;
        if (state.compared) p += `&compared=${state.compared}`;
        if (state.rating) p += `&min_stars=${state.rating}`;
        if (state.folder) p += `&folder=${encodeURIComponent(state.folder)}`;
        if (state.flag) p += `&flag=${state.flag}`;
        return p;
    }

    function currentFilterState() {
        return {
            orientation: filters.orientation || '',
            compared: filters.compared || '',
            rating: filters.rating || '',
            folder: filters.folder || '',
            flag: filters.flag || '',
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

        if (!baseState.flag) {
            pushState({ flag: 'picked' });
            pushState({ flag: 'rejected' });
        } else {
            pushState({ flag: '' });
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
        return `/api/compare/next?n=${n}&mode=${mode}${filterParams()}`;
    }

    function currentLibraryPageSize() {
        return rankingsOffset === 0 ? INITIAL_RANKINGS_PAGE_SIZE : RANKINGS_PAGE_SIZE;
    }

    function scheduleCrossViewWarmup(fromView) {
        if (fromView === 'compare') {
            const libraryUrl = buildRankingsUrl({
                sort: 'elo',
                filterState: { orientation: '', compared: '', rating: '', folder: '', flag: '' },
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
                filterState: { orientation: '', compared: '', rating: '', folder: '', flag: '' },
                gridElo: 0,
                n: mosaicSize,
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
        'similarity':  { desc: 'similarity',    asc: 'similarity' },
    };

    async function initLibrary() {
        rankingsOffset = 0;
        rankingsExhausted = false;
        restoreFilters();

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
            if (entries[0].isIntersecting && !rankingsLoading && !rankingsExhausted) {
                loadRankings();
            }
        }, { rootMargin: '600px' });
        scrollObserver.observe(sentinel);

        // Loupe keyboard navigation
        document.addEventListener('keydown', (e) => {
            // Don't intercept when typing in an input
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

            const loupe = document.getElementById('loupe');
            const loupeOpen = loupe && !loupe.classList.contains('hidden');

            if (loupeOpen) {
                if (e.key === 'ArrowRight') { e.preventDefault(); lightboxNext(); }
                else if (e.key === 'ArrowLeft') { e.preventDefault(); lightboxPrev(); }
                else if (e.key.toLowerCase() === 'p') { e.preventDefault(); setCurrentLibraryFlag('picked'); }
                else if (e.key.toLowerCase() === 'x') { e.preventDefault(); setCurrentLibraryFlag('rejected'); }
                else if (e.key.toLowerCase() === 'u') { e.preventDefault(); setCurrentLibraryFlag('unflagged'); }
                else if (e.key === 'Escape' || e.key === 'Enter') { e.preventDefault(); closeLightbox(); }
                return;
            }

            // Grid keyboard navigation
            const cards = document.querySelectorAll('.rank-card');
            if (!cards.length) return;

            if (e.key === 'ArrowRight' || e.key === 'ArrowLeft' || e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                e.preventDefault();
                if (selectedLibraryIndex < 0) {
                    selectLibraryCard(0, cards);
                    return;
                }
                if (e.key === 'ArrowRight') {
                    selectLibraryCard(Math.min(selectedLibraryIndex + 1, cards.length - 1), cards);
                } else if (e.key === 'ArrowLeft') {
                    selectLibraryCard(Math.max(selectedLibraryIndex - 1, 0), cards);
                } else if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                    const target = findCardInDirection(cards, selectedLibraryIndex, e.key === 'ArrowDown' ? 1 : -1);
                    selectLibraryCard(target, cards);
                }
            } else if (e.key === 'Enter' && selectedLibraryIndex >= 0 && selectedLibraryIndex < libraryImages.length) {
                e.preventDefault();
                openLightbox(libraryImages[selectedLibraryIndex]);
            } else if (e.key.toLowerCase() === 'p') {
                e.preventDefault();
                setCurrentLibraryFlag('picked');
            } else if (e.key.toLowerCase() === 'x') {
                e.preventDefault();
                setCurrentLibraryFlag('rejected');
            } else if (e.key.toLowerCase() === 'u') {
                e.preventDefault();
                setCurrentLibraryFlag('unflagged');
            } else if (e.key === 'Escape' && selectedLibraryIndex >= 0) {
                e.preventDefault();
                deselectLibraryCard(cards);
            } else if (e.key === 'Tab') {
                e.preventDefault();
                window.location.href = '/compare';
            }
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
                    updateSimilaritySortOption();
                    // Search is a filter — reload rankings with the query
                    rankingsOffset = 0;
                    rankingsExhausted = false;
                    libraryImages = [];
                    rankingsLoading = false;
                    loadRankings(true);
                }, 300);
            });
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') clearSearch();
            });
        }
    }

    function initRankings() { initLibrary(); }

    function updateSimilaritySortOption() {
        const select = document.getElementById('sort-field');
        if (!select) return;
        let opt = select.querySelector('option[value="similarity"]');
        if (searchQuery) {
            if (!opt) {
                opt = document.createElement('option');
                opt.value = 'similarity';
                opt.textContent = 'Similarity';
                select.appendChild(opt);
            }
        } else {
            if (opt) {
                // If similarity was selected, switch back to elo
                if (select.value === 'similarity') {
                    select.value = 'elo';
                    setSortField('elo');
                }
                opt.remove();
            }
        }
    }

    function clearSearch() {
        searchQuery = '';
        const input = document.getElementById('search-input');
        const clearBtn = document.getElementById('search-clear');
        if (input) input.value = '';
        if (clearBtn) clearBtn.classList.add('hidden');
        updateSimilaritySortOption();
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
            card.className = 'rank-card' + (flagClass(img.flag) ? ' ' + flagClass(img.flag) : '');
            card.dataset.imageId = img.id;
            card.dataset.ar = ar;
            card.style.height = thumbHeight + 'px';
            card.style.flexGrow = ar;
            card.style.flexBasis = (thumbHeight * ar) + 'px';
            card.onclick = () => openLightbox(img);

            const simPct = (img.similarity * 100).toFixed(0);

            card.innerHTML = `
                <img src="${img.thumb_url}" alt="${img.filename}" loading="lazy" onload="this.classList.add('loaded')">
                ${flagBadge(img.flag)}
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
        rankingsSort = sort;
        rankingsOffset = 0;
        rankingsExhausted = false;
        libraryImages = [];
        // Clear grid only after new data arrives to avoid blank flash
        rankingsLoading = false; // allow loadRankings to proceed
        loadRankings(true);  // true = clear grid before appending
    }

    function setSortField(field) {
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

    function flagClass(flag) {
        if (flag === 'picked') return 'flag-picked';
        if (flag === 'rejected') return 'flag-rejected';
        return '';
    }

    function flagBadge(flag) {
        if (flag === 'picked') return '<div class="rank-flag flag-picked">P</div>';
        if (flag === 'rejected') return '<div class="rank-flag flag-rejected">X</div>';
        return '';
    }

    function updateImageFlagLocal(imageId, flag) {
        for (const img of libraryImages) {
            if (img.id === imageId) img.flag = flag;
        }

        document.querySelectorAll(`.rank-card[data-image-id="${imageId}"]`).forEach((card) => {
            card.classList.remove('flag-picked', 'flag-rejected');
            const cls = flagClass(flag);
            if (cls) card.classList.add(cls);
            card.querySelector('.rank-flag')?.remove();
            if (flag !== 'unflagged') {
                card.insertAdjacentHTML('beforeend', flagBadge(flag));
            }
        });

        document.querySelectorAll(`.filmstrip-thumb[data-image-id="${imageId}"]`).forEach((thumb) => {
            thumb.classList.remove('flag-picked', 'flag-rejected');
            const cls = flagClass(flag);
            if (cls) thumb.classList.add(cls);
        });

        if (lightboxIndex >= 0 && libraryImages[lightboxIndex]?.id === imageId) {
            updateLoupeFlagDisplay(flag);
        }
    }

    async function setImageFlag(imageId, flag) {
        if (!imageId || !['picked', 'unflagged', 'rejected'].includes(flag)) return;
        updateImageFlagLocal(imageId, flag);
        try {
            const res = await fetch(`/api/image/${imageId}/flag`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ flag }),
            });
            if (!res.ok) throw new Error('flag update failed');
        } catch {
            showToast('Flag update failed');
        }
    }

    function setCurrentLibraryFlag(flag) {
        const loupe = document.getElementById('loupe');
        const loupeOpen = loupe && !loupe.classList.contains('hidden');
        const img = loupeOpen
            ? libraryImages[lightboxIndex]
            : libraryImages[selectedLibraryIndex];
        if (!img) return;
        setImageFlag(img.id, flag);
    }

    async function loadRankings(clearFirst = false) {
        if (rankingsLoading) return;
        rankingsLoading = true;
        const requestOffset = rankingsOffset;
        const limit = currentLibraryPageSize();
        let url = `/api/rankings?limit=${limit}&offset=${requestOffset}&sort=${rankingsSort}${filterParams()}`;
        if (searchQuery) url += `&q=${encodeURIComponent(searchQuery)}`;
        const data = (requestOffset === 0 ? takeWarmCache(`library:${url}`) : null) || await fetchWarmJson(url);
        if (!data) {
            rankingsLoading = false;
            return;
        }
        const grid = document.getElementById('rankings-grid');
        if (clearFirst) { grid.innerHTML = ''; selectedLibraryIndex = -1; }
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
            card.className = 'rank-card' + (tier ? ' ' + tier : '') + (flagClass(img.flag) ? ' ' + flagClass(img.flag) : '');
            card.dataset.imageId = img.id;
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
                ${flagBadge(img.flag)}
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

    function selectLibraryCard(index, cards) {
        if (!cards) cards = document.querySelectorAll('.rank-card');
        if (index < 0 || index >= cards.length) return;
        cards.forEach(c => c.classList.remove('kb-selected'));
        selectedLibraryIndex = index;
        cards[index].classList.add('kb-selected');
        scrollCardFullyVisible(cards[index]);
    }

    function scrollCardFullyVisible(el) {
        const rect = el.getBoundingClientRect();
        const barHeight = document.querySelector('.bottom-bar')?.offsetHeight || 48;
        const viewBottom = window.innerHeight - barHeight;
        if (rect.bottom > viewBottom) {
            window.scrollBy({ top: rect.bottom - viewBottom + 8, behavior: 'smooth' });
        } else if (rect.top < 0) {
            window.scrollBy({ top: rect.top - 8, behavior: 'smooth' });
        }
    }

    function deselectLibraryCard(cards) {
        if (!cards) cards = document.querySelectorAll('.rank-card');
        cards.forEach(c => c.classList.remove('kb-selected'));
        selectedLibraryIndex = -1;
    }

    function findCardInDirection(cards, currentIdx, direction) {
        const current = cards[currentIdx];
        if (!current) return currentIdx;
        const rect = current.getBoundingClientRect();
        const centerX = rect.left + rect.width / 2;
        let best = currentIdx;
        let bestDist = Infinity;

        for (let i = 0; i < cards.length; i++) {
            if (i === currentIdx) continue;
            const r = cards[i].getBoundingClientRect();
            const isBelow = direction > 0 ? r.top > rect.bottom - 5 : r.bottom < rect.top + 5;
            if (!isBelow) continue;
            const dx = (r.left + r.width / 2) - centerX;
            const dy = (r.top + r.height / 2) - (rect.top + rect.height / 2);
            const dist = Math.abs(dx) + Math.abs(dy) * 0.1;
            if (dist < bestDist) { bestDist = dist; best = i; }
        }
        return best;
    }

    function openLightbox(img) {
        lightboxIndex = libraryImages.findIndex(i => i.id === img.id);
        if (lightboxIndex < 0) lightboxIndex = 0;
        buildFilmstrip();
        showLoupeImage(libraryImages[lightboxIndex], 0);
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
            thumb.className = 'filmstrip-thumb' + (i === lightboxIndex ? ' active' : '') + (flagClass(img.flag) ? ' ' + flagClass(img.flag) : '');
            thumb.dataset.idx = i;
            thumb.dataset.imageId = img.id;
            thumb.onclick = () => {
                const direction = Math.sign(i - lightboxIndex);
                lightboxIndex = i;
                showLoupeImage(libraryImages[i], direction);
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
    let loupeTierProbes = [];
    const loupeRefLong = 3840; // lg thumbnail long side used before original dimensions are known
    const LOUPE_PRELOAD_RADIUS = 3;

    function showLoupeImage(img, direction = 0) {
        const loupe = document.getElementById('loupe');
        const loupeImg = document.getElementById('loupe-img');
        if (!loupe || !loupeImg) return;

        // Pre-calculate fit dimensions from aspect ratio so all progressive
        // loads (sm/md/lg) display at the same screen size — no size jumps
        const token = ++loupeImageToken;
        loupeCurrentImage = img;
        loupeFullLoadToken = 0;
        loupeTierProbes = [];
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

        // Progressive loading: sm -> md -> lg -> original. Slow tiers time out
        // so the next tier still gets a chance, while late arrivals can still
        // upgrade the image if they are sharper than the current display.
        loupeImg.alt = img.filename || '';
        loupeImg.src = img.thumb_url;
        loupeDisplayedTierRank = 0;
        runLoupeProgressiveLoad(img, token);

        // Populate metadata overlay
        const filenameEl = document.getElementById('loupe-overlay-filename');
        const exifEl = document.getElementById('loupe-overlay-exif');
        const statsEl = document.getElementById('loupe-overlay-stats');
        const tierEl = document.getElementById('loupe-overlay-tier');

        if (filenameEl) filenameEl.textContent = img.filename;
        if (exifEl) exifEl.textContent = '';
        if (tierEl) tierEl.textContent = LOUPE_TIER_LABELS[0];

        const stars = eloToStars(img.elo, img.comparisons);
        const starStr = stars > 0 ? '★'.repeat(stars) + '☆'.repeat(5 - stars) + '  ' : '';
        if (statsEl) statsEl.textContent = `${starStr}${img.elo} Elo · ${img.comparisons} comparisons`;
        updateLoupeFlagDisplay(img.flag || 'unflagged');

        updateFilmstripActive();

        preloadLoupeNeighbors(direction);
        warmLoupeHotSet(direction);

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

    async function getMediaStatus(imageId, { force = false } = {}) {
        const cached = mediaStatusCache.get(imageId);
        if (!force && cached && Date.now() - cached.time < MEDIA_STATUS_MAX_AGE_MS) {
            return cached.data;
        }
        try {
            const res = await fetch(`/api/image/${imageId}/media-status`);
            if (!res.ok) return null;
            const data = await res.json();
            mediaStatusCache.set(imageId, { time: Date.now(), data });
            return data;
        } catch {
            return null;
        }
    }

    function loupeTierUrl(tier, imageId, cachedOnly = false) {
        if (tier === 'full') {
            return `/api/full/${imageId}${cachedOnly ? '?cached=1' : ''}`;
        }
        return `/api/thumb/${tier}/${imageId}${cachedOnly ? '?cached=1' : ''}`;
    }

    function updateLoupeFlagDisplay(flag) {
        const tierEl = document.getElementById('loupe-overlay-tier');
        if (!tierEl) return;
        const tier = LOUPE_TIER_LABELS[loupeDisplayedTierRank] || 'Image';
        const label = flag === 'picked' ? 'Picked' : flag === 'rejected' ? 'Rejected' : 'Unflagged';
        tierEl.textContent = `${tier} · ${label}`;
    }

    function loupeApplyImageSize() {
        const img = document.getElementById('loupe-img');
        if (!img || !loupeNatW || !loupeNatH) return;
        img.style.width = `${loupeNatW}px`;
        img.style.height = `${loupeNatH}px`;
    }

    async function runLoupeProgressiveLoad(img, token) {
        const status = await getMediaStatus(img.id);
        if (!isCurrentLoupeImage(img, token)) return;

        const cachedBest = status?.best_cached;
        if (cachedBest && cachedBest !== 'sm') {
            const rank = LOUPE_TIER_RANKS[cachedBest];
            await loadLoupeTier(img, loupeTierUrl(cachedBest, img.id, true), rank, token, {
                adoptDimensions: cachedBest === 'full',
                timeoutMs: 900,
            });
            if (!isCurrentLoupeImage(img, token)) return;
        }

        const tiers = [
            { name: 'md', adoptDimensions: false },
            { name: 'lg', adoptDimensions: false },
            { name: 'full', adoptDimensions: true },
        ];
        for (const tier of tiers) {
            const rank = LOUPE_TIER_RANKS[tier.name];
            if (rank <= loupeDisplayedTierRank) continue;
            if (tier.name === 'full') loupeFullLoadToken = token;
            await loadLoupeTier(img, loupeTierUrl(tier.name, img.id), rank, token, {
                adoptDimensions: tier.adoptDimensions,
                timeoutMs: LOUPE_TIER_TIMEOUTS[tier.name],
            });
            if (!isCurrentLoupeImage(img, token)) return;
        }
    }

    function loadLoupeTier(img, url, rank, token, { adoptDimensions = false, timeoutMs = 0 } = {}) {
        if (!url) return Promise.resolve(false);
        return new Promise((resolve) => {
            const probe = new Image();
            let settled = false;
            let timer = null;
            loupeTierProbes.push(probe);

            const releaseProbe = () => {
                const idx = loupeTierProbes.indexOf(probe);
                if (idx >= 0) loupeTierProbes.splice(idx, 1);
            };
            const finish = (loaded) => {
                if (settled) return;
                settled = true;
                if (timer) clearTimeout(timer);
                resolve(loaded);
            };

            probe.decoding = 'async';
            if ('fetchPriority' in probe) probe.fetchPriority = rank >= 2 ? 'high' : 'auto';
            probe.onload = () => {
                releaseProbe();
                if (!probe.naturalWidth || !probe.naturalHeight) {
                    finish(false);
                    return;
                }
                if (isCurrentLoupeImage(img, token) && rank > loupeDisplayedTierRank) {
                    if (adoptDimensions && probe.naturalWidth > 0 && probe.naturalHeight > 0) {
                        loupeAdoptSourceDimensions(probe.naturalWidth, probe.naturalHeight);
                    }

                    const loupeImg = document.getElementById('loupe-img');
                    if (loupeImg) {
                        loupeDisplayedTierRank = rank;
                        loupeImg.src = probe.src;
                        updateLoupeFlagDisplay(img.flag || 'unflagged');
                    }
                }
                finish(true);
            };
            probe.onerror = () => {
                releaseProbe();
                finish(false);
            };
            if (timeoutMs > 0) {
                timer = setTimeout(() => finish(false), timeoutMs);
            }
            probe.src = url;
        });
    }

    function requestLoupeFullImage(img = loupeCurrentImage, token = loupeImageToken) {
        if (!img || !isCurrentLoupeImage(img, token) || loupeFullLoadToken === token) return;
        loupeFullLoadToken = token;
        if (loupeFullLoadTimer) {
            clearTimeout(loupeFullLoadTimer);
            loupeFullLoadTimer = null;
        }
        loadLoupeTier(img, loupeTierUrl('full', img.id), 3, token, {
            adoptDimensions: true,
            timeoutMs: LOUPE_TIER_TIMEOUTS.full,
        });
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

    function loupeNeighborOffsets(radius, direction = 0) {
        const offsets = [];
        for (let distance = 1; distance <= radius; distance++) {
            if (direction > 0) offsets.push(distance, -distance);
            else if (direction < 0) offsets.push(-distance, distance);
            else offsets.push(-distance, distance);
        }
        return offsets;
    }

    function preloadLoupeNeighbors(direction = 0) {
        for (const offset of loupeNeighborOffsets(LOUPE_PRELOAD_RADIUS, direction)) {
            const ni = lightboxIndex + offset;
            if (ni < 0 || ni >= libraryImages.length) continue;
            const neighbor = libraryImages[ni];
            preloadLoupeNeighbor(neighbor, Math.abs(offset));
        }
    }

    function warmLoupeHotSet(direction = 0) {
        if (lightboxIndex < 0 || lightboxIndex >= libraryImages.length) return;
        const current = libraryImages[lightboxIndex];
        const md = [];
        const lg = current?.id ? [current.id] : [];
        const full = current?.id ? [current.id] : [];

        for (const offset of loupeNeighborOffsets(8, direction)) {
            const ni = lightboxIndex + offset;
            if (ni < 0 || ni >= libraryImages.length) continue;
            const id = libraryImages[ni]?.id;
            if (!id) continue;
            const distance = Math.abs(offset);
            if (distance <= 8) md.push(id);
            if (distance <= 4) lg.push(id);
            if (distance <= 1 && (direction === 0 || Math.sign(offset) === direction)) full.push(id);
        }

        warmImageTiers({ md, lg, full });
    }

    async function preloadLoupeNeighbor(img, distance = 1) {
        await preloadImageWithTimeout(img.thumb_url, 'low', 1200);
        const status = await getMediaStatus(img.id);
        const tiers = status?.tiers || {};
        if (tiers.md?.cached) {
            await preloadImageWithTimeout(tiers.md.cached_url, 'low', LOUPE_TIER_TIMEOUTS.md);
        }
        if (tiers.lg?.cached) {
            await preloadImageWithTimeout(tiers.lg.cached_url, 'low', LOUPE_TIER_TIMEOUTS.lg);
        }
        if (distance === 1) {
            if (!tiers.md?.cached) {
                await preloadImageWithTimeout(loupeTierUrl('md', img.id), 'low', LOUPE_TIER_TIMEOUTS.md);
            }
            if (!tiers.lg?.cached) {
                await preloadImageWithTimeout(loupeTierUrl('lg', img.id), 'low', LOUPE_TIER_TIMEOUTS.lg);
            }
        }
    }

    function preloadImageWithTimeout(url, priority, timeoutMs) {
        return Promise.race([
            preloadImage(url, priority),
            new Promise((resolve) => setTimeout(resolve, timeoutMs)),
        ]);
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
        showLoupeImage(libraryImages[lightboxIndex], 1);
    }

    function lightboxPrev() {
        if (lightboxIndex <= 0) return;
        lightboxIndex--;
        showLoupeImage(libraryImages[lightboxIndex], -1);
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
        loupeTierProbes = [];
        loupeDisplayedTierRank = -1;
        loupeIsFit = true;
        loupeZoomMode = 'fit';
        loupeNatW = 0;
        loupeNatH = 0;
        lightboxIndex = -1;
    }

    function setThumbSize(value) {
        thumbHeight = parseInt(value);

        // Compare page: slider controls mosaic grid size
        const mosaicGrid = document.getElementById('mosaic-grid');
        if (mosaicGrid) {
            // Map slider 120-400 → mosaic count 24-4 (small thumb = more images)
            const t = (thumbHeight - 120) / (400 - 120); // 0..1
            const newSize = Math.round(24 - t * 20);     // 24 down to 4
            if (newSize !== mosaicSize) {
                mosaicSize = newSize;
                loadMosaicBatch();
            }
            return;
        }

        // Library page: slider controls card height
        document.documentElement.style.setProperty('--thumb-height', thumbHeight + 'px');
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
        saveFilters();
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
        saveFilters();
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
        saveFilters();
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
            card.className = 'rank-card' + (flagClass(simg.flag) ? ' ' + flagClass(simg.flag) : '');
            card.dataset.imageId = simg.id;
            card.dataset.ar = ar;
            card.style.height = thumbHeight + 'px';
            card.style.flexGrow = ar;
            card.style.flexBasis = (thumbHeight * ar) + 'px';
            card.onclick = () => openLightbox(simg);

            card.innerHTML = `
                <img src="${simg.thumb_url}" alt="${simg.filename}" loading="lazy" onload="this.classList.add('loaded')">
                ${flagBadge(simg.flag)}
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
        'cache_profile',
        'ssd_cache_dir',
        'ssd_cache_gb',
        'pregenerate_on_idle',
        'search_similarity_threshold',
    ];
    const THUMB_OUTPUT_FIELDS = ['thumb_size_sm', 'thumb_size_md', 'thumb_size_lg', 'thumb_quality'];
    let settingsPoller = null;
    let settingsPageData = null;
    let savedThumbnailOutput = null;

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
        const progressCount = Number((tier.progress_count ?? tier.count) || 0);
        const staleCount = Number(tier.stale_count || 0);
        const progress = progressTotal > 0
            ? `${progressCount.toLocaleString()} / ${progressTotal.toLocaleString()} (${Number(tier.progress_pct || 0).toFixed(1)}%)`
            : `${Number(tier.count || 0).toLocaleString()} cached`;
        const budgetText = budget > 0
            ? `${formatBytes(tier.bytes)} / ${formatBytes(budget)}`
            : `${formatBytes(tier.bytes)} / off`;
        const fallbackText = staleCount > 0 ? ` · ${staleCount.toLocaleString()} older usable` : '';
        return `${label}: ${budgetText} · ${progress}${fallbackText}`;
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
            const state = pregen.replacement_mode ? 'refreshing previews' : (pregen.state || 'idle');
            const phase = pregen.active_phase ? ` · ${pregen.active_phase}` : '';
            const message = pregen.message ? ` · ${pregen.message}` : '';
            const rate = Number(pregen.recent_images_per_min || pregen.overall_images_per_min || 0);
            const eta = pregen.eta_seconds ? ` · ETA ${formatEta(pregen.eta_seconds)}` : '';
            const speed = rate > 0 ? ` · ${formatRatePerMinute(rate)}` : '';
            pregenEl.textContent = `${state}${phase}${message}${speed}${eta}`;
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
                `review ${Number(settings.review_prefetch_limit || 0)} · ` +
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
        settingsPageData = data || settingsPageData;
        const pathEl = document.getElementById('settings-path');
        renderCacheSettingsStatus(data.cache_stats);
        if (pathEl && data.settings_path) {
            pathEl.textContent = data.settings_path;
        }
        renderAutoTuningStatus(data.settings);
        renderModelStatus(data.model_status);
        renderAISettingsStatus(data.ai_status);
        renderCacheTierGuide(data.cache_stats, data.settings);
        updateCacheProfileHint();
    }

    function renderCacheTierGuide(cs, settings = {}) {
        const el = document.getElementById('cache-tier-table');
        const titleEl = document.getElementById('cache-health-title');
        const subtitleEl = document.getElementById('cache-health-subtitle');
        const adviceEl = document.getElementById('cache-storage-advice');
        const rec = cs?.recommendations;
        if (!el || !rec?.tiers) return;

        const disk = cs.disk || {};
        const diskTiers = disk.tiers || {};
        const profile = rec.budget?.profile || 'original_heavy';
        const profileLabel = cacheProfileLabel(profile);
        const warmed = (name) => Number(diskTiers[name]?.progress_pct || 0);
        const smWarm = warmed('sm');
        const mdWarm = warmed('md');
        const estimateIsEarly = (name) => {
            const plan = rec.tiers[name] || {};
            const tier = diskTiers[name] || {};
            const count = Number(plan.sample_count ?? tier.count ?? 0);
            const total = Number(tier.progress_total || 0);
            if (count <= 0) return true;
            if (count < 5000) return true;
            return total > 0 && count / total < 0.05;
        };
        const headline = mdWarm >= 95
            ? 'Fast browsing cache is fully warmed'
            : smWarm >= 95
                ? 'Grid browsing is warmed; loupe previews are still building'
                : 'photoArchive is building the fast cache';

        if (titleEl) titleEl.textContent = headline;
        if (subtitleEl) {
            subtitleEl.textContent =
                `${profileLabel} priority · ${formatBytes(disk.used_bytes)} used of ${formatBytes(disk.limit_bytes)} on SSD`;
        }

        const card = (name, title, copy) => {
            const plan = rec.tiers[name] || {};
            const actual = diskTiers[name] || {};
            const pct = Math.max(0, Math.min(100, Number(actual.progress_pct || 0)));
            const capacityPct = Math.max(0, Math.min(100, Number(plan.coverage_pct || 0)));
            const progressCount = Number((actual.progress_count ?? actual.count) || 0);
            const staleCount = Number(actual.stale_count || 0);
            const cached = progressCount.toLocaleString();
            const total = Number(actual.progress_total || (name === 'full' ? rec.total_images : rec.eligible_images) || 0).toLocaleString();
            const estimated = Number(plan.estimated_cached || 0).toLocaleString();
            const capacityText = capacityPct >= 95
                ? estimateIsEarly(name) && name !== 'full' ? 'Likely space for all' : 'Space for all'
                : `Space for ~${estimated}`;
            let state = 'Not started';
            let cls = '';
            if (pct >= 95) {
                state = actual.replacement_mode ? 'Refreshed' : 'Generated';
                cls = 'ready';
            } else if (pct > 0) {
                state = actual.replacement_mode ? `${pct.toFixed(0)}% refreshed` : `${pct.toFixed(0)}% generated`;
                cls = 'selective';
            } else if (actual.replacement_mode && staleCount > 0) {
                state = 'Refreshing';
                cls = 'selective';
            }
            const remaining = Number(cs.pregen?.phases?.[name]?.remaining || 0);
            const etaText = remaining > 0 && cs.pregen?.eta_seconds
                ? `<div class="cache-card-meta"><span>${remaining.toLocaleString()} remaining</span><span>ETA ${formatEta(cs.pregen.eta_seconds)}</span></div>`
                : '';
            const availabilityText = actual.replacement_mode && staleCount > 0
                ? `${cached} refreshed · ${staleCount.toLocaleString()} older usable`
                : `${cached} of ${total} generated`;
            return `
                <div class="cache-friendly-card ${cls}">
                    <div class="cache-card-meta"><strong>${title}</strong><span>${state}</span></div>
                    <div class="cache-progress"><div class="cache-progress-fill" style="width:${pct}%"></div></div>
                    <span>${copy}</span>
                    <div class="cache-card-meta"><span>${availabilityText}</span><span>${capacityText}</span></div>
                    ${etaText}
                </div>
            `;
        };
        el.innerHTML = [
            card('sm', 'Grid scrolling', 'Small previews that make the library feel instant.'),
            card('md', 'Loupe previews', 'Medium previews used first when opening an image.'),
            card('lg', 'High-res previews', 'Sharper previews for zooming and large displays.'),
            card('full', 'Original files', 'Originals copied to SSD when there is room.'),
        ].join('');

        if (adviceEl) {
            const needs = (name) => Number(rec.tiers[name]?.full_archive_bytes || 0);
            const instantBytes = needs('sm') + needs('md');
            const allHighResBytes = instantBytes + needs('lg');
            const originalsBytes = needs('full');
            const ssdBudget = Number(disk.limit_bytes || 0);
            const memoryBudget = Number(cs.memory?.limit_bytes || 0);
            const lgPlan = rec.tiers.lg || {};
            const fullPlan = rec.tiers.full || {};
            const quality = Number(settings?.thumb_quality || 0);
            const previewSampleCount = Math.min(
                Number(rec.tiers.sm?.sample_count || 0),
                Number(rec.tiers.md?.sample_count || 0),
                Number(rec.tiers.lg?.sample_count || 0),
            );
            const previewEstimateEarly = ['sm', 'md', 'lg'].some(estimateIsEarly);
            const previewEstimateLabel = previewEstimateEarly
                ? `Early estimate${quality ? ` at JPEG ${quality}` : ''}`
                : `Estimate${quality ? ` at JPEG ${quality}` : ''}`;
            const previewEstimateNote = previewEstimateEarly
                ? ` Based on the first ${previewSampleCount.toLocaleString()} regenerated previews, so this will refine as the cache rebuilds.`
                : '';
            const currentHotBytes = Math.max(0, ssdBudget - instantBytes);
            const ssdFitText = ssdBudget >= allHighResBytes
                ? `Based on the current estimate, your ${formatBytes(ssdBudget)} SSD budget can fit all preview tiers, with about ${formatBytes(ssdBudget - allHighResBytes)} left for hot originals.`
                : ssdBudget >= instantBytes
                    ? `Your current ${formatBytes(ssdBudget)} SSD budget covers the everyday target and leaves about ${formatBytes(currentHotBytes)} for hot high-res previews and originals.`
                    : `Your current ${formatBytes(ssdBudget)} SSD budget is below the everyday target, so photoArchive will favor the images you touch most.`;
            const hotPlanText =
                `At the current priority, the dynamic area has room for about ${Number(lgPlan.estimated_cached || 0).toLocaleString()} high-res previews and ${Number(fullPlan.estimated_cached || 0).toLocaleString()} originals.`;
            const ramText = memoryBudget >= 3 * 1024 * 1024 * 1024
                ? `${formatBytes(memoryBudget)} RAM is generous for this app; it will keep recent medium/high-res previews hot while SSD does the durable work.`
                : memoryBudget >= 1 * 1024 * 1024 * 1024
                    ? `${formatBytes(memoryBudget)} RAM is a solid browsing cache. More helps long loupe sessions, but SSD cache is still the bigger lever.`
                    : `${formatBytes(memoryBudget)} RAM is conservative. Browsing will still work, but recently viewed previews will cycle out sooner.`;
            adviceEl.innerHTML = `
                <div><strong>Current SSD budget:</strong> ${ssdFitText}</div>
                <div><strong>Current hot cache:</strong> ${hotPlanText}</div>
                <div><strong>Everyday target:</strong> ${formatBytes(instantBytes)} covers grid scrolling and loupe previews for this archive. ${formatBytes(Math.ceil(instantBytes * 1.25))} leaves comfortable breathing room.</div>
                <div><strong>All high-res previews:</strong> ${previewEstimateLabel}: about ${formatBytes(allHighResBytes)} for small + medium + large preview tiers.${previewEstimateNote}</div>
                <div><strong>All originals:</strong> about ${formatBytes(originalsBytes)}, so originals are treated as a selective hot cache instead of a full duplicate archive.</div>
                <div><strong>RAM budget:</strong> ${ramText}</div>
            `;
        }
    }

    function cacheProfileLabel(profile) {
        if (profile === 'browse_fast') return 'Fastest browsing';
        if (profile === 'balanced') return 'Balanced';
        return 'Best quality';
    }

    function updateCacheProfileHint() {
        const input = document.getElementById('cache_profile');
        const hint = document.getElementById('cache-profile-hint');
        if (!input || !hint) return;
        const profile = input.value;
        if (profile === 'browse_fast') {
            hint.textContent = 'Fastest browsing puts extra space toward previews before originals.';
        } else if (profile === 'balanced') {
            hint.textContent = 'Balanced divides extra space between high-res previews and originals.';
        } else {
            hint.textContent = 'Best quality fills grid and loupe previews first, then favors originals when there is room.';
        }
    }

    function recommendedMemoryGb(settings = settingsPageData?.settings || {}) {
        const systemRam = Number(settings.system_memory_gb || 0);
        if (systemRam >= 32) return 4;
        if (systemRam >= 16) return 2;
        if (systemRam >= 8) return 1;
        return 0.5;
    }

    function applyRecommendedCache() {
        const profileInput = document.getElementById('cache_profile');
        const memoryInput = document.getElementById('memory_cache_gb');
        const warmInput = document.getElementById('pregenerate_on_idle');
        if (profileInput) profileInput.value = 'original_heavy';
        if (memoryInput) memoryInput.value = recommendedMemoryGb();
        if (warmInput) warmInput.checked = true;
        updateCacheProfileHint();
        saveSettings();
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
        const total = Number(aiStatus.total_images ?? aiStatus.total_kept ?? 0);
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
        setThumbnailCachePolicy('keep');
        updateThumbnailChangeNotice();
    }

    function rememberThumbnailOutput(settings) {
        savedThumbnailOutput = {};
        for (const field of THUMB_OUTPUT_FIELDS) {
            savedThumbnailOutput[field] = Number(settings?.[field] || 0);
        }
    }

    function currentThumbnailOutput() {
        const values = {};
        for (const field of THUMB_OUTPUT_FIELDS) {
            const input = document.getElementById(field);
            values[field] = Number(input?.value || 0);
        }
        return values;
    }

    function thumbnailOutputChanges() {
        if (!savedThumbnailOutput) return [];
        const current = currentThumbnailOutput();
        const labels = {
            thumb_size_sm: 'small size',
            thumb_size_md: 'medium size',
            thumb_size_lg: 'large size',
            thumb_quality: 'JPEG quality',
        };
        const changes = [];
        for (const field of THUMB_OUTPUT_FIELDS) {
            if (Number(savedThumbnailOutput[field] || 0) !== Number(current[field] || 0)) {
                changes.push(`${labels[field]} ${savedThumbnailOutput[field]} → ${current[field]}`);
            }
        }
        return changes;
    }

    function setThumbnailCachePolicy(policy) {
        const input = document.querySelector(`input[name="thumbnail_cache_policy"][value="${policy}"]`);
        if (input) input.checked = true;
    }

    function selectedThumbnailCachePolicy() {
        return document.querySelector('input[name="thumbnail_cache_policy"]:checked')?.value || 'keep';
    }

    function updateThumbnailChangeNotice() {
        const notice = document.getElementById('thumbnail-change-notice');
        const copy = document.getElementById('thumbnail-change-copy');
        if (!notice) return;
        const changes = thumbnailOutputChanges();
        notice.classList.toggle('hidden', changes.length === 0);
        if (copy && changes.length) {
            copy.textContent = `${changes.join(', ')}. Keeping existing previews avoids extra work. Refreshing them keeps old previews usable while photoArchive replaces each file in the background.`;
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
        payload.thumbnail_cache_policy = selectedThumbnailCachePolicy();
        return payload;
    }

    async function loadSettingsPage(showStatus = true) {
        const res = await fetch('/api/settings');
        const data = await res.json();
        populateSettingsForm(data.settings || {});
        rememberThumbnailOutput(data.settings || {});
        updateThumbnailChangeNotice();
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
        document.getElementById('cache_profile')?.addEventListener('change', updateCacheProfileHint);
        for (const field of THUMB_OUTPUT_FIELDS) {
            document.getElementById(field)?.addEventListener('input', updateThumbnailChangeNotice);
        }

        try {
            await loadSettingsPage(false);
            // Load scan folder and stats
            const statsRes = await fetch('/api/stats');
            const stats = await statsRes.json();
            const folderInput = document.getElementById('scan-folder');
            const totalEl = document.getElementById('scan-total-images');
            if (totalEl) totalEl.textContent = (stats.total_images || 0).toLocaleString();
            // Infer folder from DB
            try {
                const folderRes = await fetch('/api/scan/folder');
                const folderData = await folderRes.json();
                if (folderInput && folderData.folder) folderInput.value = folderData.folder;
            } catch {}

            setSettingsStatus('Ready. Save to apply changes immediately.', 'muted');
            if (settingsPoller) clearInterval(settingsPoller);
            settingsPoller = setInterval(() => refreshSettingsMeta().catch(() => {}), 5000);
        } catch (err) {
            setSettingsStatus(`Could not load settings: ${err.message}`, 'error');
        }
    }

    let _scanPoller = null;
    async function startScan() {
        const folderInput = document.getElementById('scan-folder');
        const folder = folderInput?.value?.trim();
        if (!folder) return;

        const btn = document.getElementById('scan-btn');
        if (btn) { btn.disabled = true; btn.textContent = 'Scanning…'; }
        const progress = document.getElementById('scan-progress');
        if (progress) progress.classList.remove('hidden');

        try {
            await fetch('/api/scan', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ folder }),
            });
        } catch {}

        // Poll scan status
        if (_scanPoller) clearInterval(_scanPoller);
        _scanPoller = setInterval(async () => {
            try {
                const res = await fetch('/api/scan/status');
                const data = await res.json();
                const countEl = document.getElementById('scan-progress-count');
                if (countEl) countEl.textContent = (data.total_found || 0).toLocaleString();
                const totalEl = document.getElementById('scan-total-images');
                if (totalEl) totalEl.textContent = (data.total_inserted || 0).toLocaleString();
                if (!data.scanning) {
                    clearInterval(_scanPoller);
                    _scanPoller = null;
                    if (btn) { btn.disabled = false; btn.textContent = 'Scan Folder'; }
                    if (progress) progress.classList.add('hidden');
                    setSettingsStatus(`Scan complete. ${(data.total_inserted || 0).toLocaleString()} images in database.`, 'success');
                }
            } catch {}
        }, 1000);
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
            rememberThumbnailOutput(data.settings || {});
            updateThumbnailChangeNotice();
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
            rememberThumbnailOutput(data.settings || {});
            updateThumbnailChangeNotice();
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
            rememberThumbnailOutput(saveData.settings || {});
            updateThumbnailChangeNotice();
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
        initCompare,
        initLibrary,
        initRankings,
        initSettings,
        clearSearch,
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
        mosaicShuffle,
        setMosaicStrategy,
        toggleAIPanel,
        saveSettings,
        applyRecommendedCache,
        resetSettings,
        clearThumbnailCache,
        startCachePregeneration,
        stopCachePregeneration,
        installAIModel,
        startScan,
    };
})();
