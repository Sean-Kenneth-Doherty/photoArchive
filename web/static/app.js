const PhotoArchive = (() => {
    // Track browser-loaded images: url -> Promise that resolves when loaded
    const preloaded = new Map();

    // --- Compare Mode State ---
    let comparePairs = [];
    let compareIndex = 0;
    let compareMode = 'swiss';
    let compareModeTransitionToken = 0;
    let compareBusy = false;
    let compareActionSeq = 0;
    let compareStats = {};
    let compareImageToken = 0;
    const compareDisplayedTier = { left: -1, right: -1 };

    // --- Rankings State ---
    let rankingsOffset = 0;
    const INITIAL_RANKINGS_PAGE_SIZE = 48;
    const RANKINGS_PAGE_SIZE = 100;
    const BACKGROUND_WARM_DELAY_MS = 250;
    const CROSS_VIEW_WARM_DELAY_MS = 1000;
    const WARMUP_QUEUE_CONCURRENCY = 1;
    const LIBRARY_NEIGHBOR_LIMIT = 24;
    const MOSAIC_NEIGHBOR_LIMIT = 8;
    const COMPARE_NEIGHBOR_PAIRS = 4;
    const FILMSTRIP_WINDOW_RADIUS = 55;
    const LOUPE_TIER_LABELS = ['Thumbnail (sm)', 'Medium (md)', 'Large (lg)', 'Original'];
    const LOUPE_TIER_NAMES = ['sm', 'md', 'lg', 'full'];
    const LOUPE_TIER_TIMEOUTS = { md: 1800, lg: 2600, full: 5000 };
    const LOUPE_TIER_RANKS = { sm: 0, md: 1, lg: 2, full: 3 };
    const LOUPE_BLANK_SRC = 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==';
    const MEDIA_STATUS_MAX_AGE_MS = 15000;
    const mediaStatusCache = new Map();
    const mediaStatusInflight = new Map();
    let uiSettings = { show_loupe_cache_status: true };
    let uiSettingsPromise = null;
    let selectedLibraryIndex = -1;
    let selectedMosaicIndex = -1;
    const backgroundWarmTimers = new Map();
    const backgroundWarmTokens = new Map();
    const warmupQueue = [];
    let warmupActive = 0;
    let warmupGeneration = 0;
    const WARM_CACHE_PREFIX = 'photoarchive:warm:';
    const WARM_CACHE_MAX_AGE_MS = 30000;
    const WARM_TIER_DEDUPE_MS = 8000;
    const WARM_TIER_BATCH_DELAY_MS = 120;
    const recentWarmTierIds = new Map();
    const pendingWarmTiers = new Map();
    let warmTierFlushTimer = null;
    let bottomBarResizeObserver = null;
    let bottomBarResizeListenerAdded = false;

    function updateBottomBarHeightVar() {
        const bar = document.querySelector('.bottom-bar');
        const height = bar?.offsetHeight || 0;
        document.documentElement.style.setProperty('--current-bottom-bar-height', `${height}px`);
        if (document.getElementById('mosaic-grid')) scheduleMosaicRender();
    }

    function initBottomBarMeasurement() {
        updateBottomBarHeightVar();
        if (bottomBarResizeObserver) bottomBarResizeObserver.disconnect();

        const bar = document.querySelector('.bottom-bar');
        if (bar && 'ResizeObserver' in window) {
            bottomBarResizeObserver = new ResizeObserver(updateBottomBarHeightVar);
            bottomBarResizeObserver.observe(bar);
        }
        if (!bottomBarResizeListenerAdded) {
            window.addEventListener('resize', updateBottomBarHeightVar);
            bottomBarResizeListenerAdded = true;
        }
    }

    function clearWarmups() {
        warmupGeneration++;
        for (const timer of backgroundWarmTimers.values()) clearTimeout(timer);
        backgroundWarmTimers.clear();
        for (const item of warmupQueue.splice(0)) {
            if (item.onDrop) item.onDrop();
        }
    }

    function enqueueWarmup(task, { generation = warmupGeneration, onDrop = null } = {}) {
        warmupQueue.push({ task, generation, onDrop });
        pumpWarmupQueue();
    }

    async function pumpWarmupQueue() {
        if (warmupActive >= WARMUP_QUEUE_CONCURRENCY) return;
        const item = warmupQueue.shift();
        if (!item) return;
        if (item.generation !== warmupGeneration) {
            if (item.onDrop) item.onDrop();
            pumpWarmupQueue();
            return;
        }

        warmupActive++;
        try {
            await item.task();
        } catch {
            // Warmups are opportunistic.
        } finally {
            warmupActive = Math.max(0, warmupActive - 1);
            pumpWarmupQueue();
        }
    }

    function scheduleBackgroundWarm(key, task, delay = BACKGROUND_WARM_DELAY_MS) {
        const token = (backgroundWarmTokens.get(key) || 0) + 1;
        backgroundWarmTokens.set(key, token);
        const generation = warmupGeneration;

        const existingTimer = backgroundWarmTimers.get(key);
        if (existingTimer) clearTimeout(existingTimer);

        const timer = setTimeout(() => {
            backgroundWarmTimers.delete(key);
            enqueueWarmup(
                () => task(token, generation),
                { generation },
            );
        }, delay);

        backgroundWarmTimers.set(key, timer);
    }

    function isWarmTokenCurrent(key, token, generation = warmupGeneration) {
        return backgroundWarmTokens.get(key) === token && generation === warmupGeneration;
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

    async function warmImageUrls(urls, generation = warmupGeneration) {
        for (const url of urls || []) {
            if (generation !== warmupGeneration) return;
            await preloadImageWithTimeout(url, 'low', 1200);
        }
    }

    function warmImageTiers(tiers) {
        const now = Date.now();
        let queued = false;
        for (const [tier, ids] of Object.entries(tiers || {})) {
            const unique = [...new Set((ids || []).map((id) => Number(id)).filter((id) => id > 0))];
            for (const id of unique) {
                const key = `${tier}:${id}`;
                const lastWarm = recentWarmTierIds.get(key) || 0;
                if (now - lastWarm < WARM_TIER_DEDUPE_MS) continue;
                recentWarmTierIds.set(key, now);
                if (!pendingWarmTiers.has(tier)) pendingWarmTiers.set(tier, new Set());
                pendingWarmTiers.get(tier).add(id);
                queued = true;
            }
        }
        if (!queued || warmTierFlushTimer) return;
        warmTierFlushTimer = setTimeout(flushWarmImageTiers, WARM_TIER_BATCH_DELAY_MS);
    }

    function flushWarmImageTiers() {
        warmTierFlushTimer = null;
        const payload = {};
        for (const [tier, ids] of pendingWarmTiers.entries()) {
            if (ids.size) payload[tier] = Array.from(ids).slice(0, 96);
        }
        pendingWarmTiers.clear();
        if (!Object.keys(payload).length) return;
        const cutoff = Date.now() - (WARM_TIER_DEDUPE_MS * 3);
        for (const [key, warmedAt] of recentWarmTierIds.entries()) {
            if (warmedAt < cutoff) recentWarmTierIds.delete(key);
        }
        fetch('/api/images/warm', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tiers: payload }),
            keepalive: true,
        }).then(() => {
            const currentId = Number(loupeCurrentImage?.id || 0);
            if (!currentId) return;
            const includesCurrent = Object.values(payload).some((ids) => (ids || []).includes(currentId));
            if (includesCurrent) refreshLoupeMediaStatus(loupeCurrentImage, loupeImageToken);
        }).catch(() => {});
    }

    async function loadUiSettings() {
        if (uiSettingsPromise) return uiSettingsPromise;
        uiSettingsPromise = fetch('/api/ui/settings')
            .then((res) => (res.ok ? res.json() : null))
            .then((data) => {
                const values = data?.settings || data || {};
                uiSettings = {
                    ...uiSettings,
                    show_loupe_cache_status: values.show_loupe_cache_status !== false,
                };
                renderLoupeStatusLine();
                return uiSettings;
            })
            .catch(() => uiSettings)
            .finally(() => {
                uiSettingsPromise = null;
            });
        return uiSettingsPromise;
    }

    async function warmRequests(key, token, generation, requests) {
        for (const request of requests) {
            if (!isWarmTokenCurrent(key, token, generation)) return;
            const data = await fetchWarmJson(request.url);
            if (!data || !isWarmTokenCurrent(key, token, generation)) return;
            if (request.cacheKey) saveWarmCache(request.cacheKey, data);
            await warmImageUrls(request.extract(data), generation);
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
    let mosaicResizeRaf = null;

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
        if (!grid) return;
        grid.innerHTML = '';
        selectedMosaicIndex = -1;
        const token = ++mosaicRenderToken;

        // Calculate row height to fit all images in the viewport
        // using the same justified flex layout as the library
        const gap = 3;
        const containerW = grid.clientWidth || grid.parentElement?.clientWidth || window.innerWidth;
        const barH = document.querySelector('.bottom-bar')?.offsetHeight || 60;
        const containerH = Math.max(
            80,
            grid.clientHeight || grid.parentElement?.clientHeight || window.innerHeight - barH - 4,
        );

        // Simulate row packing to find the right row height
        // Binary search for the height that fits all images in the container
        let lo = 60, hi = Math.max(60, containerH);
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
            cell.innerHTML = `<img src="${escapeHtml(img.thumb_url)}" alt="${escapeHtml(img.filename)}" data-tier-rank="0" onload="this.classList.add('loaded')">`;
            preloadImage(img.thumb_url);
            grid.appendChild(cell);
            scheduleMosaicImageUpgrade(cell, img, rowH, token, index);
        }
    }

    function scheduleMosaicRender() {
        if (mosaicResizeRaf) cancelAnimationFrame(mosaicResizeRaf);
        mosaicResizeRaf = requestAnimationFrame(() => {
            mosaicResizeRaf = null;
            if (compareMode === 'mosaic' && mosaicImages.length) renderMosaic();
        });
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

    function mosaicFillReplacements() {
        if (mosaicFilling || mosaicReplacements.length >= 10) return;
        mosaicFilling = true;
        const generation = warmupGeneration;
        const renderToken = mosaicRenderToken;
        enqueueWarmup(async () => {
            try {
                if (generation !== warmupGeneration || renderToken !== mosaicRenderToken) return;
                const excludeIds = [
                    ...mosaicImages.map(img => img.id),
                    ...mosaicReplacements.map(img => img.id),
                ].join(',');
                const res = await fetch(buildMosaicUrl({ n: 10, exclude: excludeIds }));
                const data = await res.json();
                if (generation !== warmupGeneration || renderToken !== mosaicRenderToken) return;
                if (data.stats) {
                    compareStats = data.stats;
                    updateCompareProgress();
                }
                // Deduplicate against current grid and existing replacements
                const onGrid = new Set(mosaicImages.map(img => img.id));
                const inBuffer = new Set(mosaicReplacements.map(img => img.id));
                const replacementUrls = [];
                for (const img of data.images) {
                    if (!onGrid.has(img.id) && !inBuffer.has(img.id)) {
                        mosaicReplacements.push(img);
                        inBuffer.add(img.id);
                        if (img.thumb_url) replacementUrls.push(img.thumb_url);
                    }
                }
                warmImageUrls(replacementUrls, generation).catch(() => {});
            } catch {} finally {
                mosaicFilling = false;
            }
        }, {
            generation,
            onDrop: () => {
                mosaicFilling = false;
            },
        });
    }

    let mosaicBusy = false;
    let mosaicActionSeq = 0;

    function mosaicClick(id) {
        if (mosaicBusy) return;
        const idx = mosaicImages.findIndex(img => img.id === id);
        if (idx === -1) return;
        mosaicBusy = true;
        const actionSeq = ++mosaicActionSeq;

        const otherIds = mosaicImages.filter(img => img.id !== id).map(img => img.id);

        const snapshot = {
            renderToken: mosaicRenderToken,
            images: mosaicImages.slice(),
            age: mosaicAge.slice(),
            replacements: mosaicReplacements.slice(),
            stats: { ...compareStats },
            propagationCounts: { ...mosaicPropagationCounts },
        };

        const savePick = fetch('/api/mosaic/pick', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ winner_id: id, loser_ids: otherIds }),
        }).then(async (res) => {
            let payload = {};
            try {
                payload = await res.json();
            } catch {}
            if (!res.ok || payload.ok === false) {
                throw new Error(payload.error || 'Failed to save pick');
            }
            return payload;
        }).then(
            (payload) => ({ ok: true, payload }),
            (error) => ({ ok: false, error }),
        );

        // Update stats using precomputed propagation count if available
        const propagated = mosaicPropagationCounts[id] || 0;
        bumpRankingSignals(otherIds.length + propagated, otherIds.length);
        updateCompareProgress();
        const needsPropagationPoll = propagated <= 0;
        if (propagated > 0) {
            showPropagationBadge(propagated);
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

        if (mosaicRenderToken === snapshot.renderToken) {
            for (const ri of replaceIndices) {
                const targetCell = cells[ri];
                if (!targetCell) continue;
                if (mosaicReplacements.length === 0) {
                    targetCell.classList.remove('mosaic-picked');
                    mosaicFillReplacements();
                    continue;
                }
                const newImg = mosaicReplacements.shift();
                mosaicImages[ri] = newImg;
                mosaicAge[ri] = 0;
                targetCell.dataset.id = newImg.id;
                targetCell.onclick = () => mosaicClick(newImg.id);
                const imgEl = targetCell.querySelector('img');
                if (imgEl) {
                    imgEl.dataset.tierRank = '0';
                    imgEl.classList.remove('loaded');
                    imgEl.src = newImg.thumb_url;
                    imgEl.alt = newImg.filename;
                }
                targetCell.classList.remove('mosaic-picked');
                scheduleMosaicImageUpgrade(targetCell, newImg, targetCell.clientHeight || 220, mosaicRenderToken, ri);
            }
            mosaicFillReplacements();
        }

        mosaicBusy = false;

        savePick.then((saveResult) => {
            if (!saveResult.ok) {
                if (mosaicRenderToken === snapshot.renderToken && mosaicActionSeq === actionSeq) {
                    mosaicImages = snapshot.images;
                    mosaicAge = snapshot.age;
                    mosaicReplacements = snapshot.replacements;
                    compareStats = snapshot.stats;
                    mosaicPropagationCounts = snapshot.propagationCounts;
                    renderMosaic();
                    updateCompareProgress();
                    showToast('Failed to save pick; restored the previous grid');
                } else {
                    showToast('Failed to save pick');
                }
                return;
            }

            // Refill replacement buffer and recompute propagation for new grid
            if (needsPropagationPoll) {
                fetchPropagationCount(0);
            }
            mosaicFillReplacements();
            precomputePropagation();

            if (mosaicImages.length < 2) {
                showCompareEmpty();
            }
        });
    }

    function showToast(msg) {
        updateBottomBarHeightVar();
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
        clearWarmups();
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
        initBottomBarMeasurement();
        document.addEventListener('keydown', handleCompareKey);
        window.addEventListener('resize', scheduleMosaicRender);
        document.getElementById('compare-left').addEventListener('click', () => submitComparison('left'));
        document.getElementById('compare-right').addEventListener('click', () => submitComparison('right'));
        // Set slider to match default mosaic size (12 images → slider ~168)
        const slider = document.getElementById('thumb-size');
        if (slider) {
            const t = (24 - mosaicSize) / 20; // invert: size→t
            slider.value = Math.round(120 + t * 280);
        }
        restoreFilters();
        restoreSearchState();
        setCompareMode('mosaic');
        pollAIStatus();
        setInterval(pollAIStatus, 5000);
        loadFolderList();
        loadFilterOptions();
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
                    modelText.textContent = `${Number(data.rated_images ?? data.compared ?? 0).toLocaleString()} ranked images · Elo propagation active`;
                    modelText.className = 'ai-panel-value trained';
                } else {
                    modelText.textContent = 'Not started';
                    modelText.className = 'ai-panel-value';
                }
            }
            if (predText) {
                if (Number(data.ranking_signal_count || 0) > 0) {
                    predText.textContent = `${Number(data.ranking_signal_count || 0).toLocaleString()} ranking signals`;
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
        imgEl.src = img.thumb_url || `/api/thumb/md/${img.id}`;
        compareDisplayedTier[side] = LOUPE_TIER_RANKS.md;
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

    function displayedRankingSignalCount() {
        return Number(compareStats.ranking_signal_count ?? compareStats.total_comparisons ?? 0);
    }

    function visibleTotalLabel(visible, total) {
        const visibleNum = Number(visible ?? 0);
        const totalNum = Number(total ?? visibleNum);
        const visibleText = visibleNum.toLocaleString();
        if (Number.isFinite(totalNum) && totalNum !== visibleNum) {
            return `${visibleText} / ${totalNum.toLocaleString()}`;
        }
        return visibleText;
    }

    function poolVisibleCount(stats = compareStats) {
        return Number(
            stats.filtered_pool_visible
            ?? stats.visible_images
            ?? stats.filtered_pool
            ?? stats['ke' + 'pt']
            ?? 0
        );
    }

    function poolTotalCount(stats = compareStats) {
        const visible = poolVisibleCount(stats);
        return Number(
            stats.filtered_pool_total
            ?? stats.total_images
            ?? stats.total_kept
            ?? stats.filtered_pool
            ?? visible
        );
    }

    function bumpRankingSignals(signalDelta, directDelta = 0) {
        const next = Math.max(0, displayedRankingSignalCount() + Number(signalDelta || 0));
        compareStats.ranking_signal_count = next;
        compareStats.total_comparisons = next;
        if (compareStats.direct_comparison_rows !== undefined) {
            compareStats.direct_comparison_rows = Math.max(
                0,
                Number(compareStats.direct_comparison_rows || 0) + Number(directDelta || 0),
            );
        }
    }

    function updateCompareProgress() {
        const total = displayedRankingSignalCount();
        const compEl = document.getElementById('compare-stat-comparisons');
        const poolEl = document.getElementById('compare-stat-pool');
        if (poolEl) poolEl.textContent = visibleTotalLabel(poolVisibleCount(), poolTotalCount());
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
                bumpRankingSignals(total, directCount);
                updateCompareProgress();
                if (data.count > 0) showPropagationBadge(data.count);
            }
        }).catch(() => {
            // Propagation fetch failed — still apply direct count
            if (directCount > 0) {
                bumpRankingSignals(directCount, directCount);
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

    function submitComparison(side) {
        if (compareBusy || compareIndex >= comparePairs.length) return;
        compareBusy = true;
        const actionSeq = ++compareActionSeq;

        const pair = comparePairs[compareIndex];
        const previousIndex = compareIndex;
        const winnerId = side === 'left' ? pair.left.id : pair.right.id;
        const loserId = side === 'left' ? pair.right.id : pair.left.id;

        compareIndex++;
        showComparePair();
        compareBusy = false;

        fetch('/api/compare', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ winner_id: winnerId, loser_id: loserId, mode: compareMode }),
        }).then(async (res) => {
            let result = {};
            try {
                result = await res.json();
            } catch {}
            if (!res.ok) throw new Error(result.error || 'Failed to save comparison');

            if (side === 'left') {
                pair.left.elo = result.winner_elo;
                pair.right.elo = result.loser_elo;
            } else {
                pair.right.elo = result.winner_elo;
                pair.left.elo = result.loser_elo;
            }
            fetchPropagationCount(1);
        }).catch(() => {
            if (compareActionSeq === actionSeq && compareMode !== 'mosaic') {
                compareIndex = previousIndex;
                showComparePair();
                showToast('Failed to save comparison; restored the previous pair');
            } else {
                showToast('Failed to save comparison');
            }
        });
    }

    async function undoComparison() {
        if (compareBusy) return;
        compareBusy = true;
        try {
            const res = await fetch('/api/compare/undo', { method: 'POST' });
            if (res.ok) {
                const result = await res.json();
                bumpRankingSignals(-Number(result.comparisons_undone || 1), -Number(result.comparisons_undone || 1));
                updateCompareProgress();
                if (compareMode !== 'mosaic' && compareIndex > 0) {
                    compareIndex--;
                    showComparePair();
                } else if (compareMode === 'mosaic') {
                    showToast(`Undid ${Number(result.comparisons_undone || 1)} comparison${Number(result.comparisons_undone || 1) === 1 ? '' : 's'}`);
                }
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
        return findElementInAdjacentVisualRow(cells, currentIdx, direction);
    }

    function findElementInAdjacentVisualRow(elements, currentIdx, direction) {
        const current = elements[currentIdx];
        if (!current) return currentIdx;

        const boxes = Array.from(elements, (el, index) => {
            const rect = el.getBoundingClientRect();
            return {
                index,
                centerX: rect.left + rect.width / 2,
                centerY: rect.top + rect.height / 2,
                height: rect.height,
            };
        });
        const currentBox = boxes[currentIdx];
        const sameRowTolerance = Math.max(2, Math.min(12, currentBox.height * 0.08));
        const targetRowTolerance = Math.max(4, Math.min(32, currentBox.height * 0.25));
        let targetRowDelta = Infinity;

        for (const box of boxes) {
            if (box.index === currentIdx) continue;
            const rowDelta = (box.centerY - currentBox.centerY) * direction;
            if (rowDelta <= sameRowTolerance) continue;
            targetRowDelta = Math.min(targetRowDelta, rowDelta);
        }

        if (!Number.isFinite(targetRowDelta)) return currentIdx;

        let best = currentIdx;
        let bestDist = Infinity;
        for (const box of boxes) {
            if (box.index === currentIdx) continue;
            const rowDelta = (box.centerY - currentBox.centerY) * direction;
            if (Math.abs(rowDelta - targetRowDelta) > targetRowTolerance) continue;
            const dist = Math.abs(box.centerX - currentBox.centerX);
            if (dist < bestDist) {
                bestDist = dist;
                best = box.index;
            }
        }
        return best;
    }

    function setCompareMode(mode) {
        clearWarmups();
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
        const visibleContainer = [abContainer, mosaicContainer].find(el => el && !el.classList.contains('hidden'));
        const transitionToken = ++compareModeTransitionToken;

        if (visibleContainer) visibleContainer.classList.add('fading');

        setTimeout(() => {
            if (transitionToken !== compareModeTransitionToken) return;
            if (mode === 'mosaic') {
                compareImageToken++;
                if (abContainer) abContainer.classList.add('hidden');
                if (mosaicContainer) {
                    mosaicContainer.classList.remove('hidden');
                    mosaicContainer.classList.remove('fading');
                }
                if (strategies) strategies.classList.remove('hidden');
                if (hints) hints.classList.add('hidden');
                loadMosaicBatch();
            } else {
                if (abContainer) {
                    abContainer.classList.remove('hidden');
                    abContainer.classList.remove('fading');
                }
                if (mosaicContainer) mosaicContainer.classList.add('hidden');
                if (strategies) strategies.classList.add('hidden');
                if (hints) hints.classList.remove('hidden');
                comparePairs = [];
                compareIndex = 0;
                fetchComparePairs().then(() => showComparePair());
            }
        }, 150);
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
    let lastDateGroup = null;
    let dateGroupsData = [];
    let dateScrubberGeneration = 0;
    let dateJumpGeneration = 0;
    let dateScrubberScrollRoot = null;
    let dateScrubberScrollHandler = null;
    let dateScrubberScrollRaf = null;
    let searchQuery = '';
    let searchDebounce = null;
    let rankingsLoading = false;
    let rankingsLoadPromise = null;
    let rankingsExhausted = false;
    let libraryRequestGeneration = 0;
    let thumbHeight = 220;
    let libraryImages = [];
    let lightboxIndex = -1;
    let loupeStandaloneImage = null;
    const FILTER_STORAGE_KEY = 'pa_filters';
    const SORT_STORAGE_KEY = 'pa_sort';
    const SEARCH_STORAGE_KEY = 'pa_search_query';
    const SEARCH_SORT_STORAGE_KEY = 'pa_search_sort';
    const EMPTY_FILTERS = {
        orientation: '',
        compared: '',
        rating: '',
        folder: '',
        flag: '',
        taken: '',
        fileType: '',
        camera: '',
        lens: '',
    };
    let filters = { ...EMPTY_FILTERS };

    function saveFilters() {
        try { sessionStorage.setItem(FILTER_STORAGE_KEY, JSON.stringify(currentFilterState())); } catch {}
    }

    function restoreFilters() {
        try {
            const saved = sessionStorage.getItem(FILTER_STORAGE_KEY);
            if (!saved) return;
            const parsed = JSON.parse(saved);
            filters = normalizeFilterState(parsed);

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
            if (filters.taken) {
                const sel = document.getElementById('filter-taken');
                if (sel) sel.value = filters.taken;
            }
            if (filters.fileType) {
                const sel = document.getElementById('filter-type');
                if (sel) sel.value = filters.fileType;
            }
            if (filters.camera) {
                const sel = document.getElementById('filter-camera');
                if (sel) sel.value = filters.camera;
            }
            if (filters.lens) {
                const sel = document.getElementById('filter-lens');
                if (sel) sel.value = filters.lens;
            }
            if (filters.flag) {
                document.querySelectorAll('.filter-flag').forEach(btn => {
                    btn.classList.toggle('active', btn.dataset.flag === filters.flag);
                });
            }
            updateMetadataFilterButton();
        } catch {}
    }

    function activeMetadataFilterCount() {
        return ['taken', 'fileType', 'camera', 'lens'].filter(key => Boolean(filters[key])).length;
    }

    function updateMetadataFilterButton() {
        const btn = document.getElementById('metadata-filter-btn');
        if (!btn) return;
        const count = activeMetadataFilterCount();
        btn.textContent = count ? `Metadata (${count})` : 'Metadata';
        btn.classList.toggle('active', count > 0);
    }

    function toggleMetadataFilters() {
        const panel = document.getElementById('metadata-filter-panel');
        const btn = document.getElementById('metadata-filter-btn');
        if (!panel) return;
        panel.classList.toggle('hidden');
        if (btn) btn.setAttribute('aria-expanded', panel.classList.contains('hidden') ? 'false' : 'true');
    }

    function normalizeFilterState(state = {}) {
        return {
            orientation: state.orientation || '',
            compared: state.compared || '',
            rating: state.rating || '',
            folder: state.folder || '',
            flag: state.flag || '',
            taken: state.taken || '',
            fileType: state.fileType || '',
            camera: state.camera || '',
            lens: state.lens || '',
        };
    }

    function currentFilterState() {
        return normalizeFilterState(filters);
    }

    function appendFilterParams(params, state = currentFilterState()) {
        const normalized = normalizeFilterState(state);
        if (normalized.orientation) params.set('orientation', normalized.orientation);
        if (normalized.compared) params.set('compared', normalized.compared);
        if (normalized.rating) params.set('min_stars', normalized.rating);
        if (normalized.folder) params.set('folder', normalized.folder);
        if (normalized.flag) params.set('flag', normalized.flag);
        if (normalized.taken) params.set('date_taken', normalized.taken);
        if (normalized.fileType) params.set('file_type', normalized.fileType);
        if (normalized.camera) params.set('camera', normalized.camera);
        if (normalized.lens) params.set('lens', normalized.lens);
        return params;
    }

    function filterParams(state = currentQueryState()) {
        const filtersOnly = state.filters ? state.filters : state;
        const params = appendFilterParams(new URLSearchParams(), filtersOnly);
        const query = params.toString();
        return query ? `&${query}` : '';
    }

    function filterQueryString(state = currentQueryState()) {
        const filtersOnly = state.filters ? state.filters : state;
        return appendFilterParams(new URLSearchParams(), filtersOnly).toString();
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

        if (baseState.taken) {
            pushState({ taken: '' });
        }

        if (baseState.fileType) {
            pushState({ fileType: '' });
        }

        if (baseState.camera) {
            pushState({ camera: '' });
        }

        if (baseState.lens) {
            pushState({ lens: '' });
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

    function buildRankingsUrl({
        queryState = currentQueryState(),
        sort = queryState.sort || rankingsSort,
        filterState = null,
        limit = LIBRARY_NEIGHBOR_LIMIT,
        offset = 0,
    } = {}) {
        const state = currentQueryState({
            ...queryState,
            filters: filterState || queryState.filters,
            sort,
        });
        return `/api/rankings?${rankingQueryString({ queryState: state, sort, limit, offset })}`;
    }

    function buildMosaicUrl({
        strategy = mosaicStrategy,
        queryState = currentQueryState(),
        filterState = null,
        gridElo = mosaicGridElo(),
        n = MOSAIC_NEIGHBOR_LIMIT,
        exclude = '',
    } = {}) {
        const state = currentQueryState({
            ...queryState,
            filters: filterState || queryState.filters,
        });
        const params = new URLSearchParams();
        params.set('n', String(n));
        params.set('strategy', strategy);
        params.set('grid_elo', String(gridElo));
        appendFilterParams(params, state.filters);
        if (state.searchMode === 'search' && state.searchQuery) params.set('q', state.searchQuery);
        if (exclude) params.set('exclude', exclude);
        return `/api/mosaic/next?${params.toString()}`;
    }

    function buildCompareUrl(mode, n = COMPARE_NEIGHBOR_PAIRS, queryState = currentQueryState()) {
        const state = currentQueryState(queryState);
        const params = new URLSearchParams();
        params.set('n', String(n));
        params.set('mode', mode);
        appendFilterParams(params, state.filters);
        if (state.searchMode === 'search' && state.searchQuery) params.set('q', state.searchQuery);
        return `/api/compare/next?${params.toString()}`;
    }

    function currentLibraryPageSize() {
        return rankingsOffset === 0 ? INITIAL_RANKINGS_PAGE_SIZE : RANKINGS_PAGE_SIZE;
    }

    function resetLibraryResults({ clearBatch = false } = {}) {
        clearWarmups();
        libraryRequestGeneration++;
        rankingsOffset = 0;
        rankingsExhausted = false;
        libraryImages = [];
        lastDateGroup = null;
        rankingsLoading = false;
        rankingsLoadPromise = null;
        selectedLibraryIndex = -1;
        hideLibraryEmptyState();
        if (clearBatch) clearBatchSelection();
    }

    function hasActiveLibraryFilters() {
        return Object.values(currentFilterState()).some(Boolean);
    }

    function hideLibraryEmptyState() {
        document.getElementById('library-empty')?.classList.add('hidden');
    }

    function updateLibraryEmptyState() {
        const empty = document.getElementById('library-empty');
        const text = document.getElementById('library-empty-text');
        const action = document.getElementById('library-empty-action');
        if (!empty || !text || !action) return;
        if (libraryImages.length > 0) {
            empty.classList.add('hidden');
            return;
        }

        empty.classList.remove('hidden');
        if (hasActiveTextSearch(searchQuery)) {
            text.textContent = `No results for '${searchQuery}'`;
            action.textContent = 'Clear Search';
            action.onclick = () => clearSearch();
        } else if (hasActiveLibraryFilters()) {
            text.textContent = 'No photos match these filters';
            action.textContent = 'Clear Filters';
            action.onclick = () => clearLibraryFilters();
        } else {
            text.textContent = 'No photos in your catalog yet';
            action.textContent = 'Scan Folder';
            action.onclick = () => { window.location.href = '/catalog'; };
        }
    }

    function libraryScrollRoot() {
        return document.querySelector('.library-container');
    }

    function scrollLibraryContainerToElement(el, behavior = 'smooth') {
        const root = libraryScrollRoot();
        if (!root || !el) {
            el?.scrollIntoView({ behavior, block: 'start' });
            return;
        }
        const rootRect = root.getBoundingClientRect();
        const rect = el.getBoundingClientRect();
        const top = root.scrollTop + rect.top - rootRect.top;
        root.scrollTo({ top: Math.max(0, top - 4), behavior });
    }

    function syncDateScrubberVisibility() {
        const scrubber = document.getElementById('date-scrubber');
        const active = Boolean(scrubber) && libraryView !== 'map' && isDateScrubberActive();
        document.body.classList.toggle('date-scrubber-active', active);
        if (scrubber) scrubber.classList.toggle('hidden', !active);
    }

    function scheduleCrossViewWarmup(fromView) {
        if (fromView === 'compare') {
            const libraryUrl = buildRankingsUrl({
                sort: 'elo',
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
                (token, generation) => warmRequests('crossview-library', token, generation, requests),
                CROSS_VIEW_WARM_DELAY_MS,
            );
        } else if (fromView === 'library') {
            const compareUrl = buildMosaicUrl({
                strategy: 'explore',
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
                (token, generation) => warmRequests('crossview-compare', token, generation, requests),
                CROSS_VIEW_WARM_DELAY_MS,
            );
        }
    }

    function scheduleLibraryNeighborWarmup() {
        if (searchQuery || rankingsExhausted) return;
        const nextUrl = buildRankingsUrl({
            queryState: currentQueryState({ sort: rankingsSort }),
            sort: rankingsSort,
            limit: RANKINGS_PAGE_SIZE,
            offset: rankingsOffset,
        });
        const requests = [{ url: nextUrl, cacheKey: `library:${nextUrl}`, extract: imageThumbUrls }];

        scheduleBackgroundWarm(
            'library-next-page',
            (token, generation) => warmRequests('library-next-page', token, generation, requests),
        );
    }

    function scheduleCompareNeighborWarmup(mode = compareMode) {
        if (mode === 'mosaic') return;
        const requests = [];
        requests.push({
            url: buildCompareUrl(mode, COMPARE_NEIGHBOR_PAIRS),
            extract: compareThumbUrls,
        });

        scheduleBackgroundWarm(
            'compare-next-pairs',
            (token, generation) => warmRequests('compare-next-pairs', token, generation, requests),
            350,
        );
    }

    const SORT_KEYS = {
        'elo':         { desc: 'elo',           asc: 'elo_asc', defaultDesc: true },
        'comparisons': { desc: 'comparisons',   asc: 'least_compared', defaultDesc: true },
        'date_taken':  { desc: 'date_taken',    asc: 'date_taken_asc', defaultDesc: true },
        'date_modified': { desc: 'date_modified', asc: 'date_modified_asc', defaultDesc: true },
        'file_size':   { desc: 'file_size',     asc: 'file_size_asc', defaultDesc: true },
        'resolution':  { desc: 'resolution',    asc: 'resolution_asc', defaultDesc: true },
        'camera':      { desc: 'camera_desc',   asc: 'camera', defaultDesc: false },
        'filename':    { desc: 'filename_desc', asc: 'filename', defaultDesc: false },
        'similarity':  { desc: 'similarity',    asc: 'similarity', defaultDesc: true },
    };

    function sortValueForState(field = sortField, desc = sortDesc) {
        const key = SORT_KEYS[field];
        return key ? (desc ? key.desc : key.asc) : field;
    }

    function sortStateFromValue(sort) {
        for (const [field, key] of Object.entries(SORT_KEYS)) {
            if (sort === key.desc) return { field, desc: true };
            if (sort === key.asc) return { field, desc: false };
        }
        return null;
    }

    function hasActiveTextSearch(value = searchQuery) {
        return Boolean(value && value !== '__similar__');
    }

    function currentSearchMode() {
        if (searchQuery === '__similar__') return 'similar';
        return searchQuery ? 'search' : 'library';
    }

    function currentQueryState(overrides = {}) {
        const field = overrides.sortField || sortField;
        const desc = overrides.sortDesc ?? sortDesc;
        const mode = overrides.searchMode || currentSearchMode();
        const query = overrides.searchQuery ?? (mode === 'search' ? searchQuery : '');
        return {
            filters: normalizeFilterState(overrides.filters || overrides.filterState || filters),
            sortField: field,
            sortDesc: Boolean(desc),
            sort: overrides.sort || sortValueForState(field, desc),
            searchMode: mode,
            searchQuery: query,
        };
    }

    function applySortState(field, desc, { persist = true, persistSearch = true } = {}) {
        if (!SORT_KEYS[field]) return;
        sortField = field;
        sortDesc = Boolean(desc);
        rankingsSort = sortValueForState(sortField, sortDesc);
        syncSortControls();
        if (persist && sortField !== 'similarity') saveSortState();
        if (persistSearch && hasActiveTextSearch()) saveSearchSortState();
    }

    function saveSortState() {
        try {
            sessionStorage.setItem(SORT_STORAGE_KEY, JSON.stringify({ field: sortField, desc: sortDesc }));
        } catch {}
    }

    function saveSearchState() {
        try {
            if (hasActiveTextSearch()) {
                sessionStorage.setItem(SEARCH_STORAGE_KEY, searchQuery);
            } else {
                sessionStorage.removeItem(SEARCH_STORAGE_KEY);
            }
        } catch {}
    }

    function saveSearchSortState() {
        try {
            if (hasActiveTextSearch()) {
                sessionStorage.setItem(SEARCH_SORT_STORAGE_KEY, JSON.stringify({ field: sortField, desc: sortDesc }));
            } else {
                sessionStorage.removeItem(SEARCH_SORT_STORAGE_KEY);
            }
        } catch {}
    }

    function clearPersistedSearchState() {
        try {
            sessionStorage.removeItem(SEARCH_STORAGE_KEY);
            sessionStorage.removeItem(SEARCH_SORT_STORAGE_KEY);
        } catch {}
    }

    function restoreSortState() {
        try {
            const saved = sessionStorage.getItem(SORT_STORAGE_KEY);
            if (!saved) return;
            const parsed = JSON.parse(saved);
            const field = SORT_KEYS[parsed?.field] && parsed.field !== 'similarity' ? parsed.field : 'elo';
            applySortState(field, parsed?.desc !== false, { persist: false, persistSearch: false });
        } catch {}
    }

    function restoreSearchSortState() {
        try {
            const saved = sessionStorage.getItem(SEARCH_SORT_STORAGE_KEY);
            if (!saved) return null;
            const parsed = JSON.parse(saved);
            const field = SORT_KEYS[parsed?.field] ? parsed.field : '';
            if (!field) return null;
            return { field, desc: parsed?.desc !== false };
        } catch {
            return null;
        }
        return null;
    }

    function restoreSearchState() {
        try {
            const saved = (sessionStorage.getItem(SEARCH_STORAGE_KEY) || '').trim();
            searchQuery = saved;
        } catch {
            searchQuery = '';
        }
        updateSimilaritySortOption();
        if (hasActiveTextSearch()) {
            const restoredSort = restoreSearchSortState() || { field: 'similarity', desc: true };
            applySortState(restoredSort.field, restoredSort.desc, { persist: false });
        }
        updateSearchControls();
    }

    function updateSearchControls() {
        const input = document.getElementById('search-input');
        const clearBtn = document.getElementById('search-clear');
        if (input && searchQuery !== '__similar__') input.value = searchQuery;
        if (clearBtn) clearBtn.classList.toggle('hidden', !searchQuery);
        updateSimilaritySortOption();
        syncSortControls();
        updateCompareSearchIndicator();
        updateSortDirIcon();
    }

    function updateCompareSearchIndicator() {
        const chip = document.getElementById('compare-search-active');
        if (!chip) return;
        const queryEl = document.getElementById('compare-search-query');
        const active = hasActiveTextSearch();
        chip.classList.toggle('hidden', !active);
        if (queryEl) queryEl.textContent = active ? searchQuery : '';
        updateBottomBarHeightVar();
    }

    function syncSortControls() {
        const select = document.getElementById('sort-field');
        if (select && select.querySelector(`option[value="${sortField}"]`)) {
            select.value = sortField;
        }
        updateSortDirIcon();
    }

    function rankingQueryString({
        queryState = currentQueryState(),
        limit = LIBRARY_NEIGHBOR_LIMIT,
        offset = 0,
        sort = queryState.sort,
    } = {}) {
        const state = currentQueryState({ ...queryState, sort });
        const params = new URLSearchParams();
        params.set('limit', String(limit));
        params.set('offset', String(offset));
        params.set('sort', sort || state.sort);
        appendFilterParams(params, state.filters);
        if (state.searchMode === 'search' && state.searchQuery) {
            params.set('q', state.searchQuery);
        }
        return params.toString();
    }

    async function initLibrary() {
        initBottomBarMeasurement();
        resetLibraryResults();
        restoreFilters();
        restoreSortState();
        restoreSearchState();
        loadUiSettings();

        // Fire all init requests in parallel — don't block on rankings
        const rankingsPromise = loadRankings();
        const statsPromise = fetch('/api/stats').then(r => r.json()).then(stats => {
            compareStats = stats;
            updateCompareProgress();
        }).catch(() => {});

        loadFolderList();
        loadFilterOptions();
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
            if (entries[0].isIntersecting && libraryView === 'grid' && !rankingsLoading && !rankingsExhausted) {
                loadRankings();
            }
        }, { root: libraryScrollRoot(), rootMargin: '600px 0px' });
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
                if (batchSelected.size > 0) batchFlag('picked');
                else setCurrentLibraryFlag('picked');
            } else if (e.key.toLowerCase() === 'x') {
                e.preventDefault();
                if (batchSelected.size > 0) batchFlag('rejected');
                else setCurrentLibraryFlag('rejected');
            } else if (e.key.toLowerCase() === 'u') {
                e.preventDefault();
                if (batchSelected.size > 0) batchFlag('unflagged');
                else setCurrentLibraryFlag('unflagged');
            } else if (e.key === 'Escape') {
                e.preventDefault();
                if (batchSelected.size > 0) clearBatchSelection();
                else if (selectedLibraryIndex >= 0) deselectLibraryCard(cards);
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
                    const wasSearching = hasActiveTextSearch();
                    searchQuery = e.target.value.trim();
                    if (hasActiveTextSearch()) {
                        saveSearchState();
                        updateSimilaritySortOption();
                        if (!wasSearching) {
                            applySortState('similarity', true, { persist: false });
                        } else {
                            saveSearchSortState();
                        }
                    } else {
                        clearPersistedSearchState();
                        if (sortField === 'similarity') {
                            restoreSortState();
                            if (sortField === 'similarity') {
                                applySortState('elo', true, { persist: false, persistSearch: false });
                            }
                        }
                    }
                    updateSearchControls();
                    // Search is a filter — reload rankings with the query
                    resetLibraryResults({ clearBatch: true });
                    loadRankings(true);
                    updateDateScrubber();
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
        if (hasActiveTextSearch()) {
            if (!opt) {
                opt = document.createElement('option');
                opt.value = 'similarity';
                opt.textContent = 'Similarity';
                select.appendChild(opt);
            }
        } else {
            if (opt) {
                opt.remove();
            }
        }
    }

    function clearSearch() {
        const wasSimilaritySort = sortField === 'similarity';
        searchQuery = '';
        clearPersistedSearchState();
        if (wasSimilaritySort) {
            restoreSortState();
            if (sortField === 'similarity') {
                applySortState('elo', true, { persist: false, persistSearch: false });
            }
        }
        const input = document.getElementById('search-input');
        if (input) input.value = '';
        const sortToggles = document.getElementById('sort-toggles');
        if (sortToggles) sortToggles.style.opacity = '';
        updateSearchControls();
        reloadForFilters();
    }

    function setRankingsSort(sort, { persist = true } = {}) {
        rankingsSort = sort;
        const state = sortStateFromValue(sort);
        if (state) {
            applySortState(state.field, state.desc, { persist });
        }
        resetLibraryResults({ clearBatch: true });
        dateGroupsData = [];
        loadRankings(true);  // true = clear grid before appending
        updateDateScrubber();
    }

    function setSortField(field) {
        if (!SORT_KEYS[field]) return;
        const key = SORT_KEYS[field];
        applySortState(field, key?.defaultDesc !== false, { persist: field !== 'similarity' });
        resetLibraryResults({ clearBatch: true });
        dateGroupsData = [];
        loadRankings(true);
        updateDateScrubber();
    }

    function toggleSortDir() {
        if (sortField === 'similarity') return;
        applySortState(sortField, !sortDesc);
        resetLibraryResults({ clearBatch: true });
        dateGroupsData = [];
        loadRankings(true);
        updateDateScrubber();
    }

    function updateSortDirIcon() {
        const btn = document.getElementById('sort-dir-btn');
        if (btn) btn.classList.toggle('active', !sortDesc);
    }

    function parseMetadataDate(value) {
        if (!value) return null;
        if (typeof value === 'number') {
            const millis = value > 100000000000 ? value : value * 1000;
            const parsed = new Date(millis);
            return Number.isNaN(parsed.getTime()) ? null : parsed;
        }
        const normalized = String(value).trim().replace(' ', 'T');
        const parsed = new Date(normalized);
        return Number.isNaN(parsed.getTime()) ? null : parsed;
    }

    function formatShortDate(value) {
        const date = parseMetadataDate(value);
        if (!date) return value || '';
        return date.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
    }

    function formatDateTime(value) {
        const date = parseMetadataDate(value);
        if (!date) return value || '';
        return date.toLocaleString(undefined, {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
        });
    }

    function cameraLabel(img) {
        return [img.camera_make, img.camera_model].filter(Boolean).join(' ').trim();
    }

    function resolutionLabel(img) {
        const width = Number(img.width || 0);
        const height = Number(img.height || 0);
        if (!width || !height) return '';
        const megapixels = (width * height) / 1000000;
        return `${width} x ${height} (${megapixels.toFixed(megapixels >= 10 ? 0 : 1)} MP)`;
    }

    function imageAspectRatio(img) {
        const ar = Number(img?.aspect_ratio || 0);
        if (ar > 0) return ar;
        const width = Number(img?.width || 0);
        const height = Number(img?.height || 0);
        if (width > 0 && height > 0) return width / height;
        return 1.5;
    }

    function imageMetadataTitle(img) {
        const parts = [img.filename];
        const camera = cameraLabel(img);
        if (camera) parts.push(camera);
        if (img.lens) parts.push(img.lens);
        if (img.date_taken) parts.push(`Taken ${formatDateTime(img.date_taken)}`);
        const resolution = resolutionLabel(img);
        if (resolution) parts.push(resolution);
        if (img.file_size) parts.push(formatBytes(img.file_size));
        if (img.file_ext) parts.push(img.file_ext.toUpperCase());
        return parts.filter(Boolean).join('\n');
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

    function similarityLabel(img) {
        const similarity = Number(img?.similarity);
        return Number.isFinite(similarity) ? `${(similarity * 100).toFixed(0)}% match` : 'metadata match';
    }

    function updateImageFlagLocal(imageId, flag) {
        for (const img of libraryImages) {
            if (img.id === imageId) img.flag = flag;
        }
        if (loupeStandaloneImage?.id === imageId) loupeStandaloneImage.flag = flag;
        if (loupeCurrentImage?.id === imageId) loupeCurrentImage.flag = flag;

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

        if (
            (lightboxIndex >= 0 && libraryImages[lightboxIndex]?.id === imageId)
            || (lightboxIndex < 0 && loupeStandaloneImage?.id === imageId)
        ) {
            updateLoupeFlagDisplay(flag);
        }
    }

    async function setImageFlag(imageId, flag) {
        if (!imageId || !['picked', 'unflagged', 'rejected'].includes(flag)) return;
        const img = libraryImages.find(item => item.id === imageId);
        const previousFlag = img?.flag || 'unflagged';
        updateImageFlagLocal(imageId, flag);
        try {
            const res = await fetch(`/api/image/${imageId}/flag`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ flag }),
            });
            if (!res.ok) throw new Error('flag update failed');
        } catch {
            updateImageFlagLocal(imageId, previousFlag);
            showToast('Flag update failed');
        }
    }

    function setCurrentLibraryFlag(flag) {
        const loupe = document.getElementById('loupe');
        const loupeOpen = loupe && !loupe.classList.contains('hidden');
        const img = loupeOpen
            ? (libraryImages[lightboxIndex] || loupeStandaloneImage)
            : libraryImages[selectedLibraryIndex];
        if (!img) return;
        setImageFlag(img.id, flag);
    }

    async function loadRankings(clearFirst = false) {
        if (rankingsLoading) return rankingsLoadPromise || 0;
        const promise = loadRankingsBatch(clearFirst);
        rankingsLoadPromise = promise;
        try {
            return await promise;
        } finally {
            if (rankingsLoadPromise === promise) rankingsLoadPromise = null;
        }
    }

    async function loadRankingsBatch(clearFirst = false) {
        rankingsLoading = true;
        const requestGeneration = libraryRequestGeneration;
        const requestOffset = rankingsOffset;
        const limit = currentLibraryPageSize();
        const url = buildRankingsUrl({
            queryState: currentQueryState({ sort: rankingsSort }),
            limit,
            offset: requestOffset,
            sort: rankingsSort,
        });
        try {
            const data = (requestOffset === 0 ? takeWarmCache(`library:${url}`) : null) || await fetchWarmJson(url);
            if (!data) return 0;
            if (requestGeneration !== libraryRequestGeneration) return 0;
            if (requestOffset === 0 && typeof data.total_images === 'number') {
                const visible = Number(data.visible_images ?? data.total_images ?? 0);
                const total = Number(data.total_images ?? data.total_kept ?? visible);
                compareStats = {
                    ...compareStats,
                    filtered_pool: visible,
                    filtered_pool_visible: visible,
                    filtered_pool_total: total,
                };
                updateCompareProgress();
            }
            const grid = document.getElementById('rankings-grid');
            if (clearFirst) { grid.innerHTML = ''; selectedLibraryIndex = -1; lastDateGroup = null; }
            const showRank = (rankingsSort === 'elo' || rankingsSort === 'elo_asc');
            const isDateSort = (rankingsSort === 'date_taken' || rankingsSort === 'date_taken_asc');
            const rowH = thumbHeight;

            // Batch DOM writes with DocumentFragment to avoid per-card reflows
            const frag = document.createDocumentFragment();
            const baseIndex = libraryImages.length;
            for (let i = 0; i < data.images.length; i++) {
                const img = data.images[i];
                const rank = rankingsOffset + i + 1;
                const ar = img.aspect_ratio || 1.5;
                const tier = getTierClass(img.elo, img.comparisons);
                const conf = img.comparisons > 0 ? getConfidenceClass(img.comparisons) : '';

                // Insert date group header when group changes
                if (isDateSort) {
                    const group = img.date_group || '';
                    if (group !== lastDateGroup) {
                        lastDateGroup = group;
                        const header = document.createElement('div');
                        header.className = 'date-group-header';
                        header.dataset.dateGroup = group;
                        header.textContent = group ? formatDateGroup(group) : 'No Date';
                        frag.appendChild(header);
                    }
                }

                const card = document.createElement('div');
                card.className = 'rank-card' + (tier ? ' ' + tier : '') + (flagClass(img.flag) ? ' ' + flagClass(img.flag) : '');
                if (batchMode) card.classList.add('selectable');
                if (batchSelected.has(img.id)) card.classList.add('selected');
                card.dataset.imageId = img.id;
                card.dataset.ar = ar;
                card.style.height = rowH + 'px';
                card.style.flexGrow = ar;
                card.style.flexBasis = (rowH * ar) + 'px';
                card.title = imageMetadataTitle(img);
                card.onclick = (e) => handleCardClick(e, img, card, baseIndex + i);

                const confDot = conf ? `<div class="rank-confidence ${conf}"></div>` : '';
                const infoLine = libraryCardInfoLine(img, rank, showRank);

                card.innerHTML = `
                    <img src="${escapeHtml(img.thumb_url)}" alt="${escapeHtml(img.filename)}" loading="lazy" onload="this.classList.add('loaded')">
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
            if (data.images.length < limit) {
                rankingsExhausted = true;
            }
            if (data.images.length > 0) {
                // Always warm neighbors — not just on first load
                scheduleLibraryNeighborWarmup();
                if (requestOffset === 0) scheduleCrossViewWarmup('library');
                if (isDateScrubberActive()) setupScrubberScrollObserver();
            }
            updateLibraryEmptyState();
            return data.images.length;
        } finally {
            if (requestGeneration === libraryRequestGeneration) rankingsLoading = false;
        }
    }

    function libraryCardInfoLine(img, rank, showRank) {
        if (showRank) {
            return `<span class="rank-number">#${rank}</span><span class="rank-elo">${escapeHtml(img.elo)}</span>`;
        }
        if (rankingsSort === 'date_taken' || rankingsSort === 'date_taken_asc') {
            const label = formatShortDate(img.date_taken) || 'No date';
            return `<span class="rank-elo">${escapeHtml(img.elo)}</span><span class="rank-comparisons">${escapeHtml(label)}</span>`;
        }
        if (rankingsSort === 'file_size' || rankingsSort === 'file_size_asc') {
            return `<span class="rank-elo">${escapeHtml(img.elo)}</span><span class="rank-comparisons">${escapeHtml(formatBytes(img.file_size))}</span>`;
        }
        if (rankingsSort === 'date_modified' || rankingsSort === 'date_modified_asc') {
            const label = formatShortDate(img.file_modified_at) || 'No modified date';
            return `<span class="rank-elo">${escapeHtml(img.elo)}</span><span class="rank-comparisons">${escapeHtml(label)}</span>`;
        }
        if (rankingsSort === 'resolution' || rankingsSort === 'resolution_asc') {
            const label = resolutionLabel(img) || 'No size';
            return `<span class="rank-elo">${escapeHtml(img.elo)}</span><span class="rank-comparisons">${escapeHtml(label)}</span>`;
        }
        if (rankingsSort === 'camera' || rankingsSort === 'camera_desc') {
            const label = cameraLabel(img) || 'No camera';
            return `<span class="rank-elo">${escapeHtml(img.elo)}</span><span class="rank-comparisons">${escapeHtml(label)}</span>`;
        }
        if (rankingsSort === 'newest' || rankingsSort === 'oldest') {
            const label = formatShortDate(img.created_at) || 'Added';
            return `<span class="rank-elo">${escapeHtml(img.elo)}</span><span class="rank-comparisons">${escapeHtml(label)}</span>`;
        }
        return `<span class="rank-elo">${escapeHtml(img.elo)}</span><span class="rank-comparisons">${escapeHtml(img.comparisons)} signal${Number(img.comparisons || 0) === 1 ? '' : 's'}</span>`;
    }

    function formatDateGroup(dateStr) {
        // dateStr is "YYYY-MM" format
        try {
            const [year, month] = dateStr.split('-');
            const date = new Date(parseInt(year), parseInt(month) - 1);
            return date.toLocaleDateString(undefined, { year: 'numeric', month: 'long' });
        } catch {
            return dateStr;
        }
    }

    function isDateSortActive() {
        return rankingsSort === 'date_taken' || rankingsSort === 'date_taken_asc';
    }

    function isDateScrubberActive() {
        return isDateSortActive() && currentSearchMode() === 'library';
    }

    function findDateGroupHeader(group) {
        return Array.from(document.querySelectorAll('.date-group-header'))
            .find(header => header.dataset.dateGroup === group);
    }

    function dateGroupOffset(group) {
        let offset = 0;
        for (const item of dateGroupsData) {
            const itemGroup = item.date || '';
            if (itemGroup === group) return offset;
            offset += Number(item.count || 0);
        }
        return null;
    }

    function setActiveDateScrubberGroup(group) {
        const year = group ? group.substring(0, 4) : '';
        document.querySelectorAll('.scrubber-label, .scrubber-year').forEach(el => {
            const elGroup = el.dataset.dateGroup || '';
            const isYear = el.classList.contains('scrubber-year');
            const active = isYear && year
                ? elGroup.substring(0, 4) === year
                : elGroup === group;
            el.classList.toggle('active', active);
        });
    }

    function updateActiveDateScrubberGroup() {
        const root = libraryScrollRoot();
        const headers = Array.from(document.querySelectorAll('.date-group-header'));
        if (!root || !headers.length) return;

        const currentTop = root.scrollTop + 8;
        let activeHeader = headers[0];
        for (const header of headers) {
            if (header.offsetTop <= currentTop) activeHeader = header;
            else break;
        }
        setActiveDateScrubberGroup(activeHeader?.dataset.dateGroup || '');
    }

    function teardownDateScrubberScrollTracking() {
        if (dateScrubberScrollRoot && dateScrubberScrollHandler) {
            dateScrubberScrollRoot.removeEventListener('scroll', dateScrubberScrollHandler);
        }
        if (dateScrubberScrollRaf) cancelAnimationFrame(dateScrubberScrollRaf);
        dateScrubberScrollRoot = null;
        dateScrubberScrollHandler = null;
        dateScrubberScrollRaf = null;
    }

    function setupDateScrubberScrollTracking() {
        teardownDateScrubberScrollTracking();
        const root = libraryScrollRoot();
        const headers = document.querySelectorAll('.date-group-header');
        if (!root || !headers.length) return;

        dateScrubberScrollRoot = root;
        dateScrubberScrollHandler = () => {
            if (dateScrubberScrollRaf) return;
            dateScrubberScrollRaf = requestAnimationFrame(() => {
                dateScrubberScrollRaf = null;
                updateActiveDateScrubberGroup();
            });
        };
        root.addEventListener('scroll', dateScrubberScrollHandler, { passive: true });
        updateActiveDateScrubberGroup();
    }

    async function jumpToDateGroup(group) {
        if (!isDateScrubberActive()) return;

        const existingHeader = findDateGroupHeader(group);
        if (existingHeader) {
            setActiveDateScrubberGroup(group);
            scrollLibraryContainerToElement(existingHeader);
            return;
        }

        const offset = dateGroupOffset(group);
        if (offset === null) return;

        const gen = ++dateJumpGeneration;
        libraryRequestGeneration++;
        rankingsOffset = offset;
        rankingsExhausted = false;
        libraryImages = [];
        lastDateGroup = null;
        rankingsLoading = false;
        rankingsLoadPromise = null;
        selectedLibraryIndex = -1;

        const grid = document.getElementById('rankings-grid');
        if (grid) grid.innerHTML = '';
        libraryScrollRoot()?.scrollTo({ top: 0, behavior: 'auto' });

        await loadRankings(true);
        if (gen !== dateJumpGeneration) return;

        const loadedHeader = findDateGroupHeader(group);
        if (loadedHeader) {
            scrollLibraryContainerToElement(loadedHeader, 'auto');
            setActiveDateScrubberGroup(group);
        }
    }

    async function updateDateScrubber() {
        const existing = document.getElementById('date-scrubber');
        if (!isDateScrubberActive()) {
            if (existing) existing.remove();
            if (window._scrubberObserver) window._scrubberObserver.disconnect();
            teardownDateScrubberScrollTracking();
            syncDateScrubberVisibility();
            return;
        }
        const gen = ++dateScrubberGeneration;
        const query = filterQueryString(currentQueryState());
        const url = `/api/date-groups${query ? `?${query}` : ''}`;
        try {
            const data = await fetch(url).then(r => r.json());
            if (gen !== dateScrubberGeneration) return;
            dateGroupsData = data.groups || [];
            if (rankingsSort === 'date_taken_asc') dateGroupsData.reverse();
            renderDateScrubber();
        } catch {
            // silently fail
        }
    }

    function renderDateScrubber() {
        let scrubber = document.getElementById('date-scrubber');
        if (!dateGroupsData.length) {
            if (scrubber) scrubber.remove();
            teardownDateScrubberScrollTracking();
            syncDateScrubberVisibility();
            return;
        }
        if (!scrubber) {
            scrubber = document.createElement('div');
            scrubber.id = 'date-scrubber';
            scrubber.className = 'date-scrubber';
        }
        const host = document.querySelector('body.library-shell main') || document.body;
        if (scrubber.parentElement !== host) host.appendChild(scrubber);
        syncDateScrubberVisibility();

        // Build scrubber labels — show years prominently, months smaller
        let currentYear = '';
        let html = '';
        for (const group of dateGroupsData) {
            if (!group.date) {
                html += '<div class="scrubber-label" data-date-group="">No Date</div>';
                continue;
            }
            const year = group.date.substring(0, 4);
            const month = group.date.substring(5, 7);
            const monthName = new Date(parseInt(year), parseInt(month) - 1).toLocaleDateString(undefined, { month: 'short' });
            if (year !== currentYear) {
                currentYear = year;
                html += `<div class="scrubber-year" data-date-group="${escapeHtml(group.date)}">${escapeHtml(year)}</div>`;
            }
            html += `<div class="scrubber-label" data-date-group="${escapeHtml(group.date)}" title="${escapeHtml(group.label)} (${escapeHtml(group.count)})">${escapeHtml(monthName)}</div>`;
        }
        scrubber.innerHTML = html;

        // Click to jump
        scrubber.querySelectorAll('[data-date-group]').forEach(label => {
            label.onclick = () => {
                jumpToDateGroup(label.dataset.dateGroup || '');
            };
        });

        // Scroll observer to highlight current group
        setupScrubberScrollObserver();
    }

    function setupScrubberScrollObserver() {
        if (window._scrubberObserver) window._scrubberObserver.disconnect();
        setupDateScrubberScrollTracking();
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
        const root = libraryScrollRoot();
        if (root) {
            const rootRect = root.getBoundingClientRect();
            if (rect.bottom > rootRect.bottom) {
                root.scrollBy({ top: rect.bottom - rootRect.bottom + 8, behavior: 'smooth' });
            } else if (rect.top < rootRect.top) {
                root.scrollBy({ top: rect.top - rootRect.top - 8, behavior: 'smooth' });
            }
            return;
        }
        if (rect.bottom > window.innerHeight) {
            window.scrollBy({ top: rect.bottom - window.innerHeight + 8, behavior: 'smooth' });
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
        return findElementInAdjacentVisualRow(cards, currentIdx, direction);
    }

    function openLightbox(img) {
        lightboxIndex = libraryImages.findIndex(i => i.id === img.id);
        if (lightboxIndex < 0) {
            openStandaloneLightbox(img);
            return;
        }
        loupeStandaloneImage = null;
        updateFilmstripCounter();
        buildFilmstrip();
        showLoupeImage(libraryImages[lightboxIndex], 0);
    }

    function openStandaloneLightbox(img) {
        loupeStandaloneImage = img;
        lightboxIndex = -1;
        clearFilmstrip();
        showLoupeImage(img, 0);
    }

    let _filmstripBuiltFor = null;  // track which image set the filmstrip was built for
    let _filmstripWindowStart = 0;
    let _filmstripWindowEnd = 0;

    function clearFilmstrip() {
        const scroll = document.getElementById('filmstrip-scroll');
        if (scroll) scroll.innerHTML = '';
        updateFilmstripCounter();
        _filmstripBuiltFor = null;
        _filmstripWindowStart = 0;
        _filmstripWindowEnd = 0;
    }

    function updateFilmstripCounter() {
        const counter = document.getElementById('filmstrip-counter');
        if (!counter) return;
        counter.textContent = lightboxIndex >= 0 && libraryImages.length
            ? `${lightboxIndex + 1} / ${libraryImages.length}`
            : '';
    }

    function buildFilmstrip() {
        const scroll = document.getElementById('filmstrip-scroll');
        if (!scroll) return;
        updateFilmstripCounter();
        if (lightboxIndex < 0) {
            clearFilmstrip();
            return;
        }
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
            thumb.style.aspectRatio = String(imageAspectRatio(img));
            thumb.title = imageMetadataTitle(img);
            thumb.onclick = () => {
                const direction = Math.sign(i - lightboxIndex);
                lightboxIndex = i;
                updateFilmstripCounter();
                showLoupeImage(libraryImages[i], direction);
            };
            const thumbImg = document.createElement('img');
            thumbImg.src = img.thumb_url || '';
            thumbImg.alt = img.filename || '';
            thumbImg.loading = 'lazy';
            thumb.appendChild(thumbImg);
            scroll.appendChild(thumb);
        }
        // rAF needed: DOM just rebuilt, need reflow before reading offsetLeft
        requestAnimationFrame(() => centerFilmstripActive(scroll, true));
    }

    function updateFilmstripActive() {
        const scroll = document.getElementById('filmstrip-scroll');
        if (!scroll) return;
        if (lightboxIndex < 0) return;
        updateFilmstripCounter();
        if (lightboxIndex < _filmstripWindowStart || lightboxIndex >= _filmstripWindowEnd) {
            buildFilmstrip();
            return;
        }
        scroll.querySelectorAll('.filmstrip-thumb').forEach((el) => {
            el.classList.toggle('active', Number(el.dataset.idx) === lightboxIndex);
        });
        // No rAF needed: DOM already laid out, just class toggle
        centerFilmstripActive(scroll);
    }

    function centerFilmstripActive(scroll, instant) {
        const active = scroll.querySelector('.filmstrip-thumb.active');
        if (!active) return;

        const maxScroll = Math.max(0, scroll.scrollWidth - scroll.clientWidth);
        const centeredLeft = active.offsetLeft + (active.offsetWidth / 2) - (scroll.clientWidth / 2);
        const targetLeft = Math.max(0, Math.min(maxScroll, centeredLeft));
        scroll.scrollTo({ left: targetLeft, behavior: instant ? 'auto' : 'smooth' });
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
    let loupeHideTimer = null;
    let loupeCurrentMediaStatus = null;
    const loupeTierProbes = new Set();
    const loupeRefLong = 3840; // lg thumbnail long side used before original dimensions are known
    const LOUPE_PRELOAD_RADIUS = 3;

    function cancelLoupeProbes() {
        for (const probe of Array.from(loupeTierProbes)) {
            probe.cancelled = true;
            if (probe.timer) clearTimeout(probe.timer);
            probe.img.onload = null;
            probe.img.onerror = null;
            probe.img.src = '';
            loupeTierProbes.delete(probe);
            probe.done = true;
            probe.resolveOnce(false);
        }
    }

    function showLoupeImage(img, direction = 0) {
        const loupe = document.getElementById('loupe');
        const loupeImg = document.getElementById('loupe-img');
        if (!loupe || !loupeImg) return;
        if (loupeHideTimer) {
            clearTimeout(loupeHideTimer);
            loupeHideTimer = null;
        }

        // Pre-calculate fit dimensions from aspect ratio so all progressive
        // loads (sm/md/lg) display at the same screen size — no size jumps
        const token = ++loupeImageToken;
        clearWarmups();
        loupeCurrentImage = img;
        loupeFullLoadToken = 0;
        cancelLoupeProbes();
        if (loupeFullLoadTimer) {
            clearTimeout(loupeFullLoadTimer);
            loupeFullLoadTimer = null;
        }
        loupeDisplayedTierRank = -1;
        loupeCurrentMediaStatus = null;
        loupeIsFit = true;
        loupeZoomMode = 'fit';
        loupeImg.onload = null;
        loupeImg.onerror = null;
        loupeImg.style.opacity = '0';
        loupeImg.src = LOUPE_BLANK_SRC;
        const ar = imageAspectRatio(img);
        loupeNatW = ar >= 1 ? loupeRefLong : Math.round(loupeRefLong * ar);
        loupeNatH = ar >= 1 ? Math.round(loupeRefLong / ar) : loupeRefLong;
        loupeImg.style.transition = 'opacity 0.15s';
        loupeApplyImageSize();

        document.body.classList.add('loupe-open');
        if (loupe.classList.contains('hidden')) {
            loupe.classList.remove('hidden');
            requestAnimationFrame(() => loupe.classList.add('loupe-visible'));
        }
        loupeCenterFit({ animate: false });

        // Progressive loading: sm -> md -> lg -> original. Slow tiers time out
        // so the next tier still gets a chance, while late arrivals can still
        // upgrade the image if they are sharper than the current display.
        loupeImg.alt = img.filename || '';
        runLoupeProgressiveLoad(img, token);

        // Populate metadata overlay
        const filenameEl = document.getElementById('loupe-overlay-filename');
        const exifEl = document.getElementById('loupe-overlay-exif');
        const statsEl = document.getElementById('loupe-overlay-stats');
        const tierEl = document.getElementById('loupe-overlay-tier');

        if (filenameEl) filenameEl.textContent = img.filename;
        renderLoupeMetadata(img, exifEl);
        if (tierEl) tierEl.textContent = '';

        const stars = eloToStars(img.elo, img.comparisons);
        const starStr = stars > 0 ? '★'.repeat(stars) + '☆'.repeat(5 - stars) + '  ' : '';
        if (statsEl) statsEl.textContent = `${starStr}${img.elo} Elo · ${img.comparisons} ranking signals`;
        renderLoupeStatusLine(img.flag || 'unflagged');

        updateFilmstripActive();

        preloadLoupeNeighbors(direction);
        warmLoupeHotSet(direction);

        // Load EXIF
        fetch(`/api/image/${img.id}/exif`).then(r => r.json()).then(data => {
            if (!data.exif || !isCurrentLoupeImage(img, token)) return;
            renderLoupeMetadata({ ...img, ...data.exif }, exifEl);
        }).catch(() => {});
    }

    function loupeMetadataParts(e = {}) {
        const parts = [];
        const camera = [e.camera_make, e.camera_model].filter(Boolean).join(' ');
        if (camera) parts.push(camera);
        if (e.date_taken || e.date) parts.push('Taken ' + formatDateTime(e.date_taken || e.date));
        const settings = [];
        if (e.focal_length) settings.push(e.focal_length);
        if (e.focal_length_35mm) settings.push(`${e.focal_length_35mm} equiv`);
        if (e.aperture) settings.push(e.aperture);
        if (e.shutter_speed) settings.push(e.shutter_speed + 's');
        if (e.iso) settings.push('ISO ' + e.iso);
        if (settings.length) parts.push(settings.join('  '));
        if (e.lens) parts.push(e.lens);
        const exposure = [e.exposure_program, e.exposure_bias, e.metering_mode, e.white_balance, e.flash].filter(Boolean);
        if (exposure.length) parts.push(exposure.join('  '));
        const fileBits = [];
        if (e.dimensions) fileBits.push(e.dimensions);
        else {
            const resolution = resolutionLabel(e);
            if (resolution) fileBits.push(resolution);
        }
        if (e.filesize) fileBits.push(e.filesize);
        else if (e.file_size) fileBits.push(formatBytes(e.file_size));
        if (e.file_ext) fileBits.push(String(e.file_ext).replace('.', '').toUpperCase());
        if (fileBits.length) parts.push(fileBits.join('  '));
        if (e.file_modified) parts.push('Modified ' + formatDateTime(e.file_modified));
        else if (e.file_modified_at) parts.push('Modified ' + formatDateTime(e.file_modified_at));
        if (e.filepath) parts.push(e.filepath);
        return parts;
    }

    function renderLoupeMetadata(metadata, exifEl = document.getElementById('loupe-overlay-exif')) {
        if (!exifEl) return;
        exifEl.textContent = loupeMetadataParts(metadata).join('\n');
    }

    function isCurrentLoupeImage(img, token) {
        const current = lightboxIndex >= 0 ? libraryImages[lightboxIndex] : loupeStandaloneImage;
        return token === loupeImageToken && current?.id === img.id;
    }

    async function getMediaStatus(imageId, { force = false } = {}) {
        const cached = mediaStatusCache.get(imageId);
        if (!force && cached && Date.now() - cached.time < MEDIA_STATUS_MAX_AGE_MS) {
            return cached.data;
        }
        if (!force && mediaStatusInflight.has(imageId)) {
            return mediaStatusInflight.get(imageId);
        }
        let request;
        request = (async () => {
            try {
                const res = await fetch(`/api/image/${imageId}/media-status`);
                if (!res.ok) return null;
                const data = await res.json();
                mediaStatusCache.set(imageId, { time: Date.now(), data });
                return data;
            } catch {
                return null;
            } finally {
                if (mediaStatusInflight.get(imageId) === request) mediaStatusInflight.delete(imageId);
            }
        })();
        mediaStatusInflight.set(imageId, request);
        return request;
    }

    function loupeTierUrl(tier, imageId, cachedOnly = false) {
        if (tier === 'full') {
            return `/api/full/${imageId}${cachedOnly ? '?cached=1' : ''}`;
        }
        return `/api/thumb/${tier}/${imageId}${cachedOnly ? '?cached=1' : ''}`;
    }

    function updateLoupeFlagDisplay(flag) {
        renderLoupeStatusLine(flag);
    }

    function loupeCacheMark(status, tier) {
        const tierStatus = status?.tiers?.[tier];
        if (!tierStatus) return 'x';
        return tierStatus.cached ? '✓' : 'x';
    }

    function loupeViewingTierName() {
        return LOUPE_TIER_NAMES[loupeDisplayedTierRank] || 'none';
    }

    function loupeCacheStatusText(status = loupeCurrentMediaStatus) {
        return [
            'ssd:',
            `sm ${loupeCacheMark(status, 'sm')}`,
            `md ${loupeCacheMark(status, 'md')}`,
            `lg ${loupeCacheMark(status, 'lg')}`,
            `original ${loupeCacheMark(status, 'full')}`,
            `· viewing ${loupeViewingTierName()}`,
        ].join(' ');
    }

    function renderLoupeStatusLine(flag = loupeCurrentImage?.flag || 'unflagged') {
        const tierEl = document.getElementById('loupe-overlay-tier');
        if (!tierEl) return;
        const tier = LOUPE_TIER_LABELS[loupeDisplayedTierRank] || 'Image';
        const label = flag === 'picked' ? 'Picked' : flag === 'rejected' ? 'Rejected' : 'Unflagged';
        if (uiSettings.show_loupe_cache_status) {
            tierEl.textContent = `${loupeCacheStatusText()} · ${label}`;
        } else {
            tierEl.textContent = `${tier} · ${label}`;
        }
    }

    function applyLoupeMediaStatus(status, img = loupeCurrentImage, token = loupeImageToken) {
        if (!status || !img || !isCurrentLoupeImage(img, token)) return false;
        loupeCurrentMediaStatus = status;
        renderLoupeStatusLine(img.flag || 'unflagged');
        return true;
    }

    async function refreshLoupeMediaStatus(img = loupeCurrentImage, token = loupeImageToken, { force = true } = {}) {
        if (!img || !isCurrentLoupeImage(img, token)) return null;
        const status = await getMediaStatus(img.id, { force });
        applyLoupeMediaStatus(status, img, token);
        return status;
    }

    function loupeApplyImageSize() {
        const img = document.getElementById('loupe-img');
        if (!img || !loupeNatW || !loupeNatH) return;
        img.style.width = `${loupeNatW}px`;
        img.style.height = `${loupeNatH}px`;
    }

    async function runLoupeProgressiveLoad(img, token) {
        const smUrl = img.thumb_url || loupeTierUrl('sm', img.id);
        loadLoupeTier(img, smUrl, LOUPE_TIER_RANKS.sm, token, {
            timeoutMs: LOUPE_TIER_TIMEOUTS.md,
        });

        const statusRequest = getMediaStatus(img.id, { force: true });
        statusRequest.then((latest) => applyLoupeMediaStatus(latest, img, token)).catch(() => {});
        const status = await withTimeout(statusRequest, 500, null);
        if (!isCurrentLoupeImage(img, token)) return;
        applyLoupeMediaStatus(status, img, token);

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
            const loaded = await loadLoupeTier(img, loupeTierUrl(tier.name, img.id), rank, token, {
                adoptDimensions: tier.adoptDimensions,
                timeoutMs: LOUPE_TIER_TIMEOUTS[tier.name],
            });
            if (tier.name === 'full' && !loaded && isCurrentLoupeImage(img, token) && loupeDisplayedTierRank < rank) {
                loupeFullLoadToken = 0;
            }
            if (!isCurrentLoupeImage(img, token)) return;
        }
    }

    function loadLoupeTier(img, url, rank, token, { adoptDimensions = false, timeoutMs = 0 } = {}) {
        if (!url) return Promise.resolve(false);
        return new Promise((resolve) => {
            const probeImg = new Image();
            const probe = {
                img: probeImg,
                timer: null,
                resolved: false,
                resolveOnce(value) {
                    if (probe.resolved) return;
                    probe.resolved = true;
                    resolve(value);
                },
                done: false,
                cancelled: false,
            };
            loupeTierProbes.add(probe);

            const cleanup = () => {
                if (probe.done) return;
                probe.done = true;
                if (probe.timer) clearTimeout(probe.timer);
                loupeTierProbes.delete(probe);
            };

            const finish = (loaded) => {
                cleanup();
                probe.resolveOnce(loaded);
            };

            probeImg.decoding = 'async';
            if ('fetchPriority' in probeImg) probeImg.fetchPriority = rank >= 2 ? 'high' : 'auto';
            probeImg.onload = () => {
                if (probe.cancelled) {
                    finish(false);
                    return;
                }
                if (!probeImg.naturalWidth || !probeImg.naturalHeight) {
                    finish(false);
                    return;
                }
                const isCurrent = isCurrentLoupeImage(img, token);
                if (isCurrent && rank > loupeDisplayedTierRank) {
                    if (adoptDimensions && probeImg.naturalWidth > 0 && probeImg.naturalHeight > 0) {
                        loupeAdoptSourceDimensions(probeImg.naturalWidth, probeImg.naturalHeight);
                    }

                    const loupeImg = document.getElementById('loupe-img');
                    if (loupeImg) {
                        loupeDisplayedTierRank = rank;
                        if (rank === LOUPE_TIER_RANKS.full) loupeFullLoadToken = token;
                        loupeImg.src = probeImg.src;
                        loupeImg.style.opacity = '1';
                        renderLoupeStatusLine(img.flag || 'unflagged');
                    }
                }
                if (isCurrent) refreshLoupeMediaStatus(img, token);
                finish(true);
            };
            probeImg.onerror = () => {
                finish(false);
            };
            if (timeoutMs > 0) {
                probe.timer = setTimeout(() => {
                    probe.timer = null;
                    probe.resolveOnce(false);
                }, timeoutMs);
            }
            probeImg.src = url;
        });
    }

    function withTimeout(promise, timeoutMs, fallback) {
        let timer = null;
        return Promise.race([
            Promise.resolve(promise),
            new Promise((resolve) => {
                timer = setTimeout(() => resolve(fallback), timeoutMs);
            }),
        ]).finally(() => {
            if (timer) clearTimeout(timer);
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
        }).then((loaded) => {
            if (!loaded && isCurrentLoupeImage(img, token) && loupeDisplayedTierRank < LOUPE_TIER_RANKS.full) {
                loupeFullLoadToken = 0;
            }
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
        if (lightboxIndex < 0) return;
        const token = loupeImageToken;
        const generation = warmupGeneration;
        for (const offset of loupeNeighborOffsets(LOUPE_PRELOAD_RADIUS, direction)) {
            const ni = lightboxIndex + offset;
            if (ni < 0 || ni >= libraryImages.length) continue;
            const neighbor = libraryImages[ni];
            const distance = Math.abs(offset);
            enqueueWarmup(async () => {
                if (!loupeCurrentImage || !isCurrentLoupeImage(loupeCurrentImage, token)) return;
                await preloadLoupeNeighbor(neighbor, distance, token, generation);
            }, { generation });
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

    async function preloadLoupeNeighbor(img, distance = 1, token = loupeImageToken, generation = warmupGeneration) {
        if (generation !== warmupGeneration || !loupeCurrentImage || !isCurrentLoupeImage(loupeCurrentImage, token)) return;
        await preloadImageWithTimeout(img.thumb_url, 'low', 1200);
        if (generation !== warmupGeneration || !loupeCurrentImage || !isCurrentLoupeImage(loupeCurrentImage, token)) return;
        const status = await getMediaStatus(img.id);
        if (generation !== warmupGeneration || !loupeCurrentImage || !isCurrentLoupeImage(loupeCurrentImage, token)) return;
        const tiers = status?.tiers || {};
        if (tiers.md?.cached) {
            await preloadImageWithTimeout(tiers.md.cached_url, 'low', LOUPE_TIER_TIMEOUTS.md);
        }
        if (generation !== warmupGeneration || !loupeCurrentImage || !isCurrentLoupeImage(loupeCurrentImage, token)) return;
        if (tiers.lg?.cached) {
            await preloadImageWithTimeout(tiers.lg.cached_url, 'low', LOUPE_TIER_TIMEOUTS.lg);
        }
        if (generation !== warmupGeneration || !loupeCurrentImage || !isCurrentLoupeImage(loupeCurrentImage, token)) return;
        if (distance === 1) {
            if (!tiers.md?.cached) {
                await preloadImageWithTimeout(loupeTierUrl('md', img.id), 'low', LOUPE_TIER_TIMEOUTS.md);
            }
            if (generation !== warmupGeneration || !loupeCurrentImage || !isCurrentLoupeImage(loupeCurrentImage, token)) return;
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

    async function ensureLibraryImageIndex(index) {
        if (index < libraryImages.length) return true;
        if (searchQuery === '__similar__') return false;
        while (index >= libraryImages.length && !rankingsExhausted) {
            const before = libraryImages.length;
            const loaded = await loadRankings(false);
            if (libraryImages.length <= before && !loaded) break;
        }
        return index < libraryImages.length;
    }

    function lightboxNext() {
        if (lightboxIndex < 0) return;
        const nextIndex = lightboxIndex + 1;
        if (nextIndex < libraryImages.length) {
            lightboxIndex = nextIndex;
            updateFilmstripCounter();
            showLoupeImage(libraryImages[lightboxIndex], 1);
            return;
        }
        const fromIndex = lightboxIndex;
        ensureLibraryImageIndex(nextIndex).then((ok) => {
            if (!ok || lightboxIndex !== fromIndex) return;
            lightboxIndex = nextIndex;
            updateFilmstripCounter();
            showLoupeImage(libraryImages[lightboxIndex], 1);
        });
    }

    function lightboxPrev() {
        if (lightboxIndex <= 0) return;
        lightboxIndex--;
        updateFilmstripCounter();
        showLoupeImage(libraryImages[lightboxIndex], -1);
    }

    function closeLightbox() {
        const loupe = document.getElementById('loupe');
        if (loupe) {
            loupe.classList.remove('loupe-visible');
            if (loupeHideTimer) clearTimeout(loupeHideTimer);
            loupeHideTimer = setTimeout(() => {
                if (!document.body.classList.contains('loupe-open')) loupe.classList.add('hidden');
                loupeHideTimer = null;
            }, 150);
        }
        document.body.classList.remove('loupe-open');
        loupeImageToken++;
        clearWarmups();
        cancelLoupeProbes();
        if (loupeFullLoadTimer) {
            clearTimeout(loupeFullLoadTimer);
            loupeFullLoadTimer = null;
        }
        loupeCurrentImage = null;
        loupeStandaloneImage = null;
        loupeFullLoadToken = 0;
        loupeCurrentMediaStatus = null;
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
                clearWarmups();
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
        clearWarmups();
        // Reload the appropriate view based on which page we're on
        const grid = document.getElementById('rankings-grid');
        if (grid) {
            resetLibraryResults({ clearBatch: true });
            loadRankings(true);
            if (isDateSortActive()) updateDateScrubber();
            if (libraryView === 'map') loadMap();
        } else {
            // Compare page — reload the active compare surface with the same filters
            if (compareMode === 'mosaic') {
                loadMosaicBatch();
            } else {
                comparePairs = [];
                compareIndex = 0;
                fetchComparePairs().then(() => showComparePair());
            }
        }
    }

    function setFilter(key, value) {
        filters[key] = value;
        updateMetadataFilterButton();
        saveFilters();
        reloadForFilters();
    }

    function clearLibraryFilters() {
        filters = { ...EMPTY_FILTERS };
        document.querySelectorAll('.bar-filters .filter-icon').forEach(btn => btn.classList.remove('active'));
        document.querySelectorAll('.filter-star').forEach(s => {
            s.classList.remove('lit', 'hovered');
            s.textContent = '☆';
        });
        ['filter-folder', 'filter-taken', 'filter-type', 'filter-camera', 'filter-lens'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.value = '';
        });
        updateMetadataFilterButton();
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
            if (filters.folder) sel.value = filters.folder;
        }).catch(() => {});
    }

    function loadFilterOptions() {
        fetch('/api/filter-options').then(r => r.json()).then(data => {
            function populateSelect(selectId, allLabel, items, valueKey, labelFn, currentValue) {
                const sel = document.getElementById(selectId);
                if (!sel) return;
                sel.innerHTML = `<option value="">${allLabel}</option>`;
                for (const item of items || []) {
                    const value = item[valueKey] || '';
                    if (!value) continue;
                    const opt = document.createElement('option');
                    opt.value = value;
                    opt.textContent = labelFn(item);
                    opt.title = value;
                    sel.appendChild(opt);
                }
                sel.value = currentValue || '';
            }

            const takenSel = document.getElementById('filter-taken');
            if (takenSel) {
                const current = filters.taken || '';
                takenSel.innerHTML = '<option value="">All Dates</option>';
                for (const item of data.years || []) {
                    const opt = document.createElement('option');
                    opt.value = item.year;
                    opt.textContent = `${item.year} (${item.count})`;
                    takenSel.appendChild(opt);
                }
                if (Number(data.undated || 0) > 0) {
                    const opt = document.createElement('option');
                    opt.value = 'undated';
                    opt.textContent = `Undated (${data.undated})`;
                    takenSel.appendChild(opt);
                }
                takenSel.value = current;
            }

            populateSelect(
                'filter-type',
                'All Types',
                data.file_types || [],
                'ext',
                item => `${String(item.ext || '').replace('.', '').toUpperCase()} (${item.count})`,
                filters.fileType,
            );
            populateSelect(
                'filter-camera',
                'All Cameras',
                data.cameras || [],
                'camera',
                item => `${item.camera} (${item.count})`,
                filters.camera,
            );
            populateSelect(
                'filter-lens',
                'All Lenses',
                data.lenses || [],
                'lens',
                item => `${item.lens} (${item.count})`,
                filters.lens,
            );
            updateMetadataFilterButton();
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
        if (comparisons === 0 && Math.abs(Number(elo || 1200) - 1200) < 0.01) return 0;
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
        if (comparisons < 5 && Math.abs(Number(elo || 1200) - 1200) < 0.01) return '';
        if (elo >= 1500) return 'tier-gold';
        if (elo >= 1350) return 'tier-silver';
        if (elo >= 1250) return 'tier-bronze';
        return '';
    }

    async function findSimilar() {
        if (lightboxIndex < 0 || lightboxIndex >= libraryImages.length) return;
        const img = libraryImages[lightboxIndex];
        closeLightbox();
        clearWarmups();

        // Clear grid and show similar images
        searchQuery = '__similar__';
        clearPersistedSearchState();
        const input = document.getElementById('search-input');
        if (input) input.value = `Similar to: ${img.filename}`;
        const clearBtn = document.getElementById('search-clear');
        if (clearBtn) clearBtn.classList.remove('hidden');
        const sortToggles = document.getElementById('sort-toggles');
        if (sortToggles) sortToggles.style.opacity = '0.3';
        updateDateScrubber();

        libraryRequestGeneration++;
        clearBatchSelection();
        libraryImages = [];
        rankingsOffset = 0;
        rankingsExhausted = true;
        const grid = document.getElementById('rankings-grid');
        grid.innerHTML = '';

        const requestGeneration = libraryRequestGeneration;
        const res = await fetch(`/api/similar/${img.id}?limit=100`);
        const data = await res.json();
        if (requestGeneration !== libraryRequestGeneration) return;
        compareStats = {
            ...compareStats,
            filtered_pool: Number(data.visible_images ?? data.images?.length ?? 0),
            filtered_pool_visible: Number(data.visible_images ?? data.images?.length ?? 0),
            filtered_pool_total: Number(data.total_images ?? data.images?.length ?? 0),
        };
        updateCompareProgress();

        for (let i = 0; i < data.images.length; i++) {
            const simg = data.images[i];
            const ar = simg.aspect_ratio || 1.5;
            const simLabel = similarityLabel(simg);

            const card = document.createElement('div');
            card.className = 'rank-card' + (flagClass(simg.flag) ? ' ' + flagClass(simg.flag) : '');
            card.dataset.imageId = simg.id;
            card.dataset.ar = ar;
            card.style.height = thumbHeight + 'px';
            card.style.flexGrow = ar;
            card.style.flexBasis = (thumbHeight * ar) + 'px';
            card.onclick = () => openLightbox(simg);

            card.innerHTML = `
                <img src="${escapeHtml(simg.thumb_url)}" alt="${escapeHtml(simg.filename)}" loading="lazy" onload="this.classList.add('loaded')">
                ${flagBadge(simg.flag)}
                <div class="rank-card-info">
                    <span class="rank-elo">${escapeHtml(simg.elo)} Elo</span>
                    <span class="rank-similarity">${escapeHtml(simLabel)}</span>
                </div>
            `;
            grid.appendChild(card);
            libraryImages.push(simg);
        }
        rankingsOffset = libraryImages.length;
    }

    // ==================== BATCH SELECTION ====================

    let batchMode = false;
    let batchSelected = new Set();
    let lastClickedIndex = -1;

    function toggleBatchMode() {
        if (batchMode) {
            clearBatchSelection();
        } else {
            batchMode = true;
            document.querySelectorAll('.rank-card').forEach(c => c.classList.add('selectable'));
            updateBatchBar();
        }
    }

    function clearBatchSelection() {
        batchMode = false;
        batchSelected.clear();
        lastClickedIndex = -1;
        document.querySelectorAll('.rank-card').forEach(c => {
            c.classList.remove('selectable', 'selected');
        });
        updateBatchBar();
    }

    function handleCardClick(e, img, card, index) {
        if (e.ctrlKey || e.metaKey) {
            // Ctrl/Cmd-click: toggle individual selection
            e.preventDefault();
            if (batchSelected.has(img.id)) {
                batchSelected.delete(img.id);
                card.classList.remove('selected');
            } else {
                batchSelected.add(img.id);
                card.classList.add('selected');
            }
            lastClickedIndex = index;
            if (batchSelected.size === 0) {
                clearBatchSelection();
            } else {
                batchMode = true;
                document.querySelectorAll('.rank-card').forEach(c => c.classList.add('selectable'));
                updateBatchBar();
            }
        } else if (e.shiftKey && lastClickedIndex >= 0) {
            // Shift-click: range selection
            e.preventDefault();
            const cards = document.querySelectorAll('.rank-card');
            const start = Math.min(lastClickedIndex, index);
            const end = Math.max(lastClickedIndex, index);
            for (let i = start; i <= end; i++) {
                if (i < libraryImages.length && i < cards.length) {
                    batchSelected.add(libraryImages[i].id);
                    cards[i].classList.add('selected');
                }
            }
            batchMode = true;
            document.querySelectorAll('.rank-card').forEach(c => c.classList.add('selectable'));
            updateBatchBar();
        } else if (batchMode) {
            // In batch mode, plain click toggles selection
            if (batchSelected.has(img.id)) {
                batchSelected.delete(img.id);
                card.classList.remove('selected');
            } else {
                batchSelected.add(img.id);
                card.classList.add('selected');
            }
            lastClickedIndex = index;
            if (batchSelected.size === 0) {
                clearBatchSelection();
            } else {
                batchMode = true;
                document.querySelectorAll('.rank-card').forEach(c => c.classList.add('selectable'));
                updateBatchBar();
            }
        } else {
            // Normal click: open lightbox
            lastClickedIndex = index;
            openLightbox(img);
        }
    }

    function updateBatchBar() {
        let bar = document.getElementById('batch-bar');
        if (batchSelected.size > 0) {
            if (!bar) {
                bar = document.createElement('div');
                bar.id = 'batch-bar';
                bar.className = 'batch-bar';
                document.body.appendChild(bar);
            }
            bar.innerHTML = `
                <span>${batchSelected.size} selected</span>
                <button class="batch-flag-btn flag-picked" onclick="PhotoArchive.batchFlag('picked')">P</button>
                <button class="batch-flag-btn flag-unflagged" onclick="PhotoArchive.batchFlag('unflagged')">U</button>
                <button class="batch-flag-btn flag-rejected" onclick="PhotoArchive.batchFlag('rejected')">X</button>
                <span class="batch-divider"></span>
                <button onclick="PhotoArchive.batchExport('json')">Export JSON</button>
                <button onclick="PhotoArchive.batchExport('csv')">Export CSV</button>
                <button class="batch-cancel" onclick="PhotoArchive.clearBatchSelection()">✕</button>
            `;
        } else if (bar) {
            bar.remove();
        }
    }

    async function batchFlag(flag) {
        if (!batchSelected.size) return;
        const ids = Array.from(batchSelected);
        const previousFlags = ids.map(id => {
            const img = libraryImages.find(item => item.id === id);
            return { id, flag: img?.flag || 'unflagged' };
        });
        // Update UI immediately
        ids.forEach(id => updateImageFlagLocal(id, flag));
        try {
            const res = await fetch('/api/images/flag', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ image_ids: ids, flag }),
            });
            if (!res.ok) throw new Error('batch flag failed');
            showToast(`${ids.length} image${ids.length > 1 ? 's' : ''} → ${flag}`);
            clearBatchSelection();
        } catch {
            previousFlags.forEach(item => updateImageFlagLocal(item.id, item.flag));
            showToast('Batch flag update failed');
            return;
        }
    }

    function batchExport(format) {
        const ids = Array.from(batchSelected).join(',');
        window.open(`/api/export?format=${format}&ids=${ids}`, '_blank');
    }

    function exportRankings(format) {
        window.open(`/api/export?format=${format}`, '_blank');
    }

    // ==================== MAP VIEW ====================

    let mapInstance = null;
    let mapMarkerLayer = null;
    let mapCenter = [20, 0];
    let mapZoom = 2;
    let libraryView = 'grid';
    let libraryScrollBeforeMap = 0;
    let leafletLoaded = false;
    let leafletLoadPromise = null;
    let mapRequestGeneration = 0;

    function setLibraryView(mode) {
        clearWarmups();
        libraryView = mode;
        const grid = document.getElementById('rankings-grid');
        const mapEl = document.getElementById('map-container');
        const gridBtn = document.getElementById('view-grid-btn');
        const mapBtn = document.getElementById('view-map-btn');
        const scrollRoot = libraryScrollRoot();

        if (mode === 'map') {
            clearBatchSelection();
            libraryScrollBeforeMap = scrollRoot?.scrollTop || 0;
            scrollRoot?.classList.add('map-active');
            scrollRoot?.scrollTo({ top: 0, behavior: 'auto' });
            grid?.classList.add('hidden');
            mapEl?.classList.remove('hidden');
            gridBtn?.classList.remove('active');
            mapBtn?.classList.add('active');
            syncDateScrubberVisibility();
            loadMap();
        } else {
            if (mapInstance) {
                mapCenter = [mapInstance.getCenter().lat, mapInstance.getCenter().lng];
                mapZoom = mapInstance.getZoom();
            }
            scrollRoot?.classList.remove('map-active');
            grid?.classList.remove('hidden');
            mapEl?.classList.add('hidden');
            gridBtn?.classList.add('active');
            mapBtn?.classList.remove('active');
            syncDateScrubberVisibility();
            requestAnimationFrame(() => {
                if (scrollRoot) scrollRoot.scrollTop = libraryScrollBeforeMap;
            });
        }
    }

    function loadExternalScript(src) {
        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = src;
            script.onload = resolve;
            script.onerror = () => reject(new Error(`Failed to load ${src}`));
            document.head.appendChild(script);
        });
    }

    async function loadLeaflet() {
        if (leafletLoaded) return;
        if (leafletLoadPromise) return leafletLoadPromise;
        leafletLoadPromise = (async () => {
        // Load Leaflet CSS
        const link = document.createElement('link');
        link.rel = 'stylesheet';
        link.href = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css';
        document.head.appendChild(link);

        // Load MarkerCluster CSS
        const mcLink = document.createElement('link');
        mcLink.rel = 'stylesheet';
        mcLink.href = 'https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css';
        document.head.appendChild(mcLink);
        const mcDefaultLink = document.createElement('link');
        mcDefaultLink.rel = 'stylesheet';
        mcDefaultLink.href = 'https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css';
        document.head.appendChild(mcDefaultLink);

        // Load Leaflet JS
        await loadExternalScript('https://unpkg.com/leaflet@1.9.4/dist/leaflet.js');

        // Load MarkerCluster JS
        await loadExternalScript('https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js');

        if (!window.L || typeof window.L.markerClusterGroup !== 'function') {
            throw new Error('Map library did not initialize');
        }

        leafletLoaded = true;
        })();
        try {
            await leafletLoadPromise;
        } catch (e) {
            leafletLoadPromise = null;
            throw e;
        }
    }

    function showMapError(container, message) {
        if (!container) return;
        let errorEl = container.querySelector('.map-error');
        if (!errorEl) {
            errorEl = document.createElement('div');
            errorEl.className = 'map-error';
            container.appendChild(errorEl);
        }
        errorEl.textContent = message;
    }

    function clearMapError(container) {
        container?.querySelector('.map-error')?.remove();
    }

    function buildMapPopup(markerData) {
        const wrap = document.createElement('div');
        wrap.className = 'map-popup';

        const img = document.createElement('img');
        img.src = markerData.thumb_url || '';
        img.alt = markerData.filename || '';
        img.className = 'map-popup-thumb';
        img.addEventListener('click', () => openLightboxById(Number(markerData.id)));
        wrap.appendChild(img);

        const name = document.createElement('div');
        name.className = 'map-popup-name';
        name.textContent = markerData.filename || '';
        wrap.appendChild(name);
        return wrap;
    }

    async function loadMap() {
        const container = document.getElementById('map-container');
        const requestGeneration = ++mapRequestGeneration;
        try {
            await loadLeaflet();
        } catch (e) {
            console.error('Map library load error:', e);
            showMapError(container, 'Map unavailable. Check your connection and try again.');
            return;
        }
        if (requestGeneration !== mapRequestGeneration) return;
        clearMapError(container);

        if (!mapInstance) {
            mapInstance = L.map(container).setView(mapCenter, mapZoom);
            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '&copy; OpenStreetMap contributors',
                maxZoom: 19,
            }).addTo(mapInstance);
        } else {
            // Resize map to fit container
            setTimeout(() => mapInstance.invalidateSize(), 100);
        }

        // Fetch markers
        const query = filterQueryString(currentQueryState());
        const url = `/api/map/markers${query ? `?${query}` : ''}`;
        try {
            const data = await fetch(url).then(r => r.json());
            if (requestGeneration !== mapRequestGeneration) return;
            if (mapMarkerLayer) {
                mapInstance.removeLayer(mapMarkerLayer);
            }
            mapMarkerLayer = L.markerClusterGroup();

            for (const m of data.markers) {
                const marker = L.marker([m.lat, m.lng]);
                marker.bindPopup(buildMapPopup(m), { maxWidth: 200 });
                mapMarkerLayer.addLayer(marker);
            }
            mapInstance.addLayer(mapMarkerLayer);

            // Update stats
            const mapInfo = document.getElementById('map-info');
            if (!mapInfo) {
                const info = document.createElement('div');
                info.id = 'map-info';
                info.className = 'map-info';
                container.appendChild(info);
            }
            const infoEl = document.getElementById('map-info');
            const gpsVisible = Number(data.gps_count ?? data.markers?.length ?? 0);
            const gpsTotal = Number(data.gps_total_count ?? gpsVisible);
            const catalogTotal = Number(data.total_count ?? gpsTotal);
            infoEl.textContent = `${visibleTotalLabel(gpsVisible, gpsTotal)} located photos shown · ${catalogTotal.toLocaleString()} photos in pool`;

            // Fit bounds if markers exist and map hasn't been manually positioned
            if (data.markers.length > 0 && mapZoom === 2) {
                mapInstance.fitBounds(mapMarkerLayer.getBounds(), { padding: [30, 30] });
            }
        } catch (e) {
            console.error('Map load error:', e);
            showMapError(container, 'Map markers could not be loaded.');
        }
    }

    function openLightboxById(id) {
        const img = libraryImages.find(i => i.id === id);
        if (img) {
            openLightbox(img);
        } else {
            // Image is outside the active ordered list, so open it without mutating that list.
            fetch(`/api/image/${id}/exif`).then(r => r.json()).then(data => {
                const exif = data.exif || {};
                openStandaloneLightbox({
                    id,
                    filename: exif.filename || `Image ${id}`,
                    thumb_url: `/api/thumb/sm/${id}`,
                    aspect_ratio: 1.5,
                    elo: 0,
                    comparisons: 0,
                    flag: 'unflagged',
                    ...exif,
                });
            }).catch(() => {});
        }
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
        'embed_batch_size',
        'search_similarity_threshold',
        'show_loupe_cache_status',
    ];
    const THUMB_OUTPUT_FIELDS = ['thumb_size_sm', 'thumb_size_md', 'thumb_size_lg', 'thumb_quality'];
    let settingsPoller = null;
    let settingsPageData = null;
    let savedThumbnailOutput = null;
    let catalogSources = [];
    let catalogBrowsePath = '';

    function escapeHtml(value) {
        return String(value ?? '').replace(/[&<>"']/g, (ch) => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#39;',
        }[ch]));
    }

    function jsString(value) {
        return JSON.stringify(String(value ?? '')).replace(/</g, '\\u003c').replace(/>/g, '\\u003e');
    }

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

    function sourceState(source) {
        if (!source.included) return { label: 'Removed', cls: 'removed' };
        if (!source.online) return { label: 'Offline', cls: 'offline' };
        return { label: 'Online', cls: 'online' };
    }

    function renderCatalogSources(catalog) {
        const stats = catalog?.stats || {};
        const activeEl = document.getElementById('scan-total-images');
        const catalogEl = document.getElementById('catalog-total-images');
        if (activeEl) activeEl.textContent = Number(stats.active_images ?? stats.total_images ?? 0).toLocaleString();
        if (catalogEl) catalogEl.textContent = Number(stats.total_catalog_images ?? stats.total_images ?? 0).toLocaleString();

        catalogSources = catalog?.sources || [];
        const list = document.getElementById('catalog-source-list');
        if (!list) return;
        if (!catalogSources.length) {
            list.innerHTML = '<div class="catalog-empty">No folders added yet.</div>';
            return;
        }
        list.innerHTML = catalogSources.map((source) => {
            const state = sourceState(source);
            const count = Number(source.image_count || 0).toLocaleString();
            const active = Number(source.active_image_count || 0).toLocaleString();
            const lastScan = source.last_scan_at ? formatDateTime(source.last_scan_at) : 'Never scanned';
            const canScan = source.online ? '' : 'disabled';
            const restoreLabel = source.included ? 'Rescan' : 'Restore + Scan';
            return `
                <div class="catalog-source ${state.cls}">
                    <div class="catalog-source-main">
                        <div class="catalog-source-title">
                            <strong>${escapeHtml(source.display_name || source.path)}</strong>
                            <span class="catalog-source-state ${state.cls}">${state.label}</span>
                        </div>
                        <code>${escapeHtml(source.path)}</code>
                        <div class="catalog-source-meta">${active} active · ${count} catalog · ${escapeHtml(lastScan)}</div>
                    </div>
                    <div class="catalog-source-actions">
                        <button class="bar-btn" type="button" ${canScan} onclick="PhotoArchive.rescanCatalogSource(${source.id})">${restoreLabel}</button>
                        <button class="bar-btn danger" type="button" onclick="PhotoArchive.openRemoveSourceDialog(${source.id})">Remove</button>
                    </div>
                </div>
            `;
        }).join('');
    }

    async function loadCatalogSources() {
        try {
            const res = await fetch('/api/catalog');
            const data = await res.json();
            renderCatalogSources(data);
            return data;
        } catch (err) {
            setSettingsStatus(`Could not load catalog sources: ${err.message}`, 'error');
            return null;
        }
    }

    function setScanBusy(busy, label = 'Add + Scan') {
        const btn = document.getElementById('scan-btn');
        if (btn) {
            btn.disabled = busy;
            btn.textContent = busy ? 'Scanning...' : label;
        }
        const progress = document.getElementById('scan-progress');
        if (progress) progress.classList.toggle('hidden', !busy);
    }

    async function pollScanUntilDone() {
        if (_scanPoller) clearInterval(_scanPoller);
        setScanBusy(true);
        _scanPoller = setInterval(async () => {
            try {
                const res = await fetch('/api/scan/status');
                const data = await res.json();
                const countEl = document.getElementById('scan-progress-count');
                if (countEl) countEl.textContent = Number(data.total_found || 0).toLocaleString();
                if (!data.scanning) {
                    clearInterval(_scanPoller);
                    _scanPoller = null;
                    setScanBusy(false);
                    await loadCatalogSources();
                    if (data.error) {
                        setSettingsStatus(`Scan stopped: ${data.error}`, 'error');
                    } else {
                        setSettingsStatus(`Scan complete. ${Number(data.total_found || 0).toLocaleString()} photos found.`, 'success');
                    }
                }
            } catch {}
        }, 1000);
    }

    async function addCatalogSource() {
        const folderInput = document.getElementById('scan-folder');
        const folder = folderInput?.value?.trim();
        if (!folder) return;

        setSettingsStatus('Adding folder and starting scan...', 'muted');
        try {
            const res = await fetch('/api/catalog/sources', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ path: folder, scan: true }),
            });
            const data = await res.json();
            if (!res.ok || !data.ok) throw new Error(data.error || 'Could not add folder');
            renderCatalogSources(data.catalog || { sources: [data.source], stats: settingsPageData?.catalog?.stats || {} });
            pollScanUntilDone();
        } catch (err) {
            setScanBusy(false);
            setSettingsStatus(`Add folder failed: ${err.message}`, 'error');
        }
    }

    async function rescanCatalogSource(sourceId) {
        setSettingsStatus('Starting folder scan...', 'muted');
        try {
            const res = await fetch(`/api/catalog/sources/${sourceId}/rescan`, { method: 'POST' });
            const data = await res.json();
            if (!res.ok || !data.ok) throw new Error(data.error || 'Could not start scan');
            pollScanUntilDone();
        } catch (err) {
            setSettingsStatus(`Scan failed to start: ${err.message}`, 'error');
        }
    }

    function openRemoveSourceDialog(sourceId) {
        const source = catalogSources.find((item) => Number(item.id) === Number(sourceId));
        if (!source) return;
        document.getElementById('catalog-remove-modal')?.remove();
        const modal = document.createElement('div');
        modal.id = 'catalog-remove-modal';
        modal.className = 'modal-backdrop';
        modal.innerHTML = `
            <div class="catalog-remove-dialog">
                <h2>Remove Folder</h2>
                <p>${escapeHtml(source.path)}</p>
                <div class="catalog-remove-actions">
                    <button class="bar-btn" type="button" onclick="PhotoArchive.removeCatalogSource(${source.id}, 'keep')">Remove Folder, Keep Catalog Data</button>
                    <button class="bar-btn danger" type="button" onclick="PhotoArchive.removeCatalogSource(${source.id}, 'delete')">Remove Folder and Delete Catalog Data</button>
                    <button class="bar-btn" type="button" onclick="PhotoArchive.closeRemoveSourceDialog()">Cancel</button>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
    }

    function closeRemoveSourceDialog() {
        document.getElementById('catalog-remove-modal')?.remove();
    }

    async function removeCatalogSource(sourceId, mode) {
        closeRemoveSourceDialog();
        setSettingsStatus(mode === 'delete' ? 'Deleting folder catalog data...' : 'Removing folder from active catalog...', 'muted');
        try {
            const res = await fetch(`/api/catalog/sources/${sourceId}/remove`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mode }),
            });
            const data = await res.json();
            if (!res.ok || !data.ok) throw new Error(data.error || 'Remove failed');
            renderCatalogSources(data.catalog);
            setSettingsStatus(
                mode === 'delete'
                    ? `Catalog data deleted for ${Number(data.images_deleted || 0).toLocaleString()} photos.`
                    : 'Folder removed from active views. Catalog data was kept.',
                'success',
            );
        } catch (err) {
            setSettingsStatus(`Remove failed: ${err.message}`, 'error');
        }
    }

    async function chooseCatalogFolder() {
        const input = document.getElementById('scan-folder');
        const button = document.getElementById('choose-folder-btn');
        const currentPath = input?.value?.trim() || '';
        if (button) button.disabled = true;
        setSettingsStatus('Opening folder chooser...', 'muted');
        try {
            const res = await fetch('/api/catalog/select-folder', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ path: currentPath }),
            });
            const data = await res.json();
            if (data.cancelled) {
                setSettingsStatus('Folder selection cancelled.', 'muted');
                return;
            }
            if (!res.ok || !data.ok) throw new Error(data.error || 'Native folder chooser is unavailable');
            selectBrowsedDirectory(data.path);
            catalogBrowsePath = data.path || catalogBrowsePath;
            setSettingsStatus('Folder selected. Add + Scan when ready.', 'success');
        } catch (err) {
            setSettingsStatus(`Folder chooser unavailable: ${err.message}. Paste a folder path instead.`, 'error');
        } finally {
            if (button) button.disabled = false;
        }
    }

    async function browseDirectory(path = '') {
        try {
            const query = path ? `?path=${encodeURIComponent(path)}` : '';
            const res = await fetch(`/api/catalog/browse${query}`);
            const data = await res.json();
            catalogBrowsePath = data.path || '';
            renderDirectoryBrowser(data);
        } catch (err) {
            const errEl = document.getElementById('directory-browser-error');
            if (errEl) errEl.textContent = err.message;
        }
    }

    function renderDirectoryBrowser(data) {
        const currentEl = document.getElementById('directory-browser-current');
        const rootsEl = document.getElementById('directory-browser-roots');
        const listEl = document.getElementById('directory-browser-list');
        const errEl = document.getElementById('directory-browser-error');
        if (errEl) errEl.textContent = data.error || '';
        const currentPath = data.path || '/';
        if (currentEl) currentEl.innerHTML = directoryCrumbs(currentPath);
        if (rootsEl) {
            rootsEl.innerHTML = (data.roots || []).map((root) =>
                `<button class="bar-btn" type="button" onclick="PhotoArchive.browseDirectory(${jsString(root.path)})">${escapeHtml(root.label)}</button>`
            ).join('');
        }
        if (!listEl) return;
        const entries = data.entries || [];
        const currentName = currentPath.replace(/\/+$/, '').split('/').filter(Boolean).pop() || '/';
        const currentRow = `
            <div class="directory-tree-row current">
                <div class="directory-tree-open">
                    <span class="tree-expander">-</span>
                    <span class="tree-folder">${escapeHtml(currentName)}</span>
                    <code>${escapeHtml(currentPath)}</code>
                </div>
                <button class="bar-btn" type="button" onclick="PhotoArchive.useBrowsedDirectory()">Use</button>
            </div>
        `;
        const childRows = entries.map((entry) => `
            <div class="directory-tree-row child ${entry.readable ? '' : 'restricted'}">
                <button class="directory-tree-open" type="button" onclick="PhotoArchive.browseDirectory(${jsString(entry.path)})" title="${escapeHtml(entry.path)}">
                    <span class="tree-branch"></span>
                    <span class="tree-expander">+</span>
                    <span class="tree-folder">${escapeHtml(entry.name)}</span>
                    <small>${entry.readable ? 'Folder' : 'Restricted'}</small>
                </button>
                <button class="bar-btn" type="button" onclick="PhotoArchive.selectBrowsedDirectory(${jsString(entry.path)})">Use</button>
            </div>
        `).join('');
        const empty = entries.length ? '' : '<div class="catalog-empty tree-empty">No readable subfolders here.</div>';
        listEl.innerHTML = currentRow + childRows + empty;
    }

    function directoryCrumbs(path) {
        const normalized = String(path || '/').replace(/\/+$/, '') || '/';
        const parts = normalized.split('/').filter(Boolean);
        const crumbs = [
            `<button type="button" onclick="PhotoArchive.browseDirectory('/')">/</button>`,
        ];
        let current = '';
        for (const part of parts) {
            current += `/${part}`;
            crumbs.push(
                `<button type="button" onclick="PhotoArchive.browseDirectory(${jsString(current)})">${escapeHtml(part)}</button>`
            );
        }
        return crumbs.join('<span>/</span>');
    }

    function toggleDirectoryBrowser() {
        const browser = document.getElementById('directory-browser');
        const input = document.getElementById('scan-folder');
        if (!browser) return;
        const willOpen = browser.classList.contains('hidden');
        browser.classList.toggle('hidden', !willOpen);
        if (willOpen) browseDirectory(input?.value?.trim() || catalogBrowsePath || '');
    }

    function browseDirectoryParent() {
        if (!catalogBrowsePath) return;
        browseDirectory(catalogBrowsePath.replace(/\/+$/, '').split('/').slice(0, -1).join('/') || '/');
    }

    function selectBrowsedDirectory(path) {
        const input = document.getElementById('scan-folder');
        if (input) input.value = path;
    }

    function useBrowsedDirectory() {
        if (catalogBrowsePath) selectBrowsedDirectory(catalogBrowsePath);
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
        const pregenDiagnosticsEl = document.getElementById('cache-pregen-diagnostics');
        const thumbPauseBtn = document.getElementById('thumb-pause-btn');
        const thumbResumeBtn = document.getElementById('thumb-resume-btn');

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
        if (pregenDiagnosticsEl) {
            const reads = Number(pregen.recent_source_reads_per_min || 0);
            const writes = Number(pregen.recent_thumbnails_written_per_min || 0);
            const mbps = Number(pregen.recent_read_mbps || 0);
            const readMs = Number(pregen.avg_source_read_seconds || 0) * 1000;
            const encodeMs = Number(pregen.avg_decode_encode_seconds || 0) * 1000;
            const failures = Number(pregen.source_read_failures || 0);
            pregenDiagnosticsEl.textContent =
                `${reads.toFixed(1)} reads/min · ${writes.toFixed(1)} thumbs/min · ` +
                `${mbps.toFixed(1)} MB/s · read ${readMs.toFixed(0)}ms · encode ${encodeMs.toFixed(0)}ms · ` +
                `${failures.toLocaleString()} failures`;
        }
        const thumbnailsPaused = Boolean(pregen.manual_pause);
        if (thumbPauseBtn) thumbPauseBtn.disabled = thumbnailsPaused;
        if (thumbResumeBtn) thumbResumeBtn.disabled = !thumbnailsPaused && pregen.state === 'running';
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
        renderWorkBanner(data.ai_status, data.cache_stats);
        renderCacheTierGuide(data.cache_stats, data.settings);
        if (data.catalog) renderCatalogSources(data.catalog);
        updateCacheProfileHint();
    }

    function renderWorkBanner(aiStatus, cacheStatus) {
        const el = document.getElementById('work-banner');
        if (!el) return;

        const ai = aiStatus || {};
        const pregen = cacheStatus?.pregen || {};
        const disk = cacheStatus?.disk || {};

        // Embedding progress
        const embedTotal = Number(ai.total_images ?? ai.total_kept ?? 0);
        const embedDone = Number(ai.embedded || 0);
        const embedRemaining = Number(ai.remaining || 0);
        const embedPct = Number(ai.progress_pct || 0);
        const embedPaused = Boolean(ai.embedding_manual_pause);
        const embedAvailable = ai.worker_state !== 'unavailable';
        const embedComplete = embedRemaining <= 0 && embedTotal > 0;

        // Preview cache progress stays separate from original SSD warming.
        const diskTiers = disk.tiers || {};
        const previewTierNames = ['sm', 'md', 'lg'];
        const previewSummary = pregen.preview || {};
        let previewDone = Number(previewSummary.count || 0);
        let previewTotal = Number(previewSummary.total || 0);
        if (previewTotal <= 0) {
            for (const name of previewTierNames) {
                const t = diskTiers[name] || {};
                previewDone += Number(t.progress_count ?? t.count ?? 0);
                previewTotal += Number(t.progress_total || 0);
            }
        }
        const previewPct = previewTotal > 0 ? Math.min(100, previewDone / previewTotal * 100) : 0;
        const thumbPaused = Boolean(pregen.manual_pause);
        const previewComplete = previewPct >= 95;

        const fullTier = diskTiers.full || {};
        const originals = pregen.originals || {};
        const originalDone = Number(originals.count ?? fullTier.progress_count ?? fullTier.count ?? 0);
        const originalTotal = Number(originals.total ?? fullTier.progress_total ?? 0);
        const originalBudget = Number(originals.budget_bytes ?? fullTier.budget_bytes ?? 0);
        const originalPct = originalTotal > 0
            ? Math.min(100, originalDone / originalTotal * 100)
            : Math.min(100, Number(originals.utilization_pct ?? fullTier.utilization_pct ?? 0));
        const originalEnabled = originalBudget > 0 || originalTotal > 0 || originalDone > 0;
        const originalComplete = !originalEnabled || originalPct >= 95 || Number(originals.remaining || 0) <= 0;

        // ETAs
        let embedEta = '';
        if (embedComplete) embedEta = 'Done';
        else if (ai.eta_seconds) embedEta = formatEta(ai.eta_seconds);
        else if (ai.worker_state === 'embedding') embedEta = 'Measuring\u2026';

        let previewEta = '';
        if (previewComplete) previewEta = 'Done';
        else if (pregen.eta_seconds) previewEta = formatEta(pregen.eta_seconds);
        else if (pregen.state === 'running') previewEta = 'Measuring\u2026';

        let originalEta = '';
        if (originalComplete) originalEta = 'Done';
        else if (pregen.original_eta_seconds) originalEta = formatEta(pregen.original_eta_seconds);
        else if (pregen.state === 'running' && pregen.active_phase === 'full') originalEta = 'Measuring\u2026';

        // Speeds
        const embedSpeed = Number(ai.recent_images_per_min || ai.overall_images_per_min || 0);
        const thumbSpeed = Number(pregen.recent_images_per_min || pregen.overall_images_per_min || 0);

        // Build HTML
        const allDone = embedComplete && previewComplete && originalComplete;
        const pauseAllLabel = (embedPaused && thumbPaused) ? 'Resume All' : 'Pause All';
        const pauseAllAction = (embedPaused && thumbPaused) ? 'resumeAllWork' : 'pauseAllWork';

        el.innerHTML = `
            <div class="work-banner-head">
                <span class="work-banner-title">Background Work</span>
                ${!allDone ? `<div class="work-banner-actions">
                    <button class="bar-btn" type="button" onclick="PhotoArchive.${pauseAllAction}()">${pauseAllLabel}</button>
                </div>` : ''}
            </div>
            <div class="work-banner-rows">
                <div class="work-row embed">
                    <div class="work-row-head">
                        <span class="work-row-label">AI Embeddings</span>
                        <span class="work-row-stat">${embedDone.toLocaleString()} / ${embedTotal.toLocaleString()}</span>
                    </div>
                    <div class="work-row-bar"><div class="work-row-fill" style="width:${embedPct}%"></div></div>
                    <div class="work-row-detail">
                        <span>${embedSpeed > 0 ? formatRatePerMinute(embedSpeed) : (embedComplete ? 'Complete' : embedPaused ? 'Paused' : 'Waiting')}</span>
                        <span class="work-row-eta">${embedEta}</span>
                    </div>
                </div>
                <div class="work-row thumb">
                    <div class="work-row-head">
                        <span class="work-row-label">Preview Cache</span>
                        <span class="work-row-stat">${previewDone.toLocaleString()} / ${previewTotal.toLocaleString()}</span>
                    </div>
                    <div class="work-row-bar"><div class="work-row-fill" style="width:${previewPct}%"></div></div>
                    <div class="work-row-detail">
                        <span>${thumbSpeed > 0 ? formatRatePerMinute(thumbSpeed) : (previewComplete ? 'Complete' : thumbPaused ? 'Paused' : 'Waiting')}</span>
                        <span class="work-row-eta">${previewEta}</span>
                    </div>
                </div>
                <div class="work-row thumb">
                    <div class="work-row-head">
                        <span class="work-row-label">Original SSD Cache</span>
                        <span class="work-row-stat">${originalDone.toLocaleString()} / ${originalTotal.toLocaleString()}</span>
                    </div>
                    <div class="work-row-bar"><div class="work-row-fill" style="width:${originalPct}%"></div></div>
                    <div class="work-row-detail">
                        <span>${originalComplete ? 'Complete' : thumbPaused ? 'Paused' : pregen.active_phase === 'full' ? 'Warming' : 'Waiting'}</span>
                        <span class="work-row-eta">${originalEta}</span>
                    </div>
                </div>
            </div>
        `;
    }

    function pauseAllWork() {
        pauseEmbeddings();
        stopCachePregeneration();
    }

    function resumeAllWork() {
        resumeEmbeddings();
        startCachePregeneration();
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
        const originalsRemaining = Number(cs.pregen?.originals?.remaining || 0);
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
            ? (originalsRemaining > 0 ? 'Fast browsing cache is warm; originals are warming' : 'Fast browsing cache is fully warmed')
            : smWarm >= 95
                ? 'Grid browsing is warmed; loupe previews are still building'
                : 'photoArchive is building the fast cache';

        if (titleEl) {
            const building = mdWarm < 95 || originalsRemaining > 0;
            titleEl.innerHTML = headline + (building ? ' <span class="cache-building-dot"></span>' : '');
        }
        if (subtitleEl) {
            subtitleEl.textContent =
                `${profileLabel} priority · ${formatBytes(disk.used_bytes)} used of ${formatBytes(disk.limit_bytes)} on SSD`;
        }

        const card = (name, title) => {
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
            const remaining = name === 'full'
                ? Number(cs.pregen?.originals?.remaining || 0)
                : Number(cs.pregen?.phases?.[name]?.remaining || 0);
            const etaSeconds = name === 'full' ? cs.pregen?.original_eta_seconds : cs.pregen?.eta_seconds;
            const etaText = remaining > 0 && etaSeconds
                ? `<div class="cache-card-meta"><span>${remaining.toLocaleString()} remaining</span><span>ETA ${formatEta(etaSeconds)}</span></div>`
                : '';
            const availabilityText = actual.replacement_mode && staleCount > 0
                ? `${cached} refreshed · ${staleCount.toLocaleString()} older usable`
                : name === 'full'
                    ? `${cached} of ${total} cached`
                    : `${cached} of ${total} generated`;
            return `
                <div class="cache-friendly-card tier-${name} ${cls}">
                    <div class="cache-card-meta"><strong>${title}</strong><span>${state}</span></div>
                    <div class="cache-progress"><div class="cache-progress-fill" style="width:${pct}%"></div></div>
                    <div class="cache-card-meta"><span>${availabilityText}</span><span>${capacityText}</span></div>
                    ${etaText}
                </div>
            `;
        };
        el.innerHTML = [
            card('sm', 'Grid scrolling'),
            card('md', 'Loupe previews'),
            card('lg', 'High-res previews'),
            card('full', 'Original SSD cache'),
        ].join('');

        if (adviceEl) {
            const needs = (name) => Number(rec.tiers[name]?.full_archive_bytes || 0);
            const used = (name) => Number(diskTiers[name]?.used_bytes || 0);
            const instantBytes = needs('sm') + needs('md');
            const allHighResBytes = instantBytes + needs('lg');
            const originalsBytes = needs('full');
            const ssdBudget = Number(disk.limit_bytes || 0);
            const memoryBudget = Number(cs.memory?.limit_bytes || 0);

            // Budget bar segments (proportional to actual usage within the budget)
            const seg = (name) => ssdBudget > 0 ? Math.max(0, Math.min(100, used(name) / ssdBudget * 100)) : 0;
            const usedTotal = used('sm') + used('md') + used('lg') + used('full');
            const freePct = ssdBudget > 0 ? Math.max(0, 100 - (usedTotal / ssdBudget * 100)) : 100;

            // RAM assessment (short)
            const ramNote = memoryBudget >= 3 * 1024 * 1024 * 1024
                ? `${formatBytes(memoryBudget)} RAM — generous; recent previews stay hot.`
                : memoryBudget >= 1 * 1024 * 1024 * 1024
                    ? `${formatBytes(memoryBudget)} RAM — solid for browsing.`
                    : `${formatBytes(memoryBudget)} RAM — conservative; previews cycle out faster.`;

            adviceEl.innerHTML = `
                <div class="cache-budget-bar">
                    <div class="cache-budget-seg seg-sm" style="width:${seg('sm')}%"></div>
                    <div class="cache-budget-seg seg-md" style="width:${seg('md')}%"></div>
                    <div class="cache-budget-seg seg-lg" style="width:${seg('lg')}%"></div>
                    <div class="cache-budget-seg seg-full" style="width:${seg('full')}%"></div>
                </div>
                <div class="cache-budget-legend">
                    <span class="leg-sm">Grid ${formatBytes(used('sm'))}</span>
                    <span class="leg-md">Loupe ${formatBytes(used('md'))}</span>
                    <span class="leg-lg">High-res ${formatBytes(used('lg'))}</span>
                    <span class="leg-full">Originals ${formatBytes(used('full'))}</span>
                    <span class="leg-free">${formatBytes(Math.max(0, ssdBudget - usedTotal))} free</span>
                </div>
                <div class="cache-metrics">
                    <div class="cache-metric">
                        <div class="cache-metric-label">Everyday target</div>
                        <div class="cache-metric-value">${formatBytes(instantBytes)}</div>
                    </div>
                    <div class="cache-metric">
                        <div class="cache-metric-label">All previews</div>
                        <div class="cache-metric-value">${formatBytes(allHighResBytes)}</div>
                    </div>
                    <div class="cache-metric">
                        <div class="cache-metric-label">All originals</div>
                        <div class="cache-metric-value">${formatBytes(originalsBytes)}</div>
                    </div>
                </div>
                <div class="cache-ram-note">${ramNote}</div>
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
        // Embedding progress display moved to the work banner.
        // This function now only exists for any remaining per-element updates.
        if (!aiStatus) return;
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
        initBottomBarMeasurement();
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
            await loadCatalogSources();
            // Load scan folder and stats
            const statsRes = await fetch('/api/stats');
            const stats = await statsRes.json();
            const folderInput = document.getElementById('scan-folder');
            const totalEl = document.getElementById('scan-total-images');
            if (totalEl) totalEl.textContent = Number(stats.active_images ?? stats.total_images ?? 0).toLocaleString();
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
        return addCatalogSource();
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

    async function pauseEmbeddings() {
        setSettingsStatus('Pausing background embeddings...', 'muted');
        try {
            const res = await fetch('/api/ai/embeddings/pause', { method: 'POST' });
            const data = await res.json();
            if (!res.ok || !data.ok) {
                throw new Error(data.error || 'Could not pause embeddings');
            }
            renderAISettingsStatus(data.ai_status);
            setSettingsStatus('Embeddings paused. Current in-flight batch may finish first.', 'success');
        } catch (err) {
            setSettingsStatus(`Could not pause embeddings: ${err.message}`, 'error');
        }
    }

    async function resumeEmbeddings() {
        setSettingsStatus('Resuming background embeddings...', 'muted');
        try {
            const res = await fetch('/api/ai/embeddings/resume', { method: 'POST' });
            const data = await res.json();
            if (!res.ok || !data.ok) {
                throw new Error(data.error || 'Could not resume embeddings');
            }
            renderAISettingsStatus(data.ai_status);
            setSettingsStatus('Embeddings resumed.', 'success');
        } catch (err) {
            setSettingsStatus(`Could not resume embeddings: ${err.message}`, 'error');
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

    function showShortcuts() {
        document.getElementById('shortcut-overlay')?.classList.remove('hidden');
    }

    function hideShortcuts() {
        document.getElementById('shortcut-overlay')?.classList.add('hidden');
    }

    function handleShortcutOverlayKey(e) {
        const overlay = document.getElementById('shortcut-overlay');
        const overlayVisible = overlay && !overlay.classList.contains('hidden');
        if (overlayVisible && e.key === 'Escape') {
            e.preventDefault();
            e.stopImmediatePropagation();
            hideShortcuts();
            return;
        }
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
        if (e.key === '?' || (e.key === '/' && e.shiftKey)) {
            e.preventDefault();
            showShortcuts();
        }
    }

    document.addEventListener('keydown', handleShortcutOverlayKey);

    // ==================== PUBLIC API ====================

    return {
        initCompare,
        initLibrary,
        initRankings,
        initSettings,
        showShortcuts,
        hideShortcuts,
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
        toggleMetadataFilters,
        toggleFilter,
        toggleStar,
        findSimilar,
        toggleBatchMode,
        clearBatchSelection,
        batchExport,
        batchFlag,
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
        addCatalogSource,
        rescanCatalogSource,
        openRemoveSourceDialog,
        closeRemoveSourceDialog,
        removeCatalogSource,
        chooseCatalogFolder,
        toggleDirectoryBrowser,
        browseDirectory,
        browseDirectoryParent,
        selectBrowsedDirectory,
        useBrowsedDirectory,
        pauseEmbeddings,
        resumeEmbeddings,
        pauseAllWork,
        resumeAllWork,
        setLibraryView,
        openLightboxById,
    };
})();
