# Deferred Refactor Notes

## Phase 2

- Decide whether the frontend should move to native ES modules or keep ordered script tags until the state objects are smaller.
- Pick a router import strategy before splitting FastAPI endpoints so route aliases like `/rankings` and `/library` stay explicit.
- Migrate tests incrementally only after the API-shape suite covers the current route contracts.
- Revisit cache placement once helper ownership is stable, especially the line between app-level visibility rules and thumbnail cache internals.
- Audit fetch-wrapper edge cases before centralizing requests: aborted generations, warm-cache reads, stale search state, and non-JSON error responses.

## Phase 3

- Split FastAPI routers after Phase 1/2 have settled.
- Split JS modules after feature state has clear ownership boundaries.
- Prefer native ES modules unless later review shows ordered scripts are simpler for this app.
