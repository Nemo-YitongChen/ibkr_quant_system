# 2026-06-08 Review Seed Source Registry

## Goal

Expand the small-account candidate supply for ASX, HK, and XETRA without
automatically promoting symbols into the execution universe or relaxing risk,
edge, cost, liquidity, data-quality, or submit gates.

## Changes

- Added `config/watchlist_seed_sources.yaml` as a review-only source registry.
- Added two ETF candidates each for ASX, HK, and XETRA, with source URL,
  verification date, rationale, and IBKR mapping status.
- Added source-registry intake to watchlist expansion and generated review files
  under `reports_supervisor/watchlist_expansion/seed_review/`.
- Added report-layer scoring support for review seed candidates.
- Forced review seed candidates to `WATCH` and `execution_ready=0` until manual
  promotion completes broker mapping and trading-quality review.
- Added source-candidate counts and markets to auto-order readiness and dashboard
  evidence.
- Added a Supervisor fallback so scoped runtime readiness can consume the
  canonical watchlist expansion artifact.

## Safety Boundary

- `auto_apply=false`
- `review_only=true`
- No symbol-master mutation
- No order submission
- No risk, edge, cost, liquidity, or submit-gate relaxation

The registry is candidate research input, not a recommendation to trade.

## Current Evidence

- Source candidates: `6`
- Source markets: `ASX`, `HK`, `XETRA`
- Watchlist expansion selected count: `0`
- Auto-order readiness: `blocked`
- Primary blocker: `ibkr_gateway_unavailable`
- Nearest submit frontier: US `SPLG`, one-share limit plan, quality `PASS`,
  blocked by stale preflight

## Validation

- Python compile checks passed.
- Focused strategy, evidence, dashboard, workflow, and Supervisor tests:
  `149 passed`.
- Refreshed watchlist expansion, auto-order readiness, and dashboard artifacts.
- Dashboard retained all six source candidates after scoped refresh.

