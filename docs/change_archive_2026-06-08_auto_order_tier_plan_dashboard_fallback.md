# 2026-06-08 Auto-Order Tier Plan and Dashboard Fallback

## Goal

Improve automatic order planning visibility for different account sizes while
keeping the current safety model intact.

## Changes

- Added a display-only fallback in dashboard v2 so legacy auto-order readiness
  artifacts can derive seed proposal/source metrics from the canonical
  watchlist expansion summary.
- Added `account_growth_tier_plan` to watchlist expansion summary.
- Exposed small/medium/large account growth path fields to dashboard v2:
  profile, primary action, expansion mode, submit frequency mode, max orders per
  run, and max order value.
- Reused `account_profile_summary()` in watchlist expansion CLI instead of
  duplicating partial account profile fields.

## Current Small-Account Plan

- Profile: `small`
- Equity: `1000 AUD`
- Max orders per run: `1`
- Max order value: `100 AUD`
- Expansion mode: `whole_share_tradable_etf_first`
- Submit frequency mode: `single_small_limit_order_until_fill_quality_passes`
- Primary action: `verify_seed_etfs_in_candidate_report_before_submit`

## Safety Boundary

- No order submission.
- No symbol-master mutation.
- No automatic promotion of review seed candidates.
- No relaxation of risk, edge, cost, liquidity, data-quality, or submit gates.

## Validation

- `55 passed` for watchlist expansion, dashboard blocks, auto-order readiness,
  account profile, and dashboard helper tests.
- Regenerated watchlist expansion and dashboard artifacts.
- Dashboard now preserves six ASX/HK/XETRA review seed source candidates even
  when an older readiness artifact lacks seed source fields.

