# 2026-06-29 Seed Quality Feedback Refactor

This change centralizes seed promotion quality rejection analysis so watchlist
expansion, auto-order readiness, and dashboard blocks all explain the same
candidate-source replacement action.

## What changed

- Added `summarize_seed_promotion_quality()` in
  `src/common/watchlist_expansion.py`.
- `summarize_watchlist_expansion()` now emits:
  - `seed_quality_feedback`
  - `seed_promotion_quality_reason_counts`
  - `seed_promotion_primary_quality_reason`
  - `seed_replacement_primary_action`
- `build_auto_order_frequency_plan()` now consumes the same feedback instead
  of duplicating quality-rejection aggregation.
- Dashboard v2 exposes the replacement action in the Auto Order and Watchlist
  Expansion blocks.

## Current interpretation

When all seed candidates are quality rejected, the system now returns a
reason-specific replacement action. For example:

- `score_below_min` -> `source_higher_score_candidates`
- `expected_edge_below_min` -> `source_candidates_with_stronger_expected_edge`
- `whole_share_not_tradable` -> `require_whole_share_tradability_precheck`
- `expected_cost_above_max` -> `source_lower_cost_candidates`

The default fallback remains
`source_higher_quality_lower_cost_seed_candidates` when no precise reason is
available.

## Trading boundary

This change does not promote symbols, change watchlists automatically, submit
orders, or relax risk/edge/cost/liquidity/market-rule/Gateway/submit-quality
gates. It only makes the next source-improvement action explicit when candidate
supply is the blocker.
