# 2026-06-24 Growth-Aware Stale Refresh Ranking

## Context

Auto-order readiness remains blocked for submit, but the stale execution refresh plan is the next safe step because it is no-submit evidence collection.

Before this change, a high-scoring US stale artifact could become the primary refresh target even when the same row had `gateway_budget_degraded`. That made the plan wait for US Gateway budget recovery instead of using available request capacity on another market with growth candidates.

## Change

- `build_stale_execution_refresh_plan` now sorts non-Gateway-hard-blocked rows before Gateway-hard-blocked rows.
- Rows with post-cost positive candidates, close `WAIT_PULLBACK` candidates, or a current BUY plan are tagged as `growth_refresh_candidate`.
- SELL-only stale plans get an explicit `sell_only_current_plan` flag and a small score penalty.
- Refresh rows now expose:
  - `ranking_bucket`
  - `planned_sell_order_value`
  - `has_current_buy_plan`
  - `sell_only_current_plan`
  - `growth_candidate_supply`
- The auto-order readiness markdown stale refresh table now shows bucket, Gateway blocked, buy, and sell columns.

## Current Evidence

After refreshing the read-only artifact:

- `stale_execution_refresh_plan.status=READY_FOR_TARGETED_NO_SUBMIT_REFRESH`
- `primary_market=HK`
- `primary_portfolio_id=HK:resolved_hk_top100_tech_growth`
- `primary_score=144`
- `submit_orders=false`

US `US:watchlist` still has a higher raw score, but it is Gateway-hard-blocked and sell-only in the stale plan, so it is no longer the primary no-submit refresh target.

## Safety Boundary

This change does not submit orders and does not relax any trading gate:

- risk gate unchanged
- edge gate unchanged
- cost gate unchanged
- liquidity gate unchanged
- market-rule gate unchanged
- Gateway budget gate unchanged
- submit-quality gate unchanged

The plan remains read-only and paper-only. It only selects the next target for a single report plus execution dry-run refresh.

## Validation

Targeted test suite:

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_dashboard_blocks.py -k 'stale_execution_refresh_plan or auto_order_readiness'
57 passed, 9 deselected
```

## Next Step

Run one targeted no-submit refresh for `HK:resolved_hk_top100_tech_growth` only when operationally acceptable. Do not submit orders unless the refreshed artifact produces a BUY plan and all submit gates pass.
