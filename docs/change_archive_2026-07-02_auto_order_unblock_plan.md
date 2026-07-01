# 2026-07-02 Auto-order Next Unblock Plan

## Summary

Auto-order readiness now emits a single `unblock_plan` contract that states the
next non-submit action required to move automated paper submit forward. The plan
is derived from existing submit, recovery, frequency, and remediation evidence.
It does not create new submit authority and does not relax any risk, edge,
cost, liquidity, market-rule, Gateway budget, or submit-quality gate.

## Changes

- Added `build_auto_order_unblock_plan` in `src.common.auto_order_readiness`.
- `build_auto_order_readiness_summary` now includes `summary.unblock_plan`.
- `review_auto_order_readiness.md` now includes a `Next Unblock Plan` section.
- Dashboard v2 Auto Order block now exposes:
  - `unblock_plan_status`
  - `unblock_plan_primary_action`
  - `unblock_plan_phase`
  - `unblock_plan_source`
  - `unblock_plan_requires_gateway`
  - `unblock_plan_submit_orders`
  - `unblock_plan_target_market`
  - `unblock_plan_target_portfolio_id`
- Dashboard can build a fallback unblock plan from legacy artifacts that do not
  yet include `summary.unblock_plan`.

## Decision Rules

- If a safe paper submit candidate is already selected, the plan is
  `submit_review_ready` and does not refresh evidence.
- If Supervisor runtime is stale or missing code revision, the plan is
  `runtime_restart_required` and requires no IBKR Gateway request.
- If weekly review is stale or missing, the plan is
  `local_weekly_review_required` and requires no IBKR Gateway request.
- If preflight is stale or failed, the plan is `local_preflight_refresh_required`.
- If Gateway is unavailable or over budget, the plan points to Gateway restore
  or wait actions before any targeted dry-run refresh.
- If no operational unblock exists, candidate supply and manual review actions
  remain explicitly non-submit.

## Trading Impact

This is a control-plane clarity improvement. It reduces the chance that the
system consumes Gateway request budget on the wrong recovery path, and makes the
highest-priority unblock visible in JSON, Markdown, and dashboard surfaces.

The plan is intentionally conservative: `submit_orders=false`,
`does_not_submit_orders=true`, and `does_not_change_submit_decision=true`.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_dashboard_blocks.py`
  - `77 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x`
  - `779 passed`
