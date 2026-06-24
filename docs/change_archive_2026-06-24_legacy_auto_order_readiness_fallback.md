# 2026-06-24 Legacy Auto-Order Readiness Fallback

## Context

The active Supervisor process was still an older long-running process and wrote
a legacy `auto_order_readiness.json` without top-level `submit_plan` and
`stale_execution_refresh_plan` fields. That made the dashboard lose the ranked
no-submit stale execution refresh target even though the readiness rows still
contained enough evidence to calculate it.

## Change

- `dashboard_blocks.build_auto_order_readiness_block` now backfills
  `stale_execution_refresh_plan` from the readiness rows when the summary field
  is missing.
- The fallback uses the existing
  `build_stale_execution_refresh_plan` implementation from
  `src.common.auto_order_readiness`.
- The fallback annotates the plan with
  `source=dashboard_legacy_readiness_fallback`.
- The contract remains read-only:
  - `paper_only=true`
  - `submit_orders=false`
  - `does_not_relax_submit_gates=true`

## Runtime Verification

After regenerating the dashboard against the legacy artifact, the Auto Order
block exposed:

- `stale_execution_refresh_status=WAIT_GATEWAY_BUDGET`
- `stale_execution_refresh_target_count=6`
- `stale_execution_refresh_primary_market=US`
- `stale_execution_refresh_primary_portfolio_id=US:watchlist`
- `stale_execution_refresh_primary_score=214`
- `stale_execution_refresh_submit_orders=0`

Then `review_auto_order_readiness` was run with the latest code and wrote a new
schema artifact containing `summary.stale_execution_refresh_plan` directly.
The dashboard no longer needed the fallback source.

## Current Trading State

The strongest current blocker is `gateway_budget_degraded`, not missing outcome
evidence. The Gateway budget status is degraded with maximum usage around
`784%`; automated submit remains blocked. The next action is to keep high-request
paths suppressed until budget recovers, then run one ranked no-submit stale
execution refresh before considering paper submit.
