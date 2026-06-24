# 2026-06-24 Stale Refresh Supervisor Recovery

## Context

The previous growth-aware stale refresh ranking selected `HK:resolved_hk_top100_tech_growth` as the next no-submit refresh target. However, Supervisor only consumed `recovery_plan`, while `stale_execution_refresh_plan` primarily fed artifacts and dashboard blocks.

That meant the ranking was visible but not guaranteed to drive the runtime flow.

## Change

- `build_auto_order_recovery_plan` now accepts `stale_execution_refresh_plan`.
- When no quality-passing submit frontier exists and stale refresh is ready, recovery emits:
  - `status=stale_execution_refresh_required`
  - `primary_action=refresh_stale_execution_target_no_submit`
  - one target market / portfolio
  - `gateway_refresh_portfolio_limit=1`
  - `estimated_gateway_refresh_count=1`
- `evaluate_auto_order_recovery_eligibility` now allows this stale refresh state even when target submit quality is `NO_ORDERS` or `NO_BUY_ORDERS`, because the action is no-submit evidence refresh.
- Supervisor now builds the stale refresh plan inside `_auto_order_recovery_context`.
- Supervisor converts an eligible stale refresh into the existing recovery checkpoint format to prevent repeated refresh attempts every poll cycle.
- Supervisor action routing now force-runs only the selected target report and execution dry-run.

## Current Evidence

After refreshing read-only auto-order readiness:

- `recovery_plan.status=stale_execution_refresh_required`
- `target_market=HK`
- `target_portfolio_id=HK:resolved_hk_top100_tech_growth`
- `recovery_eligibility.eligible=true`
- `allowed_actions=["generate_investment_report", "run_investment_execution_no_submit"]`
- `submit_orders=false`

## Safety Boundary

This does not submit orders and does not relax any gate:

- risk gate unchanged
- edge gate unchanged
- cost gate unchanged
- liquidity gate unchanged
- market-rule gate unchanged
- Gateway budget gate unchanged
- submit-quality gate unchanged

The only enabled runtime action is a single target report plus execution dry-run refresh.

## Validation

Targeted recovery tests:

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_supervisor_cli.py -k 'auto_order_recovery or stale_execution_refresh_plan or stale_execution_refresh'
16 passed, 168 deselected
```

## Next Step

Let Supervisor run the eligible target-scoped no-submit refresh, or run a controlled single-cycle recovery window. Review the refreshed execution evidence before any paper submit.
