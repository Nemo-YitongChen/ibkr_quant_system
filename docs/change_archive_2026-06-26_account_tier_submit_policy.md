# 2026-06-26 Account-tier submit policy context

## Scope

Connected existing account growth tier evidence to auto-order submit planning so account-size strategy is enforced and visible in the same place that ranks submit candidates.

This change does not submit orders, does not increase configured submit capacity, and does not relax risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gates.

## What changed

- `build_auto_order_submit_plan` accepts `account_growth_tier_plan`.
- Effective per-portfolio order count and gross order value caps are now the stricter of:
  - the configured `auto_order_readiness` policy; and
  - the current account growth tier plan.
- The submit policy snapshot now retains both effective values and configured values for audit:
  - `max_submit_gross_order_value`
  - `configured_max_submit_gross_order_value`
  - `account_growth_profile`
  - `account_growth_max_orders_per_run`
  - `account_growth_max_order_value`
- Rejected candidates now explain account-tier blocks explicitly:
  - `account_growth_order_count_exceeds_profile`
  - `account_growth_order_value_exceeds_profile`
- `build_auto_order_readiness_summary` passes `watchlist_expansion_summary.account_growth_tier_plan` into the submit plan.
- Supervisor direct submit-plan and recovery-plan paths use the same account-tier context.
- Dashboard Auto Order block surfaces the effective account-growth submit caps directly.

## Trading interpretation

For the current small-account profile, the intended path remains:

- whole-share tradable ETF first;
- one small limit order per run;
- approximately `100 AUD` maximum gross order value;
- require BUY leg for growth submit;
- require paper evidence before any capacity increase.

This improves automatic order quality control and frequency governance. It does not make the system submit more often by bypassing gates; it makes future frequency increases depend on account tier and realized evidence.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_dashboard_blocks.py`
