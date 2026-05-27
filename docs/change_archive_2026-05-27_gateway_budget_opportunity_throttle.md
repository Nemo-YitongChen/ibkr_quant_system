# Change Archive: Gateway Budget Opportunity Throttle

Date: 2026-05-27

## Context

The current auto-order readiness frontier shows a tradable US whole-share ETF candidate, but every auto-submit candidate is blocked by degraded IBKR Gateway request budgets. Weekly telemetry shows the highest request source is `run_investment_opportunity:* / positions`.

## Changes

- Added a supervisor-level opportunity throttle driven by weekly `ibkr_gateway_budget_rows`.
- When a market's Gateway budget row is `degraded`, supervisor skips high-request opportunity scans for that market and records `gateway_budget_degraded` in `opportunity_skip_reasons`.
- Added config defaults in `config/supervisor.yaml`:
  - `suppress_opportunity_when_degraded: true`
  - `suppress_opportunity_statuses: ["degraded"]`
- Added integration tests for both the degraded-budget skip path and the policy-disable path.

## Trading Impact

This does not loosen risk, edge, market-rule, or order-submit gates. It reduces redundant Gateway load so the request budget can recover, which is required before the existing high-quality paper submit candidates can move from blocked to actionable.

## Next Step

After the projected Gateway recovery time, rerun weekly review and auto-order readiness. If budget status returns to `ok` or `warning`, prioritize the frontier candidate with `submit_quality_status=PASS` and small whole-share order value.
