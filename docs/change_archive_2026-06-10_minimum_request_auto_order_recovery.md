# 2026-06-10 Minimum-Request Auto-Order Recovery

## Goal

Increase the chance of reaching a safe paper submit frontier without increasing
IBKR Gateway load or relaxing execution-quality gates.

## Changes

- Added structured Gateway budget telemetry fields to each auto-order readiness
  row.
- Added `build_auto_order_recovery_plan()` to produce a target-scoped,
  minimum-request recovery sequence.
- Recovery planning now selects only the highest-quality `PASS` submit frontier.
- Added dashboard v2 recovery metrics and structured recovery steps.
- Added a legacy fallback so a long-running older Supervisor can still expose
  the new recovery plan from existing rows and submit frontier data.
- Added recovery-plan rendering to the auto-order readiness markdown report.
- Preserved projected Gateway recovery timestamps from legacy hard-block detail
  strings.

## Current Recovery Plan

- Status: `wait_gateway_budget`
- Target: `US:watchlist`
- Symbol: `SPLG`
- Target quality: `PASS`
- Target net edge: approximately `10.84 bps`
- Target edge margin: approximately `4.84 bps`
- Gateway budget recovery estimate: `2026-06-12T23:59:59.999999+00:00`
- Gateway-backed refresh limit: `1` portfolio

Sequence:

1. Hold high-request scans until the US Gateway request budget recovers.
2. Refresh only `US:watchlist` report and execution in no-submit mode.
3. Rebuild market readiness, auto-order readiness, and dashboard locally.

## Completed Recovery Action

Supervisor preflight was refreshed locally:

- PASS: `29`
- WARN: `0`
- FAIL: `0`

This removed `preflight_stale` from the current auto-order frontier without
issuing orders or running broad market scans.

## Safety Boundary

- Paper-only.
- No order submission.
- No automatic review-seed promotion.
- No risk, expected-edge, cost, liquidity, data-quality, or submit-gate
  relaxation.
- No all-market Gateway refresh while request budget is degraded.

## Validation

- Focused auto-order, dashboard, dashboard-helper, and workflow smoke tests:
  `45 passed`.
- Full integration tier: `110 passed, 551 deselected`.
- Python compile checks passed.
- Regenerated readiness and dashboard artifacts.
- Verified dashboard legacy fallback after the older running Supervisor
  overwrote the readiness artifact.
