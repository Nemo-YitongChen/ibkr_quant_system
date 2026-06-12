# Gateway Request Lanes And Nonblocking Evidence Maintenance

Date: 2026-06-12

## Objective

Prevent historical research-request overuse and stale evidence maintenance from globally pausing paper order submission.

## Changes

- Added `execution`, `protective`, `research`, and `unknown` request lanes to IBKR telemetry.
- Added independent weekly execution/protective reserves plus 24-hour and 10-minute research controls.
- Preserved the seven-day total budget as health and attribution evidence.
- Changed auto-order readiness so a degraded total budget blocks submit only when the new artifact explicitly reports `submit_blocking=true`.
- Kept backward compatibility: old degraded artifacts without the new field still block.
- Added a Supervisor pre-request governor that recomputes local telemetry before Gateway-backed subprocess launch.
- Added a five-minute local artifact refresh so dashboard evidence does not depend on weekly review or recovery.
- Research throttling suppresses report/opportunity-style request paths without consuming the execution reserve.
- Protective broker snapshot and guard paths remain available.
- Execution is blocked only when its own weekly or short-window reserve is exhausted.
- Reclassified stale frontier evidence from global recovery to target-scoped, nonblocking evidence maintenance.
- Evidence maintenance forces one target report and one no-submit execution refresh while other portfolios continue normal scheduling.

## Evidence

The local evidence refresh at implementation time reported:

- Seven-day Gateway requests: `9084`
- Markets over the historical total budget: `4`
- Markets blocking submit: `0`
- Markets currently research-throttled: `0`
- US execution requests: `0 / 300`
- US research requests in the latest 24 hours: `0 / 129`
- US short-window requests: `0 / 50`

Auto-order readiness no longer reports `gateway_budget_degraded` as a hard block. Historical budget pressure remains visible as `gateway_budget_warning` or `gateway_budget_research_degraded`.

The highest-quality frontier remained `US:watchlist / SPLG`, with submit quality `PASS`. Its remaining blocker was stale execution evidence, so the generated plan became:

- `status=evidence_maintenance_required`
- `recovery_eligibility.active=false`
- `maintenance_active=true`

Supervisor then completed the target-scoped refresh:

- IBKR connection succeeded.
- One no-submit order plan was produced.
- Planned notional was approximately `29.14 AUD`.
- The refresh did not consume the normal submit slot.

The fresh report changed the top order from the prior SPLG evidence to VEA. The new execution evidence reported:

- `primary_no_order_reason=MARKET_CLOSED_FOR_SUBMIT`
- net edge `7.80 bps`
- edge margin `1.80 bps`

Those edge values were below the configured submit thresholds, so the system correctly kept the order blocked. Gateway request budget was not the blocker.

## Safety

- No risk, edge, cost, liquidity, market-rule, submit-quality, or paper/live boundary was relaxed.
- Evidence maintenance never submits orders.
- Other ready portfolios are not suppressed by one target's stale evidence.
- Old evidence schemas remain fail-safe.
- All request-budget calculations are local telemetry reads and add no IBKR Gateway traffic.

## Verification

- Telemetry, budget, readiness, and weekly helper tests passed.
- Guardrail tier passed.
- Supervisor tests cover lane export, research throttling, execution/protective preservation, and non-target scheduling during evidence maintenance.
