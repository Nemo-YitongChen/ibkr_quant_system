# 2026-06-15 Bounded Execution Evidence Maintenance

## Goal

Keep paper auto-order diagnostics current without turning recovery into a
high-request workflow or allowing stale Gateway failures to block ordering
indefinitely.

## Changes

- Added a pure execution-evidence maintenance planner.
- The planner only considers portfolios that are:
  - paper accounts;
  - execution-enabled;
  - backed by a fresh existing investment report;
  - currently marked `STALE` or `DEGRADED_GATEWAY`;
  - on a market with available execution reserve.
- `CN` remains excluded through the existing auto-order market policy.
- At most one portfolio can be selected per maintenance cycle.
- The selected execution always runs with:
  - `submit_investment_execution=false`;
  - `--recovery_evidence_only`;
  - no report generation;
  - no opportunity scan;
  - no risk, edge, cost, market-rule, or submit-gate relaxation.
- Maintenance is disabled while a recovery checkpoint is active, after any
  normal execution in the same cycle, or while any configured trading market
  is open.
- The existing live Gateway budget decision is checked again immediately
  before process launch, protecting against capacity changes after planning.
- Maintenance state is written atomically to
  `execution_evidence_maintenance.json`.
- Auto-order readiness and dashboard v2 now expose the latest maintenance
  status, reason, target market, target portfolio, and `submit_orders=false`.
- Candidate selection and state persistence live in a support module rather
  than adding policy logic to the Supervisor run loop.

## Real Runtime Evidence

Before enabling the automatic path, a real ASX target-scoped execution dry-run
was performed with `--recovery_evidence_only`:

- Gateway connected successfully on paper;
- `submitted=false`;
- `order_count=0`;
- `consumes_submit_slot=false`;
- the old `IBKR_GATEWAY_UNAVAILABLE` artifact was replaced by a fresh
  `BLOCKED_OPPORTUNITY` diagnosis;
- the leading `AZJ.AX` intent passed the configured edge gate but remained
  blocked by `WAIT_PULLBACK`, so no order was submitted.

The current planner preview selects
`HK:resolved_hk_top100_bluechip` first because its artifact is still
`DEGRADED_GATEWAY`. It will not run until all configured markets are closed and
execution reserve remains available.

## Safety Boundary

This maintenance path improves evidence freshness, not expected return by
itself. It cannot:

- submit an order;
- consume a normal submit slot;
- increase paper submit capacity;
- add a watchlist seed to the tradable universe;
- bypass strategy, opportunity, risk, execution, market-rule, or post-cost
  quality gates.

## Validation

- Full test suite: `722 passed`.
- Integration tier: `125 passed`.
- Guardrail tier: `26 passed`.
- Focused execution-evidence and dashboard tests: passed.
- `python -m py_compile`: passed.
- `pip check`: no broken requirements.

## Next Technical Step

After stale/degraded execution artifacts have been refreshed, rank the remaining
real blockers by market:

1. calibrate `WAIT_PULLBACK` and opportunity anchors using blocked-vs-allowed
   ex-post outcomes;
2. calibrate expected cost and edge margins without reducing the minimum safety
   thresholds;
3. promote only review seeds with fresh candidate reports, valid IBKR mapping,
   whole-share affordability, and positive post-cost edge;
4. increase paper order frequency only after fill, slippage, realized-edge, and
   error-rate evidence unlocks the existing trial capacity stage.
