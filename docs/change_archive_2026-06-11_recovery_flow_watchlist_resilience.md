# 2026-06-11 Recovery Flow and Watchlist Resilience

## Goal

Follow the paper recovery sequence without bypassing Gateway budget, execution
quality, risk, cost, or submit gates, while keeping the candidate universe
stable during network failures.

## Recovery Actions

- Replaced the Supervisor process that had been running since 2026-06-08 and
  was still writing the pre-recovery artifact schema.
- Refreshed supervisor preflight locally:
  - PASS: `29`
  - WARN: `0`
  - FAIL: `0`
- Rebuilt auto-order readiness and dashboard artifacts with the current schema.
- Detected and stopped two concurrent Supervisor processes that were writing the
  same scoped artifacts and could issue duplicate Gateway requests.
- Added an OS-level scoped instance lock. A duplicate Supervisor now exits
  before starting dashboard control, scheduling, or any IBKR task.
- Confirmed that no report, opportunity scan, execution, or order submission is
  currently eligible.
- Current target remains `US:watchlist / SPLG`, with submit quality `PASS`.
- Current Gateway budget recovery projection is
  `2026-06-13T23:59:59.999999+00:00`, or 2026-06-14 09:59:59 in Sydney.

## Watchlist Resilience

- `refresh_watchlist` now preserves the last-known-good resolved watchlist when
  all configured dynamic sources fail.
- Partial source failures also preserve the existing list when the proposed
  replacement falls below the configured retention ratio.
- A successful refresh writes through a temporary file and atomically replaces
  the destination.
- Source success and failure counts are included in newly written resolved
  watchlists.
- The default retention ratio is `0.6` and can be overridden per source config.

## Artifact Noise Reduction

- Auto-order readiness signatures now ignore continuously changing age-hour
  values.
- Status, blocker, recovery eligibility, policy, and dependency changes still
  trigger immediate rewrites.
- Artifact staleness still forces a rewrite at the configured maximum age.
- This prevents the 30-second Supervisor loop from rebuilding readiness and the
  dashboard when only fractional age values changed.
- Runtime verification confirmed that the one expected second-cycle summary
  rewrite was caused by the completed watchlist refresh state transition, not
  continuous time drift.

## Safety Boundary

- Paper-only.
- No order submission.
- No Gateway-backed report or execution before budget evidence clears.
- Broker snapshots show active US/HK positions, so risk guard remains exempt
  from recovery throttling. It is a protective request path, not a return
  generation path.
- Snapshot outcome labeling is now paused while the recovery plan is active
  because it can fan out into market-data requests. The local weekly review
  remains enabled.
- No risk, edge, cost, liquidity, market-rule, or submit-quality relaxation.
- No automatic promotion of new stock or ETF symbols.
- Existing local resolved HK watchlist edits remain outside this commit.

## Validation

- Supervisor preflight: `29 PASS / 0 WARN / 0 FAIL`.
- Integration tier: `115 passed, 558 deselected`.
- Recovery, readiness, instance-lock, and watchlist focus tests:
  `14 passed, 107 deselected`.
- Recovery labeling follow-up tests: `5 passed, 110 deselected`.
- `python -m pip check`: no broken requirements.
- Python compile and `git diff --check`: passed.
