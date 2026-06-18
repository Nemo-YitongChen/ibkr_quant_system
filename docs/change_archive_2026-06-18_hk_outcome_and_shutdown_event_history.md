# 2026-06-18 HK Outcome Validation and Supervisor Shutdown Event History

## Context

The active objective remains improving paper auto-ordering quality and frequency without bypassing risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gates. This change validates current HK opportunity evidence and improves Supervisor shutdown diagnostics.

## HK outcome validation

- Ran HK-only `review_opportunity_outcomes` against current `market_readiness.json` and `reports_investment_weekly/weekly_unified_evidence.csv`.
- Output was written to `/private/tmp/ibkr_hk_outcome_validation/` to avoid replacing the all-market dashboard artifact.
- HK positive post-cost candidates remain supported by mature historical outcomes:
  - `HK:resolved_hk_top100_bluechip`: 878 mature 5d samples, 692 mature 20d samples, avg 5d `+138.32bps`, avg 20d `+307.90bps`.
  - `HK:resolved_hk_top100_tech_growth`: 895 mature 5d samples, 709 mature 20d samples, avg 5d `+138.69bps`, avg 20d `+313.60bps`.
- HK close `WAIT_PULLBACK` groups also remain positive overall:
  - `HK:resolved_hk_top100_bluechip`: 1,331 mature 5d samples, 1,002 mature 20d samples, avg 5d `+125.96bps`, avg 20d `+212.07bps`.
  - `HK:resolved_hk_top100_tech_growth`: 1,355 mature 5d samples, 1,020 mature 20d samples, avg 5d `+126.74bps`, avg 20d `+222.09bps`.
- The group result does not justify broad relaxation. `1288.HK` and `2359.HK` have negative per-symbol close `WAIT_PULLBACK` outcome evidence, so any near-entry trial must stay symbol-aware, paper-only, small, limit-only, whole-share feasible, and post-cost positive.

## Current auto-order implication

- HK evidence supports continuing paper-only calibration review for:
  - HK post-cost threshold paper trial.
  - HK close `WAIT_PULLBACK` near-entry limit paper trial.
- Current HK auto-submit remains blocked by non-outcome gates:
  - execution artifact stale,
  - no BUY order in the current execution plan,
  - stale strategy suggestion,
  - Gateway research budget degraded.
- This archive does not change YAML, does not submit orders, and does not loosen any submit gate.

## Supervisor shutdown diagnostics

- Current runtime evidence shows Supervisor is still running and the latest status is `running / ignored_signal:SIGHUP`.
- `SIGHUP` is likely caused by terminal/session disconnect and is intentionally ignored by current code.
- Added append-only `supervisor_shutdown_events.jsonl` next to `supervisor_shutdown_status.json` so future shutdown diagnosis has event history instead of only the latest overwritten status.
- Fixed final shutdown status selection so an exception path remains `crashed` and is not overwritten by the `finally` cleanup status `stopped`.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_shutdown_status.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_opportunity_outcomes.py`

Both targeted suites passed.
