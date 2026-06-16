# 2026-06-16 HK Outcome Validation and Supervisor Shutdown Diagnostics

## Scope

This change validates the current HK positive post-cost candidates and close `WAIT_PULLBACK` rows against mature 5/20d outcome evidence, then hardens Supervisor shutdown diagnostics.

## Changes

- Extended `opportunity_calibration` to retain bounded evidence rows for:
  - positive post-cost candidates
  - close `WAIT_PULLBACK` candidates
- Added outcome validation helpers that compare a current candidate symbol group against mature historical weekly unified evidence.
- Added `src/tools/review_opportunity_outcomes.py`, an on-demand tool that streams `weekly_unified_evidence.csv`, filters only relevant market/portfolio/symbol rows, and writes:
  - `opportunity_outcome_validation.json`
  - `opportunity_outcome_validation.csv`
  - `opportunity_outcome_validation.md`
- Added Supervisor lifecycle diagnostics:
  - `supervisor_shutdown_status.json`
  - structured `started/running/stopping/stopped/crashed` status
  - signal name capture
  - SIGHUP handling that keeps the Supervisor alive when a foreground terminal or PTY detaches
- SIGINT/SIGTERM still stop Supervisor gracefully.

## HK Validation Result

Command used:

```bash
python -m src.tools.review_opportunity_outcomes \
  --market HK \
  --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json \
  --weekly_unified_evidence reports_investment_weekly/weekly_unified_evidence.csv \
  --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor
```

Current result:

- `HK:resolved_hk_top100_bluechip` positive post-cost group: 5 symbols matched, 1,115 mature 5d samples, 878 mature 20d samples, average 5d outcome `141.120058` bps, average 20d outcome `245.529243` bps.
- `HK:resolved_hk_top100_tech_growth` positive post-cost group: 5 symbols matched, 1,134 mature 5d samples, 897 mature 20d samples, average 5d outcome `140.871289` bps, average 20d outcome `253.727164` bps.
- `HK:resolved_hk_top100_bluechip` close `WAIT_PULLBACK` group: 5 symbols matched, 903 mature 5d samples, 666 mature 20d samples, average 5d outcome `133.66323` bps, average 20d outcome `307.692041` bps.
- `HK:resolved_hk_top100_tech_growth` close `WAIT_PULLBACK` group: 5 symbols matched, 920 mature 5d samples, 683 mature 20d samples, average 5d outcome `134.116433` bps, average 20d outcome `313.606113` bps.

All four HK validation groups are currently `OUTCOME_SUPPORTS_GROUP`.

## Interpretation

- This is historical same-symbol validation, not a claim that the latest 2026-06-16 candidate snapshot already has mature future 5/20d outcomes.
- The current HK positive post-cost and close `WAIT_PULLBACK` symbols are not obviously bad groups. The right next action is to keep gates intact and monitor realized outcomes/fills as fresh samples mature.
- This does not justify lowering risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gates.

## Shutdown Diagnosis

The previous Supervisor lifecycle made “automatic shutdown” hard to diagnose because shutdown reason was only visible in terminal output, and local log files were empty. The code also did not handle SIGHUP, so a terminal/PTY disconnect could terminate a foreground Supervisor with little persistent evidence.

Now the system writes `supervisor_shutdown_status.json` and ignores SIGHUP while still honoring SIGINT/SIGTERM. If the process exits from an unhandled exception, the status artifact records `crashed` with the exception class.

## Verification

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider \
  tests/test_opportunity_calibration.py \
  tests/test_review_opportunity_outcomes.py \
  tests/test_supervisor_shutdown_status.py
```

Result: `11 passed`.
