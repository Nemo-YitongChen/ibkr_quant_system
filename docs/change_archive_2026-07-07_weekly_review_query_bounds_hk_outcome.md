# 2026-07-07 Weekly Review Query Bounds and HK Outcome Revalidation

## Summary

- Revalidated HK opportunity outcome groups from the current `market_readiness.json` against the existing streamed `weekly_unified_evidence.csv`.
- Added bounded query windows to weekly review audit reads so `fills`, `risk_events`, and `investment_positions` no longer fall back to unbounded full-table reads.
- Added configurable weekly review windows for candidate outcome calibration and position baselines.
- Added Storage query indexes for weekly/evidence paths that were exposed by the 37GB `audit.db` runtime.

## HK Outcome Result

- Fresh HK-only validation artifact: `runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/opportunity_outcome_validation.json`.
- `positive_post_cost_candidates` remains weak/mixed:
  - `HK:resolved_hk_top100_bluechip`: `0005.HK,2359.HK`, 5d `+65.87bps`, 20d `-129.20bps`.
  - `HK:resolved_hk_top100_tech_growth`: `0005.HK,2359.HK`, 5d `+72.54bps`, 20d `-127.94bps`.
- `close_wait_pullback` is also mixed on the current narrowed candidate set:
  - `HK:resolved_hk_top100_bluechip`: `0002.HK`, 5d `-95.57bps`, 20d `+146.91bps`.
  - `HK:resolved_hk_top100_tech_growth`: `0002.HK`, 5d `-82.04bps`, 20d `+215.51bps`.
- Trading implication: do not relax HK post-cost gates or WAIT_PULLBACK anchors from this evidence. Current action is continued evidence collection, not automatic parameter widening.

## Supervisor Shutdown Diagnosis

- Current Supervisor status is `running`, reason `cycle_complete`, PID `54465`, with `consecutive_cycle_error_count=0`.
- Supervisor exits only on these top-level paths:
  - `--once` normal one-cycle completion.
  - Duplicate instance lock.
  - SIGINT/SIGTERM/KeyboardInterrupt.
  - Consecutive `run_cycle()` exceptions reaching `max_consecutive_cycle_errors_before_shutdown`.
- The observed shutdown-like behavior is mostly evidence/runtime degradation:
  - stale or old-code Supervisor heartbeat,
  - stale weekly review artifacts,
  - market readiness stale,
  - weekly review subprocess killed with exit `137` while scanning a large SQLite audit database.
- This blocks submit through readiness gates but is not the strategy intentionally shutting down.

## Code Changes

- `src/tools/review_investment_weekly.py`
  - Added `--feedback_calibration_lookback_days`.
  - Added `--position_lookback_days`.
  - Added `_weekly_audit_where()` to centralize safe timestamp/market/portfolio filters.
  - Restricted candidate snapshot/outcome evidence to active portfolio ids when the review is global.
  - Avoids querying tables without the expected timestamp column.
- `src/app/supervisor.py`
  - Passes weekly review feedback and position lookback settings from config.
- `config/supervisor.yaml`
  - Paper runtime uses `weekly_review_feedback_calibration_lookback_days: 45`.
  - Position baseline uses `weekly_review_position_lookback_days: 45`.
- `src/common/storage.py`
  - Added weekly/evidence lookup indexes for fills, risk events, positions, trades, candidate snapshots, and candidate outcomes.

## Remaining Operational Note

- Existing 37GB `audit.db` still needs the new indexes to be created before a full 180-day weekly review should be expected to complete quickly.
- A runtime retry with a 45-day calibration window still exited with code `137` before the new indexes existed, so the immediate blocker is existing-database indexing, not only the lookback length.
- Do the first indexed startup or explicit index migration outside active trading windows because SQLite index creation can be IO-heavy and may briefly lock the database.
- Until that migration is done, HK outcome can still be validated safely through the streamed CSV-based `review_opportunity_outcomes` path.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_investment_weekly.py -k weekly_audit_where`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_storage_sqlite_locking.py`
- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/review_investment_weekly.py src/app/supervisor.py src/common/storage.py`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes --market HK --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence reports_investment_weekly/weekly_unified_evidence.csv --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation`
