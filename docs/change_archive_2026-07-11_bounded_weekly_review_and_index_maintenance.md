# 2026-07-11 Bounded Weekly Review and Evidence Index Maintenance

## Summary

- Replaced implicit heavy weekly/evidence index creation with an explicit maintenance tool.
- Added bounded candidate snapshot/outcome loading for weekly review.
- Reduced snapshot `details` JSON parsing by filtering before enrichment.
- Refreshed weekly review successfully against the 37GB runtime `audit.db`.
- Rebuilt auto-order readiness and dashboard so the next blocker is now market readiness, not stale weekly evidence.

## Runtime Findings

- Before this change, `review_investment_weekly` could run for minutes or exit `137` because it loaded and parsed too much candidate evidence from the large audit database.
- `evidence_index_maintenance` dry-run on the real runtime database reported `status=ready`, `missing=0`, `ready=6`, so the expected weekly/evidence indexes now exist.
- With bounded candidate evidence:
  - Runtime weekly review completed in about 16 seconds under `runtime_data/.../reports_investment_weekly`.
  - Configured weekly review directory `reports_investment_weekly` completed in about 10 seconds.
  - `review_auto_order_readiness` no longer reports `weekly_review_stale`.
- Current auto-order primary block is `market_readiness_not_ready`; recovery plan recommends one no-submit stale execution refresh for `US:watchlist`.

## Code Changes

- `src/common/evidence_index_maintenance.py`
  - Defines required weekly/evidence indexes in one place.
  - Supports read-only inspection and explicit apply.
- `src/tools/maintain_evidence_indexes.py`
  - Adds CLI for JSON/Markdown maintenance artifacts.
  - Default mode is dry-run/read-only.
  - `--apply` is required to create missing indexes.
- `src/common/storage.py`
  - No longer implicitly creates heavy weekly/evidence indexes during normal Storage initialization.
- `src/tools/review_investment_weekly.py`
  - Adds `--candidate_snapshot_limit` and `--candidate_outcome_limit`.
  - Supervisor paper config uses `20000` snapshots and `60000` outcomes.
- `src/tools/review_weekly_execution_support.py`
  - Filters snapshot rows before parsing `details`, avoiding all-row JSON parsing.

## Trading Implication

- This does not relax risk, edge, cost, liquidity, market-rule, Gateway, or submit-quality gates.
- It makes the evidence pipeline fast enough to refresh, so automatic ordering is blocked by real market/execution readiness rather than stale weekly-review infrastructure.
- Next operational step remains no-submit execution refresh for the ranked stale target before any submit.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_evidence_index_maintenance.py tests/test_storage_sqlite_locking.py tests/test_review_weekly_execution_support.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_investment_weekly.py`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.maintain_evidence_indexes --db runtime_data/paper_investment_only_duq152001/audit.db --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor/evidence_index_maintenance`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_investment_weekly --db runtime_data/paper_investment_only_duq152001/audit.db --out_dir reports_investment_weekly --labeling_dir reports_investment_labeling --preflight_dir reports_preflight --feedback_thresholds_config reports_investment_weekly/weekly_feedback_threshold_overrides.yaml --dashboard_control_audit runtime_data/paper_investment_only_duq152001/reports_supervisor/dashboard_control_action_audit.jsonl --days 7 --feedback_calibration_lookback_days 45 --position_lookback_days 45 --candidate_snapshot_limit 20000 --candidate_outcome_limit 60000`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_auto_order_readiness --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`
