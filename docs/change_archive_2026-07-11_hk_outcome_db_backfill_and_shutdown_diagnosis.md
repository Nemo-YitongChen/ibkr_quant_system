# 2026-07-11 HK Outcome DB Backfill and Supervisor Shutdown Diagnosis

## Scope

- Validated HK positive post-cost candidates and close `WAIT_PULLBACK` candidates against mature 5/20d outcomes.
- Fixed `review_opportunity_outcomes` so bounded weekly evidence does not create a false `OUTCOME_PENDING` result when the audit DB still has mature candidate outcomes.
- Rebuilt root and HK-specific opportunity outcome artifacts plus dashboard JSON/HTML.
- Reviewed Supervisor shutdown evidence; no order submission was triggered.

## Findings

- Latest bounded `weekly_unified_evidence.csv` had HK candidate rows but no HK outcome values, because recent candidate snapshots were not yet mature and older mature snapshots were outside the bounded snapshot export.
- Direct `investment_candidate_outcomes` evidence was available in `audit.db`, so `review_opportunity_outcomes --db ...` now backfills from that long table only when weekly evidence has no target outcome values.
- HK positive post-cost groups remain mixed, not a reason to relax cost/post-cost gates:
  - `HK:resolved_hk_top100_bluechip` `0005.HK,2359.HK`: 365 mature 5d samples, 222 mature 20d samples, avg 5d `+243.514603` bps, avg 20d `-27.548436` bps.
  - `HK:resolved_hk_top100_tech_growth` `0005.HK,2359.HK`: 365 mature 5d samples, 218 mature 20d samples, avg 5d `+247.990826` bps, avg 20d `-4.269162` bps.
- HK close `WAIT_PULLBACK` is split:
  - Bluechip `0002.HK`: 105 mature 5d samples, 28 mature 20d samples, avg 5d `+6.958223` bps, avg 20d `-41.930381` bps, so no trial.
  - Tech growth `0002.HK`: 108 mature 5d samples, 31 mature 20d samples, avg 5d `+9.998494` bps, avg 20d `+2.897014` bps, so only a P2 manual paper-only near-entry limit trial is suggested.
- The root dashboard artifact now shows `opportunity_outcome_validation.outcome_source=investment_candidate_outcomes`, 46 matched symbols, 12,002 mature 5d samples, and 4,230 mature 20d samples.

## Supervisor Shutdown Diagnosis

- Current shutdown status artifact reports `status=stopped`, `reason=signal:SIGTERM`, `last_signal_name=SIGTERM`, `pid=54465`.
- The available evidence points to an external or recovery-triggered graceful stop, not an unhandled Supervisor crash.
- Known Supervisor top-level exit paths remain:
  - `--once` completes one cycle and exits.
  - Another instance already holds `supervisor.lock`, so the new process exits.
  - `SIGINT` or `SIGTERM` is received and recorded as a graceful stop.
  - Consecutive `run_cycle()` exceptions reach `max_consecutive_cycle_errors_before_shutdown`, which records `status=crashed`.
  - `SIGHUP` is ignored and recorded as `ignored_signal:SIGHUP`; it should not stop Supervisor.
- Child trading process stop during closed market windows is expected behavior and is not Supervisor shutdown.

## Trading Implication

- Do not relax HK risk, edge, cost, liquidity, market-rule, Gateway, or submit-quality gates based on these outcomes.
- Treat HK post-cost candidate evidence as mixed and keep collecting realized/future outcomes.
- If HK tech growth `0002.HK` later has a fresh BUY plan that already passes whole-share, post-cost, limit-order, Gateway-budget, and submit-quality gates, it can be reviewed for a small paper-only near-entry trial; no automatic application is allowed.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_opportunity_outcomes.py` -> `5 passed`.
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes --market HK --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence runtime_data/paper_investment_only_duq152001/reports_investment_weekly/weekly_unified_evidence.csv --db runtime_data/paper_investment_only_duq152001/audit.db --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation`.
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence runtime_data/paper_investment_only_duq152001/reports_investment_weekly/weekly_unified_evidence.csv --db runtime_data/paper_investment_only_duq152001/audit.db --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`.
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`.
