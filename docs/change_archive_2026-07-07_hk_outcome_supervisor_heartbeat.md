# 2026-07-07 HK Outcome and Supervisor Heartbeat Diagnosis

## Summary

Revalidated HK positive post-cost candidates and close `WAIT_PULLBACK` outcome
groups against the latest local weekly evidence, then tightened Supervisor
runtime diagnosis so an alive-but-stale main process is not reported as healthy.

This change is diagnostic only. It does not connect to IBKR, submit orders,
change market YAML, or relax any risk, edge, cost, liquidity, market-rule,
Gateway, or submit-quality gate.

## HK Outcome Validation

Refreshed HK-only validation was written to:

`/private/tmp/hk_opportunity_outcome_validation_20260707/`

The result remains unchanged from the prior HK review:

- `HK:resolved_hk_top100_bluechip` positive post-cost candidates are
  `OUTCOME_WEAK_OR_MIXED`: `5d=+65.87bps`, `20d=-129.20bps`, symbols
  `0005.HK,2359.HK`.
- `HK:resolved_hk_top100_tech_growth` positive post-cost candidates are
  `OUTCOME_WEAK_OR_MIXED`: `5d=+72.54bps`, `20d=-127.94bps`, symbols
  `0005.HK,2359.HK`.
- `2359.HK` remains the main drag: bluechip `5d=-127.86bps/20d=-681.37bps`;
  tech growth `5d=-83.17bps/20d=-647.84bps`.
- `0005.HK` is individually constructive but not strong enough to justify a
  broad HK post-cost threshold expansion.

Close `WAIT_PULLBACK` remains the stronger HK path:

- Bluechip group: `5d=+125.96bps`, `20d=+212.07bps`.
- Tech growth group: `5d=+126.74bps`, `20d=+222.09bps`.
- Trial-qualified symbols are `3988.HK,2388.HK,1398.HK,0939.HK,0005.HK,3328.HK`.
- `1288.HK` and `2359.HK` remain excluded because their 5/20d outcomes are
  negative.

Trading interpretation: do not broaden HK post-cost threshold settings from
this evidence. If HK proceeds, use only strict paper-only near-entry limit
trials for qualified close `WAIT_PULLBACK` symbols after fresh report, weekly
review, BUY/no-submit execution evidence, and submit-quality gates are current.

## Supervisor Shutdown Diagnosis

Current process evidence shows PID `77976` is still alive, but
`supervisor_shutdown_status.json` was last written on `2026-06-17T08:41:20Z`.
That is not a crash, but it is also not healthy current-loop evidence.

The code-level top-level Supervisor exit paths are:

- `--once` normal single-cycle exit.
- Duplicate instance lock failure.
- SIGINT/SIGTERM/KeyboardInterrupt.
- Consecutive `run_cycle()` exceptions reaching
  `max_consecutive_cycle_errors_before_shutdown`.

The child `src.main` trading process is intentionally stopped when no active
live market window is open. That is designed market-window behavior, not a
Supervisor crash.

## Code Changes

- `src/common/supervisor_runtime_status.py` now derives
  `supervisor_heartbeat_status`, `supervisor_heartbeat_age_hours`, and
  `supervisor_heartbeat_stale_hours`.
- Running/running-degraded Supervisor status with heartbeat age above 6 hours is
  now `degraded`, requires restart, blocks recovery refresh, and returns
  `next_action=restart_stale_supervisor_heartbeat_current_code`.
- `src/tools/generate_dashboard.py` surfaces stale heartbeat as
  `Supervisor 心跳过期` in the top ops health summary.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_runtime_status.py tests/test_generate_dashboard_helpers.py::test_build_ops_overview_degrades_stale_running_supervisor_heartbeat`
  -> `7 passed`.
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes --market HK --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence reports_investment_weekly/weekly_unified_evidence.csv --out_dir /private/tmp/hk_opportunity_outcome_validation_20260707`.
- `python -m src.tools.review_supervisor_runtime --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir /private/tmp/ibkr_supervisor_runtime_status_20260707`.
