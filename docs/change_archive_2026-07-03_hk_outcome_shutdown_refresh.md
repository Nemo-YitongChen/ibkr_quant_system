# 2026-07-03 HK Outcome and Shutdown Diagnosis Refresh

## Summary

Revalidated HK positive post-cost candidates and close `WAIT_PULLBACK` groups
against the latest available weekly unified evidence, then rechecked Supervisor
runtime/shutdown evidence.

This change is diagnostic only. It does not restart Supervisor, connect to IBKR,
submit orders, change YAML, or relax any risk, edge, cost, liquidity,
market-rule, Gateway, or submit-quality gate.

## HK Outcome Validation

The HK-only validation was written to:

`runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/`

Latest HK results:

- `HK:resolved_hk_top100_bluechip` positive post-cost candidates:
  `OUTCOME_WEAK_OR_MIXED`, `5d=+65.87bps`, `20d=-129.20bps`,
  symbols `0005.HK,2359.HK`.
- `HK:resolved_hk_top100_tech_growth` positive post-cost candidates:
  `OUTCOME_WEAK_OR_MIXED`, `5d=+72.54bps`, `20d=-127.94bps`,
  symbols `0005.HK,2359.HK`.
- `2359.HK` is the main drag: bluechip `5d=-127.86bps/20d=-681.37bps`;
  tech growth `5d=-83.17bps/20d=-647.84bps`.
- `0005.HK` is constructive on 5d and roughly neutral/positive on 20d, but
  does not justify broad HK post-cost threshold expansion by itself.

Close `WAIT_PULLBACK` remains supportive in aggregate:

- `HK:resolved_hk_top100_bluechip`: `5d=+125.96bps`, `20d=+212.07bps`.
- `HK:resolved_hk_top100_tech_growth`: `5d=+126.74bps`,
  `20d=+222.09bps`.
- Trial-qualified symbols are `3988.HK,2388.HK,1398.HK,0939.HK,0005.HK,3328.HK`.
- `1288.HK` and `2359.HK` remain excluded because their 5/20d outcomes are
  negative.

## Trading Interpretation

Do not broaden HK post-cost threshold settings from this evidence. The current
positive post-cost group is mixed and specifically harmed by `2359.HK`.

The safer next HK optimization remains a strict paper-only near-entry limit
trial for the qualified close `WAIT_PULLBACK` symbols only, after fresh report,
weekly review, BUY/no-submit execution evidence, and submit-quality gates are
all current.

## Shutdown Diagnosis

Current dashboard runtime evidence shows:

- `supervisor_status=running`
- `supervisor_pid=77976`
- `supervisor_liveness_status=alive`
- `supervisor_code_revision_status=missing`
- `next_action=restart_supervisor_current_code`

This means the old Supervisor process is still running and predates later
runtime-status code revision fields. It is not current evidence of a crash.

There are two different shutdown-like cases:

- Supervisor top-level exits only on `--once`, duplicate instance lock failure,
  SIGINT/SIGTERM/KeyboardInterrupt, or consecutive `run_cycle()` exceptions
  reaching `max_consecutive_cycle_errors_before_shutdown`.
- The child `src.main` trading process is intentionally stopped when no active
  live market window is open. That is a designed market-window behavior, not a
  Supervisor crash.

Current auto-order blockers are still `weekly_review_stale`,
`market_readiness_not_ready`, HK `strategy_suggestion_stale`, and stale/missing
Supervisor code revision. HK outcome evidence is not the primary blocker.

## Validation

- `python -m src.tools.review_opportunity_outcomes --market HK --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence reports_investment_weekly/weekly_unified_evidence.csv --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation`
- `python -m src.tools.review_supervisor_runtime --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir /private/tmp/ibkr_supervisor_runtime_status_20260703`
