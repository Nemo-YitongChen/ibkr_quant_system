# 2026-05-10 Follow-up Contracts

## Scope

This archive records the next evidence lifecycle implementation after IBKR Gateway request budgets.

## Completed

- Added strategy parameter suggestion follow-up rows for applied suggestions.
- Added follow-up verdicts: `IMPROVED`, `DEGRADED`, `NO_CLEAR_CHANGE`, `INSUFFICIENT_FOLLOWUP_SAMPLE`.
- Extended strategy parameter suggestion effectiveness with follow-up counts and degraded follow-up warning status.
- Added weekly CSV/JSON/summary/tuning/markdown output for `weekly_strategy_parameter_suggestion_followup`.
- Added walk-forward artifact aliases for downstream consumers:
  - `walk_forward_acceptance_summary.json`
  - `walk_forward_parameter_candidates.csv`
  - `walk_forward_market_stability.json`
- Fixed weekly review smoke failure by defining `generated_at` before IBKR request and Gateway budget payload generation.
- Fixed investment report generation crash by importing `MarketDataService` where owner-thread checks use it.
- Added tolerant dashboard control audit JSONL reader with redaction and row limiting.
- Connected weekly strategy parameter suggestions to dashboard control audit resolution history.
- Carried previous weekly strategy suggestions into current weekly review so applied suggestions can be evaluated even after the triggering warning disappears.
- Added dashboard advanced block support for walk-forward acceptance and market stability artifacts.
- Extended candidate model review and strategy suggestion follow-up fields to carry 5/20/60d outcome spread plus realized/post-cost edge context.

## Contract Notes

- Follow-up is read-only and only evaluates suggestions whose status is `APPLIED`.
- A missing or under-sampled candidate model review maps to `INSUFFICIENT_FOLLOWUP_SAMPLE`, not an exception.
- Persistent `SIGNAL_RANKING_INVERTED` after applying a signal-weight suggestion is treated as `DEGRADED`.
- Walk-forward aliases preserve existing `market_walk_forward_*` artifacts and add stable names for dashboard/weekly consumers.
- Supervisor now passes `dashboard_control_action_audit.jsonl` into weekly review, so audit-linked strategy suggestion resolutions are visible in weekly artifacts.
- Dashboard can read `dashboard_walk_forward_dir` and falls back from new walk-forward aliases to legacy `market_walk_forward_summary.json`.
- Follow-up markdown now prints 5/20/60d spread and realized edge so reviewers can distinguish short-horizon noise from persistent improvement.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_strategy_parameter_suggestions.py tests/test_review_weekly_output_support.py tests/test_review_weekly_helpers.py tests/test_walk_forward_tuning.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_investment_weekly.py tests/test_governance_health_summary.py tests/test_investment_workflow_smoke.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_dashboard_control_audit.py tests/test_review_weekly_output_support.py tests/test_strategy_parameter_suggestions.py tests/test_investment_modules.py -k "generate_investment_report or dashboard_control_audit or weekly_output_bundle or strategy_parameter_suggestion"`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_dashboard_blocks.py tests/test_generate_dashboard_helpers.py -k "dashboard_v2_blocks or advanced_html_metrics"`
