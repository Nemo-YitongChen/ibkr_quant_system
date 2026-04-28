# 2026-04-28 Dashboard Gateway / No-Trade Optimization Archive

## Scope

This change closes two dashboard and weekly-review interpretation gaps:

- Quick Read no longer treats every non-OK IB Gateway health status as disconnected.
- Weekly strategy tuning now explains how to keep improving strategy and model behavior when paper orders are fully blocked and no fills exist.

## Implementation

- Split IB Gateway connection detection from health degradation in `generate_dashboard.py`.
- Treat permission, delayed market data, account-limit, and account-cache degradation as connected-but-needing-review when there is no unresolved connectivity break.
- Keep the "start IB Gateway" next step only for unresolved connectivity failures such as `not_listening`, connection refusal, timeout, or break count greater than restore count.
- Add no-fill optimization evidence fields to market-profile tuning output:
  - `no_trade_optimization_note`
  - `counterfactual_optimization_available`
- Carry those fields into weekly strategy context, dashboard strategy rows, weekly tuning dataset, and markdown output.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/generate_dashboard.py src/tools/review_weekly_strategy_support.py src/tools/review_weekly_decision_support.py src/tools/review_weekly_markdown.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_generate_dashboard_helpers.py tests/test_review_investment_weekly.py tests/test_review_weekly_helpers.py tests/test_investment_workflow_smoke.py tests/test_supervisor_cli.py tests/test_dashboard_control_audit.py`

Result: `167 passed`.
