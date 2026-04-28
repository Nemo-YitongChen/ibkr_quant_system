# 2026-04-29 Strategy Parameter Registry Archive

## Scope

This change continues the post-dashboard-v2 optimization work by moving patch recommendation metadata out of scattered helper code and into a single configurable strategy parameter registry.

The immediate goal is not to auto-apply more strategy changes. It is to make weekly review and dashboard control recommendations use the same field metadata before later paper/shadow/limited-live governance steps.

## Implementation

- Added `config/strategy_parameter_registry.yaml` as the default registry for tunable strategy, execution, slicing, and risk fields.
- Added `src/common/strategy_parameter_registry.py` with shared helpers for:
  - field metadata
  - bounds / precision aware proposed values
  - priority labels by tuning scope
- Updated weekly feedback calibration patch builders to read field labels, suggested values, and priorities from the registry.
- Updated supervisor market-profile patch suggestions to use the same registry for proposed values and priority labels.
- Extended adaptive strategy loading to use layered config loading and expose `config_sources` in the strategy context.
- Added focused tests for registry loading, bounds, precision, custom override files, and adaptive strategy market-specific layered overrides.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/strategy_parameter_registry.py src/common/adaptive_strategy.py src/tools/review_weekly_feedback_support.py src/app/supervisor_patch_support.py tests/test_strategy_parameter_registry.py tests/test_adaptive_strategy.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_strategy_parameter_registry.py tests/test_config_layers.py tests/test_adaptive_strategy.py tests/test_review_weekly_helpers.py tests/test_review_investment_weekly.py::ReviewInvestmentWeeklyTests::test_weekly_edge_slicing_and_risk_calibration_rows tests/test_supervisor_cli.py::SupervisorCliTests::test_supervisor_paper_weekly_feedback_builds_effective_overlay_configs tests/test_supervisor_cli.py::SupervisorCliTests::test_supervisor_market_profile_manual_apply_patch_promotes_primary_item_only_when_ready`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_strategy_parameter_registry.py tests/test_config_layers.py tests/test_adaptive_strategy.py tests/test_review_weekly_helpers.py tests/test_review_investment_weekly.py tests/test_supervisor_cli.py tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py tests/test_dashboard_control_audit.py tests/test_alert_classification.py`

Result: `192 passed`.
