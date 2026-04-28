# 2026-04-29 Alert / Error Classification Archive

## Scope

This change continues the post-dashboard-v2 work after PR3 by making dashboard operations and control-action failures more explainable.

It also fixes two date-sensitive supervisor tests that started failing when local Sydney time rolled into a new calendar day while HK report artifact mtimes still belonged to the prior HK market day.

## Implementation

- Added shared alert/error classification helpers in `src/common/alert_classification.py`.
- Added `error_severity` to sanitized dashboard control action audit rows.
- Added structured `alert_class` and `alert_severity` fields to dashboard ops alert rows.
- Added error-class counters to the dashboard v2 control actions block:
  - retryable errors
  - validation errors
  - permission errors
  - transient I/O errors
  - task-failed errors
  - exception errors
- Rendered `error_class`, `severity`, `alert_class`, and `alert_severity` in advanced dashboard tables.
- Made the closed-market supervisor tests deterministic by explicitly setting report artifact mtimes to the test market day.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/alert_classification.py src/app/dashboard_control_audit.py src/tools/dashboard_blocks.py src/tools/generate_dashboard.py tests/test_supervisor_cli.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_dashboard_degraded_inputs.py tests/test_preflight_supervisor.py tests/test_investment_workflow_smoke.py tests/test_review_investment_weekly.py tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py tests/test_dashboard_control_audit.py tests/test_alert_classification.py`

Result: `173 passed`.
