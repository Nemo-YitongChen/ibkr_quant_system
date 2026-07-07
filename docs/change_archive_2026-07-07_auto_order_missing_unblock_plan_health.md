# 2026-07-07 Auto-Order Missing Unblock Plan Health

## Summary

The active runtime `auto_order_readiness.json` can still be overwritten by an
old Supervisor process that predates the `summary.unblock_plan` contract. The
current CLI path builds the correct unblock plan, but the old long-running
process can write a fresh-looking artifact with `unblock_plan=null`.

This change makes that schema gap visible in dashboard health instead of letting
the artifact appear fully fresh.

## Changes

- `_build_auto_order_readiness_health` now checks whether
  `summary.unblock_plan` is missing or empty.
- A fresh artifact with no unblock plan is marked `status=warning` and
  `reason=missing_unblock_plan`.
- Dashboard v2 Auto Order block exposes `readiness_missing_unblock_plan`.
- Supervisor writer test now asserts that current-code runtime writer output
  contains a non-empty `summary.unblock_plan` with `submit_orders=false` and
  `does_not_change_submit_decision=true`.

## Trading Impact

This improves operator safety and recovery ordering. If old Supervisor code or
legacy schema writes an incomplete auto-order readiness artifact, the dashboard
will show a warning before any recovery or submit decision is trusted.

The change does not connect to IBKR, refresh Gateway-backed evidence, submit
orders, alter candidates, or relax risk, edge, cost, liquidity, market-rule,
Gateway, or submit-quality gates.

## Runtime Observation

Running current-code `review_auto_order_readiness` builds the expected plan:

- `primary_block_reason=supervisor_code_revision_missing`
- `unblock_plan.status=runtime_restart_required`
- `unblock_plan.primary_action=restart_supervisor_current_code`
- `unblock_plan.submit_orders=false`

The active old Supervisor can still overwrite the runtime file until it is
restarted with current code, so the dashboard health warning is intentional.

## Validation

- `python -m py_compile src/tools/generate_dashboard.py src/tools/dashboard_blocks.py src/app/supervisor.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_generate_dashboard_helpers.py::test_auto_order_readiness_health_warns_when_unblock_plan_missing tests/test_dashboard_blocks.py::test_auto_order_readiness_block_surfaces_missing_unblock_plan_health tests/test_supervisor_cli.py::SupervisorCliTests::test_write_auto_order_readiness_summary_uses_summary_out_dir`
  - `3 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py tests/test_auto_order_readiness.py tests/test_supervisor_cli.py::SupervisorCliTests::test_write_auto_order_readiness_summary_uses_summary_out_dir`
  - `126 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x`
  - `785 passed`
