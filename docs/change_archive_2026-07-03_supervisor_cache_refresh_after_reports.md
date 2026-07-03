# 2026-07-03 Supervisor Cache Refresh After Reports

## Summary

Supervisor now refreshes the per-cycle auto-order caches when report generation
actually updates report artifacts before execution. This keeps the execution
submit gate from using a pre-report readiness or submit-plan snapshot after new
candidate and plan files have been produced in the same cycle.

## Changes

- Added `_auto_order_cycle_cache_from_context` to extract both submit plan and
  readiness rows from an auto-order summary context.
- `_auto_order_submit_plan_from_recovery_context` now delegates to that helper.
- `run_cycle` still seeds caches from recovery context when available.
- After `_generate_reports`, if any report transitions to the current day, the
  auto-order summary context is rebuilt and the cycle caches are refreshed.
- If cache refresh fails after report generation, the code clears the caches and
  falls back to direct per-item evaluation.

## Trading Impact

This keeps same-cycle report refresh and execution submit decisions aligned.
The system can reuse cached evidence when nothing changed, but avoids applying
stale pre-report evidence after fresh report artifacts are created.

It does not connect to IBKR, submit orders, alter selected candidates, change
order frequency, or relax any risk, edge, cost, liquidity, market-rule, Gateway,
or submit-quality gate.

## Validation

- `python -m py_compile src/app/supervisor.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_cycle_cache_from_recovery_context_returns_copies tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_readiness_for_item_uses_cycle_cached_row`
  - `2 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_auto_order_readiness.py`
  - `197 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x`
  - `783 passed`
