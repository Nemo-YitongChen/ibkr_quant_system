# 2026-07-03 Supervisor Readiness Rows Cycle Cache

## Summary

Supervisor now reuses the auto-order readiness rows already produced for the
cycle recovery context when checking individual execution items. This prevents
the execution submit gate from reloading the same artifact inputs and
re-evaluating readiness separately for each item.

## Changes

- `_auto_order_recovery_context` now returns the cycle's readiness `rows`.
- `_auto_order_readiness_for_item` accepts optional `readiness_rows`.
- When cached rows are supplied, `_auto_order_readiness_for_item` matches by
  normalized market and portfolio id, returning the cached row without reading
  artifact inputs again.
- `run_cycle` passes the recovery context rows into per-item submit-readiness
  checks.
- Missing cache matches still fall back to the existing direct evaluation path.

## Trading Impact

This keeps all execution items in one Supervisor cycle aligned to the same
auto-order readiness snapshot and reduces redundant local artifact reads. It is
especially useful when multiple markets or portfolios reach the execution submit
gate in the same cycle.

It does not connect to IBKR, submit orders, change candidates, change risk
limits, or relax any risk, edge, cost, liquidity, market-rule, Gateway, or
submit-quality gate.

## Validation

- `python -m py_compile src/app/supervisor.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_readiness_for_item_uses_cycle_cached_row tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_submit_plan_allows_only_selected_portfolio`
  - `2 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_auto_order_readiness.py`
  - `196 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x`
  - `782 passed`
