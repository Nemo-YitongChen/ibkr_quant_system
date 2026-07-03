# 2026-07-03 Supervisor Submit Plan Context Cache

## Summary

Supervisor now initializes the per-cycle submit-plan cache from the existing
auto-order recovery context summary. The recovery context already contains the
same `summary.submit_plan`, so execution submit gates no longer need to rebuild
the submit plan on the first item that reaches the gate.

## Changes

- Added `_auto_order_submit_plan_from_recovery_context`.
- `run_cycle` initializes `auto_order_submit_plan_cache` from
  `recovery_context.summary.submit_plan` when available.
- The existing lazy fallback remains in place if recovery context creation
  fails or does not include a submit plan.
- The helper returns a copy of the submit plan so later item-level annotations
  do not mutate the recovery context.

## Trading Impact

This keeps one Supervisor cycle aligned to a single auto-order summary snapshot:
readiness rows, submit plan, recovery plan, and unblock plan now all originate
from the same recovery context when available.

It does not connect to IBKR, submit orders, change selected candidates, alter
frequency, or relax any risk, edge, cost, liquidity, market-rule, Gateway, or
submit-quality gate.

## Validation

- `python -m py_compile src/app/supervisor.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_submit_plan_from_recovery_context_returns_copy tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_submit_plan_allows_only_selected_portfolio tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_readiness_for_item_uses_cycle_cached_row`
  - `3 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_auto_order_readiness.py`
  - `197 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x`
  - `783 passed`
