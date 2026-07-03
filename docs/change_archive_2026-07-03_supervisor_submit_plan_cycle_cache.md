# 2026-07-03 Supervisor Submit Plan Cycle Cache

## Summary

Supervisor now reuses one auto-order submit plan inside a single `run_cycle`
instead of rebuilding the same plan for every investment execution item that
reaches the submit gate.

## Changes

- `_auto_order_submit_plan_allows_item` accepts an optional `submit_plan`.
- `run_cycle` computes `auto_order_submit_plan_cache` lazily the first time an
  execution item needs the submit-plan gate, then reuses it for the rest of the
  cycle.
- Existing callers without an explicit plan remain compatible.
- Passing an explicit empty or blocked plan no longer falls back to recomputing
  the plan.

## Trading Impact

This reduces redundant local artifact reads and summary construction during
Supervisor cycles with multiple market/portfolio execution items. It keeps all
execution items in the same cycle aligned to the same submit-plan decision.

It does not connect to IBKR, submit orders, change candidate selection, alter
frequency, or relax any risk, edge, cost, liquidity, market-rule, Gateway, or
submit-quality gate.

## Validation

- `python -m py_compile src/app/supervisor.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_submit_plan_allows_only_selected_portfolio tests/test_supervisor_cli.py::SupervisorCliTests::test_targeted_recovery_force_run_bypasses_schedule_and_previous_dry_run`
  - `2 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_auto_order_readiness.py`
  - `195 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x`
  - `781 passed`
