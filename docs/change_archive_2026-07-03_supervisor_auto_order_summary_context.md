# 2026-07-03 Supervisor Auto-order Summary Context

## Summary

Supervisor now builds auto-order submit, recovery, and artifact summary data
through one internal summary context helper. This removes duplicated summary
composition logic and keeps submit-plan, recovery-plan, recovery eligibility,
and unblock-plan views aligned.

## Changes

- Added `_auto_order_readiness_summary_context` inside `Supervisor`.
- `_auto_order_submit_plan` now returns `summary.submit_plan` from the shared
  context instead of rebuilding submit-plan inputs separately.
- `_auto_order_recovery_context` now returns plan, unblock plan, stale refresh
  plan, eligibility, and summary from the shared context.
- `_write_auto_order_readiness_summary` now uses the same context and only adds
  execution evidence maintenance state when writing the artifact.
- Removed the direct `build_auto_order_submit_plan` import from Supervisor.

## Trading Impact

This is a control-plane refactor. It reduces the chance that automated submit,
recovery, and dashboard artifacts drift apart after future changes.

It does not connect to IBKR, submit orders, alter candidate selection, change
request budgets, or relax any risk, edge, cost, liquidity, market-rule, Gateway,
or submit-quality gate.

## Validation

- `python -m py_compile src/app/supervisor.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_auto_order_readiness.py`
  - `195 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x`
  - `781 passed`
