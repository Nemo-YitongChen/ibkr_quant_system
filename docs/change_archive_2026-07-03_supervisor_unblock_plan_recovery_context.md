# 2026-07-03 Supervisor Recovery Context Uses Auto-order Unblock Plan

## Summary

Supervisor auto-order recovery now builds its context from
`build_auto_order_readiness_summary` instead of manually composing submit,
stale-execution, and recovery plans. This keeps Supervisor's execution path
aligned with the JSON, Markdown, and dashboard views that already expose
`summary.unblock_plan`.

## Changes

- `_auto_order_recovery_context` now reuses `build_auto_order_readiness_summary`.
- Recovery context now includes:
  - `plan`
  - `unblock_plan`
  - `stale_execution_refresh_plan`
  - `eligibility`
  - `summary`
- Supervisor recovery now inherits summary-level global hard blocks, including
  stale or missing Supervisor code revision.
- Weekly-review stale blockers now route through `unblock_plan` as local
  evidence work instead of being implicitly treated as Gateway-backed recovery.

## Trading Impact

This improves automatic paper-submit safety and availability. Supervisor,
dashboard, and CLI now agree on the same next unblock action before any recovery
refresh runs.

The change does not submit orders, does not connect to IBKR, and does not relax
any risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality
gate.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_recovery_context_uses_summary_runtime_restart_unblock_plan tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_recovery_context_routes_weekly_stale_to_local_unblock tests/test_auto_order_readiness.py`
  - `65 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_dashboard_blocks.py tests/test_supervisor_cli.py`
  - `146 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x`
  - `781 passed`
