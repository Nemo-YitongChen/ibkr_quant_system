# 2026-07-02 Dashboard auto-order runtime restart overlay

## Context

- The latest code can generate `recovery_plan.status=runtime_restart_required` when a running Supervisor is missing or mismatching `code_revision`.
- The live `auto_order_readiness.json` can still be stale because the long-running old Supervisor has not loaded the new code.
- `ops_overview` already derives Supervisor revision status directly from `supervisor_shutdown_status.json`, so the dashboard can detect the runtime blocker even when the auto-order artifact is old.

## Change

- `build_auto_order_readiness_block` now reads `ops_overview.supervisor_shutdown_status` and `ops_overview.supervisor_code_revision_status`.
- When Supervisor is running and code revision is `missing` or `mismatch`, the Auto Order block:
  - injects the matching Supervisor revision block count;
  - overrides legacy recovery display with `runtime_restart_required`;
  - recomputes recovery eligibility as inactive for IBKR-backed recovery actions;
  - keeps `requires_ibkr_gateway=false` and `submit_orders=false`.

## Validation

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_dashboard_blocks.py tests/test_auto_order_readiness.py
76 passed in 0.25s
```

## Trading impact

- This is a visibility and safety improvement.
- It does not submit orders, does not refresh IBKR-backed evidence, and does not relax risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gates.
- It reduces the chance that an operator follows a stale recovery artifact while the real first step is restarting Supervisor into current code.
