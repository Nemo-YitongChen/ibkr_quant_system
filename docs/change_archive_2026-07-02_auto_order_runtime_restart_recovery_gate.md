# 2026-07-02 Auto-order runtime restart recovery gate

## Context

- Runtime evidence still shows a long-running Supervisor PID `77976` started on 2026-06-16.
- `supervisor_shutdown_status.json` from that process lacks `code_revision/current_code_revision`, so current auto-order readiness correctly hard-blocks submit with `supervisor_code_revision_missing`.
- Before this change, the submit gate was blocked but the recovery plan could still return `stale_execution_refresh_required` with `recovery_eligibility.eligible=true`.

## Change

- `build_auto_order_recovery_plan` now accepts summary-level `global_hard_blocks`.
- `supervisor_code_revision_missing` and `supervisor_code_revision_mismatch` take precedence over stale execution refresh.
- When either revision block exists, recovery becomes:
  - `status=runtime_restart_required`
  - `primary_action=restart_supervisor_current_code`
  - `request_policy=restart_supervisor_before_any_recovery_refresh`
  - no IBKR Gateway request requirement
  - no submit
  - no risk/edge/cost/liquidity/market-rule/submit-quality gate relaxation
- `evaluate_auto_order_recovery_eligibility` now treats `runtime_restart_required` as active but not eligible, with `reason=supervisor_runtime_restart_required` and no allowed recovery actions.

## Validation

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py
62 passed in 0.19s
```

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x
772 passed in 39.77s
```

Real runtime dry validation was written to `/private/tmp/ibkr_auto_order_readiness_after_runtime_gate/`:

- `primary_block_reason=supervisor_code_revision_missing`
- `recovery_plan.status=runtime_restart_required`
- `recovery_eligibility.eligible=false`
- `recovery_eligibility.allowed_actions=[]`

## Trading impact

- This does not increase submit frequency directly.
- It prevents stale Supervisor processes from running recovery refreshes or consuming IBKR request budget before the current code is actually running.
- The next operational step remains: gracefully restart Supervisor into current `HEAD`, then refresh weekly review, market readiness, and no-submit execution evidence before any paper submit can be considered.
