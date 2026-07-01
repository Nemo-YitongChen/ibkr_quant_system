# 2026-07-02 Auto-order Supervisor Runtime Contract Reuse

## Summary

Auto-order readiness now reuses the same Supervisor runtime contract as the
runtime review CLI and dashboard. The existing revision hard blocks remain
unchanged: running Supervisor processes with a missing or mismatched
`code_revision` still block automated submit, while non-running Supervisor
artifacts do not create a revision block.

## Changes

- Replaced the standalone auto-order Supervisor revision gate internals with
  `build_supervisor_runtime_status_from_payloads`.
- Preserved existing output compatibility:
  - `supervisor_code_revision_status`
  - `supervisor_code_revision`
  - `current_code_revision`
  - `primary_reason`
  - `hard_blocks`
- Added runtime contract fields to each auto-order readiness row:
  - `supervisor_runtime_next_action`
  - `supervisor_runtime_restart_required`
  - `supervisor_runtime_blocks_recovery_refresh`
  - `supervisor_runtime_request_policy`
  - `supervisor_runtime_health_status`
- Kept auto-order evaluation read-only. It does not start or stop Supervisor,
  does not query IBKR, does not refresh Gateway-backed evidence, and does not
  submit orders.

## Trading Impact

This change reduces operator ambiguity without relaxing risk controls. CLI,
dashboard, recovery plan, and auto-order readiness now point to the same
runtime action when a stale or unknown Supervisor process is active:
`restart_supervisor_current_code`.

The submit gate behavior is intentionally unchanged. The new fields explain
why submit is blocked and what should happen next, but they do not create a
new liveness or stale-lock hard block inside auto-order readiness.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_supervisor_runtime_status.py`
  - `67 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x`
  - `778 passed`
