# 2026-07-07 Supervisor Runtime Recovery Tool

## Summary

Added a safe Supervisor runtime recovery tool so stale Supervisor heartbeat
states can move from manual PID inspection to an explicit, auditable recovery
plan.

The tool is dry-run by default. It does not submit orders, change submit gates,
or connect to IBKR unless the operator explicitly asks it to start Supervisor
with `--start`.

## Changes

- Added `src/common/supervisor_runtime_recovery.py`:
  - builds a side-effect-free recovery plan from `supervisor_runtime_status`;
  - allows restart only for known runtime actions;
  - requires the target process command to match `python -m src.app.supervisor`;
  - distinguishes unavailable process command evidence from true command
    mismatch;
  - keeps `submit_orders=false` and `does_not_change_submit_gates=true`.
- Added `src/tools/recover_supervisor_runtime.py`:
  - writes `supervisor_runtime_recovery_plan.json`;
  - writes `supervisor_runtime_recovery_plan.md`;
  - defaults to dry-run;
  - requires `--apply` before sending SIGTERM or removing stale locks;
  - requires separate `--start` before launching a new Supervisor process.
- Added console script:
  - `ibkr-quant-supervisor-recovery`.

## Current Runtime Dry-Run

Dry-run against the current runtime produced:

- `status=ready`
- `reason=restart_stale_supervisor_heartbeat_current_code`
- `allowed=true`
- `applied=false`
- `terminate_pid=77976`
- `start_after_apply=false`
- `submit_orders=false`
- `connects_to_ibkr=false`

Artifacts were written to:

`/private/tmp/ibkr_supervisor_recovery_20260707/`

## Recovery Policy

Recommended operator sequence:

1. Run dry-run first and inspect the plan.
2. Run `--apply` only if the command match is true and target PID is the stale
   Supervisor.
3. Start Supervisor separately or use `--start` only when normal scheduler
   execution is acceptable.
4. After restart, refresh weekly review and market readiness before any paper
   submit expansion.

## Validation

- `python -m py_compile src/common/supervisor_runtime_recovery.py src/tools/recover_supervisor_runtime.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_runtime_recovery.py tests/test_supervisor_runtime_status.py tests/test_project_packaging.py`
  -> `14 passed`.
