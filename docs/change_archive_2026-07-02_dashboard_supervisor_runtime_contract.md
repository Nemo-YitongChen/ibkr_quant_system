# 2026-07-02 Dashboard Supervisor runtime contract reuse

## Context

- Supervisor runtime status was available through the new read-only CLI, but dashboard ops overview still carried a separate PID/revision/health calculation path.
- Keeping CLI, dashboard, and auto-order recovery logic separate increases the chance that one view says "refresh stale execution" while another says "restart Supervisor first".

## Change

- `src.common.supervisor_runtime_status` now exposes `build_supervisor_runtime_status_from_payloads`, so already-loaded `supervisor.lock` and `supervisor_shutdown_status.json` payloads can use the same contract as the CLI.
- `generate_dashboard.py` now:
  - loads `supervisor.lock`;
  - builds top-level `supervisor_runtime_status`;
  - passes that contract into `ops_overview`;
  - keeps existing `supervisor_shutdown_*` and `supervisor_code_revision_*` fields for backward compatibility.
- `dashboard_blocks.py` now surfaces these runtime fields in the `Ops Health` v2 block:
  - `supervisor_runtime_next_action`
  - `supervisor_runtime_restart_required`
  - `supervisor_runtime_blocks_recovery_refresh`
  - `supervisor_runtime_request_policy`

## Runtime Evidence

Real dashboard build against `runtime_data/paper_investment_only_duq152001/reports_supervisor` now reports:

- top-level `supervisor_runtime_status.supervisor_status=running`
- `supervisor_code_revision_status=missing`
- `next_action=restart_supervisor_current_code`
- `blocks_recovery_refresh=true`
- `submit_orders=false`

The same values are visible in:

- `ops_overview.supervisor_runtime_next_action`
- `ops_overview.supervisor_runtime_blocks_recovery_refresh`
- v2 `ops_health.metrics.supervisor_runtime_next_action`

## Trading Impact

- This is a consistency and operator-safety improvement.
- It does not connect to IBKR, does not refresh report/opportunity/execution evidence, does not start or stop Supervisor, and does not submit orders.
- It reduces duplicate runtime logic and makes dashboard, CLI, and recovery gate converge on the same next action: restart Supervisor into current code before any Gateway-backed recovery refresh.

## Validation

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_runtime_status.py tests/test_dashboard_shutdown_history.py tests/test_dashboard_blocks.py tests/test_generate_dashboard_helpers.py
66 passed
```

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x
778 passed
```

```text
python - <<'PY'
from src.tools.generate_dashboard import build_dashboard
p = build_dashboard('config/supervisor.yaml', 'runtime_data/paper_investment_only_duq152001/reports_supervisor')
print(p['supervisor_runtime_status']['next_action'])
print(p['ops_overview']['supervisor_runtime_blocks_recovery_refresh'])
PY
restart_supervisor_current_code
True
```
