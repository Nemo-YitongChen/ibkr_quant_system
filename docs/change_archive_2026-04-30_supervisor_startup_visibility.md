# Change Archive: Supervisor Startup Visibility

Date: 2026-04-30

## Context

Running:

```bash
python -m src.app.supervisor
```

starts the supervisor as a foreground long-running scheduler. It does not return to the shell until stopped. If the current cycle has no due task or no changed summary, it can appear as if nothing happened.

## Changes

- Added an explicit startup log for default long-running mode.
- Added explicit start/complete logs for `--once` mode.
- Changed SIGINT/SIGTERM handling so foreground runs can be interrupted and cleaned up instead of only setting a stop flag.
- Dashboard control startup/stop/error now writes a lightweight state file instead of building full portfolio/patch state before the first log line.
- Dashboard control `/state` now serves a lightweight cached payload plus persisted portfolios/artifacts from `dashboard_control_state.json`; browser polling no longer rebuilds the full portfolio control state inside the supervisor process.
- Lightweight dashboard-control writes preserve previously persisted portfolios/artifacts when available, so restarting supervisor does not erase the dashboard's last known control context.
- Due-report cycles now log `Starting due report workflow` and `Launching due report tasks` before invoking report subprocesses, making it clear whether the scheduler has reached the IBKR/report stage.
- Disabled `short_safety_sync` no longer appears as an active pre-report sync step.
- The default loop log now includes:
  - config path
  - enabled markets
  - poll interval
  - dashboard control enabled state
  - dashboard control URL
  - `Ctrl+C` stop hint

## Validation

```bash
PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/app/supervisor.py
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_dashboard_control_start_writes_lightweight_state_without_portfolios tests/test_supervisor_cli.py::SupervisorCliTests::test_dashboard_control_poll_state_reuses_persisted_portfolios_without_rebuild tests/test_supervisor_cli.py::SupervisorCliTests::test_supervisor_signal_handler_interrupts_foreground_process tests/test_dashboard_control_service.py
```

Local startup/interrupt smoke test also passed with a minimal supervisor config:

- startup log appeared immediately
- SIGINT printed `Supervisor stop requested`
- process exited cleanly after `Supervisor interrupted; shutting down`

Default-config startup/interrupt smoke test also passed:

- `Supervisor starting` appeared immediately
- dashboard control startup no longer blocked on full state generation
- due-report workflow logged `Launching due report tasks` and `Running task generate_investment_report...` before the report subprocess attempted IBKR connection
- SIGINT interrupted the foreground process and left no supervisor/report child process behind

## Operator Notes

- Use `python -m src.app.supervisor --once` to run one scheduler cycle and return to the shell.
- Use `python -m src.app.supervisor` for the long-running scheduler.
- If it is running correctly, the process should stay in the foreground and print the startup line immediately.
