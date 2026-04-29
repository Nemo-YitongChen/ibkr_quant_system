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
```

## Operator Notes

- Use `python -m src.app.supervisor --once` to run one scheduler cycle and return to the shell.
- Use `python -m src.app.supervisor` for the long-running scheduler.
- If it is running correctly, the process should stay in the foreground and print the startup line immediately.
