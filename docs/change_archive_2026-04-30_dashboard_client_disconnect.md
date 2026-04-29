# Change Archive: Dashboard Client Disconnect Handling

Date: 2026-04-30

## Context

The dashboard control HTTP service could print a Python `socketserver` traceback when a local client disconnected before the request line was fully read:

```text
ConnectionResetError: [Errno 54] Connection reset by peer
```

This usually happens when a browser tab, refresh request, health check, or local polling client closes the socket early. It is not a dashboard action failure, but the default `BaseHTTPRequestHandler` path prints the exception before request dispatch.

## Changes

- Added a `handle()` wrapper in `src/app/dashboard_control.py`.
- Common client disconnect errors are ignored before request dispatch:
  - `BrokenPipeError`
  - `ConnectionResetError`
  - `ConnectionAbortedError`
- Unexpected socket errors are still re-raised.
- Added tests covering pre-dispatch disconnects and unexpected socket failures.

## Validation

```bash
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_dashboard_control_service.py
PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/app/dashboard_control.py tests/test_dashboard_control_service.py
```

Result:

```text
9 passed
```

## Operational Impact

No dashboard API behavior changed. This only suppresses expected local client disconnect noise so supervisor logs remain focused on actionable failures.
