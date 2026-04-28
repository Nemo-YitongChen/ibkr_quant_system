# 2026-04-29 Gateway Runtime State Archive

## Scope

This change makes the dashboard distinguish three separate IBKR runtime states:

- IB Gateway API port is unavailable.
- IB Gateway API socket is listening, but no quant task is currently connected.
- A quant task is actively running and may hold an API client connection.

## Implementation

- Added `gateway_runtime_summary` to dashboard operations overview.
- Added a `量化客户端` row to the simple operations overview.
- Added a `量化客户端` row to each card's `一眼看懂` table.
- Clarified that IB Gateway UI showing `API客户端 已断开` is normal when no `run_once` / execution task is running, as this system connects clients per task rather than keeping a permanent client open.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/generate_dashboard.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_generate_dashboard_helpers.py tests/test_dashboard_degraded_inputs.py tests/test_dashboard_blocks.py tests/test_preflight_supervisor.py`

Result: `26 passed`.

## Residual Note

`tests/test_supervisor_cli.py` currently has two date-sensitive failures on 2026-04-29 because temporary report artifacts are inferred as the prior market day. That is unrelated to this dashboard runtime change and should be handled as a separate deterministic-date test fix.
