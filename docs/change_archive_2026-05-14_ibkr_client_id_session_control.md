# 2026-05-14 IBKR ClientId Session Control

## Problem

IB Gateway can keep a client session visible after a local script fails, times out, or is killed. The previous connection policy allowed adjacent clientId retry by default, so a blocked `1411` could become `1412`, then later another adjacent id. That makes the Gateway look busier and can hide the real issue: an old process or orphan session still owns the intended deterministic clientId.

## Decision

Default behavior is now single-clientId per task:

- supervisor still injects deterministic task offsets, for example execution uses the `run_investment_execution` offset.
- `IBKR_CLIENT_ID_RETRY_SPAN` defaults to `1`.
- `IBKR_CONNECT_MAX_ROUNDS` defaults to `3`.
- `config/supervisor.yaml` sets `ibkr_client_id_retry_span: 1`.
- `config/supervisor.yaml` sets `ibkr_connect_max_rounds: 3`.
- adjacent retry is opt-in only, for temporary operator use.

## Operator Rule

When Gateway reports `clientId already in use`:

1. Stop the old supervisor or stale Python task first.
2. Wait for Gateway orphan cleanup, or restart Gateway if the session is stuck.
3. Re-run preflight.
4. Re-run paper execution only after preflight is fresh.

Do not let automated tasks keep walking client IDs upward. More client tabs do not improve execution quality; they increase noise and request load.

## Request Load Follow-Up

For the 1000 AUD owner paper stage, high-frequency opportunity refreshes are paused and paper report configs prefer yfinance daily data. This keeps IBKR Gateway focused on broker state and order lifecycle instead of repeatedly serving 5m historical scans. Submit readiness should stay blocked while the 7-day budget window is degraded; do not raise budgets just to force paper orders.

## Follow-Up Fixes

- Research-only report generation now keeps scanner disabled unless `include_scanner_research_only: true` is explicitly set.
- Opportunity refresh now treats CSV boolean strings correctly, so `earnings_in_14d=False` no longer becomes `WAIT_EVENT`.
- yfinance stale cache selection now prefers the cache whose last bar is newest across `1y/2y/5y/10y`, preventing report and opportunity from using conflicting prices for the same symbol.
- The refreshed US paper execution artifact shows `primary_no_order_reason=NO_TARGET_WEIGHTS`: after cache normalization the current strategy has no buy target, not a clientId or execution-capital blocker.
