# IBKR Gateway Interaction Optimization - 2026-05-09

## Problem Observed

The IB Gateway log shows two separate pressure sources:

- Repeated HMDS daily history requests such as `TSM@SMART Trades ... len:90 d step:1 day` and `USO@SMART Trades ... len:90 d step:1 day`.
- Multiple API client tabs at the same time, including base market clients such as `101/111/131/141/151` and labeling clients such as `801/811/831/841/851`.

Gateway expands a single historical data call into internal `agg`, `indicativeData`, `adjustments`, and `cf` queries. That fan-out is normal inside IBKR, but repeated application-level requests make the Gateway log and pacing pressure much worse.

## Code Paths

The main project paths that interact with IBKR are:

- `src/tools/generate_investment_report.py`: connects through the market `client_id`, optionally runs scanner, qualifies contracts, and loads daily bars for ranking.
- `src/tools/run_investment_opportunity.py`: connects through the market `client_id`, loads candidate daily bars and intraday bars for entry opportunities.
- `src/tools/run_investment_guard.py`: connects through the market `client_id`, reads broker positions and daily bars for guard decisions.
- `src/tools/run_investment_execution.py`: connects through the market `client_id`, reads broker equity/positions and can submit orders.
- `src/tools/sync_investment_broker_snapshot.py`: connects through the market `client_id`, reads account summary and positions.
- `src/tools/label_investment_snapshots.py`: previously connected with `base_client_id + 700`, which explains Gateway clients `801/811/831/841/851`.

Supervisor runs these as subprocesses. Each subprocess creates its own IBKR API connection, then disconnects. That is safe from a process-isolation standpoint, but it creates repeated Gateway clients and repeated account/history requests when several maintenance tasks are due close together.

## Changes Implemented

### Daily HMDS Cache

`src/ibkr/market_data.py` now caches daily historical bars on disk under `.cache/market_data_daily`.

Defaults:

- Fresh cache TTL: 6 hours.
- Stale fallback TTL: 7 days.
- Cache key includes symbol, contract identity, days, duration, `whatToShow`, and RTH flag.

Effect:

- Repeated `get_daily_bars(symbol, days=90)` calls from report/opportunity/guard/labeling no longer hit IBKR again within the fresh TTL.
- If IBKR returns an empty daily response or fails, the system can use a stale cached daily series instead of repeatedly retrying HMDS.

### Market-Level Market Data Knobs

`src/offhours/ib_setup.py` now exposes `market_data_service_from_config()` as the standard constructor for Gateway-facing market data clients.

All main `config/ibkr_*.yaml` files now include:

```yaml
market_data:
  request_timeout_sec: 12.0
  hist_retry_attempts: 2
  hist_retry_backoff_sec: 1.5
  hist_5m_cache_ttl_sec: 90
  hist_5m_cache_stale_fallback_sec: 900
  hist_daily_cache_ttl_sec: 21600
  hist_daily_cache_stale_fallback_sec: 604800
```

Effect:

- US/HK/CN/ASX/XETRA paper and live configs can now tune historical-data retry and cache behavior separately.
- Research-heavy markets can lengthen daily cache TTL without changing live execution code.
- Existing top-level cache keys still work for compatibility, but nested `market_data` keys take precedence.

### Labeling Avoids IBKR First

`src/tools/label_investment_snapshots.py` now tries yfinance/cache daily bars before opening an IBKR connection.

Effect:

- Offline outcome labeling should usually avoid opening `base_client_id + 700` clients.
- If external/cache history is unavailable, it still falls back to IBKR, but the fallback connection is read-only.

### Broker Snapshot Single-Flight

`src/app/supervisor.py` now deduplicates broker snapshot syncs within one supervisor cycle by market, account, IBKR config, and DB path.

Effect:

- The first due portfolio performs the actual IBKR `accountSummary` / `portfolio` / `positions` sync.
- Later due portfolios sharing the same market/account/db reuse the first snapshot without opening another Gateway client.
- Reused snapshots still write portfolio-scoped summary JSON and broker position rows, so weekly review and dashboard consumers keep per-portfolio visibility.

### Supervisor IBKR Task Spacing

`src/app/supervisor.py` now spaces known IBKR Gateway subprocess tasks with `ibkr_task_min_gap_sec`.

Default config:

- `config/supervisor.yaml`: `ibkr_task_min_gap_sec: 2.0`

Affected task families:

- `generate_investment_report`
- `generate_trade_report`
- `sync_investment_broker_snapshot`
- `run_investment_execution`
- `run_investment_guard`
- `run_investment_opportunity`
- `short_safety_sync`
- `label_investment_snapshots`

Effect:

- Report, broker snapshot, opportunity, guard, and execution tasks are no longer launched back-to-back in the same second when they all become due in one cycle.
- Non-IBKR tasks such as dashboard generation, weekly review, baseline review, and local paper simulation are not delayed.

### IBKR Request Telemetry

`src/common/ibkr_telemetry.py` now records lightweight JSONL events for Gateway-facing request families.

Tracked request kinds:

- `historical_daily`
- `historical_5m`
- `scanner`
- `account_summary`
- `positions`

Runtime behavior:

- Events are appended under `.cache/ibkr_request_telemetry` by default.
- `IBKR_TELEMETRY_DIR` can redirect telemetry output.
- `IBKR_TELEMETRY_DISABLED=1` disables telemetry.
- Supervisor sets `IBKR_TELEMETRY_TOOL` and `IBKR_TELEMETRY_MARKET` for Gateway subprocesses.

Weekly artifacts:

- `weekly_ibkr_request_summary.csv`
- `weekly_ibkr_request_summary.json`

Effect:

- Weekly review can now show actual Gateway request count, cache-hit count, request kind, market, and task source.
- Cache-hit telemetry makes it possible to verify that daily HMDS caching and broker snapshot single-flight are reducing real Gateway load.

## Expected Gateway Impact

After these changes:

- Repeated same-symbol daily HMDS requests should drop sharply during a 6-hour window.
- Gateway clients `801/811/831/841/851` should not appear during labeling unless yfinance/cache history is missing.
- Base clients `101/111/131/141/151` can still appear when live report, guard, execution, opportunity, scanner, or manual tools run.
- Broker snapshot clients should appear once per market/account/db per supervisor cycle instead of once per portfolio.
- Due IBKR tasks should be spread by at least `ibkr_task_min_gap_sec` rather than launched in a burst.
- Weekly review should expose Gateway request pressure instead of requiring manual Gateway-log inspection.

## Remaining Hotspots

These paths can still create repeated clients and requests:

- Guard, opportunity, and execution are separate subprocesses and each creates a new connection.
- Scanner expansion is cached, but scanner requests still need a Gateway connection when cache expires.
- Account summary and position reads inside live execution/guard remain intentionally fresh and are not reused from the broker snapshot artifact.
- Manual tools can still open ad hoc clients; avoid running them while supervisor guard/execution windows are active.

## Next Technical Path

1. Avoid running multiple supervisors.
   - Only one supervisor should be active against the same Gateway/account.
   - Manual tools should be run outside active guard/execution windows when possible.

2. Add per-market weekly load budgets.
   - Use `weekly_ibkr_request_summary.json` to flag markets whose real Gateway request count exceeds budget.
   - Start with soft warnings; do not block trading tasks until the telemetry baseline is stable.

3. Consider a long-lived Gateway session broker only after telemetry proves subprocess connection churn is still material.
   - A shared broker is more complex than disk caches and task spacing.
   - Do not add it until weekly telemetry shows repeated fresh account/history requests remain a bottleneck.

## Acceptance Checks

- Running the same opportunity scan twice within 6 hours should not trigger a second daily HMDS request for already cached symbols.
- Changing `market_data.hist_daily_cache_ttl_sec` in a market config should affect report/opportunity/guard/labeling historical requests for that market.
- Labeling across all markets should not open `801/811/831/841/851` when daily cache/external history exists.
- Two due broker snapshot portfolios sharing one market/account/db should trigger one real sync and one reused artifact.
- Consecutive due IBKR subprocesses should be spaced by the configured `ibkr_task_min_gap_sec`.
- Weekly review should write `weekly_ibkr_request_summary.json/csv` with request counts grouped by market, tool, kind, and status.
- Gateway should show fewer orphan ECS cleanup entries after repeated scans.
- Weekly review should still produce outcome artifacts with no regression in labeled row counts.
