# Change Archive: Paper Strategy Optimization And Auto Order Readiness

Date: 2026-05-11

## Summary

This change prepares the paper investment system for controlled automated order submission without opening live trading.

The strategy change is deliberately narrow:

- Paper IBKR configs with 2026-W19 strategy evidence now use paper-only layered strategy defaults.
- The 2026-W19 `SIGNAL_RANKING_INVERTED` evidence for ASX, CN, HK, and XETRA is reflected as a one-step mean-reversion weight reduction from `0.60` to `0.55`.
- Live configs continue to reference the base market strategy defaults and are not changed.

The execution change adds an explicit auto-order readiness gate:

- `src/common/auto_order_readiness.py` evaluates whether a portfolio may submit automated investment execution.
- `src/tools/review_auto_order_readiness.py` writes a read-only JSON/markdown readiness report.
- `src/app/supervisor.py` blocks `submit_investment_execution=true` before execution when readiness has hard blocks.
- `config/supervisor.yaml` enables the paper readiness policy while keeping live submit disabled.

## Readiness Contract

Automated submit is blocked when:

- preflight is missing, stale, or failed
- weekly review is missing or stale
- gateway budget is degraded
- strategy suggestion auto-apply is detected
- strategy suggestion follow-up is degraded
- stale strategy suggestions exist
- account mode is not paper and `allow_live_submit` is not explicitly enabled

Open strategy suggestions are warnings, not hard blocks. They remain visible so the operator knows the current paper run is still collecting evidence on pending strategy calibration work.

## Blocking Condition Optimization

The readiness gate now keeps the hard-block rules conservative while reducing false positives:

- Preflight fail/warn checks are scoped to the current portfolio when the preflight artifact includes structured checks.
- Global preflight checks such as `config`, `runtime_root`, `dashboard_db`, and `ibkr_port` still affect every relevant portfolio.
- IBKR gateway budget status is scoped by market when `ibkr_gateway_budget_rows` are present.
- Strategy parameter suggestions are scoped by portfolio and de-duplicated by primary field/config path, so carried-forward suggestions do not inflate open counts.
- Strategy suggestion follow-up degradation is scoped by portfolio when follow-up rows are available.
- Each readiness row now includes `hard_block_details` and `warning_details` with remediation text.

This means one market's watchlist/config/gateway issue no longer blocks unrelated ready markets, while true global failures still block automated submit.

## IBKR Client Session Isolation

The running paper supervisor exposed a US `clientId already in use` failure when consecutive IBKR subprocesses reused the same market `client_id` before Gateway finished clearing the previous API session.

The follow-up change keeps the client footprint bounded while removing the hard collision:

- `src/common/ibkr_client_id.py` resolves supervisor-provided clientId offsets and bounded retry spans.
- `src/app/supervisor.py` injects task-specific `IBKR_CLIENT_ID_OFFSET` for Gateway subprocesses.
- `src/ibkr/connection.py` applies the offset centrally, so existing tools keep using their config files while supervisor-launched tasks get isolated clientIds.
- The connection layer retries a small adjacent clientId range when Gateway still has an orphan session.

The offsets are task-type based, not unbounded per request, so the visible Gateway clients stay predictable while report/snapshot/opportunity/execution/guard tasks stop blocking each other.

## Market Data Request Cooldown

The Gateway logs also showed repeated historical-data attempts after IBKR had already returned empty/no-permission style results. The cache layer previously stored successful bars and allowed stale fallback, but did not remember recent failures.

The follow-up change adds a bounded failure cooldown:

- `MarketDataService` writes per-request failure markers for empty historical responses and request errors.
- Subsequent identical 5m/daily requests skip Gateway during the cooldown window.
- If stale cache exists, the service returns stale cache; otherwise the adapter can fall back to yfinance or return empty.
- `market_data_service_from_config()` exposes `hist_empty_cooldown_sec`, `hist_error_cooldown_sec`, and `hist_failure_cache_dir`.

This reduces repeated IBKR traffic for known-bad request combinations without changing the order submission path.

## Dashboard Large Artifact Fallback

The live paper supervisor also exposed a dashboard timeout risk. Profiling showed that `weekly_review_summary.json` had grown to hundreds of MB because it embedded large decision/evidence rows, and `generate_dashboard` repeatedly parsed that full file for small weekly sections.

The follow-up change makes dashboard generation consume the split artifacts first:

- Oversized `weekly_review_summary.json` is skipped as a section fallback and read only through metadata-only artifact health.
- Oversized standalone weekly evidence JSON is skipped for row loading; dashboard uses bounded CSV rows for display/evidence action context.
- Artifact health still reports `generated_at`, `schema_version`, and `row_count` from the JSON header instead of treating the file as missing.

This keeps weekly review artifacts available for offline analysis while preventing dashboard refresh from blocking the supervisor loop.

## Weekly Review Summary Slimming

The weekly review writer now prevents the root summary from becoming the next dashboard bottleneck:

- `weekly_review_summary.json` no longer embeds full `decision_evidence_rows` or `unified_evidence_rows`.
- The summary keeps `decision_evidence_row_count`, `unified_evidence_row_count`, and `evidence_artifacts` references.
- `weekly_tuning_dataset.json` no longer duplicates full unified evidence rows.
- Full evidence remains in the dedicated CSV/JSON artifacts for offline analysis and contract checks.

## SQLite Lock Resilience

The running supervisor later exposed a `sqlite3.OperationalError: database is locked` during broker snapshot reuse. That path writes a reused broker snapshot into `investment_execution_runs` immediately after another investment subprocess has touched the same `audit.db`.

The follow-up change makes SQLite access less fragile across the whole project:

- `Storage` sets a longer `busy_timeout`, enables WAL where possible, and retries short-lived busy/locked failures for `execute`, `executemany`, and `commit`.
- `connect_sqlite()` centralizes lock-tolerant defaults for report/review/export tools that read the same runtime DB.
- Direct `sqlite3.connect(...)` calls under `src/tools`, `src/common`, and `src/app` have been consolidated to those two entry points.
- A regression test holds a temporary SQLite write lock, releases it, and verifies that `insert_investment_execution_run()` completes instead of surfacing a transient lock as a supervisor-crashing error.

## Current Local Verification

Commands run:

```bash
git diff --check
PYTHONDONTWRITEBYTECODE=1 python3 -m py_compile src/common/auto_order_readiness.py src/common/ibkr_client_id.py src/tools/review_auto_order_readiness.py src/app/supervisor.py src/tools/generate_investment_report.py src/ibkr/connection.py src/ibkr/market_data.py src/offhours/ib_setup.py
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_market_data_registration.py tests/test_ibkr_client_id.py tests/test_auto_order_readiness.py tests/test_strategy_config.py tests/test_project_packaging.py
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py -k "investment_execution or auto_order or preflight"
PYTHONDONTWRITEBYTECODE=1 python3 -m src.tools.preflight_supervisor --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir reports_preflight
PYTHONDONTWRITEBYTECODE=1 python3 -m src.tools.review_auto_order_readiness --config config/supervisor.yaml --out_dir reports_supervisor
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_generate_dashboard_helpers.py
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_weekly_output_support.py
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_investment_workflow_smoke.py::test_investment_workflow_cli_smoke_generates_contract_artifacts
PYTHONDONTWRITEBYTECODE=1 python3 -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir reports_supervisor
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_storage_sqlite_locking.py tests/test_imports.py tests/test_project_packaging.py tests/test_generate_dashboard_helpers.py tests/test_review_weekly_output_support.py tests/test_supervisor_cli.py::SupervisorCliTests::test_supervisor_reuses_broker_snapshot_for_same_market_account_cycle
```

Results:

- targeted unit tests: `13 passed`
- supervisor targeted tests: `3 passed, 97 deselected`
- dashboard helper tests: `37 passed`
- weekly output support tests: `3 passed`
- investment workflow smoke: `1 passed`
- sqlite lock/import/packaging/dashboard/weekly/supervisor broker reuse: `49 passed`
- dashboard generation: completed locally in about `1.31s`
- preflight: `29 pass, 0 warn, 0 fail`
- auto-order readiness: `6 ready/warning, 0 blocked, 1 disabled`

The disabled row is CN, which remains research-only for automatic execution.

## Operator Path

Before starting the paper supervisor for automated submit:

```bash
ibkr-quant-preflight --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir reports_preflight
ibkr-quant-auto-order-readiness --config config/supervisor.yaml --out_dir reports_supervisor
```

Then inspect:

- `reports_preflight/supervisor_preflight_summary.json`
- `reports_supervisor/auto_order_readiness.json`
- `reports_supervisor/auto_order_readiness.md`

Only start the long-running paper supervisor when `blocked_count == 0`:

```bash
ibkr-quant-supervisor --config config/supervisor.yaml
```

Live automation remains blocked by default and requires a separate governance change, not just a command-line start.
