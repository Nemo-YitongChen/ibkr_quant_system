# 2026-06-26 HK outcome and shutdown diagnostics

## Scope

Verified existing HK opportunity outcome artifacts and tightened Supervisor trade-engine stop diagnostics. This archive is read-only for strategy parameters: it does not relax risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gates.

## HK outcome evidence

Source artifact:

- `runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/opportunity_outcome_validation.csv`

Latest artifact timestamp:

- `2026-06-24T12:19:04.112359+00:00`

Validated rows:

| Portfolio | Group | Status | 5d avg bps | 20d avg bps | 5d positive rate | 20d positive rate | 5d sample | 20d sample |
|---|---|---|---:|---:|---:|---:|---:|---:|
| HK:resolved_hk_top100_bluechip | positive_post_cost_candidates | OUTCOME_SUPPORTS_GROUP | 122.893992 | 253.844811 | 0.719957 | 0.699454 | 932 | 732 |
| HK:resolved_hk_top100_tech_growth | positive_post_cost_candidates | OUTCOME_SUPPORTS_GROUP | 125.190715 | 264.687094 | 0.717733 | 0.701473 | 953 | 747 |
| HK:resolved_hk_top100_bluechip | close_wait_pullback | OUTCOME_SUPPORTS_GROUP | 125.961554 | 212.066284 | 0.725019 | 0.699601 | 1331 | 1002 |
| HK:resolved_hk_top100_tech_growth | close_wait_pullback | OUTCOME_SUPPORTS_GROUP | 126.738345 | 222.088142 | 0.721033 | 0.700980 | 1355 | 1020 |

Interpretation:

- Existing evidence supports keeping HK opportunity groups under active monitoring and preparing paper-only, single-field trials.
- The positive historical 5/20d outcomes do not justify bypassing current submit-quality gates.
- Current HK plan state is still blocked at execution planning: `HK:resolved_hk_top100_bluechip` is `NO_BUY_ORDERS`; `HK:resolved_hk_top100_tech_growth` is `NO_ORDERS` in `market_readiness.json`.
- Candidate-level plan rows still show `high_expected_cost` on most positive edge-score HK names, so the next safe optimization remains market-scoped, paper-only calibration of cost threshold or near-entry anchor with unchanged risk and edge gates.

## Shutdown diagnosis

Runtime evidence:

- `runtime_data/paper_investment_only_duq152001/reports_supervisor/supervisor_shutdown_status.json` currently reports `status=running`, `reason=ignored_signal:SIGHUP`, `pid=77976`.
- `runtime_data/paper_investment_only_duq152001/reports_supervisor/supervisor_cycle_summary.json` reports `trade_engine.status=stopped` when no configured live trading window is active.

Root cause:

- `src.main` is the intraday trade-engine child process managed by Supervisor.
- Supervisor starts this child only when `_active_live_market(now)` finds an enabled market whose `trading.enabled` is true and whose configured trading window is open.
- Current `config/supervisor.yaml` has `trading.enabled: false` for HK, US, XETRA, CN, and ASX, so Supervisor intentionally stops or does not start the intraday child process.
- This is not a crash. Supervisor itself is still running.

Code improvement:

- `src/app/supervisor.py` now reports the specific stop reason:
  - `all_market_trading_windows_disabled`
  - `no_market_trading_window_open`
  - `no_enabled_markets`
- `trade_engine` summary now includes enabled/live-window counts and market names so the dashboard can distinguish config-disabled shutdown from ordinary closed-window shutdown.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py`
- Result: `130 passed`
