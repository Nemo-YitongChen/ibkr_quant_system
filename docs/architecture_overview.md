# Architecture Overview

## Purpose

`ibkr_quant_system` is evolving into a personal investment operating system built around IBKR.
Its core loop is:

1. build or refresh watchlists
2. generate investment research reports
3. simulate paper portfolio changes
4. prepare broker execution plans
5. run intraday guard and opportunity checks
6. review outcomes weekly and feed adjustments back into the system

## System Layers

### Market access

- `src/ibkr/*`: broker connectivity, contracts, market data, orders, fills
- `src/enrichment/*`: FMP, FRED, Finnhub, TradingEconomics, yfinance enrichment
- `src/data/*`: adapter and shared data model layer

### Research

- `src/analysis/investment.py`: scoring and action classification
- `src/analysis/investment_backtest.py`: lightweight historical validation
- `src/analysis/report.py`: CSV, JSON and Markdown report output

### Portfolio and execution

- `src/tools/run_investment_paper.py`: local ledger and rebalance simulation
- `src/app/investment_engine.py`: broker-facing execution plan and submission
- `src/app/investment_guard.py`: defensive actions and guard plan generation
- `src/app/investment_opportunity.py`: intraday opportunity scan

### Operations

- `src/app/supervisor.py`: scheduling, orchestration, dashboard hooks
- `src/tools/preflight_supervisor.py`: lightweight readiness checks
- `src/app/dashboard_control.py`: HTTP control surface for run mode changes

### Audit and review

- `src/common/storage.py`: SQLite audit/event store
- `src/tools/review_investment_weekly.py`: weekly review, execution quality, feedback calibration
- `src/tools/reconcile_investment_broker.py`: broker reconciliation

## Runtime model

The repo separates source code from runtime outputs:

- source code: `src/`, `config/`, `docs/`, `tests/`
- runtime outputs: `reports_*`, `runtime_data/`, `.cache/`

Most runtime artifacts are already ignored by `.gitignore`, which helps keep production evidence local while preserving the operating system code in Git.

## Current market scope

- `HK`, `US`, `ASX`, `XETRA`: shared investment workflow
- `CN`: currently research-only

## Near-term engineering focus

- keep the end-to-end loop stable for `HK` and `US`
- reduce concentration of logic inside large orchestration modules
- improve CI, preflight discipline and deployment safety before adding more strategy surface area
