# 可运行代码总览

这份文档整理当前项目里可以直接运行的入口脚本，以及这些入口背后的核心模块。

Phase 0 之后的运行约定：

- 推荐先在仓库根执行 `python -m pip install -e ".[dev]"`
- 安装后优先使用 console scripts
  - `ibkr-quant-preflight`
  - `ibkr-quant-supervisor`
  - `ibkr-quant-engine`
  - `ibkr-quant-report`
  - `ibkr-quant-paper`
  - `ibkr-quant-execution`
- 原有 `python -m src...` 路径继续兼容

当前主路径已经切到中长期投资：

- `US` / `HK` 的 `config/ibkr_*.yaml` 默认是 `investment_only`
- `ASX / XETRA / CN` 也已经接入同一套 investment 路径
- `src.main` 仍可运行，但对 `US` / `HK` 不再启动短线交易引擎
- 当前最常用的命令是：
  - 跑 supervisor preflight：`src.tools.preflight_supervisor`
  - 生成投资报告：`src.tools.generate_investment_report`
  - 运行 investment paper：`src.tools.run_investment_paper`
  - 运行 broker 执行计划：`src.tools.run_investment_execution`
  - 运行投资持仓防守检查：`src.tools.run_investment_guard`
  - 运行盘中机会扫描：`src.tools.run_investment_opportunity`
  - 跑周复盘：`src.tools.review_investment_weekly`
  - 跑执行 KPI：`src.tools.review_investment_execution`
  - 跑 baseline / regression 对照：`src.tools.review_baseline_regression`
  - 跑 broker 对账：`src.tools.reconcile_investment_broker`
  - 用 broker 快照同步本地账本：`src.tools.sync_investment_paper_from_broker`
  - 自动调度：`src.app.supervisor`

约定：

- 推荐主运行方式是 console scripts
- `python -m ...` 仍然兼容
- `src/*` 里多数文件是库模块，不是直接执行入口
- `tests/*` 是测试入口

## 1. 运行前提

常见依赖：

- `config/ibkr.yaml`、`config/ibkr_us.yaml`、`config/ibkr_hk.yaml`
- `config/risk*.yaml`
- `config/strategy_defaults*.yaml`
- `audit.db`
- 可选：`symbol_master.db`
- 若涉及实盘或 paper 连接：本地 `IB Gateway`
- 当前仅支持 `IB Gateway`，不再以 `TWS` 作为运行入口
- 若涉及研究层增强：本地 `.env.local`

研究层数据源：

- `IBKR`：账户、执行、可用时的历史行情
- `yfinance`：免费日线 fallback、轻量 snapshot、轻量 news fallback
- `FMP`：基本面
- `FRED`：宏观
- `Finnhub`：新闻、财报日历、推荐趋势

常见输出：

- 审计数据库：`audit.db`
- 报告目录：`reports/`、`reports_us/`、`reports_hk/`
- 投资报告目录：`reports_investment_us/`、`reports_investment_hk/`
- 投资周报目录：`reports_investment_weekly/`
- preflight 目录：`reports_preflight/`
- 短空安全参考文件：`config/reference/short_borrow_fee_*.csv`、`config/reference/short_safety_rules_*.csv`

## 2. 启动前检查

### Supervisor Preflight

```bash
.venv/bin/python -m src.tools.preflight_supervisor --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir reports_preflight
```

输出目录：

- `reports_preflight/supervisor_preflight_summary.json`
- `reports_preflight/supervisor_preflight_report.md`

用途：

- 检查 supervisor 配置是否可读
- 检查 scoped `audit.db`
- 检查 watchlist / ibkr_config / summary 目录
- 检查本地 `127.0.0.1:4001/4002` 的 IB Gateway 端口是否在监听

说明：

- 这是轻量 preflight，不会启动 supervisor
- 如果 supervisor 的 dashboard control service 已开启，也可以在 dashboard 顶部直接点 `立即跑 Preflight`
- dashboard 的 `运维总览` 会汇总 preflight、报告新鲜度、组合健康度和执行模式偏差
- dashboard 的“本周执行质量”优先读取 `reports_investment_weekly/weekly_execution_summary.csv`
- `src.tools.review_investment_execution` 现在更适合手动深挖执行明细
## 3. 当前最常用命令

### HK 投资报告

```bash
python -m src.tools.generate_investment_report --market HK --watchlist_yaml config/watchlists/resolved_hk_top100_bluechip.yaml --out_dir reports_investment_hk
```

输出目录：

- `reports_investment_hk/resolved_hk_top100_bluechip/investment_report.md`
- `reports_investment_hk/resolved_hk_top100_bluechip/investment_candidates.csv`
- `reports_investment_hk/resolved_hk_top100_bluechip/investment_plan.csv`

### HK investment paper

```bash
python -m src.tools.run_investment_paper --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --force
```

输出目录：

- `reports_investment_hk/resolved_hk_top100_bluechip/investment_paper_report.md`
- `reports_investment_hk/resolved_hk_top100_bluechip/investment_portfolio.csv`
- `reports_investment_hk/resolved_hk_top100_bluechip/investment_rebalance_trades.csv`

### HK 投资周复盘

```bash
python -m src.tools.review_investment_weekly --market HK --db audit.db --out_dir reports_investment_weekly --portfolio_id HK:resolved_hk_top100_bluechip --days 7
```

输出目录：

- `reports_investment_weekly/weekly_review.md`
- `reports_investment_weekly/weekly_portfolio_summary.csv`
- `reports_investment_weekly/weekly_holdings_change.csv`
- `reports_investment_weekly/weekly_sector_exposure.csv`

### HK broker 执行计划

```bash
python -m src.tools.run_investment_execution --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
python -m src.tools.run_investment_execution --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --submit
```

输出目录：

- `reports_investment_hk/resolved_hk_top100_bluechip/investment_execution_plan.csv`
- `reports_investment_hk/resolved_hk_top100_bluechip/investment_execution_summary.json`
- `reports_investment_hk/resolved_hk_top100_bluechip/investment_execution_report.md`

### HK investment guard

```bash
python -m src.tools.run_investment_guard --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
python -m src.tools.run_investment_guard --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --submit
```

输出目录：

- `reports_investment_hk/resolved_hk_top100_bluechip/investment_guard_plan.csv`
- `reports_investment_hk/resolved_hk_top100_bluechip/investment_guard_summary.json`
- `reports_investment_hk/resolved_hk_top100_bluechip/investment_guard_report.md`

### HK investment opportunity

```bash
python -m src.tools.run_investment_opportunity --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
```

输出目录：

- `reports_investment_hk/resolved_hk_top100_bluechip/investment_opportunity_scan.csv`
- `reports_investment_hk/resolved_hk_top100_bluechip/investment_opportunity_summary.json`
- `reports_investment_hk/resolved_hk_top100_bluechip/investment_opportunity_report.md`

### HK 执行 KPI

```bash
python -m src.tools.review_investment_execution --market HK --db audit.db --out_dir reports_investment_execution --portfolio_id HK:resolved_hk_top100_bluechip --days 14
```

输出目录：

- `reports_investment_execution/investment_execution_kpi.md`
- `reports_investment_execution/investment_execution_summary.json`
- `reports_investment_execution/investment_execution_runs.csv`
- `reports_investment_execution/investment_execution_orders.csv`
- `reports_investment_execution/investment_execution_fills.csv`

### HK broker 对账

```bash
python -m src.tools.reconcile_investment_broker --market HK --db audit.db --portfolio_id HK:resolved_hk_top100_bluechip --out_dir reports_investment_reconcile
```

输出目录：

- `reports_investment_reconcile/broker_reconciliation.md`
- `reports_investment_reconcile/broker_reconciliation.csv`
- `reports_investment_reconcile/broker_reconciliation_summary.json`

### HK 用 broker 快照同步本地账本

```bash
python -m src.tools.sync_investment_paper_from_broker --market HK --db audit.db --portfolio_id HK:resolved_hk_top100_bluechip --out_dir reports_investment_sync
```

输出目录：

- `reports_investment_sync/broker_sync_report.md`
- `reports_investment_sync/broker_sync_positions.csv`
- `reports_investment_sync/broker_sync_summary.json`

### US 投资报告

```bash
python -m src.tools.generate_investment_report --market US --watchlist_yaml config/watchlist.yaml --out_dir reports_investment_us
```

输出目录：

- `reports_investment_us/watchlist/investment_report.md`
- `reports_investment_us/watchlist/investment_candidates.csv`
- `reports_investment_us/watchlist/investment_plan.csv`

### US investment paper

```bash
python -m src.tools.run_investment_paper --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist --force
```

输出目录：

- `reports_investment_us/watchlist/investment_paper_report.md`
- `reports_investment_us/watchlist/investment_portfolio.csv`
- `reports_investment_us/watchlist/investment_rebalance_trades.csv`

### US 投资周复盘

```bash
python -m src.tools.review_investment_weekly --market US --db audit.db --out_dir reports_investment_weekly --portfolio_id US:watchlist --days 7
```

### US broker 执行计划

```bash
python -m src.tools.run_investment_execution --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
python -m src.tools.run_investment_execution --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist --submit
```

### US investment guard

```bash
python -m src.tools.run_investment_guard --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
python -m src.tools.run_investment_guard --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist --submit
```

### US investment opportunity

```bash
python -m src.tools.run_investment_opportunity --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
```

### XETRA 最小命令

```bash
python -m src.main --market XETRA --startup-check-only
python -m src.tools.generate_investment_report --market XETRA --watchlist_yaml config/watchlists/xetra_top_quality.yaml --out_dir reports_investment_xetra
python -m src.tools.run_investment_paper --market XETRA --report_dir reports_investment_xetra/xetra_top_quality --portfolio_id XETRA:xetra_top_quality --force
python -m src.tools.run_investment_execution --market XETRA --report_dir reports_investment_xetra/xetra_top_quality --portfolio_id XETRA:xetra_top_quality
python -m src.tools.run_investment_guard --market XETRA --report_dir reports_investment_xetra/xetra_top_quality --portfolio_id XETRA:xetra_top_quality
python -m src.tools.run_investment_opportunity --market XETRA --report_dir reports_investment_xetra/xetra_top_quality --portfolio_id XETRA:xetra_top_quality
```

### US 执行 KPI

```bash
python -m src.tools.review_investment_execution --market US --db audit.db --out_dir reports_investment_execution --portfolio_id US:watchlist --days 14
```

### US broker 对账

```bash
python -m src.tools.reconcile_investment_broker --market US --db audit.db --portfolio_id US:watchlist --out_dir reports_investment_reconcile
```

### US 用 broker 快照同步本地账本

```bash
python -m src.tools.sync_investment_paper_from_broker --market US --db audit.db --portfolio_id US:watchlist --out_dir reports_investment_sync
```

### 自动调度

```bash
python -m src.app.supervisor
```

单次执行当前时刻应触发的任务并退出：

```bash
python -m src.app.supervisor --once
```

说明：

- `src.app.supervisor` 是常驻轮询进程，不是一次性脚本
- 启动后会按照 `config/supervisor.yaml` 持续检查时间窗口并自动触发任务
- 前台停止方式是 `Ctrl+C`
- `--once` 会只跑一轮当前时刻应触发的任务，然后退出
- `--once` 不是 dry-run；如果当前时间已到点，会真实执行那一轮任务
- supervisor 支持任务超时，默认：
  - watchlist `180s`
  - report `1200s`
  - investment paper `300s`
  - investment execution `300s`
  - investment guard `240s`
  - investment opportunity `240s`
- investment execution 支持 `execution_day_offset`
  - 当前 HK / US 都配置为“报告成功后的下一交易日再执行”
- 当前默认 `submit_investment_execution: false`
  - supervisor 会自动生成 execution plan
  - 但不会自动向 broker 提交订单
- 当前默认 `submit_investment_guard: false`
  - supervisor 会自动生成 guard plan
  - 但不会自动向 broker 提交保护性止损/止盈单

## 2.1 最小工作流

### HK

```bash
python -m src.main --market HK --startup-check-only
python -m src.tools.generate_investment_report --market HK --watchlist_yaml config/watchlists/resolved_hk_top100_bluechip.yaml --out_dir reports_investment_hk
python -m src.tools.run_investment_paper --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --force
python -m src.tools.run_investment_execution --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
python -m src.tools.review_investment_weekly --market HK --db audit.db --out_dir reports_investment_weekly --portfolio_id HK:resolved_hk_top100_bluechip --days 7
python -m src.tools.review_investment_execution --market HK --db audit.db --out_dir reports_investment_execution --portfolio_id HK:resolved_hk_top100_bluechip --days 14
python -m src.tools.reconcile_investment_broker --market HK --db audit.db --portfolio_id HK:resolved_hk_top100_bluechip --out_dir reports_investment_reconcile
```

### US

```bash
python -m src.main --market US --startup-check-only
python -m src.tools.generate_investment_report --market US --watchlist_yaml config/watchlist.yaml --out_dir reports_investment_us
python -m src.tools.run_investment_paper --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist --force
python -m src.tools.run_investment_execution --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
python -m src.tools.review_investment_weekly --market US --db audit.db --out_dir reports_investment_weekly --portfolio_id US:watchlist --days 7
python -m src.tools.review_investment_execution --market US --db audit.db --out_dir reports_investment_execution --portfolio_id US:watchlist --days 14
python -m src.tools.reconcile_investment_broker --market US --db audit.db --portfolio_id US:watchlist --out_dir reports_investment_reconcile
```

## 3. 主入口

### `src/main.py`

命令示例：

```bash
python -m src.main --market US
python -m src.main --market HK
python -m src.main --market US --startup-check-only
```

功能：

- 项目主交易入口
- 读取市场配置、风险配置、策略配置
- 装配 `IBKRConnection`、`MarketDataService`、`OrderService`、`FillProcessor`
- 装配 `DailyRiskGate`、`ShortSafetyGate`、`PortfolioAllocator`、`EntryGuard`
- 启动 `TradingEngine` 与 `EngineStrategy`
- 当市场配置是 `investment_only` 时，只执行启动自检并退出

适用场景：

- 检查市场配置和数据依赖
- 只有在启用 `intraday` 时才用于直接跑 paper/live 交易
- 启动前做配置和数据自检

关键参数：

- `--market`：选择市场，如 `US`、`HK`
- `--ibkr-config`：覆盖默认 IBKR 配置
- `--startup-check-only`：只做启动自检，不进入交易循环

主要依赖模块：

- `src/app/engine.py`
- `src/strategies/engine_strategy.py`
- `src/risk/model.py`
- `src/risk/limits.py`
- `src/risk/short_safety.py`
- `src/common/storage.py`

### `src/app/supervisor.py`

命令示例：

```bash
python -m src.app.supervisor
```

功能：

- 自动化调度总入口
- 按 `config/supervisor.yaml` 的时间窗口执行任务
- 定时刷新 watchlist
- 定时生成投资报告
- 定时运行 investment paper 账本
- 可选定时同步短空安全参考数据
- 若配置允许，也可拉起 `src.main`

当前调度时间：

- 时区：`Australia/Sydney`
- HK：
  - `19:00` 刷新 watchlist
  - `20:00` 生成投资报告
  - 报告成功后自动运行 investment paper
  - 报告成功后的下一交易日 `12:35` 自动运行 investment execution
  - 市场开盘窗口内每 `30` 分钟自动运行一次 investment guard
- US：
  - `08:30` 生成投资报告
  - 报告成功后自动运行 investment paper
  - 报告成功后的下一交易日 `00:40` 自动运行 investment execution
  - 市场开盘窗口内每 `30` 分钟自动运行一次 investment guard
- 当前 `HK` 和 `US` 都是 `trading.enabled: false`
  - 所以 supervisor 不会启动短线 live/paper 交易引擎
- 当前自动 execution 默认是 dry-run
  - 若要自动提交 broker paper 单，需要把对应 report 项里的 `submit_investment_execution` 改成 `true`
- 当前自动 guard 默认也是 dry-run
  - 若要自动提交保护性 broker 单，需要把对应 report 项里的 `submit_investment_guard` 改成 `true`

适用场景：

- 需要自动跑 weekly/monthly investment report 与 investment paper
- 需要统一调度 watchlist、报告、账本和复盘链路

执行建议：

- 如果你要自动按时间表跑任务，在每天开始前启动一次 `python -m src.app.supervisor`
- 如果你只想临时手动跑某一步，不要启动 supervisor，直接运行单条命令

主要依赖模块：

- `src/tools/refresh_watchlist.py`
- `src/tools/sync_short_safety_from_ibkr.py`
- `src/tools/generate_investment_report.py`
- `src/tools/run_investment_paper.py`
- `src/tools/run_investment_execution.py`
- `src/tools/run_investment_guard.py`
- `src/main.py`

## 4. 报告与分析入口

### `src/tools/generate_investment_report.py`

命令示例：

```bash
python -m src.tools.generate_investment_report --market HK --watchlist_yaml config/watchlists/resolved_hk_top100_bluechip.yaml --out_dir reports_investment_hk
python -m src.tools.generate_investment_report --market US --watchlist_yaml config/watchlists/resolved_us_top100_growth.yaml --out_dir reports_investment_us
python -m src.tools.generate_investment_report --market HK --watchlist_yaml config/watchlists/resolved_hk_top100_bluechip.yaml --top_n 20 --backtest_top_k 10 --fundamentals_top_k 20 --out_dir reports_investment_hk
```

功能：

- 用历史日线和市场环境生成中长期投资候选
- 结合 `long + mid + regime + macro + fundamentals`
- 生成候选排名、投资计划、持有期回测、基本面增强和 Markdown 报告

主要输出：

- `universe_candidates.csv`
- `investment_candidates.csv`
- `investment_plan.csv`
- `investment_backtest.csv`
- `fundamentals.json`
- `enrichment.json`
- `investment_report.md`

关键参数：

- `--market`
- `--watchlist_yaml`
- `--out_dir`
- `--top_n`
- `--max_universe`
- `--db`
- `--symbol_master_db`
- `--use_audit_recent`
- `--backtest_top_k`
- `--fundamentals_top_k`

主要依赖模块：

- `src/offhours/compute_long.py`
- `src/offhours/compute_mid.py`
- `src/analysis/investment.py`
- `src/analysis/investment_backtest.py`
- `src/analysis/report.py`
- `src/enrichment/providers.py`

### `src/tools/run_investment_paper.py`

命令示例：

```bash
python -m src.tools.run_investment_paper --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --force
python -m src.tools.run_investment_paper --market US --report_dir reports_investment_us/resolved_us_top100_growth --portfolio_id US:resolved_us_top100_growth --force
python -m src.tools.run_investment_paper --market HK --reports_root reports_investment_hk --watchlist_yaml config/watchlists/resolved_hk_top100_bluechip.yaml --portfolio_id HK:resolved_hk_top100_bluechip
```

功能：

- 读取投资报告结果，运行 investment paper 组合账本
- 按周/月再平衡频率生成 paper 交易
- 把组合状态写入 `audit.db`

主要输出：

- `investment_portfolio.csv`
- `investment_rebalance_trades.csv`
- `investment_paper_summary.json`
- `investment_paper_report.md`
- `audit.db` 中的 `investment_runs`、`investment_positions`、`investment_trades`

关键参数：

- `--market`
- `--report_dir`
- `--reports_root`
- `--watchlist_yaml`
- `--paper_config`
- `--portfolio_id`
- `--force`

主要依赖模块：

- `src/analysis/investment_portfolio.py`
- `src/common/storage.py`

### `src/tools/review_investment_weekly.py`

命令示例：

```bash
python -m src.tools.review_investment_weekly --db audit.db --out_dir reports_investment_weekly --days 7
python -m src.tools.review_investment_weekly --market HK --db audit.db --out_dir reports_investment_weekly --portfolio_id HK:resolved_hk_top100_bluechip --days 7
python -m src.tools.review_investment_weekly --market US --db audit.db --out_dir reports_investment_weekly --portfolio_id US:resolved_us_top100_growth --days 7
```

功能：

- 汇总 investment paper 组合一周内的权益、交易、持仓变化和行业暴露
- 生成周收益、回撤、换手和交易原因汇总

主要输出：

- `weekly_review.md`
- `weekly_review_summary.json`
- `weekly_portfolio_summary.csv`
- `weekly_trade_log.csv`
- `weekly_holdings_change.csv`
- `weekly_sector_exposure.csv`
- `weekly_reason_summary.csv`
- `weekly_equity_curve.csv`

关键参数：

- `--market`
- `--db`
- `--out_dir`
- `--days`
- `--portfolio_id`
- `--include_legacy`

### `src/tools/run_investment_execution.py`

命令示例：

```bash
python -m src.tools.run_investment_execution --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
python -m src.tools.run_investment_execution --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --submit
python -m src.tools.run_investment_execution --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
```

功能：

- 读取投资报告里的目标配置
- 拉取指定 IBKR paper 账户的真实持仓和现金
- 生成或提交再平衡订单
- 把执行 run、计划单、broker 前后快照写入 `audit.db`

主要输出：

- `investment_execution_plan.csv`
- `investment_execution_summary.json`
- `investment_execution_report.md`
- `audit.db` 中的 `investment_execution_runs`、`investment_execution_orders`、`investment_broker_positions`
- 若 `--submit` 且成交，则还会写入 `orders`、`fills`、`risk_events`

关键参数：

- `--market`
- `--ibkr_config`
- `--execution_config`
- `--paper_config`
- `--db`
- `--report_dir`
- `--portfolio_id`
- `--submit`

主要依赖模块：

- `src/app/investment_engine.py`
- `src/ibkr/investment_orders.py`
- `src/portfolio/investment_allocator.py`
- `src/common/storage.py`

### `src/tools/run_investment_guard.py`

命令示例：

```bash
python -m src.tools.run_investment_guard --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
python -m src.tools.run_investment_guard --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --submit
python -m src.tools.run_investment_guard --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
```

功能：

- 基于当前 broker 持仓做盘中防守检查
- 使用延迟/历史行情评估固定止损、移动止损和盈利回撤止盈
- 只做保护性减仓或退出，不负责新的激进加仓
- 把 guard run、计划单、broker 前后快照写入 `audit.db`

主要输出：

- `investment_guard_plan.csv`
- `investment_guard_summary.json`
- `investment_guard_report.md`
- `audit.db` 中的 `investment_execution_runs`、`investment_execution_orders`、`investment_broker_positions`
- 若 `--submit` 且成交，则还会写入 `orders`、`fills`、`risk_events`

关键参数：

- `--market`
- `--ibkr_config`
- `--execution_config`
- `--guard_config`
- `--db`
- `--report_dir`
- `--portfolio_id`
- `--submit`

主要依赖模块：

- `src/app/investment_guard.py`
- `src/ibkr/investment_orders.py`
- `src/ibkr/market_data.py`
- `src/common/storage.py`

### `src/tools/review_investment_execution.py`

命令示例：

```bash
python -m src.tools.review_investment_execution --market HK --db audit.db --out_dir reports_investment_execution --portfolio_id HK:resolved_hk_top100_bluechip --days 14
python -m src.tools.review_investment_execution --market US --db audit.db --out_dir reports_investment_execution --portfolio_id US:watchlist --days 14
```

功能：

- 汇总 broker-backed investment execution 的运行次数、订单状态、成交、佣金、滑点和最新 broker 仓位
- 区分 broker 标记的 `Filled` 与数据库里是否真的存在 `fill audit`
- 输出执行 KPI，便于判断系统是否真的完成了 broker 下单和成交回写

主要输出：

- `investment_execution_kpi.md`
- `investment_execution_summary.json`
- `investment_execution_runs.csv`
- `investment_execution_orders.csv`
- `investment_execution_fills.csv`
- `investment_execution_risk_events.csv`
- `investment_execution_latest_broker_positions.csv`
- `investment_execution_symbols.csv`

关键参数：

- `--market`
- `--db`
- `--out_dir`
- `--days`
- `--since`
- `--portfolio_id`

### `src/tools/review_baseline_regression.py`

命令示例：

```bash
python -m src.tools.review_baseline_regression --market HK --portfolio_id HK:resolved_hk_top100_bluechip --report_dir reports_investment_hk/resolved_hk_top100_bluechip --out_dir reports_baseline_hk --baseline_name hk_current
python -m src.tools.review_baseline_regression --market US --portfolio_id US:watchlist --report_dir reports_investment_us/watchlist --out_dir reports_baseline_us --baseline_name us_current
python -m src.tools.review_baseline_regression --market XETRA --portfolio_id XETRA:xetra_top_quality --report_dir reports_investment_xetra/xetra_top_quality --out_dir reports_baseline_xetra --baseline_name xetra_current
```

功能：

- 固定保存当前投资报告、paper、execution、opportunity 的样例快照
- 生成核心指标摘要，作为 refactor 前后的 baseline 对照
- 支持和旧 baseline 做字段级比较

主要输出：

- `baseline_snapshot.json`
- `baseline_review.md`
- `baseline_comparison.csv`
- `baseline_comparison.json`
- `samples/`

关键参数：

- `--market`
- `--portfolio_id`
- `--report_dir`
- `--out_dir`
- `--baseline_name`
- `--compare_to`

### `src/tools/generate_trade_report.py`

命令示例：

```bash
python -m src.tools.generate_trade_report --market US
python -m src.tools.generate_trade_report --market HK --top_n 20
```

功能：

- 构建候选池
- 回放共享信号核，得到 `short_sig / total_sig / channel / stability / mid_scale`
- 叠加事件、流动性、借券等 overlay
- 输出正式报告和交易计划

主要输出：

- `universe_candidates.csv`
- `ranked_candidates.csv`
- `trade_plan.csv`
- `enrichment.json`
- `report.md`

适用场景：

- 日常研究
- 盘前决策
- 检查报告与实盘是否仍共用同一套信号语言

关键参数：

- `--market`
- `--ibkr_config`
- `--out_dir`
- `--top_n`
- `--watchlist_yaml`
- `--use_audit_recent`
- `--use_scanner`

主要依赖模块：

- `src/offhours/compute_short.py`
- `src/analysis/scoring.py`
- `src/analysis/plan.py`
- `src/analysis/report.py`

### `src/tools/generate_offhours_lists.py`

命令示例：

```bash
python -m src.tools.generate_offhours_lists --market US --out_dir reports/offhours_us
```

功能：

- 用历史数据生成盘后候选清单
- 计算 short / mid / long 三类 watchlist
- 输出轻量 enrichment 信息
- 不下单，只做准备

主要输出：

- `short_watchlist.csv`
- `mid_watchlist.csv`
- `long_watchlist.csv`
- `enrichment.json`

适用场景：

- 盘后整理下一交易日候选
- 快速生成离线观察清单

### `src/tools/paper_kpi_report.py`

命令示例：

```bash
python -m src.tools.paper_kpi_report --db audit.db --out_dir reports/paper_kpi
python -m src.tools.paper_kpi_report --db audit.db --out_dir reports/paper_kpi --since 2026-03-04T00:00:00+00:00
```

功能：

- 从 `audit.db` 汇总 paper 交易链路
- 串联 `signals_audit -> risk_events -> orders -> fills`
- 按 `market/source` 输出成交转化、阻断数、滑点、净盈亏

主要输出：

- `paper_kpi_summary.json`
- `paper_kpi_pipeline.csv`
- `paper_kpi_symbols.csv`
- `paper_kpi.md`

适用场景：

- 评估 paper 交易效果
- 只看某次修复之后的新样本
- 检查 `HIST`、`REALTIME`、`UNKNOWN` 来源是否被正确分层

### `src/tools/review_shadow_burnin.py`

命令示例：

```bash
python -m src.tools.review_shadow_burnin --db audit.db --out_dir reports/shadow_burnin
```

功能：

- 专门复盘 `SHORT_SAFETY_SHADOW_BLOCK`
- 把 shadow 事件与后续真实短空 entry fill 匹配
- 对比 shadow 样本与 control 样本的滑点和盈亏

主要输出：

- `shadow_burnin_summary.json`
- `shadow_burnin.md`
- `shadow_event_matches.csv`
- `short_entry_fills.csv`
- `blocked_reason_summary.csv`

适用场景：

- 判断某条 short safety 规则是否值得从 shadow 切成硬阻断

## 5. 风控与数据维护入口

### `src/tools/sync_short_safety_from_ibkr.py`

命令示例：

```bash
python -m src.tools.sync_short_safety_from_ibkr --market US
python -m src.tools.sync_short_safety_from_ibkr --market HK
```

功能：

- 从 IBKR 拉取 shortable / bid / ask / shortable level
- 结合 `short_data_sources` 合并外部 borrow / SSR / spread 数据
- 生成运行时使用的短空参考文件

主要输出：

- `config/reference/short_borrow_fee_us.csv`
- `config/reference/short_borrow_fee_hk.csv`
- `config/reference/short_safety_rules_us.csv`
- `config/reference/short_safety_rules_hk.csv`

适用场景：

- 开盘前刷新短空安全数据
- 修正 `borrow_data_unknown`、`spread_unknown`

### `src/tools/refresh_watchlist.py`

命令示例：

```bash
python -m src.tools.refresh_watchlist --config config/watchlists/hk_top100_bluechip.yaml --out config/watchlists/resolved_hk_top100_bluechip.yaml
```

功能：

- 从网页抓取成分股
- 生成标准化后的 resolved watchlist YAML

适用场景：

- 周期性刷新市场观察名单

### `src/tools/build_symbol_master.py`

命令示例：

```bash
python -m src.tools.build_symbol_master --all
python -m src.tools.build_symbol_master --market US
```

功能：

- 从 `config/markets/*/universe.yaml` 和 watchlist 汇总 symbol
- 生成本地 `symbol_master.db`

适用场景：

- 初始化或刷新市场 universe 数据

## 6. 手工与诊断入口

### `src/tools/manual_buy.py`

命令示例：

```bash
python -m src.tools.manual_buy --symbol TSLA --qty 2
```

功能：

- 手工发一笔买单
- 验证连接、下单、成交、状态变更是否正常

适用场景：

- 只验证经纪商链路
- 不适合当作策略入口

注意：

- 这是诊断脚本，不走完整策略和风控链

### `src/tools/export_phase1_quality.py`

命令示例：

```bash
python -m src.tools.export_phase1_quality --db audit.db --out phase1_md_quality.csv
```

功能：

- 导出 `md_quality` 表
- 检查重复桶、缺口、市场数据质量

### `src/tools/export_phase1_top_signals.py`

命令示例：

```bash
python -m src.tools.export_phase1_top_signals --db audit.db --limit 50
```

功能：

- 导出每个 symbol 最新一条 signal audit
- 方便快速查看 top signals

### `src/tools/export_phase1_report.py`

功能：

- 当前与 `src/tools/export_phase1_quality.py` 基本重复
- 属于历史兼容脚本，不是主路径

## 7. 测试入口

### `tests/test_risk_pipeline.py`

命令示例：

```bash
python -m unittest tests.test_risk_pipeline
```

功能：

- 测 `TradeRiskModel`
- 测 `PortfolioAllocator`
- 测 `ShortSafetyGate`
- 测共享信号核回放

### `tests/test_shadow_burnin_review.py`

命令示例：

```bash
python -m unittest tests.test_shadow_burnin_review
```

功能：

- 测 shadow burn-in 匹配与报告逻辑

### `tests/test_paper_execution_reporting.py`

命令示例：

```bash
python -m unittest tests.test_paper_execution_reporting
```

功能：

- 测 paper 订单样本保留逻辑
- 测 fill 行为归一化
- 测 `SOURCE_EXEC_BLOCK` / `PRETRADE_RISK_BLOCK`
- 测 KPI 报表聚合

### `tests/test_investment_modules.py`

命令示例：

```bash
python -m unittest tests.test_investment_modules
```

功能：

- 测投资打分回测
- 测 investment paper 再平衡逻辑
- 测目标权重分配

### `tests/test_review_investment_weekly.py`

命令示例：

```bash
python -m unittest tests.test_review_investment_weekly
```

功能：

- 测周复盘的回撤、持仓变化、行业暴露汇总逻辑

### 全量测试

命令示例：

```bash
python -m unittest discover -s tests
```

## 8. 核心模块职责

这些文件通常不直接执行，但它们是运行入口背后的核心逻辑。

### `src/app/engine.py`

功能：

- 交易循环
- 实时 5 分钟 bar 聚合
- 历史 fallback
- 调用策略产生信号

### `src/strategies/engine_strategy.py`

功能：

- 共享信号核
- 组合 `MR + breakout + mid regime`
- 生成 `TradeSignal`
- 执行前调用 pretrade risk、short safety、entry guard、allocator

### `src/risk/model.py`

功能：

- 构建 `PreTradeRiskSnapshot`
- 输出 ATR 止损、滑点、gap、流动性、借券附加
- 生成显式 `stop_price / take_profit_price`

### `src/risk/short_safety.py`

功能：

- 做空执行许可层
- 检查 locate、borrow、SSR、spread、timing、事件风险、流动性

### `src/risk/limits.py`

功能：

- 日内风险门
- 连亏控制
- 事件风险和借券上下文刷新

### `src/portfolio/allocator.py`

功能：

- 场景化 sizing
- 根据 `risk_per_share` 和流动性折扣决定下单量

### `src/ibkr/orders.py`

功能：

- 下 bracket order
- 支持显式 stop / take-profit
- 落订单审计信息

### `src/ibkr/fills.py`

功能：

- 回写 fills
- 计算实际滑点与偏差
- 触发成交后的风险事件和账本更新

### `src/common/storage.py`

功能：

- `audit.db` 的统一读写层
- 持久化 `orders`、`fills`、`risk_events`、`signals_audit`
- 持久化 `investment_runs`、`investment_positions`、`investment_trades`

## 9. 建议优先使用的入口

如果按当前项目主路径排序，建议优先使用：

1. `src/app/supervisor.py`
   用于自动化调度全流程。
2. `src/tools/generate_investment_report.py`
   用于生成中长期投资报告。
3. `src/tools/run_investment_paper.py`
   用于运行 investment paper 组合账本。
4. `src/tools/review_investment_weekly.py`
   用于每周复盘组合表现。
5. `src/main.py`
   当前主要用于启动自检；只有切回 `intraday` 时才用于交易引擎。
6. `src/tools/generate_trade_report.py`
   保留的短线报告入口。
7. `src/tools/paper_kpi_report.py`
   保留的短线 paper KPI 入口。
8. `src/tools/sync_short_safety_from_ibkr.py`
   用于短空安全数据刷新。

## 10. 不建议当主入口使用的脚本

- `src/tools/manual_buy.py`
  只适合诊断，不代表完整策略执行。
- `src/tools/export_phase1_report.py`
  历史兼容脚本，不是当前主路径。
- `src/tools/export_phase1_quality.py`
  只做数据库导出，不参与主策略链。
- `src/tools/export_phase1_top_signals.py`
  只做数据库导出，不参与主策略链。
