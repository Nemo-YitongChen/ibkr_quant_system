# ibkr_quant_system

## Docs

- `docs/runnable_code_summary.md`
  - 汇总项目里可直接运行的入口脚本
  - 说明每个入口的功能、输入、输出和典型用途
- `docs/supervisor_runbook.md`
  - 汇总 `paper / live` 的日常启动、preflight 和 dashboard 控制模式

## Data Sources

当前投资研究层的数据源分工：

- `IBKR`
  - broker 持仓、账户、执行
  - 可用时优先提供历史行情
- `yfinance`
  - 免费日线 fallback
  - 轻量 market snapshot
  - 轻量 market news fallback
- `FMP`
  - 基本面主补充源
  - `PE / 利润率 / 增长 / ROE / 行业`
- `FRED`
  - 宏观主源
  - 当前已接利率、失业率、通胀
- `Finnhub`
  - 新闻、财报日历、推荐趋势主补充源
  - 当前项目支持 `FINNHUB_API_KEY` 和 `FINNHUB_WEBHOOK_SECRET`

本地环境变量：

```bash
FMP_API_KEY=
FRED_API_KEY=
TE_API_KEY=
FINNHUB_API_KEY=
FINNHUB_WEBHOOK_SECRET=
```

## Current Path

项目当前主路径是中长期投资，不是短线交易。

现在已支持：

- `US`
- `HK`
- `ASX`
- `XETRA`
- `CN`

当前推荐的运行方式：

- `config/supervisor.yaml`
  - `paper` 主配置
  - 覆盖 `HK / US / ASX / XETRA / CN`
  - `CN` 保持 `research-only`
- `config/supervisor_live.yaml`
  - `live` 一键配置
  - 覆盖 `HK / US / ASX / XETRA`

最推荐先跑的命令：

```bash
.venv/bin/python -m src.tools.preflight_supervisor --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir reports_preflight
.venv/bin/python -m src.app.supervisor --config config/supervisor.yaml
.venv/bin/python -m src.app.supervisor --config config/supervisor_live.yaml
```

最常用命令：

```bash
python -m src.tools.generate_investment_report --market HK --watchlist_yaml config/watchlists/resolved_hk_top100_bluechip.yaml --out_dir reports_investment_hk
python -m src.tools.run_investment_paper --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --force
python -m src.tools.run_investment_execution --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
python -m src.tools.run_investment_guard --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
python -m src.tools.run_investment_opportunity --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
python -m src.tools.review_investment_weekly --market HK --db audit.db --out_dir reports_investment_weekly --portfolio_id HK:resolved_hk_top100_bluechip --days 7
python -m src.tools.review_investment_execution --market HK --db audit.db --out_dir reports_investment_execution --portfolio_id HK:resolved_hk_top100_bluechip --days 14
python -m src.tools.review_baseline_regression --market HK --portfolio_id HK:resolved_hk_top100_bluechip --report_dir reports_investment_hk/resolved_hk_top100_bluechip --out_dir reports_baseline_hk --baseline_name hk_current
python -m src.tools.reconcile_investment_broker --market HK --db audit.db --portfolio_id HK:resolved_hk_top100_bluechip --out_dir reports_investment_reconcile
python -m src.tools.sync_investment_paper_from_broker --market HK --db audit.db --portfolio_id HK:resolved_hk_top100_bluechip --out_dir reports_investment_sync
python -m src.tools.generate_investment_report --market US --watchlist_yaml config/watchlist.yaml --out_dir reports_investment_us
python -m src.tools.run_investment_paper --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist --force
python -m src.tools.run_investment_execution --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
python -m src.tools.run_investment_guard --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
python -m src.tools.run_investment_opportunity --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
python -m src.tools.review_investment_weekly --market US --db audit.db --out_dir reports_investment_weekly --portfolio_id US:watchlist --days 7
python -m src.tools.review_investment_execution --market US --db audit.db --out_dir reports_investment_execution --portfolio_id US:watchlist --days 14
python -m src.tools.review_baseline_regression --market US --portfolio_id US:watchlist --report_dir reports_investment_us/watchlist --out_dir reports_baseline_us --baseline_name us_current
python -m src.tools.reconcile_investment_broker --market US --db audit.db --portfolio_id US:watchlist --out_dir reports_investment_reconcile
python -m src.tools.sync_investment_paper_from_broker --market US --db audit.db --portfolio_id US:watchlist --out_dir reports_investment_sync
python -m src.tools.generate_investment_report --market XETRA --watchlist_yaml config/watchlists/xetra_top_quality.yaml --out_dir reports_investment_xetra
python -m src.tools.run_investment_paper --market XETRA --report_dir reports_investment_xetra/xetra_top_quality --portfolio_id XETRA:xetra_top_quality --force
python -m src.tools.run_investment_execution --market XETRA --report_dir reports_investment_xetra/xetra_top_quality --portfolio_id XETRA:xetra_top_quality
python -m src.tools.run_investment_guard --market XETRA --report_dir reports_investment_xetra/xetra_top_quality --portfolio_id XETRA:xetra_top_quality
python -m src.tools.run_investment_opportunity --market XETRA --report_dir reports_investment_xetra/xetra_top_quality --portfolio_id XETRA:xetra_top_quality
python -m src.tools.review_baseline_regression --market XETRA --portfolio_id XETRA:xetra_top_quality --report_dir reports_investment_xetra/xetra_top_quality --out_dir reports_baseline_xetra --baseline_name xetra_current
python -m src.app.supervisor
```

`src.app.supervisor` 是常驻轮询进程，不是一次性命令。执行后会一直运行，直到你手动停止。

启动方式：

```bash
python -m src.app.supervisor
```

单次执行当前时刻应触发的任务并退出：

```bash
python -m src.app.supervisor --once
```

注意：

- `--once` 不是 dry-run
- 如果当前时间已经过了某个市场的计划时间，它会真实执行那一轮任务

停止方式：

- 当前终端前台运行时：`Ctrl+C`
- 如果你放到后台或由进程管理器托管：用对应的 stop/kill 命令结束

如果只想在启动前做轻量检查：

```bash
.venv/bin/python -m src.tools.preflight_supervisor --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir reports_preflight
```

## Minimal Workflow

HK 最小流程：

```bash
python -m src.main --market HK --startup-check-only
python -m src.tools.generate_investment_report --market HK --watchlist_yaml config/watchlists/resolved_hk_top100_bluechip.yaml --out_dir reports_investment_hk
python -m src.tools.run_investment_paper --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --force
python -m src.tools.run_investment_execution --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
python -m src.tools.run_investment_guard --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
python -m src.tools.review_investment_weekly --market HK --db audit.db --out_dir reports_investment_weekly --portfolio_id HK:resolved_hk_top100_bluechip --days 7
python -m src.tools.review_investment_execution --market HK --db audit.db --out_dir reports_investment_execution --portfolio_id HK:resolved_hk_top100_bluechip --days 14
python -m src.tools.reconcile_investment_broker --market HK --db audit.db --portfolio_id HK:resolved_hk_top100_bluechip --out_dir reports_investment_reconcile
```

US 最小流程：

```bash
python -m src.main --market US --startup-check-only
python -m src.tools.generate_investment_report --market US --watchlist_yaml config/watchlist.yaml --out_dir reports_investment_us
python -m src.tools.run_investment_paper --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist --force
python -m src.tools.run_investment_execution --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
python -m src.tools.run_investment_guard --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
python -m src.tools.review_investment_weekly --market US --db audit.db --out_dir reports_investment_weekly --portfolio_id US:watchlist --days 7
python -m src.tools.review_investment_execution --market US --db audit.db --out_dir reports_investment_execution --portfolio_id US:watchlist --days 14
python -m src.tools.reconcile_investment_broker --market US --db audit.db --portfolio_id US:watchlist --out_dir reports_investment_reconcile
```

## Refactor Notes

当前重构基线已经补上：

- `src/tools/review_baseline_regression.py`
  - 固定保存投资报告、paper、execution、opportunity 的样例快照
  - 可和旧 baseline 做字段级对比

当前结构化对象已经落地：

- `src/events/models.py`
  - `MarketEvent`
  - `SignalDecision`
  - `RiskDecision`
  - `ExecutionIntent`
- `src/regime/state.py`
  - `RegimeStateV2`
- `src/data/adapters.py`
  - `MarketDataAdapter`

这些对象会被逐步用于 report、live、paper、execution 的统一解释。

## Supervisor Schedule

`src.app.supervisor` 现在按“市场本地时区”解释每个市场的时间窗口，不需要再手动把开闭市时间换算成 Sydney 时间。

调度补充：

- supervisor 现在支持任务超时控制，默认：
  - watchlist `180s`
  - report `1200s`
  - investment paper `300s`
  - investment execution `300s`
  - investment guard `240s`
  - investment opportunity `240s`
- `HK / US / ASX / XETRA` 当前都按同交易日执行配置运行，不再依赖 `execution_day_offset`
- `paper` 默认会自动跑 `weekly review`，并把执行/风险反馈回写到 scoped auto feedback 配置
- dashboard 的“本周执行质量”现在优先读取 `reports_investment_weekly/weekly_execution_summary.csv`
- `review_investment_execution` 仍保留，但更适合手动深挖执行明细，不再是 dashboard 的主来源
- dashboard control service 支持 `AUTO / REVIEW_ONLY / PAUSED`
- dashboard 顶部现在有 `运维总览` 和 `立即跑 Preflight`，会汇总 preflight、报告新鲜度、组合健康度和模式偏差
- 如果 preflight 有关键 warning/fail，trade 页面顶部会直接出现 `Preflight 关键提示`
- 当顶部出现执行模式建议时，优先切到建议模式，再看周报和 preflight 报告

什么时候执行 `supervisor`：

- 如果你要让这些任务自动按时间表跑，就在每天开始前启动一次 `python -m src.app.supervisor`
- 它会一直轮询，跨过上述时间点后自动执行
- 如果你只是想手动跑某一步，不要启动 supervisor，直接执行对应的单条命令
- 如果你只想检查“现在这一刻按时间表会不会触发任务”，用 `python -m src.app.supervisor --once`

## Short Safety Inputs

短空执行安全门依赖两类可编辑输入文件：

- `config/reference/short_borrow_fee_us.csv`
- `config/reference/short_borrow_fee_hk.csv`
- `config/reference/short_safety_rules_us.csv`
- `config/reference/short_safety_rules_hk.csv`

这些文件现在可以由 `src.tools.sync_short_safety_from_ibkr` 自动填充。`config/supervisor.yaml` 已默认在 report 前和 live 前触发同步，实时行情优先，取不到时会回退到 delayed-frozen。

如果你有真实的 borrow fee / SSR 远端源，可以在 `config/risk_us.yaml` 或 `config/risk_hk.yaml` 的 `short_data_sources` 里配置，sync 脚本会先拉 IBKR 实时 shortable / spread，再把远端源里的 borrow fee、SSR、locate、uptick 信息合并进去。

当前默认配置里已经接了两条实源通路：

- `config/risk_us.yaml`
  - `iborrowdesk_borrow`: 补 US borrow fee / locate
  - `yahoo_quote_spread`: 补 US spread
- `config/risk_hk.yaml`
  - `yahoo_quote_spread`: 补 HK spread

另外 HK 默认不再要求 `SSR` 状态，因为 US 的 Rule 201 约束不适用于 HK 市场。

### Borrow Fee CSV

列名：

- `symbol`
- `borrow_fee_bps`
- `source`

示例：

```csv
symbol,borrow_fee_bps,source
AAPL,18,desk
TSLA,95,prime
```

如果 IBKR Socket API 无法直接提供借券费，自动同步会保留该 symbol 的行，但 `borrow_fee_bps` 为空，并在 `note` 里写明原因。运行时会把它视为 `borrow_data_unknown`，不会误判为可安全做空。

### Short Safety Rules CSV

列名：

- `symbol`
- `locate_status`
- `ssr_status`
- `spread_bps`
- `has_uptick_data`
- `source`

示例：

```csv
symbol,locate_status,ssr_status,spread_bps,has_uptick_data,source
AAPL,AVAILABLE,OFF,4.5,true,desk
TSLA,AVAILABLE,ON,16.0,true,desk
```

支持值约定：

- `locate_status`: `AVAILABLE` / `LOCATED` / `UNAVAILABLE` / `BLOCKED` / `UNKNOWN`
- `ssr_status`: `OFF` / `ON` / `ACTIVE` / `SSR` / `UNKNOWN`
- `has_uptick_data`: `true` / `false`

如果 locate、SSR、spread、borrow 数据缺失，当前默认策略是 `unknown => block`，不会自动放行短空执行。

## Auto Sync

手动执行一次同步：

```bash
python -m src.tools.sync_short_safety_from_ibkr --market US
python -m src.tools.sync_short_safety_from_ibkr --market HK
```

可选参数：

- `--market_data_type 1`：实时行情优先
- `--fallback_market_data_type 4`：实时不可用时回退到 delayed-frozen
- `--watchlist_yaml ...` / `--symbols ...`：缩小同步范围
- `--max_symbols ...` / `--batch_size ...`：控制批量请求规模

自动同步输出：

- 借券文件会尽量写入 `symbol/source/note`
- 短空安全文件会写入 `locate_status`、`spread_bps`、`shortable_shares`、`shortable_level`
- `ssr_status` 和 `has_uptick_data` 如果券商接口当前拿不到，会保持 `UNKNOWN` / 空值，由安全门继续阻断

### Remote Source Config

`short_data_sources` 支持的最小字段：

- `enabled`
- `name`
- `url`
- `format`: `csv` 或 `json`
- `symbol_key`
- `borrow_fee_key`
- `borrow_fee_unit`: `bps` / `pct` / `decimal`
- `ssr_key`
- `locate_key`
- `has_uptick_key`
- `spread_bps_key`
- `note_key`

可选认证字段：

- `headers`
- `headers_from_env`
- `params`
- `params_from_env`
- `username_env`
- `password_env`

如果源是 CSV，默认分隔符是逗号；如果是 JSON，可以用 `root_key` 指到记录数组。

### Built-In Providers

除了通用 `csv/json` 源，当前还支持内建 provider：

- `provider: iborrowdesk`
  - 默认 URL: `https://iborrowdesk.com/api/ticker/{symbol}`
  - 输出 `borrow_fee_bps`，并尽量从可用股数推断 `locate_status`
- `provider: yahoo_quote`
  - 默认 URL: `https://query1.finance.yahoo.com/v7/finance/quote`
  - 通过 `bid/ask` 计算 `spread_bps`

`yahoo_quote` 在 US 会自动把 `BRK.B` 这类内部 symbol 转成 `BRK-B` 查询，再映射回系统符号。

## Paper KPI

`src.tools.paper_kpi_report` 现在支持 `--since`，可以只看某个时间点之后的新样本：

```bash
python -m src.tools.paper_kpi_report --db audit.db --out_dir reports/paper_kpi --since 2026-03-04T00:00:00+00:00
```

当 `--since` 存在时，会覆盖 `--days` 的时间窗。

### US SSR

`config/risk_us.yaml` 默认启用了 `short_safety.ssr_rule201`。当没有外部 SSR feed 时，同步脚本会用 IBKR 的实时价格和昨收盘推导 Rule 201 触发状态，并把触发日写到 `config/reference/short_ssr_state_us.json`。这会把 US 市场里一部分原本长期 `UNKNOWN` 的 SSR 状态收敛成 `ON/OFF`。
