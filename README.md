# ibkr_quant_system

基于 IB Gateway 的 IBKR 投资研究、paper 执行、live 监督系统。当前主线已经收敛到“研究报告 -> paper 组合 -> execution / guard -> 周复盘 -> supervisor 自动调度”这条闭环，而不是短线实验脚本集合。

当前仅支持 `IB Gateway`，不再把 `TWS` 作为推荐或兼容入口。

## 项目目标

- 稳定连接 `IB Gateway`，完成取数、报告、paper 执行、broker 对账和审计留痕
- 让 `paper` 环境可以持续运行，并和 `live` 保持清晰隔离
- 保持策略层、执行层、券商适配层分离，避免把交易规则绑死在 broker 细节里

## 快速开始

推荐在仓库根目录使用可编辑安装，这也是当前 `pyproject.toml` 和 console scripts 的标准路径。

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
cp .env.example .env.local
```

基础要求：

- Python `3.11`
- 本地 `IB Gateway`
- 需要研究层增强时，填写 `.env.local` 里的 API key

环境变量模板在 [`./.env.example`](./.env.example)。

## 配置入口

- `config/supervisor.yaml`
  - `paper` 主配置
  - 覆盖 `HK / US / ASX / XETRA / CN`
  - `CN` 保持 `research-only`
- `config/supervisor_live.yaml`
  - `live` 一键配置
  - 覆盖 `HK / US / ASX / XETRA`

当前数据源分工：

- `IBKR`：账户、执行、可用时优先提供历史行情
- `yfinance`：免费日线 fallback、snapshot、news fallback
- `FMP`：基本面补充
- `FRED`：宏观补充
- `Finnhub`：新闻、财报日历、推荐趋势

## 推荐入口

完成 `pip install -e ".[dev]"` 后，优先使用下面这些命令。旧的 `python -m src...` 路径仍然兼容。

- `ibkr-quant-preflight`
  - 对应 `python -m src.tools.preflight_supervisor`
- `ibkr-quant-supervisor`
  - 对应 `python -m src.app.supervisor`
- `ibkr-quant-engine`
  - 对应 `python -m src.main`
- `ibkr-quant-report`
  - 对应 `python -m src.tools.generate_investment_report`
- `ibkr-quant-paper`
  - 对应 `python -m src.tools.run_investment_paper`
- `ibkr-quant-execution`
  - 对应 `python -m src.tools.run_investment_execution`
- `ibkr-quant-guard`
  - 对应 `python -m src.tools.run_investment_guard`
- `ibkr-quant-opportunity`
  - 对应 `python -m src.tools.run_investment_opportunity`
- `ibkr-quant-weekly-review`
  - 对应 `python -m src.tools.review_investment_weekly`
- `ibkr-quant-reconcile`
  - 对应 `python -m src.tools.reconcile_investment_broker`
- `ibkr-quant-sync-paper`
  - 对应 `python -m src.tools.sync_investment_paper_from_broker`

## 最小运行闭环

### 1. 启动前检查

```bash
ibkr-quant-preflight --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir reports_preflight
```

### 2. 自动调度

```bash
ibkr-quant-supervisor --config config/supervisor.yaml
```

只跑当前时刻应触发的一轮：

```bash
ibkr-quant-supervisor --config config/supervisor.yaml --once
```

### 3. 手动最小流程

HK 示例：

```bash
ibkr-quant-engine --market HK --startup-check-only
ibkr-quant-report --market HK --watchlist_yaml config/watchlists/resolved_hk_top100_bluechip.yaml --out_dir reports_investment_hk
ibkr-quant-paper --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --force
ibkr-quant-execution --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
ibkr-quant-guard --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
ibkr-quant-weekly-review --market HK --db audit.db --out_dir reports_investment_weekly --portfolio_id HK:resolved_hk_top100_bluechip --days 7
ibkr-quant-reconcile --market HK --db audit.db --portfolio_id HK:resolved_hk_top100_bluechip --out_dir reports_investment_reconcile
```

US 示例：

```bash
ibkr-quant-engine --market US --startup-check-only
ibkr-quant-report --market US --watchlist_yaml config/watchlist.yaml --out_dir reports_investment_us
ibkr-quant-paper --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist --force
ibkr-quant-execution --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
ibkr-quant-guard --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
ibkr-quant-weekly-review --market US --db audit.db --out_dir reports_investment_weekly --portfolio_id US:watchlist --days 7
ibkr-quant-reconcile --market US --db audit.db --portfolio_id US:watchlist --out_dir reports_investment_reconcile
```

## 安全提醒

- 默认先在 `paper` 环境验证，不要直接跳 `live`
- `live` 启动前先跑 preflight，再看 dashboard 顶部的模式建议和关键 warning
- `ibkr-quant-supervisor --once` 不是 dry-run，到了触发时点会真实执行
- `resolved_*.yaml` watchlist 继续纳入版本控制；当前已避免机器绝对路径和无意义 `generated_at` 噪音

## 文档

- [`docs/runnable_code_summary.md`](./docs/runnable_code_summary.md)
  - 入口脚本总览和输出说明
- [`docs/supervisor_runbook.md`](./docs/supervisor_runbook.md)
  - `paper / live` 日常启动、排障和 dashboard 控制
- [`docs/project_status_roadmap.md`](./docs/project_status_roadmap.md)
  - 项目目标、当前进度、架构判断和下一阶段规划
- [`docs/architecture_overview.md`](./docs/architecture_overview.md)
  - 系统分层、运行路径和市场范围
- [`docs/production_governance.md`](./docs/production_governance.md)
  - 运行模式、安全门、变更治理和 CI 基线
