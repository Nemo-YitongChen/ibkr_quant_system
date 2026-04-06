# ibkr_quant_system

`ibkr_quant_system` 是一个基于 `IB Gateway` 的 IBKR 量化交易工具，面向“研究 -> paper 组合 -> 执行 -> 风控 -> 复盘 -> dashboard 监督”这条完整闭环。

当前项目的定位不是高频或多券商通用框架，而是一个以 `IB Gateway` 为唯一券商入口、支持中长期投资研究与执行管理的可维护基础盘。

当前仅支持 `IB Gateway`，不再把 `TWS` 作为推荐或兼容入口。

## 项目目标

这个项目当前围绕三个目标建设：

- 提供一套可持续运行的 `IB Gateway` 量化交易工作流，从研究、报告、paper 组合、执行、风控到复盘都能串起来。
- 保持 `paper` 和 `live` 的运行边界清晰，优先保证 paper 验证可用，再逐步放开 live 使用。
- 保持策略层、执行层、券商适配层分离，让后续新增策略、替换规则、扩展市场时不需要重写整套系统。

## 当前支持范围

- 仅支持 `IB Gateway`
- 不再把 `TWS` 作为推荐或兼容入口
- `HK` / `US` 是当前最成熟的市场
- `ASX` / `XETRA` 已接入统一流程，但成熟度低于 `HK` / `US`
- `CN` 当前保持 `research-only`，不进入自动执行链路

## 当前功能

### 1. 研究与报告

- 从 watchlist 出发生成投资研究报告
- 结合 `IBKR`、`yfinance`、`FMP`、`FRED`、`Finnhub` 做行情、基本面、宏观和新闻补充
- 输出每个市场、每个组合的研究目录和报告结果

### 2. Paper 组合与执行准备

- 根据研究报告生成 paper 组合建议
- 维护 paper ledger、组合状态和调仓建议
- 提供 execution / opportunity / guard 三条配套工具链

### 3. 执行、风控与审计

- 对接 `IB Gateway` 获取账户、订单、成交和持仓信息
- 支持 broker 对账、paper 同步、执行后审计
- 记录信号、市场数据质量、风险事件和执行留痕

### 4. 自动调度与 dashboard

- 提供 `supervisor` 做定时调度和统一控制
- 提供 dashboard 查看运行状态、研究结论、执行建议和关键告警
- dashboard 支持 `简单 / 专业` 模式切换
- dashboard 支持 `中文 / English` 切换

### 5. 周复盘与运行治理

- 提供周度复盘、经纪商对账、paper 与 broker 同步工具
- 提供 preflight 检查，帮助在启动前确认环境、端口、配置和运行模式
- 已补齐基础 CI、包入口和命令行脚本，便于其他人安装和接手

## 安装要求

基础要求：

- Python `3.11`
- 本地可用的 `IB Gateway`
- 需要增强研究数据时，在 `.env.local` 中填写对应 API key

当前环境变量模板见 [`./.env.example`](./.env.example)。

## 安装方式

在仓库根目录执行：

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
cp .env.example .env.local
```

`.env.local` 中常见可选项包括：

- `FMP_API_KEY`
- `FRED_API_KEY`
- `TE_API_KEY`
- `FINNHUB_API_KEY`
- `FINNHUB_WEBHOOK_SECRET`

## 配置文件

主要入口配置：

- [`config/supervisor.yaml`](./config/supervisor.yaml)
  - 默认 `paper` 主配置
  - 覆盖 `HK / US / ASX / XETRA / CN`
  - 其中 `CN` 维持 `research-only`
- [`config/supervisor_live.yaml`](./config/supervisor_live.yaml)
  - `live` 配置入口
  - 覆盖 `HK / US / ASX / XETRA`

如果只是首次上手，建议先只使用 `config/supervisor.yaml`。

## 如何使用

### 1. 先做启动前检查

```bash
ibkr-quant-preflight --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir reports_preflight
```

这个命令会检查当前配置、运行目录、IB Gateway 端口和关键依赖是否可用。

### 2. 启动 supervisor

持续运行：

```bash
ibkr-quant-supervisor --config config/supervisor.yaml
```

只执行当前时刻应触发的一轮：

```bash
ibkr-quant-supervisor --config config/supervisor.yaml --once
```

如果你想用 dashboard 作为主要入口，这通常是最直接的启动方式。

### 3. 生成 dashboard

```bash
ibkr-quant-dashboard --config config/supervisor.yaml --out_dir reports_supervisor
```

输出结果：

- `reports_supervisor/dashboard.json`
- `reports_supervisor/dashboard.html`

这个命令会把 supervisor、weekly review、execution KPI 和 preflight 的结果整理成 dashboard 页面；如果你已经在跑 supervisor，也可以在需要时手动刷新一次 dashboard 输出。

### 4. 手动跑一条最小闭环

以 `HK` 为例：

```bash
ibkr-quant-engine --market HK --startup-check-only
ibkr-quant-report --market HK --watchlist_yaml config/watchlists/resolved_hk_top100_bluechip.yaml --out_dir reports_investment_hk
ibkr-quant-paper --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip --force
ibkr-quant-execution --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
ibkr-quant-guard --market HK --report_dir reports_investment_hk/resolved_hk_top100_bluechip --portfolio_id HK:resolved_hk_top100_bluechip
ibkr-quant-weekly-review --market HK --db audit.db --out_dir reports_investment_weekly --portfolio_id HK:resolved_hk_top100_bluechip --days 7
ibkr-quant-reconcile --market HK --db audit.db --portfolio_id HK:resolved_hk_top100_bluechip --out_dir reports_investment_reconcile
```

以 `US` 为例：

```bash
ibkr-quant-engine --market US --startup-check-only
ibkr-quant-report --market US --watchlist_yaml config/watchlist.yaml --out_dir reports_investment_us
ibkr-quant-paper --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist --force
ibkr-quant-execution --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
ibkr-quant-guard --market US --report_dir reports_investment_us/watchlist --portfolio_id US:watchlist
ibkr-quant-weekly-review --market US --db audit.db --out_dir reports_investment_weekly --portfolio_id US:watchlist --days 7
ibkr-quant-reconcile --market US --db audit.db --portfolio_id US:watchlist --out_dir reports_investment_reconcile
```

### 5. 常用命令入口

安装完成后，推荐直接使用这些 console scripts：

- `ibkr-quant-preflight`
- `ibkr-quant-dashboard`
- `ibkr-quant-supervisor`
- `ibkr-quant-engine`
- `ibkr-quant-report`
- `ibkr-quant-paper`
- `ibkr-quant-execution`
- `ibkr-quant-guard`
- `ibkr-quant-opportunity`
- `ibkr-quant-weekly-review`
- `ibkr-quant-reconcile`
- `ibkr-quant-sync-paper`
- `ibkr-quant-execution-review`
- `ibkr-quant-label-snapshots`
- `ibkr-quant-trade-report`
- `ibkr-quant-short-safety-sync`

旧的 `python -m src...` 调用方式仍然兼容，但新安装环境建议优先用上面的命令。

### 6. 维护与诊断工具

下面这几项更偏维护、复盘和运营支持：

- `ibkr-quant-execution-review`
  - 生成 execution KPI、订单/成交明细和执行质量摘要
- `ibkr-quant-label-snapshots`
  - 对研究/执行快照做 outcome 标注，给 weekly review 和 dashboard 提供回标样本
- `ibkr-quant-trade-report`
  - 生成面向交易面的日报/摘要输出
- `ibkr-quant-short-safety-sync`
  - 从 IBKR 同步 short safety 参考数据，更新借券和可做空规则输入

## 推荐上手路径

如果是第一次接手这个项目，建议按下面顺序熟悉：

1. 先看 [`docs/project_status_roadmap.md`](./docs/project_status_roadmap.md)，了解项目目标、当前进度和市场范围。
2. 再看 [`docs/architecture_overview.md`](./docs/architecture_overview.md)，理解系统分层和主要运行链路。
3. 使用 `ibkr-quant-preflight` 检查本地环境。
4. 使用 `ibkr-quant-supervisor --config config/supervisor.yaml --once` 跑一轮 paper。
5. 打开 dashboard，先用简单模式查看当前运行状态，再根据需要切到专业模式。

## 使用建议与安全边界

- 默认先在 `paper` 环境验证，不建议直接跳到 `live`
- `ibkr-quant-supervisor --once` 不是 dry-run，到触发时点会执行真实动作
- `live` 启动前，先跑 preflight，再检查 dashboard 顶部的运行模式和关键告警
- `resolved_*.yaml` watchlist 是运行输入的一部分，应继续纳入版本控制
- 如果只是要研究和复盘，不需要启动自动执行链路

## 相关文档

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
