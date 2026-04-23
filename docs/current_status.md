# Current Status

本文件用于快速回答两个问题：

1. 这个项目现在到底是什么
2. 截至当前，项目已经做到哪一步、最近又在推进什么

如果你是第一次接手仓库，建议先看本文件，再看：

- `README.md`
- `docs/architecture_overview.md`
- `docs/supervisor_runbook.md`
- `docs/project_status_roadmap.md`

---

## 1. 一句话定位

`ibkr_quant_system` 当前不是单个策略脚本，也不是通用多券商框架；它更接近一个围绕 `IB Gateway` 的个人投资运营系统，主线是：

`研究 -> paper 账本 -> broker 执行 -> guard/opportunity -> weekly review -> dashboard/supervisor 治理`

当前项目重点是中长期投资链路，不是高频交易。

---

## 2. 截至当前的状态判断

### 总体阶段

项目已经明显超过“原型”阶段，当前更像：

- **单人主导但可实际运行的 Alpha 系统**
- 已有 paper / live 边界意识
- 已有 supervisor / dashboard / preflight / weekly review / governance
- 已经进入“长期运行系统”而不是“零散研究脚本”的阶段

### 市场成熟度

- **HK / US**：最成熟，已经形成研究、paper、执行、guard、weekly review、dashboard 的完整主路径
- **ASX / XETRA**：已接入统一框架，但成熟度弱于 HK / US
- **CN**：当前仍以 `research-only` 为主，不进入自动执行主链

### 工程状态

当前仓库已经具备：

- `pyproject.toml` 打包入口
- console scripts
- 基础 CI / pytest marker 分层
- preflight / governance / runbook 文档
- 运行模式区分：`AUTO / REVIEW_ONLY / PAUSED`

但仍然存在：

- 少数超大文件复杂度偏高
- 配置文件数量较多，后续扩张成本会继续上升
- 当前仍偏个人快速迭代，不是成熟多人协作仓库

---

## 3. main 上最近已合入的状态

### 已合入的最近一轮关键建设

当前 `main` 已经包含一轮比较明显的“工程加固”工作，核心方向是：

- 补齐 packaging / entrypoints / repo hygiene
- 将 intraday 执行边界拆得更清楚
- 把 signal audit persistence 从策略评估逻辑中拆出来
- 改善 bootstrap / engine wiring，降低跨层耦合

这部分对应最近已合入的 PR：

- **PR #1** `Harden packaging and separate engine execution boundaries`

这意味着：

- 项目主壳已经比早期更容易安装和启动
- intraday 主链的职责边界比之前更清楚
- 审计写入和执行职责分离更明确
- 仓库已经开始用 CI 和测试去守关键路径，而不只是靠手工运行

### main 的最新判断

如果只看已合入内容，当前 `main` 的关键词是：

- **主线已明确**
- **工程基础已补一轮**
- **运行治理框架已成型**
- **下一步重点不是盲目加功能，而是继续补 guardrail / regression / dashboard 语义一致性**

---

## 4. 最近未合入但值得关注的工作流

仓库里最近还有几条 **已开 PR 但尚未合入** 的工作，说明当前主要推进方向正在往“测试护栏 / 工程防回归”继续走：

### 待合入 PR

- **PR #2** `docs: add issue backlog drafts for project planning`
  - 把 backlog 整理成 issue-ready drafts
  - 说明项目开始从“只做”转向“可规划地做”

- **PR #3** `ci: add structural validation for critical startup modules`
  - 增加 startup-critical 模块结构校验
  - 重点防 `storage.py` / `engine.py` 这类关键入口出现结构性回归

- **PR #4** `test: add smoke tests for critical startup paths`
  - 增加启动路径 smoke tests
  - 重点覆盖 import、Storage 初始化、TradingEngine 构造

- **PR #5** `test: verify bracket order audit persistence`
  - 为 bracket order 的 audit persistence 增加高信号回归测试

- **PR #6** `test: verify fill processor audit and risk event chain`
  - 为 fill -> risk event -> gate callback 审计链增加高信号回归测试

### 这批 PR 说明了什么

这批未合入 PR 的共同指向很明确：

> 当前项目的优先级，已经从“快速堆功能”转向“把关键运行链路守稳”。

也就是说，仓库的最新工作重心更像是：

- 把 startup path 守住
- 把 execution audit 守住
- 把 fill/risk callback 链守住
- 把仓库逐步从个人工程推进到“更稳的运行系统”

---

## 5. 当前最稳的部分 / 最薄弱的部分

### 当前最稳的部分

1. **项目主线表达已经比较清楚**
   - README、architecture、runbook、governance 基本一致

2. **HK / US 的投资主路径已经成型**
   - report
   - paper
   - execution
   - guard
   - weekly review
   - dashboard / supervisor

3. **运行治理意识已经建立**
   - preflight
   - execution mode
   - runtime artifact 管理
   - CI baseline

### 当前最薄弱的部分

1. **大文件复杂度**
   - `src/app/supervisor.py`
   - `src/tools/review_investment_weekly.py`
   - `src/tools/generate_dashboard.py`
   - `src/common/storage.py`

2. **文档里的“当前进度快照”容易过期**
   - `docs/project_status_roadmap.md` 更像阶段分析，不是持续更新状态页

3. **dashboard / weekly review / helper 语义一致性仍在推进中**
   - 特别是 freshness / health / 运维摘要这类聚合语义，仍值得继续补测试与对齐

---

## 6. 现在最值得优先做的事

当前最合理的优先级顺序：

### P0：继续补工程护栏

优先继续推进：

- startup smoke
- structural validation
- execution audit persistence
- fill/risk-event audit chain
- dashboard helper regression tests
- artifact contract / health / governance health
- degraded-input dashboard fallback
- broker / reconcile artifact contract registry
- weekly review support 模块按 execution / governance / strategy / decision 领域继续拆分

### P1：降低大文件复杂度

优先拆分：

- `src/app/supervisor.py`
- `src/tools/review_investment_weekly.py`
- `src/tools/generate_dashboard.py`

### P2：补“状态可读性”

继续增强：

- dashboard freshness / health / mode mismatch 的表达
- `docs/current_status.md` 的持续维护
- roadmap 与当前状态页之间的分工

---

## 7. 新接手者建议阅读顺序

### 如果你只想 10 分钟理解项目

按这个顺序：

1. `docs/current_status.md`
2. `README.md`
3. `docs/architecture_overview.md`

### 如果你准备实际运行项目

继续看：

4. `docs/supervisor_runbook.md`
5. `docs/production_governance.md`
6. `config/supervisor.yaml`
7. `pyproject.toml`

### 如果你准备改代码

优先看：

8. `src/app/supervisor.py`
9. `src/tools/generate_dashboard.py`
10. `src/tools/review_investment_weekly.py`
11. `src/common/storage.py`
12. `tests/`

---

## 8. 一句话结论

截至当前，`ibkr_quant_system` 已经是一个 **主线清晰、HK/US 闭环较完整、开始重视工程护栏和运行治理** 的个人投资操作系统；最近的真实推进方向不是再堆新策略，而是 **把关键启动链路、执行审计链路和 dashboard/复盘语义继续守稳并讲清楚**。
