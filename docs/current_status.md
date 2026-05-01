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

### 2026-04-28 PR / 分支收口

当前 `main` 已完成最近一轮 PR 与旧分支收口：

- GitHub open PR 数为 `0`
- 远端只保留 `origin/main`
- PR #10 dashboard evidence follow-up 已合入
- PR #11 refined progressive optimization plan 已合入
- PR #2 的有效 backlog 文档已提取并保留到 `main`
- smoke / structure-check / fill audit / order audit / dashboard freshness 相关测试与 CI workflow 已合入
- 最新全量验证结果为 `401 passed`

详细归档见：

- `docs/change_archive_2026-04-28_pr_branch_cleanup.md`
- `docs/change_archive_2026-04-28_dashboard_gateway_no_trade.md`
- `docs/change_archive_2026-04-29_gateway_runtime_state.md`
- `docs/change_archive_2026-04-29_alert_error_classification.md`
- `docs/change_archive_2026-04-29_strategy_parameter_registry.md`
- `docs/change_archive_2026-04-29_candidate_only_evidence.md`
- `docs/change_archive_2026-04-29_candidate_model_review.md`
- `docs/change_archive_2026-04-29_pure_strategy_no_trade_tests.md`
- `docs/change_archive_2026-04-29_weekly_support_import_boundary.md`
- `docs/change_archive_2026-04-30_dashboard_client_disconnect.md`
- `docs/change_archive_2026-04-30_supervisor_startup_visibility.md`
- `docs/change_archive_2026-04-30_weekly_evidence_json_artifacts.md`
- `docs/change_archive_2026-04-30_evidence_artifact_health_contracts.md`
- `docs/change_archive_2026-04-30_dashboard_evidence_action_summary.md`
- `docs/change_archive_2026-05-01_dashboard_evidence_action_labels.md`
- `docs/change_archive_2026-05-01_dashboard_evidence_action_top_level.md`
- `docs/change_archive_2026-05-01_simple_dashboard_evidence_action.md`
- `docs/change_archive_2026-05-01_portfolio_scoped_evidence_action.md`
- `docs/change_archive_2026-05-01_evidence_action_rationale.md`
- `docs/change_archive_2026-05-01_market_evidence_action_summary.md`
- `docs/change_archive_2026-05-01_market_evidence_v2_block_metrics.md`
- `docs/change_archive_2026-05-01_simple_market_evidence_actions.md`
- `docs/change_archive_2026-05-01_evidence_focus_actions.md`
- `docs/change_archive_2026-05-01_evidence_focus_v2_block.md`
- `docs/change_archive_2026-05-01_evidence_focus_summary.md`

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
- **策略参数建议已经开始配置化**
- **adaptive strategy 已接入 layered config source 追踪**
- **无成交周也能继续保留 candidate/outcome 证据**
- **candidate-only evidence 已能生成模型/策略校准 review**
- **纯策略 no-trade 闭环已有 focused test，确认无 IBKR/无订单/无 fill 时仍能用 outcome 校准模型**
- **weekly review support 模块已开始解除循环导入，decision/execution/strategy/feedback support 可冷启动导入测试**
- **dashboard control server 已忽略常见本地客户端提前断连噪声，避免 socketserver traceback 误导运维判断**
- **supervisor 默认长驻模式已有启动日志、轻量 dashboard-control 启动 state 与可中断信号处理，运行 `python -m src.app.supervisor` 会先显示配置/市场/poll interval，Ctrl+C 会进入清理退出**
- **dashboard control `/state` 轮询已改为轻量/持久化状态读取，不再在浏览器自动刷新时实时构建完整 portfolio evidence，避免阻塞 supervisor 主循环与报告启动**
- **weekly review summary 在 supervisor 内已有 mtime/size 缓存，避免 500MB+ 周报 summary 在同一轮报告启动前被多次 JSON decode**
- **weekly unified evidence / blocked-vs-allowed review 已补 common contract 层，`src/common/investment_evidence.py` 统一维护 evidence schema、row normalization、allowed/blocked 分类和 ex-post review 聚合**
- **weekly review 现在会同时输出独立的 `weekly_unified_evidence.json` 与 `weekly_blocked_vs_allowed_expost.json`，dashboard 优先消费这些轻量 evidence artifact，再 fallback 到 summary/CSV**
- **独立 evidence JSON 已纳入 artifact health registry，旧 summary 内嵌 rows 可作为兼容 fallback，dashboard 顶部健康汇总会显示 evidence artifact 缺失/过旧/兼容读取状态**
- **dashboard v2 Trading Quality Evidence block 已增加 blocked-vs-allowed label 分布、样本就绪度与 `primary_action`，避免把“样本不足”误读成“需要立刻调 gate”**
- **dashboard v2 Trading Quality Evidence block 已把 `primary_action` 映射为 `action_label/action_note`，advanced HTML 可直接显示可读操作建议**
- **dashboard JSON 已新增顶层 `evidence_action_summary`，HTML 的 Unified Evidence 区块会直接展示当前 evidence 下一步和样本状态**
- **simple dashboard 的“一眼看懂 / 本周策略解释”已接入 `evidence_action_summary`，无成交或样本不足时会直接提示继续收集 outcome 样本，而不是误导为立刻调 gate**
- **每个 dashboard card 已使用 portfolio/market scoped evidence action，避免某个市场的 blocked-vs-allowed 结论误显示到其他组合**
- **`evidence_action_summary` 已补 `decision_basis / basis_label / rationale / blocked_label_summary`，simple dashboard 会显示建议依据，便于审计“为什么是这一步”**
- **dashboard 已新增 `market_evidence_action_summary`，US/HK/CN 市场视图会显示每个市场的 evidence action、basis 和 rationale**
- **dashboard v2 Market Views block 已新增 market-level evidence action metrics，能区分缺 evidence、gate 需复核、模型 edge 需复核与样本不足；样本不足只提示继续收集，不单独升级为告警**
- **simple dashboard 已新增市场级 Evidence 下一步表，首页可直接看到 US/HK/CN 各自是缺 evidence、样本不足、gate 复核还是 signal-edge 校准**
- **dashboard 已新增只读 `evidence_focus_actions` 优先队列，把市场级 evidence work 排序到首页，跳过 monitor/keep 这类非行动项，不自动改参数**
- **dashboard v2 已新增 `Evidence Focus Actions` block，把只读 evidence 优先队列纳入 advanced/v2 契约并统计 urgent/gate/signal/missing-evidence/sample-collection**
- **dashboard 已新增只读 `evidence_focus_summary`，把 evidence 优先队列压缩成主市场、主动作、依据、urgent 计数和可读摘要，供 simple/advanced/review 消费**

---

## 4. 最近已收口的工作流

最近一轮已收口工作说明当前主要推进方向正在往“测试护栏 / 工程防回归”继续走：

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
- pure strategy no-trade loop tests
- artifact contract / health / governance health
- degraded-input dashboard fallback
- broker / reconcile artifact contract registry
- weekly review support 模块按 execution / governance / strategy / decision 领域继续拆分
- weekly review support import boundary regression tests

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
