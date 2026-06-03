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
- `docs/change_archive_2026-05-01_evidence_focus_v2_primary_summary.md`
- `docs/change_archive_2026-05-01_evidence_focus_ops_health.md`
- `docs/change_archive_2026-05-01_evidence_focus_ops_v2_metrics.md`
- `docs/change_archive_2026-05-08_evidence_control_audit_linkage.md`
- `docs/change_archive_2026-05-08_evidence_focus_effectiveness.md`
- `docs/change_archive_2026-05-08_blocked_vs_allowed_action_mapping.md`
- `docs/change_archive_2026-05-08_dashboard_v2_information_architecture.md`
- `docs/change_archive_2026-05-08_strategy_config_pure_signal_regression.md`
- `docs/evidence_focus_lifecycle_technical_path_2026-05-08.md`
- `docs/evidence_focus_lifecycle_path_assessment_2026-05-09.md`
- `docs/development_report_2026-05-09.md`
- `docs/change_archive_2026-05-09_strategy_parameter_suggestions.md`
- `docs/change_archive_2026-05-09_strategy_suggestion_control_audit.md`
- `docs/change_archive_2026-05-09_strategy_suggestion_effectiveness.md`

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
- **dashboard v2 `Evidence Focus Actions` block 已对齐 `evidence_focus_summary`，直接暴露 primary market/action/basis/read_only，并把 summary/actions 拆成结构化 rows**
- **dashboard ops overview 已纳入 urgent evidence focus：gate/signal/缺 evidence 等紧急项会进入顶部 WARN，样本收集只显示继续收集、不触发告警**
- **dashboard v2 `Ops Health` block 已补 evidence focus metrics，可直接显示 evidence focus count、urgent count、primary market/action**
- **evidence focus action lifecycle 已抽到 `src/common/evidence_focus_actions.py`，现有 dashboard action 队列保留兼容字段，同时新增稳定 `action_id/status/urgency/linked_evidence_*`，为后续 control audit linkage 与 effectiveness review 做准备**
- **dashboard control action audit 已接入 evidence focus lifecycle：控制操作可携带 `evidence_action_id/resolution_status/resolution_note`，dashboard JSON/v2/advanced HTML 会显示 linked action，并用 audit history 回填 open urgent action 状态**
- **weekly review 已新增 `evidence_focus_effectiveness`，会统计 evidence focus action 的 new/urgent/resolved/stale urgent/avg resolution hours，并在 markdown 中渲染 Evidence Focus Effectiveness section**
- **blocked-vs-allowed 到 evidence action 的映射已补测试护栏：gate 过紧会触发 urgent gate review，样本不足只做 sample collection，GATE_OK/BLOCKING_HELPED 不触发 urgent，缺 artifact 会生成缺 evidence fallback**
- **dashboard v2 blocks 已完成 home / advanced 信息架构收敛：首页只保留 Ops Health、Evidence Focus、Execution Quality、Governance / Control Actions，高级模式展开 market views、waterfall、unified evidence、blocked-vs-allowed 与 control history**
- **策略技术债第一批已收口：MR/BO primary signal weights、mid regime sizing 和 fusion weights 已配置化，默认行为不变，并补 pure signal regression 覆盖 breakout 上下突破、short disabled 与高波动/深回撤收缩**
- **Evidence Focus Lifecycle 路径已完成 2026-05-09 复核：Step 1-7 方向正确且已落地，下一阶段不应重复补基础链路，而应推进 weekly review 基于 evidence 生成单一 primary strategy parameter 建议，继续保持只建议、不自动生效**
- **2026-05-27 已补开市交易分析健康层：dashboard JSON/v2/首页现在会把“开市、报告 fresh、auto-order gate 证据、submit blocker、数据关注”串成统一 `open_market_analysis_summary`，supervisor 每轮会把 scoped `auto_order_readiness.json` 写到当前 runtime summary，避免开市市场看起来 fresh 但缺少可提交门控证据**
- **2026-05-27 已补 Gateway budget-aware opportunity throttle：当 weekly Gateway budget 对应市场为 `degraded` 时，supervisor 会暂停该市场高请求量 `run_investment_opportunity` 扫描并记录 `gateway_budget_degraded`，优先让 request budget 恢复，而不是继续消耗阻塞自动下单的关键资源**
- **2026-05-29 已补 auto-order readiness freshness health：dashboard 会检查 `auto_order_readiness.json` 是否缺失、过旧或早于 weekly Gateway budget，并把过期自动下单证据降级为 `AUTO_ORDER/readiness_freshness` 告警；这不放宽任何风险、edge、budget 或 submit gate**
- **2026-05-29 已补 auto-order readiness dependency refresh：supervisor 会在 readiness 同签名但 artifact 过期、或 preflight/weekly Gateway budget/market readiness 比它更新时重写 readiness，并优先消费轻量 Gateway budget artifact，减少对超大 weekly summary 的依赖**
- **2026-05-29 已把 watchlist expansion 接入 dashboard v2：advanced block 会显示账户 profile、候选行数、选中数、零选中市场和主要 reject reason，当前 small profile 下 selected=`SPTM,SCHB`，ASX/HK/XETRA 主要受 `expected_cost_above_max` / whole-share tradability 阻断**
- **2026-05-29 已把 watchlist expansion 从“展示 reject reason”推进到“给市场级下一步建议”：CLI summary、dashboard JSON 和 v2 advanced block 现在共用 `src/common/watchlist_expansion.py` 的 reason aggregation / recommendation helper；当前 ASX/HK/XETRA 的主建议是先校准费用/价差假设并扩低成本、整股可交易 ETF 池，而不是放宽 risk、edge 或 submit gate**
- **2026-06-02 已把 watchlist expansion 进一步推进到“市场扩池缺口计划”：每个零选中市场会输出 asset-class coverage、preferred ETF-first gap、expansion target 和 near-miss candidates；当前 ASX/HK/XETRA 都是 `preferred_asset_class_gap=true`，下一步应先补/标注低成本、高流动性、可整股交易的 ETF-first 候选源，再重新跑 candidate report 和 paper execution，而不是把高成本 near-miss 单股直接推入自动下单**
- **2026-06-03 已新增只读 watchlist seed proposals：`watchlist_expansion_summary` 和 dashboard v2 现在会输出 `seed_proposals / seed_proposal_count / manual_seed_proposal_count / primary_seed_proposal_action`；当前 ASX/HK/XETRA 都生成 `create_or_refresh_preferred_asset_seed_watchlist` 提案，全部 `auto_apply=false` 且 `submit_gate_policy=do_not_relax_submit_gates`，用于提高后续候选覆盖面，但不会自动加入交易池或下单**
- **2026-06-03 已把 watchlist seed proposals 接入 auto-order frequency plan：`auto_order_readiness` 现在会输出只读 `frequency_plan` 和 `candidate_supply_*` 字段，区分“当前 frontier 被 preflight/Gateway/market readiness 挡住”和“候选池需要扩展”；本地最新诊断为 `frontier_blocked/preflight_stale`，同时保留 ASX/HK/XETRA 3 个手动 seed proposal，且 `does_not_change_submit_decision=true`**
- **2026-06-03 已把 seed proposal 推进为 review-only seed intake plan：`watchlist_expansion_summary` 和 dashboard v2 会输出 `seed_intake_plan`，并在 `reports_supervisor/watchlist_expansion/seed_review/` 生成 ASX/HK/XETRA 审核文件；当前三个市场都是 `NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE`，near-miss 股票只作为 `evidence_symbols`，`symbols` 为空，不会进入 symbol master 或自动下单**
- **后续开发报告已补充到 `docs/development_report_2026-05-09.md`：下一步明确为 evidence-driven single strategy parameter suggestions，后续再推进 dashboard/weekly 大文件拆分、config defaults+overrides、walk-forward acceptance rules、execution quality evidence 和 live 变更四件套**
- **weekly review 已新增 read-only `strategy_parameter_suggestions` 产物：当 candidate model review 出现 `SIGNAL_RANKING_INVERTED` 且样本足够时，每个 market/portfolio/week 只生成一个 primary strategy field 建议，带 linked evidence、acceptance rule、rollback note 和 `auto_apply=0`**
- **dashboard control audit 已能关联 strategy parameter suggestions：控制 payload 可携带 `strategy_parameter_suggestion_id/primary_field/config_path/resolution_status/resolution_note`，审计历史和 dashboard v2 Governance block 会展示 linked strategy suggestion 与处理状态，但仍不自动写配置**
- **strategy parameter suggestion 已新增 effectiveness summary：weekly summary、tuning dataset 和 markdown 会统计 open/handled/resolved/stale、resolution mix、avg resolution hours 与 `auto_apply` 违规，dashboard control audit 可通过 `linked_strategy_parameter_suggestion_id` 回填处理状态**
- **dashboard v2 已新增 `Strategy Parameter Governance` advanced block：直接展示 weekly strategy parameter suggestions、follow-up verdict 和 effectiveness summary，open/stale/degraded/auto-apply 违规会进入 warning，但仍保持只读**
- **2026-05-10 后续开发报告已补充到 `docs/development_report_2026-05-10.md`：下一步优先拆 dashboard weekly artifact loader，再推进 execution quality decision evidence 与配置 defaults+overrides**
- **paper 自动下单已补 readiness gate：有 2026-W19 `SIGNAL_RANKING_INVERTED` 证据的 ASX/CN/HK/XETRA paper configs 使用 paper-only layered strategy defaults，将 `mr_weight` 降到 `0.55`；supervisor 在 `submit_investment_execution=true` 前检查 preflight、weekly review、gateway budget 和 strategy suggestion follow-up，live submit 仍默认禁止**
- **IBKR 子任务已补 clientId 隔离：supervisor 为 report/snapshot/opportunity/execution/guard/short-safety 等 Gateway 子进程注入稳定 clientId offset，连接层在短暂 orphan session 残留时有限尝试相邻 clientId，避免 `clientId already in use` 卡住 paper 自动下单链路**
- **MarketDataService 已补历史行情失败 cooldown：当某个 symbol/request 组合返回空历史或请求错误后，会在 TTL 内跳过重复 Gateway 请求，优先使用 stale cache 或交给 yfinance fallback，从而减少 XETRA/HK/ASX 这类无权限/订阅不足场景的重复 IBKR 请求**
- **multi-market readiness 已新增独立只读 CLI：`ibkr-quant-market-readiness` 会汇总 US/HK/ASX/XETRA/CN 的 execution artifact freshness、IB Gateway 降级状态、1000 AUD equity cap、small profile 后的 min_trade/max_order/cash buffer、fee/lot 摩擦和市场准备优先级；当前准备顺序是 US regular-session 小额整股 ETF -> 刷新 US overnight/XETRA/ASX artifact -> HK 只在 ETF/board-lot/post-cost edge 通过后推进，CN 继续 research-only**
- **auto-order readiness 已消费 market readiness：`ibkr-quant-auto-order-readiness` 和 supervisor submit gate 会读取 `market_readiness.json`，当市场处于 stale/degraded gateway/market closed/config blocked 时加入 `market_readiness_not_ready` 硬阻断；如果产物缺失，CLI 和 supervisor 会即时构建只读 readiness payload 作为 fallback，避免 ASX/HK/XETRA/US 自动提交 gate 依赖人工先跑诊断**
- **auto-order readiness 已新增 remediation plan：当 preflight、gateway budget、market readiness、strategy suggestion 同时阻断时，报告会按优先级和影响组合数输出修复顺序；当前顺序是 `preflight_stale` -> `gateway_budget_degraded` -> `market_readiness_not_ready` -> `strategy_suggestions_open`，避免只看第一条 primary reason 而漏掉真正的执行阻塞**
- **auto-order readiness 已新增 submit plan 并接入 supervisor submit gate：即使所有治理检查通过，也只允许最多 1 个 portfolio、最多 1 笔小额 paper 订单、gross 不超过 100 AUD 的候选进入提交计划；supervisor 只有在当前 item 命中 `selected_portfolio_id` 时才会带 `--submit`，多个候选会要求 operator selection，订单数/金额超限会保持 blocked，避免小账户在多市场同时展开风险**

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
- weekly review evidence-driven single-parameter suggestions

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

---

## 9. 2026-05-12 运行优化补充

- Paper supervisor 已开启组合级 auto-order readiness gate：有 hard block 的组合不会自动提交，ready/warning 的 paper 组合可以继续生成并提交 paper 订单；live 自动提交仍默认关闭。
- IBKR Gateway 子任务已注入任务级 `IBKR_CLIENT_ID_OFFSET`，减少 report / broker snapshot / opportunity / execution / guard 之间的 `clientId already in use` 冲突。
- `MarketDataService` 已补历史行情失败 cooldown：同一 symbol/request 刚刚出现 empty/error 时，短时间内不再重复请求 IBKR，优先 stale cache 或 fallback。
- Dashboard 构建已避开巨型 weekly summary 的重复 JSON parse：当 `weekly_review_summary.json` 或 standalone weekly evidence JSON 过大时，dashboard 使用独立 CSV/artifact 和 metadata-only health 读取。当前本地 `generate_dashboard` 从分钟级降到约 `1.31s`。

## 10. 2026-05-13 周报输出瘦身补充

- `weekly_review_summary.json` 不再嵌入 `decision_evidence_rows` / `unified_evidence_rows` 大明细，只保留 `decision_evidence_row_count`、`unified_evidence_row_count` 和 `evidence_artifacts` 文件引用。
- `weekly_tuning_dataset.json` 不再重复嵌入完整 `unified_evidence`，只保留 row_count 与 artifact 引用。
- 完整明细仍由 `weekly_decision_evidence.csv`、`weekly_unified_evidence.csv`、`weekly_unified_evidence.json` 等独立 artifact 承载，dashboard 和周报 summary 只消费轻量索引。

## 11. 2026-05-13 SQLite 锁冲突修复

- `Storage` 连接统一启用 30 秒 `busy_timeout`，文件型数据库初始化时尽量启用 WAL，并对短暂 `database is locked/busy` 的 `execute/executemany/commit` 做有限指数退避重试。
- supervisor 报错的 broker snapshot reuse 写入路径已由锁冲突测试覆盖：短暂写锁释放后，`investment_execution_runs` 能正常落库，不再直接让 supervisor 退出。
- 所有 `src/tools`、`src/common`、`src/app` 里的直接 `sqlite3.connect(...)` 已收敛到 `Storage` 或 `connect_sqlite()`，dashboard / weekly / reconcile / export 等读库工具也使用统一 busy timeout。

## 12. 2026-05-14 Owner P0-P6 小账户增值路径

- P0-P6 已归档到 `docs/change_archive_2026-05-14_owner_p0_p6_asset_growth_path.md`，把目标明确为“先证明 paper post-cost edge 和执行质量，再考虑 micro-live”，而不是通过高杠杆追求快速翻倍。
- execution run 现在会输出 `investment_no_order_diagnostics.json/csv`，按 candidate -> target -> raw order -> blocked -> executable -> submitted 解释为什么没有订单。
- execution run 现在会输出 `investment_owner_progression_assessment.json/csv`，把当前状态归类为 `PAPER_BLOCKED`、`PAPER_PLANNED` 或 `PAPER_SUBMITTED`。
- `account_profiles.small` 已改为 1000-25000 AUD 小账户 profile：低 cash floor、低 min trade、whole-share preferred、自动 paper submit 阶段最多 1 单、limit order、仍保留 manual review。
- dashboard overview 已接入 `primary_no_order_reason` 与 `owner_progression_status`，避免 paper 没有订单时只能人工猜测。

## 13. 2026-05-14 IBKR clientId 收敛

- `DEFAULT_CLIENT_ID_RETRY_SPAN` 已收敛为 `1`，supervisor 默认写入 `IBKR_CLIENT_ID_RETRY_SPAN=1`，避免 `clientId already in use` 后自动从 1411 递增到 1412、1413 并在 IB Gateway 留下更多 client 标签。
- `config/supervisor.yaml` 新增 `ibkr_client_id_retry_span: 1` 与 `ibkr_connect_max_rounds: 3`。需要临时容忍相邻 clientId 时必须显式调大，默认路径以清理旧进程 / Gateway orphan session 为先。
- `IBKRConnection` 在连接失败和断连时会无条件尝试 `disconnect()`，用于清掉本进程的半开 socket；无法清理其他旧进程持有的 clientId，因此刷新 preflight / paper execution 前应先停掉旧 supervisor。

## 14. 2026-05-14 Owner 资金口径收敛

- paper 账户的 IBKR `NetLiquidation` 可能是约 100 万级别，不能直接代表当前 owner 的 1000 AUD 生存资金目标。
- `InvestmentExecutionConfig` 新增 `account_equity_cap`，当前 paper execution 配置统一设为 `1000.0`，让 profile、单笔上限、target equity、manual review 与 no-order diagnostics 按真实 owner 资金上限计算。
- execution summary 同时保留 `broker_equity_raw` / `broker_cash_raw` 和 cap 后的 `broker_equity` / `broker_cash`，避免误把 paper 账户体验金当作真实可冒险资本。

## 15. 2026-05-14 Gateway 请求降载

- US/HK/XETRA/ASX paper 配置新增 `research_only_yfinance: true`，报告和候选生成阶段优先使用外部日线，IBKR Gateway 只保留账户、持仓、订单和必要快照职责。
- `config/supervisor.yaml` 暂停 `run_investment_opportunity`，避免每 15-30 分钟重复拉 5m 历史行情。当前阶段先用低频 daily/weekly paper 证明 post-cost edge，不用高频 opportunity 扩大 Gateway 请求量。
- readiness 仍然会因为过去 7 日 telemetry 超预算而阻断 submit；这是正确的。新配置的目标是让后续窗口自然降载，而不是通过调高预算掩盖过量请求。

## 16. 2026-05-14 Opportunity / Cache 阻塞修复

- `generate_investment_report` 在 `research_only_yfinance` 下不再被 universe.yaml 的 `include_scanner: true` 重新打开 IBKR scanner；如确实要在 research-only 下连 scanner，必须显式设置 `include_scanner_research_only: true`。
- `InvestmentOpportunityEngine` 修复 CSV bool 解析：`"False"` 不再被 Python 当成 truthy，避免所有候选被误判为 `WAIT_EVENT`。
- yfinance 日线 stale cache 现在按“最新 bar 时间”选择缓存，而不是固定使用当前 period 或最长 period，避免 `1y` 和 `5y` 缓存价格不一致导致 report 与 opportunity 互相打架。
- 最新 US paper refresh 后，最强阻塞从假 `WAIT_EVENT` / 数据质量问题前移为 `NO_TARGET_WEIGHTS`：当前一致价格下 top candidates 全部为 `REDUCE`，且账户无持仓，所以没有买入目标。这个状态不应通过放宽 gate 强行下单。

## 17. 2026-05-20 Auto-order dashboard submit gate

- `generate_dashboard` 现在读取 `reports_supervisor/auto_order_readiness.json`，并把 `auto_order_status`、`auto_order_submit_plan_status`、`auto_order_submit_plan_reason`、选中 portfolio 和阻断原因写入 `ops_overview`。
- Dashboard v2 新增 Home block `auto_order_readiness`，直接显示自动 paper submit 是否存在单一小额安全候选、被拒候选数量、选中组合、计划订单数和计划金额。
- 简单模式运维总览新增“自动下单”行；高级模式通过 v2 block 展开 submit plan、remediation plan 和 portfolio readiness rows。
- 当前本地 dashboard 生成结果仍为 `submit_plan=BLOCKED`，原因 `no_single_safe_submit_candidate`，首要组合阻断 `preflight_stale`。下一步不是放宽 risk/edge gate，而是刷新 preflight / weekly / market readiness 后，再按新的 dashboard block 判断是否出现唯一 READY 小额 paper 计划单。

## 18. 2026-05-21 Auto-order submit frontier

- `auto_order_readiness` 的 submit plan 新增 `frontier_candidates`，用于展示“最接近可提交但仍被挡住”的 portfolio 列表；这只是诊断排序，不会让硬阻断组合通过 `--submit`。
- frontier 会同时展示 hard blocks、policy rejects、market readiness、计划订单数、计划金额、symbols 和 next action。当前真实排序第一位是 `US:watchlist`，已有计划单 `SCHX,SPLG`，但仍被 `preflight_stale`、`gateway_budget_degraded`、`market_readiness_not_ready` 以及小账户 policy 的 `order_count_exceeds_policy` / `planned_gross_value_exceeds_policy` 挡住。
- `review_auto_order_readiness` markdown 新增 `Submit Frontier` 表，dashboard v2 的 `auto_order_readiness` block 也会展示 frontier count 与明细。
- 下一步优化应聚焦把 `US:watchlist` 从两笔 116.65 AUD 调整为“一笔、<=100 AUD、整股/小额 ETF”的计划单，同时先刷新 preflight 并确认 Gateway budget 不再 degraded；不要通过关闭 readiness gate 强行 paper submit。

## 19. 2026-05-22 Small-account auto-submit alignment

- `account_profiles.small.max_orders_per_run` 从 2 收敛为 1，和 supervisor `auto_order_readiness.max_submit_orders_per_portfolio=1` 对齐，避免小账户计划生成阶段稳定制造 `order_count_exceeds_policy`。
- 这不是放宽 risk / edge / market readiness gate；相反，系统现在只允许先验证单笔 25-100 AUD 等值、整股优先、limit order 的 paper fill / slippage / post-cost edge。
- 当前已生成的 readiness artifact 仍可能显示旧的两笔 `SCHX,SPLG` 计划，必须重新跑 US report / paper execution dry-run / readiness review 后才会反映新的单笔 profile。
- 2026-05-22 no-submit refresh 尝试连接 `127.0.0.1:4002` 被拒绝，`US:watchlist` execution artifact 正确降级为 `IBKR_GATEWAY_UNAVAILABLE`，没有提交订单。
- `auto_order_readiness` 现在会把 `IBKR_GATEWAY_UNAVAILABLE` 提升为具体 hard block `ibkr_gateway_unavailable`，优先于泛化的 `preflight_stale`；frontier next action 也会指向“启动/解锁 IB Gateway paper API 并确认端口监听，再重跑 no-submit”。
- 当前 dashboard 顶部 `auto_order_primary_block_reason=ibkr_gateway_unavailable`、`submit_plan=BLOCKED`、`frontier_candidate_count=6`。下一步应先恢复 Gateway/API 与 preflight，再刷新 US report + paper execution dry-run；在出现唯一 READY、1 单、<=100 AUD 的 paper 候选前不要运行 `--submit`。

## 20. 2026-05-25 Auto-submit buy-leg guard

- IBKR paper Gateway `127.0.0.1:4002` 已恢复监听；`supervisor_preflight_summary.json` 刷新后为 29 pass、0 warn、0 fail。
- 最新 US report 刷新成功：`US:watchlist` 候选 169、ranked 15、plan 15。随后 no-submit execution 成功连接 Gateway，生成 1 笔计划单，金额约 29.30 AUD，但方向是 `SELL SCHX`，不是小账户建仓 BUY 样本。
- `auto_order_readiness.require_buy_order_for_submit=true` 已启用：自动 owner-growth paper submit 必须包含 BUY leg。当前 `US:watchlist` frontier 因 `no_buy_order_for_growth_submit` 继续 blocked，避免系统把 sell-only 退出单误当作增值试单自动提交。
- 旧 `python -m src.app.supervisor` 进程启动早于该 guard，已停止，避免旧内存配置继续运行自动 submit gate。后续必须用新代码/新配置重新启动 supervisor。
- 当前主要剩余阻塞：weekly Gateway request budget 仍为 degraded，US 周请求约 8034/2000，top tool 为历史 `run_investment_opportunity:us:watchlist`；同时 US 常规交易时段关闭。下一步先保持 opportunity 关闭、等待/刷新预算窗口，并继续校准策略让小账户产生 BUY-side whole-share ETF 候选，而不是自动提交 sell-only 单。

## 21. 2026-05-25 Whole-share ETF paper BUY frontier

- 手动 US 刷新必须使用 `config/market_structure_us.yaml`；使用全局 `config/market_structure.yaml` 会漏掉 US 小账户 ETF-first / whole-share 规则，导致 SPLG/SPTM/SCHB 不被提升。
- `account_profiles.small` 新增 `prioritize_buy_orders_for_growth_submit: true`，并在 allocator 中只对满足 `small_account_preferred_candidate=1`、`whole_share_tradable_preferred_candidate=1`、`whole_share_tradability_reason=PASS`、edge/cost/cash/order cap 通过的 BUY 候选跳过旧持仓 sell slot。这样 `max_orders_per_run=1` 不再让旧 `SCHX` 退出单抢掉唯一自动 paper submit 订单槽。
- `investment_execution_us.yaml` 新增 paper-only `whole_share_missing_opportunity_paper_sample_*` 配置：当 opportunity scan 没覆盖到已通过 risk/quality/market-rule/edge 的 whole-share ETF BUY 时，允许生成一笔小额 LMT 样本单；明确的 `WAIT_EVENT` / 非 ETF / 超 100 AUD / edge 或 quality 未通过仍会阻塞。
- runtime `US:watchlist` 已刷新到 supervisor 实际消费目录：当前计划单为 `SPLG BUY 1 @ 87.75`，`execution_order_type=LMT`，`limit_price_buffer_bps_effective=0.0`，`opportunity_status=WHOLE_SHARE_SAMPLE`，`edge_gate_status=PASS`，`quality_status=QUALITY_OK`，`market_rule_status=RULES_OK`。
- `market_readiness` 对 `US:watchlist` 已为 `READY_FOR_PAPER_REVIEW`，`planned_buy_order_value=87.75`，`planned_sell_order_value=0.0`。`auto_order_readiness` 仍 blocked 的原因只剩历史 `gateway_budget_degraded`；US frontier 没有 policy reject，也没有 `market_readiness_not_ready`。
- Gateway 降载继续推进：`_broker_positions()` 现在把成功但为空的 `portfolio(account_id)` 视为“无持仓”，不再继续请求 `portfolio()` 和 `positions()`；dry-run 没有实际 submit 时复用 before broker positions 作为 after snapshot，`investment_execution_summary.json` 输出 `broker_positions_after_reused=true`。这会降低后续 positions 类 Gateway 请求，但 7 日预算窗口需要自然滚动或重新生成周报后才会解除 degraded hard block。

## 22. 2026-05-25 Gateway budget recovery diagnostics

- `ibkr_gateway_budget` 现在保留 market/day 请求分布，并为每个市场输出 `excess_gateway_requests`、`daily_gateway_request_budget`、`projected_recovery_days`、`projected_recovery_at`。这不会放宽预算门，只把“何时可能恢复到预算内”显式写入 weekly review artifact。
- `auto_order_readiness` 的 `gateway_budget_degraded` / `gateway_budget_warning` detail 现在会显示 `requests=current/budget`、`usage`、`top_request_kind`、`top_tool` 和 projected recovery；remediation 会明确建议继续关闭高请求扫描，并在恢复时间后重跑 weekly review / readiness。
- 当前操作原则不变：`US:watchlist` 的 SPLG 小额整股 ETF 计划单已经 ready，但只允许在 Gateway 预算恢复、preflight 新鲜、market readiness 仍 ready 后进入 paper submit；不通过关闭 `block_on_gateway_budget_degraded` 来绕过 historical overuse。

## 23. 2026-05-26 Non-CN multi-market paper auto-submit policy

- `auto_order_readiness.excluded_markets=["CN"]` 已加入 supervisor policy；即使后续误把 CN report 的 `submit_investment_execution` 打开，readiness 层也会返回 `auto_submit_market_excluded`，深圳/上海继续 research-only。
- 非 CN 市场的 paper submit plan 已从“只能选择单一 portfolio”升级为“多市场小额计划”：默认最多 4 个 portfolio、每个 market 最多 1 个 portfolio、每个 portfolio 最多 1 单、单组合 gross <= 100 AUD、总 gross <= 400 AUD，并继续要求 BUY leg。
- supervisor submit gate 现在消费 `selected_portfolio_ids` 列表；只有被多市场 submit plan 选中的 portfolio 会带 `--submit`，同一市场的第二个 portfolio 会被 `market_portfolio_count_exceeds_policy` 拒绝，避免 HK/US 双 portfolio 同时抢风险额度。
- 当前真实 artifact 状态：preflight 已刷新为 29 pass / 0 warn / 0 fail；`US:watchlist` 仍是第一 frontier，计划 `SPLG BUY 1`，但所有非 CN market 仍被历史 `gateway_budget_degraded` 阻断，ASX/HK/XETRA 还需要刷新 execution artifact 后才能进入 ready。该改动让市场具备自动下单资格，不绕过预算、市场准备度或风险门。

## 24. 2026-05-26 Submit quality / frequency / growth guard

- `market_readiness` 现在读取 `investment_execution_plan.csv`，为 planned orders 计算 `submit_quality_status`、post-cost net edge、edge margin、expected cost、order ADV 占比、order type，以及 edge/quality/market-rule/shadow/manual review 状态。
- `auto_order_readiness` 新增 `block_on_submit_quality_not_pass=true`：自动 paper submit 必须看到 `submit_quality_status=PASS`。默认门槛为 `min_submit_net_edge_bps=8`、`min_submit_edge_margin_bps=3`、`max_submit_expected_cost_bps=35`、`require_limit_order_for_submit=true`、`max_submit_order_adv_pct=0.001`。
- 频率层继续由 submit plan 控制：每市场最多 1 个 portfolio、每 portfolio 最多 1 单、非 CN 总 portfolio 最多 4、总 gross <= 400 AUD。这样可以让多市场具备自动下单能力，但不会在 1000 AUD 小账户里高频扩张。
- 当前真实 `US:watchlist` SPLG 计划通过质量门：`submit_quality_status=PASS`、min net edge 约 `10.84bps`、min edge margin 约 `4.84bps`、max expected cost 约 `22.74bps`、order type 为 `LMT`。它仍被历史 `gateway_budget_degraded` 阻断，因此当前不提交订单。

## 25. 2026-05-26 Quality-tier frequency and watchlist expansion

- Submit quality 现在多一层 `submit_quality_tier`：`HIGH` 候选必须同时满足更高的 net edge、edge margin 和 cost 门槛；auto-order submit plan 会优先选择 `HIGH`，再选择普通 `PASS`，最后才按金额和市场排序。提高下单频率只发生在质量更高的候选上，而不是通过放宽风险门实现。
- `config/supervisor.yaml` 新增 high-quality 门槛：`high_quality_min_net_edge_bps=16`、`high_quality_min_edge_margin_bps=8`、`high_quality_max_expected_cost_bps=25`。当前 SPLG 是普通 `PASS`，还不是 `HIGH`，所以不会因为频率层被额外加速。
- 新增 `src/tools/expand_investment_watchlists.py` 与 `src/common/watchlist_expansion.py`：它们从本地最新 `investment_candidates.csv` 生成 auto-expanded watchlist，只纳入 `ACCUMULATE/HOLD`、execution-ready、whole-share tradable、成本/流动性/数据质量通过的候选。
- US/HK/ASX/XETRA 的 `universe.yaml` 已接入 `config/watchlists/auto_expanded/*_quality_growth.yaml`，这些文件会进入 symbol master 扩展层；CN 不纳入自动下单扩展。
- 非 CN report 的候选输出数已适度扩大：US/HK/ASX/XETRA 常规 report `top_n=15`，US overnight `top_n=10`。这会增加可分析候选面，但不增加自动提交上限。
- 本次本地生成结果：US 选中 `SPLG, SPTM, SCHB`；ASX/HK/XETRA 暂无候选通过 whole-share/cost/liquidity 组合门。完整诊断在 `reports_supervisor/watchlist_expansion/watchlist_expansion_candidates.csv`，可看到每个被拒候选的原因。

## 26. 2026-05-27 Offline recovery and freshness consistency

- 新增 `src/common/freshness.py`，把 UTC timestamp parsing、timestamp age、file age、fresh/stale labeling 从多个模块抽成公共 helper，减少重复实现并降低时区/陈旧度判断不一致的风险。
- `auto_order_readiness` 新增 `offline_recovery_required` 及 summary 字段；断网或 IBKR Gateway 不可用超过一天后，如果 preflight、execution artifact、market readiness 或 Gateway telemetry 过期，dashboard 会明确显示需要 offline recovery，而不是只散落在多个 artifact section。
- 该层不放宽任何交易门：stale preflight、stale execution artifact、Gateway budget degraded、submit quality not pass 仍然会阻断自动 paper submit。
- Dashboard ops overview 和 v2 auto-order block 现在展示 `auto_order_offline_recovery_required_count` / `offline_recovery_summary_text`，恢复顺序变为：先刷新 preflight，再跑 report + paper execution dry-run，再刷新 market readiness / auto-order readiness，最后只对仍然 READY 的小额计划评估 paper submit。
- 针对性验证通过：`tests/test_freshness.py`、`tests/test_auto_order_readiness.py`、`tests/test_dashboard_blocks.py`、`tests/test_generate_dashboard_helpers.py`、`tests/test_market_readiness.py`、`tests/test_ibkr_gateway_budget.py` 共 91 个测试。

## 27. 2026-05-27 Dashboard market data fallback label fix

- 修复 GitHub Actions `integration-suite` 中 XETRA 非 research-only fallback 被误标为 `研究Fallback` 的问题。
- `generate_dashboard` 现在只有在 market/report 本身明确 research-only 时，才把 yfinance fallback 标成 `研究Fallback`；XETRA/US/HK/ASX 等 execution-capable 市场即使为了降载使用 yfinance，也会在 IBKR 历史行情不可用时显示 `待排查`。
- 该修复只改变 dashboard 分类，不放宽下单质量、风险、edge、market-rule 或 Gateway budget gate。
- 针对性验证通过：XETRA 非 research-only fallback 用例返回 `待排查`，CN research-only fallback 仍返回 `研究Fallback`。

## 28. 2026-05-27 Account-aware watchlist expansion

- `account_profiles.yaml` 新增 `watchlist_expansion` 分层策略：小账户只扩展低价、可整股、ETF-first、低成本候选；中/大账户再逐步允许高流动性股票 basket。
- `expand_investment_watchlists` 新增 `--account_equity` / `--account_profile` / `--account_profile_config`，会按账户金额解析 small/medium/large profile，并把有效 policy 写入生成的 auto-expanded watchlist。
- `watchlist_expansion` 新增 `max_last_close`、asset-class preference 排序和 `last_close_above_account_cap` reject reason；这让 1000 AUD 小账户不会把高价股票纳入自动扩展池，即使它们分数较高。
- 用 `--account_equity 1000` 刷新后，当前本地 evidence 仅纳入 US 的 `SPTM,SCHB`；ASX/HK/XETRA 仍为空，因为整股、成本、流动性或 market-rule 证据没有通过。这是质量优先，不是漏选。
- 自动下单门槛不变：扩展 watchlist 只是候选池，真正 paper submit 仍需通过 preflight、market readiness、Gateway budget、submit quality、edge/cost 和风险门。

## 29. 2026-05-29 Auto-order readiness freshness health

- Dashboard 现在生成顶层 `auto_order_readiness_health`，检查 `auto_order_readiness.json` 是否缺失、缺少 `generated_at`、超过 `auto_order_readiness.max_artifact_age_hours`，或早于最新 weekly Gateway budget。
- `ops_overview` 新增 `auto_order_health=<status>`，并在过期时输出 `AUTO_ORDER/readiness_freshness` 告警；首页“自动下单”会优先显示“证据过旧”，避免 operator 把旧 readiness 当成当前可提交状态。
- Dashboard v2 `auto_order_readiness` block 新增 readiness health metrics；即使 submit plan 表面 READY，只要 readiness artifact 过旧，block 也会降到 warning。
- 当前本地 dashboard 重新生成后显示 `auto_order_health=warning`，原因是 `auto_order_readiness.json` 约 48 小时未刷新，且 weekly Gateway budget 更新晚于它。下一步应刷新 preflight / paper execution dry-run / auto-order readiness，再重新判断是否存在当前 READY 的小额整股 ETF 计划单。
- 这不是收益门或风险门调整：Gateway budget degraded、submit quality not pass、market readiness not ready、edge/cost 不达标仍会阻断自动 paper submit。

## 30. 2026-05-29 Auto-order readiness dependency refresh

- Supervisor 现在不会只依赖 readiness 内容签名：如果现有 `auto_order_readiness.json` 超过 `auto_order_readiness.max_artifact_age_hours`，即使 rows/summary 没变也会重写。
- 如果 preflight summary、weekly review summary、轻量 `weekly_ibkr_gateway_budget_status.json` 或 market readiness 比 readiness artifact 更新，supervisor 也会重写 readiness，并在 payload 里写入 `rewrite_reason=dependency_newer_than_artifact`。
- Auto-order readiness 的 weekly 输入现在优先用 `weekly_ibkr_gateway_budget_status.json` 覆盖 `weekly_review_summary.json` 内的旧 Gateway budget 字段，减少每轮下单门控对超大 weekly summary 的依赖。
- 交易含义不变：这只是让证据链自动追上最新 artifact；小账户 paper submit 仍必须满足 BUY leg、整股/金额上限、submit quality、Gateway budget、market readiness、edge/cost 和风险门。

## 31. 2026-05-29 Watchlist expansion dashboard

- Dashboard 现在读取 `reports_supervisor/watchlist_expansion/watchlist_expansion_summary.json`、summary CSV 和 candidate CSV，并输出顶层 `watchlist_expansion_summary`。
- Dashboard v2 新增 advanced block `watchlist_expansion`，可直接看到当前账户 profile、候选行数、选中数、零选中市场、artifact age、selected symbols 和主要 reject reason。
- 当前本地 small profile 诊断：65 条候选、2 个选中（`SPTM,SCHB`）、3 个市场零选中，主要 reject reason 是 `expected_cost_above_max`。这说明扩大 ASX/HK/XETRA 可选范围的下一步应优先校准成本/整股可交易/ETF-first universe，而不是绕过风险门。

## 32. 2026-06-03 Auto-order frequency plan

- `auto_order_readiness` 现在会消费 `watchlist_expansion/watchlist_expansion_summary.json`，并输出只读 `frequency_plan`：当已有 frontier candidate 被 hard block 挡住时，优先显示 frontier blocker；只有没有当前可修复 frontier 时，才把 seed proposals 解释为候选池供给缺口。
- `review_auto_order_readiness` markdown 新增 `Frequency Plan` section；dashboard v2 `auto_order_readiness` block 新增 `candidate_supply_status/reason/primary_action`、seed proposal 数量、seed proposal markets 和 `frequency_plan_does_not_change_submit_decision`。
- 当前真实刷新结果：`frequency_plan.status=frontier_blocked`、`reason=preflight_stale`、`primary_action=Refresh supervisor preflight before automated submit.`；同时保留 ASX/HK/XETRA 三个 `create_or_refresh_preferred_asset_seed_watchlist` 手动提案。
- 当前 submit plan 仍是 `BLOCKED/no_single_safe_submit_candidate`，第一 frontier 是 `US:watchlist` 的 `SPLG` 小额 LMT BUY 计划，但它被 `preflight_stale` 与 `gateway_budget_degraded` 挡住。另有 ASX/HK 受 `ibkr_gateway_unavailable` 影响。
- 交易含义不变：该层只提升“为什么不能提高下单频率”的可解释性，不自动加入 symbol、不自动提交 paper、不放宽 risk、edge、cost、market-rule、Gateway budget 或 submit-quality gate。

## 33. 2026-06-03 Watchlist seed intake plan

- `watchlist_expansion` 现在会把 seed proposals 转成 review-only `seed_intake_plan`：每个市场会给出 `intake_status`、目标 review watchlist path、candidate/evidence symbols、next action、acceptance rule 和 `does_not_change_symbol_master=true`。
- `expand_investment_watchlists` 会在 `reports_supervisor/watchlist_expansion/seed_review/` 写出 ASX/HK/XETRA 的 review-only YAML；这些文件不是 `config/markets/*/universe.yaml` 的 `symbol_master_watchlists`，不会改变自动下单候选池。
- 当前真实刷新结果：`selected_count=2`、`zero_selected_market_count=3`、`seed_intake_plan_count=3`、`seed_intake_external_source_count=3`，dashboard v2 `watchlist_expansion` block 已因此显示 `warn`。
- ASX/HK/XETRA 当前都是 `NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE`。近似高分股票如 `BHP.AX`、`3988.HK`、`IFX.DE` 只作为 `evidence_symbols` 保留；因为 small profile 是 ETF-first，这些股票不会被直接提升为 seed candidate。
- 下一步应补 verified low-cost ETF-first source，再重新跑 candidate report / paper execution dry-run / market readiness / auto-order readiness；只有通过 account profile、whole-share、cost、liquidity、data quality、expected edge 和 submit quality 的 symbol 才能进入 auto-expanded watchlist。
