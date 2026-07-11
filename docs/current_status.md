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
- **2026-06-08 已建立 review-only ETF seed source registry：ASX/HK/XETRA 各有 2 个带来源、验证日期和 IBKR mapping 状态的候选，report 层可评分但统一强制 `WATCH/execution_ready=0`，必须人工 promotion 后才能进入交易池；scoped auto-order readiness 已补 canonical watchlist expansion fallback，dashboard 刷新后仍保留 6 个 source candidates，且 `does_not_change_submit_decision=true`**
- **2026-06-08 已补 account growth tier plan 与 dashboard legacy fallback：watchlist expansion summary 现在会输出当前账户金额区间的只读增长路径；1000 AUD 小账户被明确限制为 ETF-first、单笔小额限价、每轮最多 1 单、单笔约 100 AUD，并要求先验证 seed ETF candidate report / fill quality；dashboard v2 会在旧 readiness artifact 缺少 seed 字段时从 canonical watchlist expansion 派生展示字段，避免长驻旧 Supervisor 把 6 个 source candidates 显示成 0**
- **2026-06-10 已新增 minimum-request auto-order recovery plan：系统会围绕最高质量 `PASS` frontier 生成 target-scoped 恢复步骤，而不是全市场批量刷新；当前目标是 US `SPLG`，先等待 US Gateway request budget 在 `2026-06-12T23:59:59.999999+00:00` 后恢复，再只刷新 `US:watchlist` 的 report + no-submit execution，Gateway-backed refresh 上限为 1 个 portfolio；本地 preflight 已刷新为 `29 PASS / 0 WARN / 0 FAIL`，`preflight_stale` 已消除**
- **2026-06-14 已新增 evidence-scaled paper submit capacity：配置仍保留未来最多 4 个组合/400 AUD 的上限，但在至少 5 个 fill、5 个 matured 5d realized-edge 样本、非负 realized edge、可接受 slippage 和 error rate 同时通过前，当前有效容量固定为 1 个组合/100 AUD；这修复了 small-account profile 与全局 submit policy 的不一致**
- **2026-06-14 已把 review seed 推进为结构化晋级复核：source freshness、candidate report、质量 gate、IBKR mapping 和人工 promotion 形成明确状态链；ASX 新增 BGBL/DHHF 两个官方来源、参考 NAV 低于 100 AUD 的 review-only ETF seed，但不会自动进入 symbol master 或下单**
- **2026-06-14 已把 submit capacity 从二元跳变升级为 baseline/trial/full 三段：5 fill + 5 matured 5d 样本只进入 2 组合/200 AUD trial；20 fill + 15 matured 样本 + 至少 2 个市场有效证据及更严格 realized edge/slippage/error 才进入 4 组合/400 AUD full；trial/full 每轮最多带 1 个尚无本市场 realized evidence 的市场**
- **2026-06-14 Supervisor 已接入零 IBKR 请求的本地 watchlist expansion refresh：每 180 分钟且全市场休市时，从已有 candidate/report artifact 重建 seed promotion evidence；auto-order readiness CLI 已统一读取 scoped runtime summary，当前 8 个 seed 中 BGBL/DHHF 需要 candidate report，另外 6 个被质量 gate 拒绝，0 个可晋级；非有限 score 已禁止写入 JSON**
- **2026-06-14 已新增 bounded seed evidence queue：Supervisor 只对 `CANDIDATE_REPORT_REQUIRED` 的 review seed 运行单市场、有限标的、yfinance-only 报告，并跳过 IBKR/scanner/benchmark/macro/news/short-book/backtest；真实 ASX 运行只分析 DHHF/BGBL，2 个候选均转为 `QUALITY_REJECTED`，队列归零且没有自动晋级或下单**
- **2026-06-15 已新增 auto-order 本地依赖自动维护：Supervisor 每 6 小时刷新轻量 preflight、每 15 分钟从现有 artifacts 重建 market readiness，并在新 execution evidence 产生后立即再构建；真实单周期得到 `29 PASS / 0 WARN / 0 FAIL`，`preflight_stale` 已消除，剩余阻塞收敛为 stale/degraded execution artifact、post-cost edge 不足和 HK strategy suggestion governance**
- **2026-06-15 已新增 bounded execution evidence maintenance：Supervisor 在全市场休市、同周期无正常 execution、无 active recovery checkpoint 且 Gateway execution reserve 可用时，只选择 1 个 paper portfolio 运行 `--recovery_evidence_only` dry-run；强制 `submit=false`、不刷新 report/opportunity、不占 submit slot，并把状态写入 auto-order readiness/dashboard。真实 ASX dry-run 已把旧 `IBKR_GATEWAY_UNAVAILABLE` 更新为 fresh `BLOCKED_OPPORTUNITY`，完整测试为 `722 passed`**
- **2026-06-16 已校准 post-cost buy-quality 诊断与 WAIT_PULLBACK evidence：market readiness 现在只用 `BUY/ACCUMULATE` planned rows 评估 submit quality，HK/US 的 `SCHX/SCHX.HK` exit-only recovery evidence 改为 `NO_BUY_ORDERS`，不再误报为低 edge 买入阻塞；WAIT_PULLBACK calibration 已进入 market readiness、auto-order readiness 和 dashboard v2 现有 Auto Order block，当前 6 个 portfolio 需要 anchor review、31 个 close wait rows、0 个 near-entry candidate，submit plan 仍为 `BLOCKED/no_single_safe_submit_candidate`**
- **2026-06-16 已把 HK post-cost calibration 接入同一 evidence 链：`market_readiness` 读取 `investment_candidates.csv` 生成 post-cost rows，`auto_order_readiness` 和 dashboard v2 直接展示；fresh HK evidence 显示每个 HK portfolio 15 个候选中 14 个 high-cost、6 个仍为正 post-cost edge，top symbols 为 `3988.HK/0939.HK/1398.HK/2388.HK/0005.HK`，结论是复核市场特定成本阈值与低成本候选来源，不是自动放宽 cost/edge gate**
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

## 34. 2026-06-10 Target-scoped recovery execution

- `auto_order_readiness` 新增 `recovery_eligibility` contract，明确区分 budget recovery 时间未到、时间已到但 budget evidence 尚未刷新、以及可执行单一目标 dry-run 三种状态。
- Supervisor 现在会消费该 contract：recovery 激活时暂停非目标 investment report/execution，并暂停 opportunity 扫描；只有目标 frontier 仍为 `PASS` 且 budget evidence 已恢复时，才允许目标 report 与一次 execution no-submit。
- 即使配置里 `submit_investment_execution=true`，targeted recovery 也会复制运行参数并强制关闭 submit，不修改原配置。
- CLI readiness JSON/markdown 与 dashboard v2 使用同一 eligibility 字段，避免展示层和调度层结论不一致。
- broker snapshot 与 risk guard 不受该恢复限流影响，避免为了减少请求而削弱账户一致性和保护性控制。
- 本次没有修改或自动提升 watchlist symbol，也没有放宽 risk、edge、cost、liquidity、market-rule、Gateway budget 或 submit-quality gate。

## 35. 2026-06-11 Recovery flow and watchlist resilience

- 已停止 2026-06-08 启动、仍加载旧 schema 的 Supervisor，并用当前代码验证 recovery gate：report、opportunity、execution、submit 当前均未放行。
- 本地 preflight 已刷新为 `29 PASS / 0 WARN / 0 FAIL`，`preflight_stale` 已从恢复步骤移除。
- 当前目标仍是 `US:watchlist / SPLG`，submit quality 为 `PASS`；Gateway budget 预计恢复时间为 UTC `2026-06-13T23:59:59.999999+00:00`，即悉尼时间 2026-06-14 09:59:59。到时仍需先刷新 budget evidence，不能只凭时间自动下单。
- `refresh_watchlist` 新增 last-known-good 保护：动态源全失败时不覆盖 resolved 文件；部分失败且候选集合异常收缩时也保留旧列表；正常刷新改为原子替换。
- auto-order readiness 内容签名忽略连续变化的 age-hour 字段，避免 Supervisor 每 30 秒重写 readiness 并重复生成 dashboard；状态、blocker、依赖更新和 stale 门槛仍会触发刷新。
- 运行态发现同一 scoped runtime 曾同时存在两个 Supervisor；现已增加 OS 级 `supervisor.lock`，重复实例会在启动任何 dashboard/Gateway/scheduler 工作前以非零状态退出。
- 最新 broker snapshot 显示 US/HK 存在活动持仓，因此 risk guard 继续作为保护性路径运行；recovery gate 仅暂停 report、opportunity、execution 和 submit 等收益生成路径。
- 运行态进一步发现 `label_investment_snapshots` 会在恢复期触发市场数据请求；现已随 active recovery 暂停，纯本地 weekly review 继续运行。
- 验证结果：integration tier `115 passed`，恢复/锁定/watchlist 聚焦测试 `14 passed`，labeling recovery follow-up `5 passed`，`pip check` 与 Python compile 均通过。
- 本轮不自动加入 symbol，不放宽 risk、edge、cost、liquidity、market-rule、Gateway budget 或 submit-quality gate。

## 36. 2026-06-12 Automatic recovery evidence and submit-slot isolation

- 新增本地轻量 `refresh_ibkr_gateway_budget`：只读取 `.cache/ibkr_request_telemetry`，原子刷新 weekly request/budget JSON 与 CSV，不连接 IBKR，也不增加 Gateway 请求。
- Supervisor 在 projected recovery 到点后自动刷新 budget evidence；只有新证据显示目标市场不再超预算，才创建持久化 `auto_order_recovery_checkpoint.json`。
- checkpoint 跨重启保留目标 market/portfolio、attempt count、next attempt 和完成状态；失败后按 cooldown 自动重试，不再需要用户回复“继续”。
- target recovery 仍只允许一个 report 和一个 no-submit execution evidence dry-run；execution artifact 新增 `execution_purpose=RECOVERY_EVIDENCE`、`recovery_evidence_only=true`、`consumes_submit_slot=false`。
- Supervisor 状态恢复会忽略 recovery evidence marker，因此恢复流程不会占用下一次正常 paper submit 的 execution slot。
- Gateway unavailable 虽然仍会生成 degraded diagnostics，但不能完成 recovery checkpoint；只有 purpose 正确、slot 隔离且 broker connection 未失败的 artifact 才算恢复证据有效。
- CLI readiness 现在优先消费轻量 Gateway budget artifact，避免 weekly summary 内嵌旧 budget 与 Supervisor 判断不一致。
- 最新本地 evidence（UTC `2026-06-12T00:13:02.619921+00:00`）：Gateway requests `9596`，4 个市场超预算；US 为 `3062/2000`、`153.1%`，projected recovery 为 UTC `2026-06-13T23:59:59.999999+00:00`。当前仍不允许 submit。
- 本轮没有放宽 risk、edge、cost、liquidity、market-rule、preflight、Gateway budget 或 submit-quality gate。

## 37. 2026-06-16 Post-cost buy quality and WAIT_PULLBACK calibration

- `market_readiness` 的 submit-quality 现在只评估计划买入腿：`BUY` / `ACCUMULATE` 才会进入 expected edge、cost、edge margin、order type 和 gate 状态计算。
- 退出/再平衡计划单会显示为 `NO_BUY_ORDERS`，并保留 `submit_quality_buy_order_count` 与 `submit_quality_non_buy_order_count`，避免把 `SELL` 行的空 edge-gate 字段误判成低质量买入。
- 新增 `opportunity_calibration`，从 `investment_opportunity_scan.csv` 聚合 `WAIT_PULLBACK` 行，输出 review band、near-entry candidate、anchor component、top wait symbols 和缺失 asset class 诊断。
- 同一模块现在也从 `investment_candidates.csv` 聚合 post-cost calibration：expected edge、expected cost、post-cost edge、高成本候选数、正 post-cost edge 候选数和 top symbols。
- `auto_order_readiness` rows 和 dashboard v2 的现有 `auto_order_readiness` block 已显示 WAIT_PULLBACK 与 post-cost calibration；没有新增 dashboard block，block 数仍为 14。
- 当前真实刷新：HK/US 的 `SCHX` / `SCHX.HK` recovery exit-only evidence 为 `NO_BUY_ORDERS`；6 个 portfolio 进入 `REVIEW_ANCHOR`，共 31 个 close wait rows，near-entry candidate 为 0。
- Fresh HK evidence：每个 HK portfolio 15 个候选中 14 个 high-cost，6 个仍有正 post-cost edge，top symbols 为 `3988.HK,0939.HK,1398.HK,2388.HK,0005.HK`；dashboard 汇总 6 个 post-cost review portfolios、69 个 high-cost candidates、41 个 positive-edge candidates。
- 下一步不是降低风险门或 edge gate，而是在相关市场窗口刷新 stale execution artifact，并用 5/20d outcome evidence 验证 close WAIT_PULLBACK 行与 HK positive post-cost candidates 是否真的改善交易质量。

## 38. 2026-06-16 HK outcome validation and Supervisor shutdown diagnostics

- 新增 `src.tools.review_opportunity_outcomes`，按需流式读取 `weekly_unified_evidence.csv`，只筛选当前 market readiness 中的相关 market/portfolio/symbol，生成 `opportunity_outcome_validation.json/csv/md`；该工具不进入 Supervisor 高频循环，避免每 30 秒读取 112MB weekly evidence。
- `opportunity_calibration` 现在保留 bounded `positive_post_cost_rows` 与 `close_wait_pullback_rows`，让后续 outcome 验证不再只能依赖 top-symbol 字符串。
- 最新 HK 验证结果：两个 HK portfolio 的 positive post-cost group 与 close WAIT_PULLBACK group 全部为 `OUTCOME_SUPPORTS_GROUP`；5d 平均约 `133.66-141.12` bps，20d 平均约 `245.53-313.61` bps，成熟样本合计 4,072 个 5d、3,124 个 20d。
- 解释边界：这是同符号历史成熟 outcome 验证，不代表 2026-06-16 最新候选本身已经拥有未来 5/20d 成熟结果；当前结论是保持 gate、继续监控 fresh realized outcomes，而不是放宽 risk/edge/cost/liquidity/market-rule/Gateway budget/submit-quality。
- Supervisor 新增 `supervisor_shutdown_status.json`，记录 `running/stopping/stopped/crashed`、pid、config、signal 和写入时间；SIGINT/SIGTERM 仍优雅停止。
- Supervisor 现在处理 SIGHUP 并保持运行，降低前台 terminal/PTY 断开导致“无声自动 shutdown”的概率；若未来仍退出，优先读取 shutdown status artifact 判断是 signal、exception 还是人为停止。

## 39. 2026-06-16 Opportunity outcome calibration suggestions

- `review_opportunity_outcomes` 已引入 calibration suggestion 层，除 validation rows 外新增 `calibration_suggestion_summary`、`calibration_suggestions` 和 `opportunity_outcome_calibration_suggestions.csv`。
- 所有建议都是只读治理建议：`read_only=true`、`auto_apply=false`、`paper_only=true`；不会自动修改 YAML，不会绕过 risk/edge/cost/liquidity/market-rule/Gateway budget/submit-quality gate。
- 最新全市场 validation：14 条验证、78 个 matched symbols、10,365 个成熟 5d 样本、7,572 个成熟 20d 样本。
- 最新建议层：14 条建议，其中 7 条 P1、6 条 `WAIT_PULLBACK_ANCHOR_REVIEW`、2 条 HK `HK_POST_COST_THRESHOLD_REVIEW`、1 条 CN `WAIT_PULLBACK_NO_ACTION`。
- HK post-cost 建议字段是 `submit_quality.max_expected_cost_bps`，但接受规则要求只在 paper 中单字段试验、必须有 fresh HK BUY plan、`expected_post_cost_edge_bps >= 0`、submit quality PASS、且 fill/slippage 与 5/20d outcome 不退化。
- WAIT_PULLBACK 建议字段是 `opportunity_entry.near_entry_gap_pct`，用于准备小额 limit paper trial；支持市场包括 HK、US、XETRA、ASX，CN 因没有 close WAIT_PULLBACK candidate group 暂不动作。
- Dashboard v2 现有 Auto Order block 已显示这些 outcome validation / calibration suggestion metrics；当前实际 dashboard 显示 validation=14、P1=7、WAIT_PULLBACK anchor review=6、HK post-cost review=2。

## 40. 2026-06-16 Opportunity calibration paper-only trial plan

- `review_opportunity_outcomes` 已升级到 v3 schema，新增 `calibration_trial_plan_summary`、`calibration_trial_plan` 和 `opportunity_outcome_calibration_trial_plan.csv`。
- 当前真实 trial plan：9 条 rows、8 条 ready for manual review、7 条 P1 ready、auto-apply rows 为 0。
- HK post-cost 纸面试验合同：字段 `auto_order_readiness.max_submit_expected_cost_bps`，当前值 `35.0 bps`，建议仅在 HK market-scoped paper review 中审查 `55.0 bps`，最大 `60.0 bps`；必须保持 fresh HK BUY plan、`expected_post_cost_edge_bps >= 0`、submit quality PASS、limit order、Gateway budget OK、fill/slippage 与 5/20d outcome 不退化。
- WAIT_PULLBACK 纸面试验合同：字段 `opportunity_entry.near_entry_gap_pct`，当前值 `1.0%`，P1 市场建议审查 `2.0%`，ASX P2 审查 `1.5%`，最大 `3.0%`；只允许小额 whole-share feasible、post-cost positive、limit paper trial。
- Dashboard v2 Auto Order block 已显示 trial plan metrics：trial=9、ready=8、P1 ready=7、auto_apply=0。
- 本步骤仍不修改 YAML、不提交真实订单、不放宽 risk/edge/liquidity/market-rule/Gateway budget/submit-quality gate；它只是把下一步人工 paper calibration 变成可审计合同。

## 41. 2026-06-18 HK outcome validation and shutdown event history

- 已用当前 `market_readiness.json` 与 `weekly_unified_evidence.csv` 刷新 HK-only outcome validation，输出到 `/private/tmp/ibkr_hk_outcome_validation/`，没有覆盖 Supervisor dashboard 正在使用的 all-market artifact。
- 最新 HK 正 post-cost candidate evidence：`HK:resolved_hk_top100_bluechip` 平均 5d `+138.32bps`、20d `+307.90bps`；`HK:resolved_hk_top100_tech_growth` 平均 5d `+138.69bps`、20d `+313.60bps`。
- 最新 HK close `WAIT_PULLBACK` evidence：bluechip 平均 5d `+125.96bps`、20d `+212.07bps`；tech growth 平均 5d `+126.74bps`、20d `+222.09bps`。
- 解释边界：HK group outcome 支持继续 paper-only calibration review，但 `1288.HK` 与 `2359.HK` 的 close `WAIT_PULLBACK` 单票 outcome 为负，因此不能无差别放宽 near-entry anchor；任何试验仍必须 symbol-aware、limit-only、whole-share feasible、post-cost positive。
- 当前 HK 自动提交仍被非 outcome gate 阻断：execution artifact stale、当前 execution plan 无 BUY 单、strategy suggestion stale、Gateway research budget degraded；本轮不修改 YAML、不提交订单、不放宽任何 submit gate。
- Supervisor 当前运行态显示 `running / ignored_signal:SIGHUP`，说明最近一次可见事件是终端/会话断开类信号且已被忽略，不是当前进程崩溃。
- Supervisor 现在会同时写最新 `supervisor_shutdown_status.json` 和追加式 `supervisor_shutdown_events.jsonl`；异常退出最终状态保持 `crashed`，不会再被 `finally` 清理阶段覆盖成 `stopped`。
- 针对性验证通过：`tests/test_supervisor_shutdown_status.py`、`tests/test_review_opportunity_outcomes.py`。

## 42. 2026-06-18 Dashboard shutdown visibility

- Dashboard 现在读取 `supervisor_shutdown_status.json` 与最近 20 条 `supervisor_shutdown_events.jsonl`，并在顶层 payload 输出 `supervisor_shutdown_status` / `supervisor_shutdown_events`。
- `ops_overview` 和现有 Ops Health v2 block 现在暴露 `supervisor_shutdown_status`、`supervisor_shutdown_health_status`、`supervisor_shutdown_reason`、`supervisor_shutdown_last_signal_name`、`supervisor_shutdown_event_count`。
- 简单模式运维表新增 `Supervisor` 行；如果状态是 `crashed/stopped/stopping` 会进入 Ops alert，`running / ignored_signal:SIGHUP` 只显示为可见状态，不作为自动下单阻断。
- 实际 scoped dashboard 已刷新，当前显示 `supervisor_shutdown_status.status=running`、`reason=ignored_signal:SIGHUP`、`ops_overview.supervisor_shutdown_health_status=ready`、`event_count=0`。
- `event_count=0` 是因为当前正在运行的 Supervisor 进程早于 event-history 代码启动；重启后会开始追加事件历史。
- 本步骤不新增 IBKR 请求，不修改 YAML，不提交订单，也不放宽 risk、edge、cost、liquidity、market-rule、Gateway budget 或 submit-quality gate；它只让意外 shutdown 成为 dashboard 可见的运维证据。

## 43. 2026-06-18 Stale execution refresh priority plan

- `auto_order_readiness` summary 新增只读 `stale_execution_refresh_plan`，用于在 execution artifact stale 且无法自动提交时，排序下一次 no-submit evidence refresh 的目标组合。
- 排序依据包括 stale artifact 状态、post-cost positive candidate 数量、high-cost positive candidate 数量、close `WAIT_PULLBACK` 数量、artifact stale gap 和已有 order evidence；disabled / research-only 行被排除。
- 该计划固定输出 `paper_only=true`、`submit_orders=false`、`does_not_relax_submit_gates=true`，并使用 `request_policy=one_stale_execution_portfolio_after_gateway_budget_ok`。
- `auto_order_readiness.md` 新增 `Stale Execution Refresh Plan` section；dashboard Auto Order block 也显示 stale refresh plan metrics 与 rows。
- 最新本地刷新结果：`status=WAIT_GATEWAY_BUDGET`、`primary_market=US`、`primary_portfolio_id=US:watchlist`、`target_count=4`、`submit_orders=false`；top rows 依次包含 `US:watchlist`、`HK:resolved_hk_top100_tech_growth`、`HK:resolved_hk_top100_bluechip`、`ASX:asx_top_quality`。
- 交易含义：当前仍需等待 Gateway budget 恢复，不能刷新高请求路径，更不能提交订单；该计划只是在预算恢复后把下一次最小请求 no-submit refresh 目标明确化。

## 44. 2026-06-18 HK outcome verification and Supervisor liveness check

- 已用 `--market HK` 单独刷新 opportunity outcome validation 到 `runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/`，没有覆盖 dashboard 使用的全市场 artifact。
- HK 正 post-cost candidates 当前仍为 outcome-supported：bluechip 平均 5d `+138.32bps`、20d `+307.90bps`；tech growth 平均 5d `+138.69bps`、20d `+313.60bps`。
- HK close `WAIT_PULLBACK` 当前也为 outcome-supported：bluechip 平均 5d `+125.96bps`、20d `+212.07bps`；tech growth 平均 5d `+126.74bps`、20d `+222.09bps`。
- 解释边界：这支持 paper-only calibration/trial review，不支持直接放宽 risk、edge、cost、liquidity、market-rule、Gateway budget 或 submit-quality gate；当前 HK execution artifact 仍 stale，且没有 BUY plan。
- Dashboard ops overview 现在会对 `supervisor_shutdown_status.status=running` 做 PID liveness 校验；如果 PID 已不存在，会把 `supervisor_shutdown_health_status` 标为 `degraded` 并产生 `SUPERVISOR` 告警。
- 当前实际 dashboard 刷新后：`status=running`、`reason=ignored_signal:SIGHUP`、`pid=77976`、`liveness_status=unknown`、`health=ready`。`unknown` 表示当前环境不允许可靠 PID 探测，不会误报；明确 dead PID 才会降级。
- 全量验证通过：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider` -> `744 passed`。

## 45. 2026-06-18 Outcome-supported trial gate plan

- Dashboard Auto Order block 新增只读 `outcome_trial_gate_plan`，把 opportunity outcome 产生的 paper trial contracts 映射到当前 auto-order readiness gate。
- 该计划只消费现有 artifacts，不增加 IBKR 请求，不提交订单，不改 YAML，也不放宽任何 gate；每条 row 固定 `paper_only=true`、`auto_apply=false`、`submit_orders=false`、`does_not_relax_submit_gates=true`。
- 当前实际 dashboard：`outcome_trial_gate_status=BLOCKED_BY_CURRENT_GATES`、`trial_count=8`、`ready_count=0`、`blocked_count=8`。
- 当前 primary trial 是 `HK:resolved_hk_top100_bluechip / HK_POST_COST_THRESHOLD_PAPER_TRIAL`，primary blocker 是 `fresh_buy_plan_required`。
- 8 条 trial blocker 分布：`fresh_buy_plan_required=7`、`buy_plan_missing=8`、`submit_quality_not_pass=8`、`strategy_suggestion_stale=4`、`gateway_budget_degraded=2`。
- 交易含义：HK outcome evidence 支持继续准备 paper trial，但当前没有任何 trial 满足自动下单前置条件；下一步必须先刷新 stale execution artifact、产生 fresh BUY plan、通过 submit-quality 和 Gateway budget，再考虑一次小额 paper-only limit trial。

## 46. 2026-06-24 Supervisor code revision health

- 发现 6 月 24 日 runtime artifacts 已更新，但 active Supervisor 仍可能是旧进程：`auto_order_readiness.json` 缺少近期新增的 stale execution refresh plan 字段，说明长运行进程可能没有加载最新代码。
- Supervisor status payload 现在写入 `code_revision`；dashboard 会读取当前 repo `HEAD` 并比较运行中 Supervisor revision。
- Dashboard / Ops Health 新增 `supervisor_code_revision`、`dashboard_code_revision`、`supervisor_code_revision_status`。
- `match` 表示运行中 Supervisor 与 dashboard 代码一致；`missing` 表示旧进程没有上报 revision，会产生 warning；`mismatch` 表示运行中进程与当前代码不同，会产生 degraded alert。
- 当前实际刷新结果：`supervisor_shutdown_status=running`、`pid=77976`、`supervisor_code_revision_status=missing`、`supervisor_shutdown_health_status=warning`、`reason=running_code_revision_missing`。
- 交易含义：在重启 Supervisor 让其加载最新代码前，不应把缺少新 schema 字段的 auto-order artifact 当作最终自动下单证据；本步骤不新增 IBKR 请求、不提交订单、不改 YAML、不放宽任何 gate。

## 47. 2026-06-24 HK outcome-qualified trial symbols and shutdown diagnosis

- 已用当前 `market_readiness.json` 与 `reports_investment_weekly/weekly_unified_evidence.csv` 刷新 HK-only `opportunity_outcome_validation`，输出到 `runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/`。
- HK 正 post-cost candidates 仍为 `OUTCOME_SUPPORTS_GROUP`：bluechip 5d `+122.89bps`、20d `+253.84bps`；tech growth 5d `+125.19bps`、20d `+264.69bps`。
- 正 post-cost trial 现在只使用单符号 5d/20d 都为正且样本成熟的 `outcome_qualified_symbols=3988.HK,0005.HK,0939.HK,2388.HK`；剔除 `2359.HK,0992.HK`，其中 `0992.HK` 当前缺成熟 outcome，`2359.HK` 当前 20d outcome 弱。
- HK close `WAIT_PULLBACK` 仍为组级 `OUTCOME_SUPPORTS_GROUP`：bluechip 5d `+125.96bps`、20d `+212.07bps`；tech growth 5d `+126.74bps`、20d `+222.09bps`。
- close `WAIT_PULLBACK` near-entry trial 现在只使用 `outcome_qualified_symbols=3988.HK,2388.HK,1398.HK,0939.HK,0005.HK,3328.HK`；剔除 `1288.HK,2359.HK`，因为这两只的单符号 5d/20d outcome 为负。
- `review_opportunity_outcomes` 已把 trial plan 的 `candidate_symbols` 改为 outcome-qualified 子集，并保留 `source_candidate_symbols`、`outcome_qualified_symbols`、`outcome_excluded_symbols` 供审计；不会自动修改配置，不提交订单，不放宽 risk/edge/cost/liquidity/market-rule/Gateway budget/submit-quality gate。
- 主程序“自动 shutdown”的当前证据更像三类情况：第二个 Supervisor 实例因 `supervisor.lock` 被 PID `77976` 持有而退出；使用 `--once` 时设计上只跑一轮就退出；收到 `SIGINT/SIGTERM` 或未捕获异常时会写入 shutdown status 并退出。
- 当前实际运行的 Supervisor 是 `python -m src.app.supervisor`，PID `77976`，父进程为 `1`，说明它已经从原终端脱离后继续运行；最近 shutdown status 是 `running / ignored_signal:SIGHUP`，不是崩溃。
- 当前 active Supervisor 缺少 `code_revision` 字段，说明它是旧代码启动的长运行进程；建议在合适窗口优雅重启一次 Supervisor，让事件历史、code revision health、outcome-qualified trial schema 全部由主进程持续生成。

## 48. 2026-06-24 Legacy auto-order readiness fallback and current submit blocker

- Dashboard Auto Order block 现在对 legacy `auto_order_readiness.json` 做只读 fallback：如果旧 artifact 缺少 `stale_execution_refresh_plan`，dashboard 会从当前 readiness rows 重新计算 no-submit stale execution refresh ranking。
- fallback 固定继承 `paper_only=true`、`submit_orders=false`、`does_not_relax_submit_gates=true`；它只用于恢复排序可见性，不会改变 submit decision。
- 已用最新代码手动刷新 `runtime_data/paper_investment_only_duq152001/reports_supervisor/auto_order_readiness.json`，现在 artifact 自身已包含 `summary.stale_execution_refresh_plan`，dashboard 不再依赖 fallback source。
- 当前 auto-order primary block 是 `gateway_budget_degraded`，不是 outcome evidence；`stale_execution_refresh_plan.status=WAIT_GATEWAY_BUDGET`。
- 当前排序的下一次 no-submit stale execution refresh top target 是 `US:watchlist`，score `214`，target_count `6`；前几位还包括 `HK:resolved_hk_top100_bluechip`、`HK:resolved_hk_top100_tech_growth`、`US:us_overnight_core`、`ASX:asx_top_quality`、`XETRA:xetra_top_quality`。
- Dashboard 当前 `ibkr_gateway_budget_status=degraded`，最大使用率约 `784%`；这意味着现在不应提交订单，也不应触发高请求刷新，应等待 Gateway budget 恢复或继续削减高请求 broker snapshot / positions 路径。

## 49. 2026-06-24 Broker snapshot budget suppression and trade engine stop diagnosis

- 已重新验证 HK outcome：正 post-cost candidates 组级仍为 `OUTCOME_SUPPORTS_GROUP`，bluechip 5d `+122.89bps`、20d `+253.84bps`；tech growth 5d `+125.19bps`、20d `+264.69bps`。
- HK close `WAIT_PULLBACK` 组级也仍为 `OUTCOME_SUPPORTS_GROUP`，bluechip 5d `+125.96bps`、20d `+212.07bps`；tech growth 5d `+126.74bps`、20d `+222.09bps`。
- 交易边界不变：trial plan 只保留 outcome-qualified 子集；`2359.HK`、`1288.HK` 等单票 5d/20d 为负或缺样本的标的不会进入 paper trial candidate 子集。
- Gateway budget degraded 的主要高请求来源是例行 `sync_investment_broker_snapshot:*` positions/protective 查询，而不是 outcome evidence 本身。
- Supervisor 现在支持 `ibkr_gateway_budgets.suppress_broker_snapshot_when_degraded=true`；对应市场 budget status 命中 `suppress_broker_snapshot_statuses` 时，跳过例行 broker snapshot sync，并在 cycle summary 写入 `broker_snapshot_gateway_budget_degraded`。
- 该 suppression 只影响例行 broker snapshot，不影响 guard/execution 的必要安全路径，不提交订单，不放宽 risk、edge、cost、liquidity、market-rule、Gateway budget 或 submit-quality gate。
- `supervisor_cycle_summary.json/md` 新增顶层 `trade_engine` 状态；当没有 active live market 时会明确写入 `status=stopped`、`reason=no_active_live_market`，用于解释 `src.main` 主交易进程按市场时段被 Supervisor 主动停掉，而不是随机崩溃。
- 当前可见 shutdown status 仍是 `running / ignored_signal:SIGHUP`；这说明最近一次记录是终端/会话断开类信号被忽略。若看到主交易进程退出，应先检查 cycle summary 的 `trade_engine.reason`、instance lock、`SIGTERM/SIGINT` 和 exception status。
- 全量验证通过：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider` -> `754 passed`。

## 50. 2026-06-24 Growth-aware stale execution refresh ranking

- 发现 `stale_execution_refresh_plan` 之前会让高分但 `gateway_budget_degraded` 的 US 组合成为 primary target，从而把整个 no-submit refresh plan 卡成 `WAIT_GATEWAY_BUDGET`。
- `build_stale_execution_refresh_plan` 现在先按 `gateway_budget_blocked=false` 排序，再优先 `growth_refresh_candidate`，最后才按 evidence score 排序；这让可刷新且有 post-cost / close `WAIT_PULLBACK` 候选供给的组合优先。
- 每条 refresh row 新增 `ranking_bucket`、`planned_sell_order_value`、`has_current_buy_plan`、`sell_only_current_plan`、`growth_candidate_supply`，便于解释为何 SELL-only stale artifact 被降权。
- `auto_order_readiness.md` 的 stale refresh 表新增 bucket、Gateway blocked、buy/sell 列，operator 可以直接看到 primary target 是否因为 Gateway 或 SELL-only 被降权。
- 刷新只读 `auto_order_readiness` 后，当前 `stale_execution_refresh_plan.status=READY_FOR_TARGETED_NO_SUBMIT_REFRESH`，primary target 从 US 切到 `HK:resolved_hk_top100_tech_growth`，score `144`，`submit_orders=false`。
- US `US:watchlist` 仍有更高 raw score `208`，但 `gateway_budget_blocked=true` 且当前 plan 是 sell-only，因此排在 non-blocked HK / ASX / XETRA 后面；这避免把有限请求预算继续浪费在当前不可刷新目标上。
- 交易边界不变：该计划只允许单目标 report + execution no-submit evidence refresh，不提交订单，不放宽 risk、edge、cost、liquidity、market-rule、Gateway budget 或 submit-quality gate。

## 51. 2026-06-24 Stale refresh plan wired into Supervisor recovery

- 之前 `stale_execution_refresh_plan` 已经能选出 HK primary target，但 Supervisor 只消费 `recovery_plan`，导致排序结果可能停留在 dashboard/artifact，不能自动进入单目标 no-submit refresh。
- `build_auto_order_recovery_plan` 现在接收 `stale_execution_refresh_plan`；当没有 quality-passing frontier 且 stale plan 为 `READY_FOR_TARGETED_NO_SUBMIT_REFRESH` 时，会生成 `status=stale_execution_refresh_required`。
- `evaluate_auto_order_recovery_eligibility` 现在允许 `stale_execution_refresh_required` 在 `NO_ORDERS` / `NO_BUY_ORDERS` 状态下运行，因为它只用于生成 fresh report + execution dry-run evidence，不用于提交订单。
- Supervisor `_auto_order_recovery_context` 现在从同一批 readiness rows 构建 stale refresh plan 并传给 recovery plan；`_prepare_auto_order_recovery_context` 会把 eligible stale refresh 转成现有 checkpoint，避免每个 poll 周期重复跑同一目标。
- `_auto_order_recovery_action_decision` 已识别 `stale_execution_refresh_required`，只对目标组合强制 report + execution no-submit；非目标组合被 recovery scope 阻断，opportunity 仍被 suppressed。
- 刷新只读 `auto_order_readiness` 后，当前 `recovery_plan.status=stale_execution_refresh_required`，target=`HK/HK:resolved_hk_top100_tech_growth`，`recovery_eligibility.eligible=true`，allowed actions=`generate_investment_report, run_investment_execution_no_submit`。
- 安全边界不变：`submit_orders=false`、`gateway_refresh_portfolio_limit=1`、`estimated_gateway_refresh_count=1`、`does_not_relax_submit_gates=true`；不会绕过 risk、edge、cost、liquidity、market-rule、Gateway budget 或 submit-quality gate。

## 52. 2026-06-26 Account-tier submit policy context

- `build_auto_order_submit_plan` 现在可消费 `account_growth_tier_plan`，把账户规模路径直接纳入 submit candidate selection 的只读 policy context。
- 当前实现只会收紧或解释 submit policy，不会扩大配置权限：如果 account profile 的 `max_orders_per_run` 或 `max_order_value` 小于静态 auto-order policy，会使用更严格值，并保留 `configured_*` 字段用于审计。
- 候选超出账户规模限制时会新增 reject reason：`account_growth_order_count_exceeds_profile` 或 `account_growth_order_value_exceeds_profile`。
- `build_auto_order_readiness_summary` 会从 watchlist expansion summary 自动传递 `account_growth_tier_plan`；Supervisor 的 submit plan/recovery context 也使用同一份 account-tier context。
- Dashboard Auto Order block 新增 `account_growth_profile`、`account_growth_primary_action`、`account_growth_submit_frequency_mode`、`account_growth_max_orders_per_run`、`account_growth_max_order_value`，operator 不用跳转到 watchlist expansion block 也能看到小账户 submit cap。
- 小账户目标仍是 `whole_share_tradable_etf_first` / 单笔小额限价 / 先 paper fill-quality evidence；该改动不提交订单，不放宽 risk、edge、cost、liquidity、market-rule、Gateway budget 或 submit-quality gate。

## 53. 2026-06-26 Seed source-only evidence queue and shutdown check

- 已复核 HK opportunity outcome artifact：正 post-cost candidates 组级仍为 `OUTCOME_SUPPORTS_GROUP`，bluechip 5d `+122.89bps`、20d `+253.84bps`；tech growth 5d `+125.19bps`、20d `+264.69bps`。
- HK close `WAIT_PULLBACK` 也仍为 `OUTCOME_SUPPORTS_GROUP`，bluechip 5d `+125.96bps`、20d `+212.07bps`；tech growth 5d `+126.74bps`、20d `+222.09bps`。
- 当前 HK 结论不变：可以准备 paper-only HK post-cost threshold / WAIT_PULLBACK near-entry 小额限价 trial，但必须继续要求 fresh BUY plan、submit-quality PASS、Gateway budget OK、whole-share feasible、post-cost positive；不得放宽 risk/edge/cost/liquidity/market-rule gate。
- 修复 watchlist seed promotion review：source registry 里的 ETF source-only row 现在不会因为缺少 score/cost/liquidity/whole-share 字段被误判为 `QUALITY_REJECTED`。
- 新增 `candidate_report_evidence_present`，明确区分“有来源和参考价”与“已有完整 candidate report 证据”；source-only row 会进入 `CANDIDATE_REPORT_REQUIRED / run_candidate_report_for_seed`，供下次 watchlist expansion 刷新生成 seed evidence queue。
- 这会让 `BGBL.AX`、`DHHF.AX` 这类价格低于小账户单笔上限且来源可验证的 ETF 进入候选报告验证路径，而不是直接污染质量失败统计；不会自动改 watchlist、symbol master 或 submit policy。
- 主程序 shutdown 分析：当前 status 是 `running / ignored_signal:SIGHUP`，没有本地 shutdown event trail 显示崩溃；真正会退出的路径主要是 `SIGINT/SIGTERM`、未捕获异常、`--once` 正常结束、或第二个 Supervisor 因 instance lock 被占用退出。
- 如果只是 intraday trade engine 停止，应优先看 `supervisor_cycle_summary.json -> trade_engine.reason`；这通常是市场窗口关闭或交易窗口 disabled，不是 Supervisor 主进程崩溃。

## 54. 2026-06-26 Auto-order frequency plan surfaces seed quality rejection

- 已用最新代码刷新本地 `watchlist_expansion_summary.json` 和 `auto_order_readiness.json`；两个工具均为只读路径，不提交订单、不改配置、不放宽任何 gate。
- 当前 seed expansion 的真实状态不是“等待跑 candidate report”，而是已有 ASX seed review evidence：`DHHF.AX`、`BGBL.AX` 等 8 个 seed 全部为 `QUALITY_REJECTED`，`seed_evidence_queue_count=0`。
- `build_auto_order_frequency_plan` 新增 `seed_evidence_queue_ready` 状态：当存在 READY seed evidence job 时，primary action 会明确为 `run_seed_candidate_evidence_review`。
- 同时新增 `seed_evidence_quality_rejected` 状态：当 seed evidence 已跑完但全部被质量门拒绝时，primary action 为 `source_higher_quality_lower_cost_seed_candidates`。
- 当前刷新后的 auto-order frequency plan：`status=seed_evidence_quality_rejected`、`reason=seed_candidate_quality_rejected:expected_edge_below_min`、`seed_promotion_quality_rejected_count=8`。
- 当前 seed quality reason 分布：`expected_edge_below_min=8`、`score_below_min=8`、`whole_share_edge_margin_below_min=8`、`whole_share_not_tradable=8`、`liquidity_below_min=5`、`expected_cost_above_max=4`。
- Dashboard v2 Auto Order block 和 `review_auto_order_readiness.md` 现在会展示 seed evidence queue / quality rejection 指标；legacy fallback 只补缺失字段，不覆盖新版 frequency source metrics。
- 交易含义：下一步扩池不应降低风险门或 submit gate，而应替换/新增更强的低价、整股可交易 ETF 或大盘股来源；自动提交仍必须先解决 `weekly_review_stale`、stale execution artifact、Gateway budget、fresh BUY plan 和 submit-quality。

## 55. 2026-06-29 HK outcome revalidation and Supervisor cycle resilience

- 已用当前 `market_readiness.json` 与 `reports_investment_weekly/weekly_unified_evidence.csv` 重新刷新 HK-only `opportunity_outcome_validation`，输出到 `runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/`。
- 当前 HK 正 post-cost candidates 已从 6/24 的组级正面证据转为 `OUTCOME_WEAK_OR_MIXED`：两个 HK portfolio 的当前正 post-cost 符号都只剩 `2359.HK,0005.HK`，5d 仍为正，但 20d 平均为负。
- 具体结果：bluechip 5d `+65.87bps`、20d `-129.20bps`；tech growth 5d `+72.54bps`、20d `-127.94bps`。`2359.HK` 是主要负贡献，单符号 20d 约 `-650bps` 至 `-681bps`。
- 当前 HK close `WAIT_PULLBACK` 仍为组级 `OUTCOME_SUPPORTS_GROUP`：bluechip 5d `+125.96bps`、20d `+212.07bps`；tech growth 5d `+126.74bps`、20d `+222.09bps`。
- WAIT_PULLBACK 的可交易解释必须保持 symbol-aware：`3988.HK`、`2388.HK`、`1398.HK`、`0939.HK`、`0005.HK`、`3328.HK` 支撑较好；`1288.HK` 和 `2359.HK` 的 5d/20d 都为负，不应进入 near-entry trial 子集。
- 交易含义：本次不支持扩大 HK post-cost threshold trial；支持继续保留 close `WAIT_PULLBACK` 的 paper-only 小额限价 near-entry 研究，但仍必须要求 fresh BUY plan、post-cost positive、submit-quality PASS、Gateway budget OK、whole-share feasible，不放宽 risk/edge/cost/liquidity/market-rule gate。
- Supervisor 主进程当前 PID `77976` 仍存活，且由 2026-06-16 启动的旧长进程持有 `supervisor.lock`；这解释了为什么 shutdown status 仍缺 `code_revision` / event history，最新代码没有被长进程加载。
- “自动 shutdown”主要有两类：`src.main` 子交易进程会在无 active live market 或 trading window disabled 时被 Supervisor 主动停止；Supervisor 顶层进程则会在 `--once`、重复实例 lock、SIGINT/SIGTERM 或未捕获异常时退出。
- Supervisor 现在新增 cycle heartbeat 与 transient cycle error tolerance：每轮成功后刷新 `supervisor_shutdown_status.json`，但不刷爆 event history；单轮 `run_cycle()` 异常会记录 `running_degraded` 并继续，连续失败达到 `max_consecutive_cycle_errors_before_shutdown` 才升级为 `crashed`。
- `config/supervisor.yaml` 新增 `max_consecutive_cycle_errors_before_shutdown: 3`。该改动只提升可用性和诊断，不提交订单，不改变 submit policy，不放宽任何交易门。

## 56. 2026-06-29 Auto-order submit gate blocks stale Supervisor code

- `auto_order_readiness` 现在读取 `supervisor_shutdown_status.json`，并把运行中 Supervisor 的 `code_revision` 与当前 repo `HEAD` 比较。
- Supervisor status 的 `code_revision` 表示进程启动时加载的代码版本；`current_code_revision` 表示写 status 时的当前 repo `HEAD`，用于审计 repo 更新后旧进程是否仍在运行。
- 只有 `status=running/running_degraded` 的 Supervisor 会触发该 gate；缺少 status artifact 的离线 CLI 分析不会被误阻断。
- 新增 hard block：`supervisor_code_revision_missing` 和 `supervisor_code_revision_mismatch`。默认配置 `block_on_supervisor_code_revision_mismatch: true`。
- Supervisor 内部 submit gate 和 `review_auto_order_readiness` CLI 都使用同一逻辑；dashboard Auto Order block 新增 `supervisor_revision_block_count`、`supervisor_code_revision_missing_count`、`supervisor_code_revision_mismatch_count`。
- 用当前真实 runtime 只读刷新到 `/private/tmp/ibkr_auto_order_revision_gate/` 验证：active PID `77976` 仍是旧长进程且未上报 `code_revision`，因此 `primary_block_reason=supervisor_code_revision_missing`，6 个可提交组合全部被阻断。
- 当前仍有其他阻塞：`weekly_review_stale=6`、`market_readiness_not_ready=6`、US `gateway_budget_degraded=2`、HK `strategy_suggestion_stale=2`。但在这些解除之前，旧 Supervisor 也不能自动 submit。
- 交易含义：这不会降低下单频率；它防止“代码已更新但旧长进程仍在用旧 submit gate”导致错误提交。下一步应在合适窗口优雅重启 Supervisor，然后刷新 weekly review / market readiness / no-submit execution evidence。

## 57. 2026-07-02 Auto-order recovery waits for current Supervisor runtime

- 当前真实 runtime 仍由 PID `77976` 持有 `supervisor.lock`，该长进程从 2026-06-16 启动，`supervisor_shutdown_status.json` 仍缺 `code_revision/current_code_revision`。
- 用当前代码只读刷新 `review_auto_order_readiness` 到 `/private/tmp/ibkr_auto_order_readiness_after_runtime_gate/` 后，`primary_block_reason=supervisor_code_revision_missing`。
- 之前 submit 已会被 stale Supervisor revision 阻断，但 recovery plan 仍可能给出 `stale_execution_refresh_required` 且 `recovery_eligibility.eligible=true`，这会让旧运行态下继续尝试 report + execution no-submit refresh。
- `build_auto_order_recovery_plan` 现在接收 summary 级 `global_hard_blocks`；只要存在 `supervisor_code_revision_missing` 或 `supervisor_code_revision_mismatch`，recovery plan 优先变成 `runtime_restart_required`。
- `runtime_restart_required` 的唯一 step 是 `restart_supervisor_current_code`，`requires_ibkr_gateway=false`、`gateway_refresh_portfolio_limit=0`、`estimated_gateway_refresh_count=0`、`submit_orders=false`。
- `evaluate_auto_order_recovery_eligibility` 对该状态返回 `active=true`、`eligible=false`、`reason=supervisor_runtime_restart_required`、`allowed_actions=[]`。
- 这使“旧 Supervisor / 代码版本未知”成为所有 Gateway-backed recovery refresh 前的硬前置条件，避免恢复流程继续消耗 IBKR 请求预算或影响自动下单路径。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py` -> `62 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `772 passed`。

## 58. 2026-07-02 Dashboard auto-order block overlays Supervisor runtime restart gate

- 发现实际 `auto_order_readiness.json` 仍由旧 Supervisor 生成，artifact 内的 recovery plan 可能仍是 legacy `manual_review_required` / stale refresh，而 dashboard `ops_overview` 已经能识别 `supervisor_code_revision_status=missing/mismatch`。
- `build_auto_order_readiness_block` 现在会读取 `ops_overview.supervisor_shutdown_status` 与 `supervisor_code_revision_status`。
- 当 Supervisor 正在运行且 code revision 为 `missing` 或 `mismatch` 时，Auto Order block 会补入对应 revision hard block count，并覆盖 legacy recovery 展示为 `runtime_restart_required`。
- Dashboard 指标现在会显示 `recovery_plan_status=runtime_restart_required`、`recovery_plan_primary_action=restart_supervisor_current_code`、`recovery_eligibility_eligible=0`、`recovery_eligibility_reason=supervisor_runtime_restart_required`。
- 这让 operator 即使只看 dashboard v2，也能先看到“重启当前代码”这个 no-IBKR 前置动作，而不是误以为下一步应该继续 stale execution refresh。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_dashboard_blocks.py tests/test_auto_order_readiness.py` -> `76 passed`。

## 59. 2026-07-02 Supervisor runtime status review CLI

- 新增只读 `src.common.supervisor_runtime_status` 与 `python -m src.tools.review_supervisor_runtime`，用于把 `supervisor.lock`、`supervisor_shutdown_status.json`、当前 git revision 和 PID liveness 组合成 `supervisor_runtime_status.json/md`。
- 该工具不启动/停止 Supervisor、不连接 IBKR、不刷新 report/opportunity/execution、不提交订单；输出显式包含 `submit_orders=false` 与 `does_not_relax_submit_gates=true`。
- 当前真实 runtime review 写到 `/private/tmp/ibkr_supervisor_runtime_status/`：`supervisor_status=running`、`supervisor_reason=ignored_signal:SIGHUP`、`supervisor_code_revision_status=missing`、`health_status=warning`、`restart_required=true`、`blocks_recovery_refresh=true`、`next_action=restart_supervisor_current_code`。
- `SIGHUP` 不是自动 shutdown 原因；Supervisor 代码中该信号被记录为 ignored 并继续运行。真正会退出的是 SIGINT/SIGTERM、KeyboardInterrupt，或连续 `run_cycle()` 异常达到 `max_consecutive_cycle_errors_before_shutdown`。
- 当前 shell `ps -o pid,ppid,etime,stat,command -p 77976` 确认 PID `77976` 仍在运行；sandbox 内直接 `os.kill(pid, 0)` 和未授权 `ps` subprocess 可能被拒绝，因此 artifact 可保守显示 `supervisor_liveness_status=unknown`。
- 同轮重新生成 HK outcome validation 到 `/private/tmp/ibkr_hk_opportunity_outcomes/`：HK positive post-cost candidates 仍为 `OUTCOME_WEAK_OR_MIXED`，bluechip `5d=65.87bps/20d=-129.20bps`，tech growth `5d=72.54bps/20d=-127.94bps`。
- HK close `WAIT_PULLBACK` 仍为 `OUTCOME_SUPPORTS_GROUP`，bluechip `5d=125.96bps/20d=212.07bps`，tech growth `5d=126.74bps/20d=222.09bps`；trial-qualified symbols 为 `3988.HK,2388.HK,1398.HK,0939.HK,0005.HK,3328.HK`，`1288.HK,2359.HK` 被排除。
- 交易含义：不支持扩大 HK post-cost threshold trial；只支持在 Supervisor 重启到当前代码并刷新 fresh BUY/no-submit execution evidence 后，继续评估严格 paper-only near-entry limit trial。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_runtime_status.py tests/test_review_opportunity_outcomes.py` -> `8 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `777 passed`。

## 60. 2026-07-02 Dashboard reuses Supervisor runtime contract

- `src.common.supervisor_runtime_status` 新增 `build_supervisor_runtime_status_from_payloads`，让 dashboard 已加载的 `supervisor.lock` / `supervisor_shutdown_status.json` 与 CLI 使用同一 runtime contract。
- `generate_dashboard.py` 现在会输出顶层 `supervisor_runtime_status`，同时 `ops_overview` 继续保留既有 `supervisor_shutdown_*` / `supervisor_code_revision_*` 字段，避免破坏旧 dashboard consumer。
- v2 `Ops Health` block 新增 `supervisor_runtime_next_action`、`supervisor_runtime_restart_required`、`supervisor_runtime_blocks_recovery_refresh`、`supervisor_runtime_request_policy`。
- 用真实 runtime summary 构建 dashboard 验证：顶层、ops overview 和 Ops Health block 都显示 `next_action=restart_supervisor_current_code`、`blocks_recovery_refresh=true`、`submit_orders=false`。
- 交易含义：dashboard、CLI、auto-order recovery gate 现在对旧 Supervisor/缺 code revision 的下一步判断一致；仍然不连接 IBKR、不刷新 Gateway-backed evidence、不提交订单、不放宽任何 submit gate。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_runtime_status.py tests/test_dashboard_shutdown_history.py tests/test_dashboard_blocks.py tests/test_generate_dashboard_helpers.py` -> `66 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `778 passed`。

## 61. 2026-07-02 Auto-order readiness reuses Supervisor runtime contract

- `auto_order_readiness` 的 Supervisor revision gate 现在复用 `build_supervisor_runtime_status_from_payloads`，不再单独维护一套 running/missing/mismatch 判断。
- 既有 submit hard block 保持兼容：运行中的 Supervisor 缺 `code_revision` 仍为 `supervisor_code_revision_missing`，版本不一致仍为 `supervisor_code_revision_mismatch`，非运行态不会因为 revision 产生 block。
- 每个 auto-order readiness row 新增 `supervisor_runtime_next_action`、`supervisor_runtime_restart_required`、`supervisor_runtime_blocks_recovery_refresh`、`supervisor_runtime_request_policy`、`supervisor_runtime_health_status`。
- READY / missing / mismatch 三类测试已锁定 runtime action：READY 指向 `continue_monitoring_supervisor_runtime`；missing/mismatch 指向 `restart_supervisor_current_code` 且 request policy 为 `no_ibkr_requests_until_supervisor_runtime_current`。
- 交易含义：auto-order CLI、dashboard、runtime review、recovery plan 对旧 Supervisor 的下一步判断现在完全一致；本次不新增 stale-lock/liveness submit block，不连接 IBKR，不刷新 evidence，不提交订单，不放宽任何 risk/edge/quality gate。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_supervisor_runtime_status.py` -> `67 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `778 passed`。

## 62. 2026-07-02 Auto-order readiness emits a single next-unblock plan

- `build_auto_order_readiness_summary` 新增 `unblock_plan`，把 submit plan、recovery plan、frequency plan、remediation plan 合成为单一“下一步先做什么”的只读 contract。
- `unblock_plan` 输出 `status`、`primary_action`、`phase`、`source`、`requires_ibkr_gateway`、`submit_orders`、target、request policy，并固定 `does_not_change_submit_decision=true`。
- 当 Supervisor runtime 过旧或缺 revision 时，unblock plan 指向 `runtime_restart_required / restart_supervisor_current_code`，且 `requires_ibkr_gateway=false`。
- 当 primary blocker 是 `weekly_review_stale` / `weekly_review_missing` 时，unblock plan 指向 `local_weekly_review_required / refresh_weekly_review`，避免把本地周报 stale 误归入 Gateway-backed refresh。
- `review_auto_order_readiness.md` 新增 `Next Unblock Plan` section；dashboard v2 Auto Order block 新增 unblock plan metrics，并能从旧 auto-order artifact fallback 构建该 plan。
- 交易含义：系统现在能在 JSON/Markdown/dashboard 上一致显示“先本地解阻塞、再 Gateway dry-run、最后才考虑 paper submit”的顺序；本次不自动执行、不连接 IBKR、不提交订单、不放宽任何 risk/edge/cost/liquidity/market-rule/Gateway/submit-quality gate。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_dashboard_blocks.py` -> `77 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `779 passed`。

## 63. 2026-07-03 Supervisor recovery context consumes the same unblock plan

- `_auto_order_recovery_context` 现在复用 `build_auto_order_readiness_summary`，不再手写 submit plan、stale execution refresh plan、recovery plan 的组合逻辑。
- recovery context 现在带出 `unblock_plan` 和完整 `summary`，Supervisor 执行路径、CLI JSON/Markdown、dashboard v2 看到的是同一套 next-unblock contract。
- 该路径继承 summary 级 global hard blocks：如果 Supervisor code revision missing/mismatch，context 的 recovery plan 直接是 `runtime_restart_required`，eligibility 为 `supervisor_runtime_restart_required`。
- 如果 primary blocker 是 weekly review stale/missing，context 的 unblock plan 指向 `local_weekly_review_required / refresh_weekly_review`，不会创建 Gateway-backed recovery checkpoint。
- 交易含义：恢复流程更难误消耗 IBKR Gateway 请求预算，也更难和 dashboard 展示分叉；本次不执行恢复、不连接 IBKR、不提交订单、不放宽任何交易门。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_recovery_context_uses_summary_runtime_restart_unblock_plan tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_recovery_context_routes_weekly_stale_to_local_unblock tests/test_auto_order_readiness.py` -> `65 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_dashboard_blocks.py tests/test_supervisor_cli.py` -> `146 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `781 passed`。

## 64. 2026-07-03 Supervisor auto-order summary context removes duplicate plan composition

- Supervisor 新增 `_auto_order_readiness_summary_context`，统一生成 rows、summary、recovery eligibility，并按需附加 execution evidence maintenance state。
- `_auto_order_submit_plan` 现在直接返回 shared summary 中的 `submit_plan`，不再单独调用 submit-plan builder。
- `_auto_order_recovery_context` 和 `_write_auto_order_readiness_summary` 也复用同一 helper，减少 submit/recovery/artifact 三条路径对 auto-order inputs 的重复组装。
- 移除了 Supervisor 对 `build_auto_order_submit_plan` 的直接依赖；submit plan 仍由 common summary builder 生成，行为保持兼容。
- 交易含义：自动下单、恢复和 dashboard artifact 更不容易在未来改动后分叉；本次不连接 IBKR、不提交订单、不改变 candidate selection、不放宽任何 risk/edge/cost/liquidity/market-rule/Gateway/submit-quality gate。
- 验证：`python -m py_compile src/app/supervisor.py`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_auto_order_readiness.py` -> `195 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `781 passed`。

## 65. 2026-07-03 Supervisor reuses submit plan within one cycle

- `_auto_order_submit_plan_allows_item` 新增可选 `submit_plan` 参数；旧调用不传参数仍会自行计算，保持兼容。
- `run_cycle` 现在在第一次需要 submit-plan gate 时懒加载 `auto_order_submit_plan_cache`，同一 cycle 内后续 execution item 复用同一份 plan。
- 显式传入空/blocked plan 时不会回退重算，避免测试或运行路径中出现“传入 plan 被忽略”的隐性分叉。
- 交易含义：多个市场/组合在同一 Supervisor cycle 内使用一致的 submit-plan 决策，减少重复读取 artifacts 和重复构建 summary；本次不连接 IBKR、不提交订单、不改变候选、不放宽任何 risk/edge/cost/liquidity/market-rule/Gateway/submit-quality gate。
- 验证：`python -m py_compile src/app/supervisor.py`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_submit_plan_allows_only_selected_portfolio tests/test_supervisor_cli.py::SupervisorCliTests::test_targeted_recovery_force_run_bypasses_schedule_and_previous_dry_run` -> `2 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_auto_order_readiness.py` -> `195 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `781 passed`。

## 66. 2026-07-03 Supervisor reuses readiness rows within one cycle

- `_auto_order_recovery_context` 现在返回本 cycle 已生成的 auto-order readiness `rows`。
- `_auto_order_readiness_for_item` 新增可选 `readiness_rows` 参数；传入缓存时按 normalized market + portfolio id 匹配，命中则直接返回 cached row，不再读取 artifacts 和重算 readiness。
- `run_cycle` 在 per-item execution submit readiness gate 中传入 recovery context 的 rows；找不到匹配时仍回退到旧的 direct evaluation 路径。
- 交易含义：同一 Supervisor cycle 内 readiness、submit plan、recovery context 使用同一份 readiness snapshot，减少重复本地 artifact 读取和分叉风险；本次不连接 IBKR、不提交订单、不改变候选、不放宽任何 risk/edge/cost/liquidity/market-rule/Gateway/submit-quality gate。
- 验证：`python -m py_compile src/app/supervisor.py`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_readiness_for_item_uses_cycle_cached_row tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_submit_plan_allows_only_selected_portfolio` -> `2 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_auto_order_readiness.py` -> `196 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `782 passed`。

## 67. 2026-07-03 Supervisor initializes submit-plan cache from recovery context

- 新增 `_auto_order_submit_plan_from_recovery_context`，从 `recovery_context.summary.submit_plan` 提取 submit plan 副本。
- `run_cycle` 的 `auto_order_submit_plan_cache` 现在优先使用 recovery context 已生成的 submit plan；只有 recovery context 缺失或无 submit plan 时，才在首次 submit gate 走原来的 lazy fallback。
- 这使 readiness rows、submit plan、recovery plan、unblock plan 在同一 cycle 内都可来自同一份 auto-order summary snapshot。
- 交易含义：进一步减少同 cycle 内重复读取 artifacts / 重建 summary 的机会，也降低 submit gate 与 recovery/dashboard 视图分叉风险；本次不连接 IBKR、不提交订单、不改变候选、不放宽任何 risk/edge/cost/liquidity/market-rule/Gateway/submit-quality gate。
- 验证：`python -m py_compile src/app/supervisor.py`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_cycle_cache_from_recovery_context_returns_copies tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_submit_plan_allows_only_selected_portfolio tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_readiness_for_item_uses_cycle_cached_row` -> `3 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_auto_order_readiness.py` -> `197 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `783 passed`。

## 68. 2026-07-03 Supervisor refreshes auto-order caches after same-cycle reports

- 新增 `_auto_order_cycle_cache_from_context`，从 auto-order summary context 同时提取 submit plan 和 readiness rows 副本；`_auto_order_submit_plan_from_recovery_context` 复用该 helper。
- `run_cycle` 仍优先从 recovery context 初始化 cache，但当 `_generate_reports` 后有任一 report 切换到当前交易日时，会重建 auto-order summary context 并刷新 `auto_order_submit_plan_cache` / `auto_order_readiness_rows_cache`。
- 如果 report 后 cache refresh 失败，系统清空缓存并回退到直接 per-item evaluation，避免使用过期 pre-report cache。
- 交易含义：同一 cycle 中“先生成 report，再尝试 execution submit”时，submit gate 不会继续使用 report 前的 readiness/submit-plan 快照；本次不连接 IBKR、不提交订单、不改变候选、不放宽任何 risk/edge/cost/liquidity/market-rule/Gateway/submit-quality gate。
- 验证：`python -m py_compile src/app/supervisor.py`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_cycle_cache_from_recovery_context_returns_copies tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_readiness_for_item_uses_cycle_cached_row` -> `2 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py tests/test_auto_order_readiness.py` -> `197 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `783 passed`。

## 69. 2026-07-03 HK outcome and Supervisor shutdown diagnosis refresh

- 重新用最新运行时 `runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json` 和 `reports_investment_weekly/weekly_unified_evidence.csv` 运行 HK-only `review_opportunity_outcomes`。
- 输出写入 `runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/`，避免覆盖全市场 validation artifact。
- HK positive post-cost candidates 当前不是整体放宽依据：bluechip `0005.HK,2359.HK` 为 `OUTCOME_WEAK_OR_MIXED`，`5d=+65.87bps`、`20d=-129.20bps`；tech growth `0005.HK,2359.HK` 为 `OUTCOME_WEAK_OR_MIXED`，`5d=+72.54bps`、`20d=-127.94bps`。
- 弱点集中在 `2359.HK`：bluechip `5d=-127.86bps/20d=-681.37bps`，tech growth `5d=-83.17bps/20d=-647.84bps`；`0005.HK` 单独是正 5d 且接近/略正 20d，但不足以支持整组 post-cost threshold 扩大。
- HK close `WAIT_PULLBACK` 仍支持严格 paper-only near-entry limit trial 复核：bluechip `5d=+125.96bps/20d=+212.07bps`，tech growth `5d=+126.74bps/20d=+222.09bps`。
- trial-qualified symbols 为 `3988.HK,2388.HK,1398.HK,0939.HK,0005.HK,3328.HK`；`1288.HK,2359.HK` 因 5/20d outcome 为负继续排除。
- Supervisor shutdown 判因：当前 dashboard runtime contract 显示 `supervisor_status=running`、PID `77976`、`liveness=alive`，但 `supervisor_code_revision_status=missing`，说明旧 Supervisor 长进程仍在运行且需要重启到当前代码；这不是 crash。
- 真正会让 Supervisor 顶层退出的路径仍是 `--once` 正常一轮退出、重复实例因 `supervisor.lock` 退出、SIGINT/SIGTERM/KeyboardInterrupt、或连续 `run_cycle()` 异常达到 `max_consecutive_cycle_errors_before_shutdown`。交易子进程 `src.main` 在无 active live market 时被 Supervisor 停止是设计行为，不等于 Supervisor 自动 shutdown。
- 当前自动下单主要阻塞仍是 `weekly_review_stale`、`market_readiness_not_ready`、HK `strategy_suggestion_stale` 和旧 Supervisor revision gate；不是 HK outcome 证据本身。
- 验证命令：`python -m src.tools.review_opportunity_outcomes --market HK --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence reports_investment_weekly/weekly_unified_evidence.csv --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation`。

## 70. 2026-07-07 Auto-order readiness flags legacy missing unblock plan

- 当前 runtime 中旧 Supervisor 进程仍可能覆盖 `auto_order_readiness.json`，导致 `summary.unblock_plan=null`；直接用当前 CLI 构建 payload 时能生成正确 `runtime_restart_required / restart_supervisor_current_code`。
- `_build_auto_order_readiness_health` 现在检查 `summary.unblock_plan` 是否缺失；如果 artifact 新鲜但 contract 缺失，会标记 `status=warning`、`reason=missing_unblock_plan`。
- Dashboard v2 Auto Order block 新增 `readiness_missing_unblock_plan` metric，让旧 schema/旧进程写出的 auto-order evidence 在 dashboard 上可见，而不是被误认为完全 fresh。
- `_write_auto_order_readiness_summary` 的测试现在锁住 runtime writer 必须输出非空 `summary.unblock_plan`，并且 `submit_orders=false`、`does_not_change_submit_decision=true`。
- 交易含义：旧 Supervisor 或旧 schema 不会再静默影响自动下单恢复顺序；operator 能先看到 runtime restart/schema contract 问题，再处理 weekly review stale、market readiness stale 和 no-BUY execution evidence。本次不连接 IBKR、不提交订单、不放宽任何 gate。
- 验证：`python -m py_compile src/tools/generate_dashboard.py src/tools/dashboard_blocks.py src/app/supervisor.py`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_generate_dashboard_helpers.py::test_auto_order_readiness_health_warns_when_unblock_plan_missing tests/test_dashboard_blocks.py::test_auto_order_readiness_block_surfaces_missing_unblock_plan_health tests/test_supervisor_cli.py::SupervisorCliTests::test_write_auto_order_readiness_summary_uses_summary_out_dir` -> `3 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py tests/test_auto_order_readiness.py tests/test_supervisor_cli.py::SupervisorCliTests::test_write_auto_order_readiness_summary_uses_summary_out_dir` -> `126 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider --maxfail=1 -x` -> `785 passed`。

## 71. 2026-07-07 Supervisor stale heartbeat and HK outcome revalidation

- 重新本地运行 HK-only `review_opportunity_outcomes` 到 `/private/tmp/hk_opportunity_outcome_validation_20260707/`，确认 HK positive post-cost 组仍为 mixed/weak：bluechip `5d=+65.87bps/20d=-129.20bps`，tech growth `5d=+72.54bps/20d=-127.94bps`。
- `2359.HK` 仍是主要拖累：bluechip `5d=-127.86bps/20d=-681.37bps`，tech growth `5d=-83.17bps/20d=-647.84bps`；`0005.HK` 单独较好，但不足以支持 HK post-cost threshold 整体放宽。
- HK close `WAIT_PULLBACK` 组仍支持严格 paper-only near-entry limit trial 方向：bluechip `5d=+125.96bps/20d=+212.07bps`，tech growth `5d=+126.74bps/20d=+222.09bps`。
- trial-qualified symbols 仍是 `3988.HK,2388.HK,1398.HK,0939.HK,0005.HK,3328.HK`；`1288.HK,2359.HK` 因 5/20d outcome 为负继续排除。
- `src/common/supervisor_runtime_status.py` 新增 Supervisor heartbeat 诊断：运行态 status 如果 `written_at` 超过 6 小时未刷新，会输出 `supervisor_heartbeat_status=stale`、`restart_required=true`、`blocks_recovery_refresh=true`、`next_action=restart_stale_supervisor_heartbeat_current_code`。
- `src/tools/generate_dashboard.py` 顶部 ops health 现在把这类进程显示为 `Supervisor 心跳过期`，避免 PID 仍存在但主循环不刷新时被误判为健康运行。
- 当前实测 PID `77976` 仍存在，但 shutdown status 最后写入时间是 `2026-06-17T08:41:20Z`，诊断为 stale heartbeat；这不同于 crash，也不同于无活动市场窗口下 `src.main` 子交易进程被设计性停止。
- 交易含义：HK outcome 证据不支持放宽 HK post-cost/cost 门；下一步仍应先重启 Supervisor 到当前代码、刷新 weekly/market readiness，再只对合格 close `WAIT_PULLBACK` 符号评估严格 paper-only 小额限价试单。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_runtime_status.py tests/test_generate_dashboard_helpers.py::test_build_ops_overview_degrades_stale_running_supervisor_heartbeat` -> `7 passed`；`PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes --market HK --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence reports_investment_weekly/weekly_unified_evidence.csv --out_dir /private/tmp/hk_opportunity_outcome_validation_20260707`；`python -m src.tools.review_supervisor_runtime --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir /private/tmp/ibkr_supervisor_runtime_status_20260707`。

## 72. 2026-07-07 Supervisor runtime recovery dry-run tool

- 新增 `src/common/supervisor_runtime_recovery.py`，把 stale Supervisor recovery 从人工 PID 判断推进为可测试、可审计的 recovery plan。
- 新增 `src/tools/recover_supervisor_runtime.py` 和 console script `ibkr-quant-supervisor-recovery`；默认 dry-run，只写 JSON/Markdown plan。
- 工具只允许已知 runtime restart action，且必须确认目标进程命令包含 `python -m src.app.supervisor`，否则拒绝终止 PID；空命令行证据现在明确标记为 `supervisor_process_command_unavailable`，不同于真实 command mismatch。
- `--apply` 才会执行 SIGTERM 或删除 stale lock；`--start` 是单独显式开关，因为启动 Supervisor 会恢复正常 scheduler 行为，可能触发配置允许的 IBKR 请求。
- 当前真实 runtime 的提升权限 dry-run 写入 `/private/tmp/ibkr_supervisor_recovery_20260707/`，结果为 `status=ready`、`reason=restart_stale_supervisor_heartbeat_current_code`、`terminate_pid=77976`、`applied=false`、`submit_orders=false`、`connects_to_ibkr=false`。
- 交易含义：后续恢复可以先生成计划，再显式 apply，避免旧 Supervisor 心跳过期继续阻塞 weekly/market readiness 刷新；本次没有终止进程、没有启动 Supervisor、没有连接 IBKR、没有提交订单。
- 验证：`python -m py_compile src/common/supervisor_runtime_recovery.py src/tools/recover_supervisor_runtime.py`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_runtime_recovery.py tests/test_supervisor_runtime_status.py tests/test_project_packaging.py` -> `14 passed`。

## 73. 2026-07-07 Weekly review query bounds and HK outcome refresh

- 重新用当前 `runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json` 和现有 `reports_investment_weekly/weekly_unified_evidence.csv` 刷新 HK-only outcome validation，输出到 `runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/`。
- 当前 HK `positive_post_cost_candidates` 不支持放宽 gate：bluechip `0005.HK,2359.HK` 为 `OUTCOME_WEAK_OR_MIXED`，`5d=+65.87bps/20d=-129.20bps`；tech growth `0005.HK,2359.HK` 为 `OUTCOME_WEAK_OR_MIXED`，`5d=+72.54bps/20d=-127.94bps`。
- 当前 HK `close_wait_pullback` 候选已收窄到 `0002.HK`，也是 mixed：bluechip `5d=-95.57bps/20d=+146.91bps`，tech growth `5d=-82.04bps/20d=+215.51bps`。因此本轮不应基于 HK outcome 放宽 WAIT_PULLBACK anchor，只应继续收集证据。
- Supervisor 当前状态文件显示 `status=running`、`reason=cycle_complete`、PID `54465`、`consecutive_cycle_error_count=0`；当前没有自动 shutdown。
- 主程序“自动 shutdown”真实路径仍是 `--once` 正常退出、重复 instance lock、SIGINT/SIGTERM/KeyboardInterrupt、或连续 `run_cycle()` 异常达到 `max_consecutive_cycle_errors_before_shutdown`。本轮看到的核心问题是 weekly review 子进程在 37GB `audit.db` 上被系统以 exit `137` 杀掉，造成 weekly evidence stale 和 submit readiness block。
- `src/tools/review_investment_weekly.py` 增加 `--feedback_calibration_lookback_days`、`--position_lookback_days` 和 `_weekly_audit_where()`，并给 `fills`、`risk_events`、`investment_positions`、candidate snapshot/outcome 查询加时间窗、portfolio scope 和缺列空结果保护。
- `src/app/supervisor.py` 现在从 config 传入 weekly review 的 calibration/position lookback；`config/supervisor.yaml` 的 paper runtime 设为 `weekly_review_feedback_calibration_lookback_days: 45` 和 `weekly_review_position_lookback_days: 45`。
- `src/common/storage.py` 新增 weekly/evidence 查询索引定义：fills、risk_events、investment_positions、investment_trades、investment_candidate_snapshots、investment_candidate_outcomes。
- 注意：现有 37GB `audit.db` 还没有实际建出新索引，即使用 45 天 calibration 窗口重跑 weekly review 仍以 exit `137` 结束；第一次受控迁移/重启可能 IO 较重，应放在非活跃交易窗口执行。在此之前，HK outcome 验证继续用 CSV streaming 路径。
- 归档：`docs/change_archive_2026-07-07_weekly_review_query_bounds_hk_outcome.md`。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_investment_weekly.py -k weekly_audit_where` -> `3 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_storage_sqlite_locking.py` -> `3 passed`；`PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/review_investment_weekly.py src/app/supervisor.py src/common/storage.py`；`PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes --market HK --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence reports_investment_weekly/weekly_unified_evidence.csv --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation`。

## 74. 2026-07-11 Bounded weekly review and explicit evidence index maintenance

- 新增 `src/common/evidence_index_maintenance.py` 和 `src/tools/maintain_evidence_indexes.py`，把 weekly/evidence 索引维护从 `Storage._init_db()` 的隐式启动迁移改成显式 dry-run/apply 工具。
- 新增 console script `ibkr-quant-evidence-indexes`；默认只检查并写 `evidence_index_maintenance.json/md`，只有 `--apply` 才实际创建缺失索引。
- `Storage` 不再隐式创建 `idx_fills_weekly_lookup`、`idx_risk_events_weekly_lookup`、`idx_investment_positions_weekly_lookup`、`idx_investment_trades_weekly_lookup`、`idx_investment_candidate_snapshots_weekly_lookup`、`idx_investment_candidate_outcomes_weekly_lookup`，避免 Supervisor 重启时对 37GB `audit.db` 做不可见长迁移。
- `review_investment_weekly` 新增 `--candidate_snapshot_limit` / `--candidate_outcome_limit`；paper Supervisor 配置为 `20000` snapshots、`60000` outcomes。
- `review_weekly_execution_support._enrich_snapshot_rows` 现在先按 execution order 需要的 `(report_dir, portfolio_id, symbol)` 或 outcome `snapshot_id` 过滤，再解析 `details` JSON，避免全量 snapshot JSON 反序列化。
- 真实运行库 dry-run：`evidence-index-maintenance: status=ready missing=0 ready=6 applied=False`，说明当前 37GB runtime DB 已具备所需索引。
- bounded weekly review 实测完成：runtime weekly out_dir 约 16 秒；配置使用的根目录 `reports_investment_weekly` 约 10 秒。随后 `review_auto_order_readiness` 的 `primary_block_reason` 从 `weekly_review_stale` 推进为 `market_readiness_not_ready`。
- 当前下一步 recovery 是单一目标 no-submit refresh：`US:watchlist`，`submit_orders=false`，原因 `STALE_EXECUTION_ARTIFACT`；本轮没有下单、没有放宽任何 risk/edge/cost/liquidity/market-rule/Gateway/submit-quality gate。
- 归档：`docs/change_archive_2026-07-11_bounded_weekly_review_and_index_maintenance.md`。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_evidence_index_maintenance.py tests/test_storage_sqlite_locking.py tests/test_review_weekly_execution_support.py` -> `8 passed`；`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_investment_weekly.py` -> `53 passed`；`PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_investment_weekly ... --candidate_snapshot_limit 20000 --candidate_outcome_limit 60000` 完成；`PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_auto_order_readiness --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor` 完成；`PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor` 完成。

## 75. 2026-07-11 HK outcome DB backfill and Supervisor shutdown diagnosis

- `review_opportunity_outcomes` 新增可选 `--db` 补源：当 bounded `weekly_unified_evidence.csv` 对目标 market/portfolio/symbol 没有任何 5d/20d outcome 值时，从 `investment_candidate_outcomes` 长表按 symbol/horizon 回补成熟 outcome。
- 这修复了 HK dense snapshot 场景的假 `OUTCOME_PENDING`：最新 weekly unified 有 HK candidate rows，但所有 HK outcome 字段为空；真实 `audit.db` 中仍有成熟 5/20d outcome。
- 重新刷新 HK-only artifact 到 `runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/`，并刷新 root `opportunity_outcome_validation.json` 与 dashboard。
- HK positive post-cost 当前仍是 mixed，不支持放宽 gate：bluechip `0005.HK,2359.HK` 为 `5d=+243.51bps/20d=-27.55bps`；tech growth `0005.HK,2359.HK` 为 `5d=+247.99bps/20d=-4.27bps`。
- HK close `WAIT_PULLBACK` 只有 tech growth `0002.HK` 同时略正：`5d=+10.00bps/20d=+2.90bps`，仅生成 P2 manual paper-only near-entry limit trial；bluechip `0002.HK` 20d 为负，不 trial。
- 根 dashboard artifact 现在显示 `opportunity_outcome_validation.outcome_source=investment_candidate_outcomes`、matched symbols `46`、matured 5d samples `12002`、matured 20d samples `4230`。
- Supervisor shutdown status 当前为 `status=stopped`、`reason=signal:SIGTERM`、`last_signal_name=SIGTERM`、PID `54465`；这说明是外部/恢复流程触发的 graceful stop，不是未捕获异常 crash。
- 主程序看似“自动 shutdown”的已知路径：`--once` 正常退出、重复 `supervisor.lock` 直接退出、SIGINT/SIGTERM/KeyboardInterrupt、或连续 `run_cycle()` 异常达到 `max_consecutive_cycle_errors_before_shutdown`；无开市窗口时停止 `src.main` 子进程是设计行为，不等于 Supervisor shutdown。
- 交易含义：不放宽 HK risk/edge/cost/liquidity/market-rule/Gateway/submit-quality gate；继续要求 fresh BUY plan、whole-share feasible、post-cost positive、limit order、Gateway budget OK 和 submit-quality pass 后，才可人工评估 paper-only 小额 trial。
- 归档：`docs/change_archive_2026-07-11_hk_outcome_db_backfill_and_shutdown_diagnosis.md`。
- 验证：`PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_opportunity_outcomes.py` -> `5 passed`；`PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes --market HK ... --db runtime_data/paper_investment_only_duq152001/audit.db` 完成；`PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes ... --db runtime_data/paper_investment_only_duq152001/audit.db --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor` 完成；`PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor` 完成。
