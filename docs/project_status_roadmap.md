# 项目目标、当前进度与未来规划

本文聚焦当前仓库 `ibkr_quant_system`，不包含同级目录下的 `booking-site`。

## 1. 项目定位

`ibkr_quant_system` 不是一个单点策略脚本，而是在朝“基于 IBKR 的中长期投资研究与执行操作系统”演进。  
它当前的核心目标是：

- 为 `US / HK / ASX / XETRA / CN` 多市场提供统一的投资研究流程
- 把“候选生成 -> 打分排序 -> 组合计划 -> paper 账本 -> broker 执行 -> 风控守护 -> 周度复盘”串成一条闭环
- 用统一的 `audit.db`、报告目录和 supervisor 调度，把研究、执行和复盘沉淀成可追踪的运行系统
- 在 `paper` 与 `live` 两种运行模式之间复用同一套流程，逐步从研究工具走向稳定的半自动投资运营系统

从当前实现看，项目主线已经明确切到“中长期投资”而不是“短线交易”。这一点可以从 `README.md`、`config/supervisor.yaml`、`config/supervisor_live.yaml` 以及大量 `investment_*` 工具链中得到验证。

## 2. 当前架构概览

系统大致分成 6 层：

### 2.1 数据与券商接入

- `src/ibkr/*`
  - IBKR 连接、合约、账户、订单、fills、市场数据
- `src/enrichment/*`
  - `yfinance / FMP / FRED / Finnhub` 等研究增强源
- `src/data/*`
  - 数据适配层与统一模型

### 2.2 研究与打分

- `src/analysis/investment.py`
  - 投资候选打分、动作分层、计划生成
- `src/analysis/investment_backtest.py`
  - 报告生成时附带轻量回测指标
- `src/analysis/investment_shadow_ml.py`
  - shadow ML 辅助打分
- `src/analysis/report.py`
  - 报告、CSV、JSON、Markdown 输出

### 2.3 组合与执行

- `src/tools/run_investment_paper.py`
  - 本地 paper 账本与调仓模拟
- `src/app/investment_engine.py`
  - broker 执行计划与提交
- `src/app/investment_guard.py`
  - 持仓防守与 guard 规则
- `src/app/investment_opportunity.py`
  - 盘中机会扫描

### 2.4 调度与运营

- `src/app/supervisor.py`
  - 多市场、多时间窗口自动调度
- `src/tools/preflight_supervisor.py`
  - 上线前轻量检查
- `src/app/dashboard_control.py`
  - dashboard 控制模式与运维开关

### 2.5 审计与复盘

- `src/common/storage.py`
  - SQLite 审计、订单、fills、risk、investment 运行记录
- `src/tools/review_investment_weekly.py`
  - 周度复盘、执行质量、风险反馈、阈值建议
- `src/tools/reconcile_investment_broker.py`
  - broker 对账
- `src/tools/sync_investment_paper_from_broker.py`
  - broker 快照回写本地账本

### 2.6 配置与市场扩展

- `config/`
  - 市场、风控、执行、paper、guard、opportunity、watchlist、holiday、regime 等配置
- 当前配置已覆盖：
  - `HK`
  - `US`
  - `ASX`
  - `XETRA`
  - `CN`

其中 `CN` 当前仍是 `research-only`，这在 `config/investment_cn.yaml` 中已有明确说明。

## 3. 当前进度判断

### 3.1 结论

基于代码规模、测试规模、运行产物和调度配置，我的判断是：

- 该项目已经明显超过“原型验证期”
- 当前更接近“单人主导、可实际运行的 Alpha 系统”
- 其中 `HK / US` 的链路成熟度最高，已具备研究、paper、执行、guard、weekly review 的完整闭环
- `ASX / XETRA` 已接入统一框架，但真实运营深度看起来弱于 `HK / US`
- `CN` 仍处在研究层验证阶段

这是一个“已经能跑、也确实跑过”的系统，但还没有完全进入多人协作、强工程治理、稳定生产化的阶段。

### 3.2 已完成内容

#### 目标层

- 已完成从“短线交易系统”向“中长期投资操作链路”的方向收敛
- 已形成统一主路径：
  - 投资报告
  - paper 账本
  - broker 执行
  - 持仓 guard
  - 机会扫描
  - 周复盘
  - baseline regression
  - broker 对账与同步

#### 工程层

- `src/` 下共有约 `98` 个 Python 源文件，约 `41180` 行代码
- `config/` 下共有约 `93` 个配置文件，说明市场与运行模式已经做了较深拆分
- 当前仅 `requirements.txt` 暴露了最小依赖，核心运行依赖轻量，但工程元数据仍偏简化
- 审计与事件存储已经统一进 SQLite，说明系统开始重视“可追踪性”而不是只输出一次性结果

#### 测试层

- `tests/` 下共有 `21` 个测试文件，约 `12838` 行
- 本次本地执行 `pytest -q` 时，已看到 `155 passed`
- 退出阶段因为人工中断出现 `KeyboardInterrupt`，同时有 `.pytest_cache` 写入权限 warning
- 这说明“测试主体是可通过的”，但当前运行环境在挂载卷上对 pytest cache 的写入不够友好

#### 运行层

- 仓库中存在大量已生成的运行产物目录，且时间分布不是一次性的
- `reports_investment_hk/*`、`reports_investment_us/*`、`reports_investment_xetra/*`、`reports_investment_asx/*`、`reports_investment_cn/*` 都已有实际输出
- `reports_preflight/` 中有较新的 preflight 与 `ibkr_history_probe` 结果
- `runtime_data/paper_investment_only_duq152001/audit.db` 说明 scoped runtime 路径已经实际使用

#### 运维层

- `config/supervisor.yaml` 与 `config/supervisor_live.yaml` 已把市场本地时区、定时任务、paper/live 区分、dashboard、weekly review、labeling 等统一进 supervisor
- dashboard control 已支持：
  - `AUTO`
  - `REVIEW_ONLY`
  - `PAUSED`
- 系统已经具备“运维视角”的设计意识，而不只是“研究脚本集合”

### 3.3 当前薄弱点

#### 文档不足

- 仓库此前只有两份核心文档：
  - `docs/runnable_code_summary.md`
  - `docs/supervisor_runbook.md`
- 对外仍缺少一份“项目目标/边界/阶段/路线图”文档

#### 模块复杂度偏高

- `src/app/supervisor.py` 约 `3123` 行
- `src/tools/review_investment_weekly.py` 约 `5252` 行
- `src/tools/generate_investment_report.py` 约 `2155` 行
- `src/common/storage.py` 约 `1378` 行

这代表系统功能已经较完整，但复杂度开始集中到少数超大文件，后续维护成本会上升。

#### 配置扩张明显

- 多市场、多模式是优势，但也带来了较重的配置复制
- `config/` 中已有大量 `investment_* / guard_* / execution_* / regime_* / ibkr_*` 文件
- 如果继续按当前方式扩展市场或策略，维护成本会持续增加

#### 协作工程化较弱

- GitHub 仓库当前为公开仓库：`Nemo-YitongChen/ibkr_quant_system`
- 本地 `main` 分支当前相对 `origin/main` 领先 `2` 个提交
- 仓库没有最近 PR 记录
- 提交历史目前较短，说明项目还主要是个人快速迭代模式，而不是标准化协作模式

## 4. 项目当前阶段总结

如果用一句话概括当前状态：

这是一个已经具备多市场研究、paper/live 执行和复盘闭环的个人量化投资操作系统雏形，功能上已经很完整，但工程治理、可维护性和生产化规范还需要补课。

更具体一点：

- 功能成熟度：中高
- 自动化程度：中高
- 可运维性：中
- 可协作性：中低
- 生产级稳健性：中低到中

## 5. 未来规划建议

这里的规划按“先稳住系统，再提升可维护性，再考虑扩展能力”的顺序来排。

### 5.0 2026 Q2 总计划

Q2 的主线不再是“继续堆功能”，而是把现有 `research -> paper -> execution -> weekly review -> dashboard -> supervisor` 这条链路做成更稳定、更可解释、更容易维护的运营系统。

#### April：收口状态视图与工程基线

- 继续统一 dashboard 的 `market state / report freshness / ops health / market-data health`
- 把 card 级状态进一步推进到 `ops overview / dashboard control / simple mode`
- 让 GitHub Actions 成为默认回归入口，分层跑 `compile / guardrail / integration / full`
- 保持旧字段兼容一版，但新增统一字段和汇总 helper，逐步减少同义字段分叉

验收口径：

- dashboard 不再依赖零散字符串拼装判断 freshness/health
- `push / PR / manual dispatch` 都能自动触发同一条 Python CI
- simple mode、advanced mode、ops/control 至少共用一套状态语义

#### May：开始降复杂度

- 优先拆分 `src/app/supervisor.py`
- 其次拆分 `src/tools/review_investment_weekly.py`
- 继续把大函数中的“构建数据 / 聚合状态 / 渲染输出”拆成独立 helper
- 收敛配置层，逐步转向“市场默认值 + override”

当前进展：

- 已开始从 `src/app/supervisor.py` 中抽离 patch governance helper，先把 `market profile / calibration patch` 的纯 helper 逻辑拆出主文件，降低后续 review/apply/evidence 路径继续膨胀的风险。
- `dashboard_control_portfolios()` 中最臃肿的一段 patch review / weekly feedback 装配，也已开始改成独立 builder，而不是继续把所有字段直接堆在主循环里。
- `dashboard control state / artifact payload` 这一层也已抽成 support helper，`service/actions/artifacts` payload、patch candidate row 和 artifact payload builder 不再继续堆在 `Supervisor` 主文件里。
- `weekly feedback overlay` 的公共 patch metadata 也已开始从 `investment / execution / paper` 三条 YAML 装配链里收口，`market_profile / calibration_patch` 这一大段重复字段已统一走 support helper，后续可以继续拆更深的 feedback-specific 差异字段。
- `investment / execution` overlay 里的 `shadow/execution confidence + reason + automation mode` 这组 feedback-specific metadata 也已开始统一到 support helper，主文件开始只保留 `signal_penalties / execution_hotspot_penalties / session-hotspot` 这类真正的差异项。
- execution overlay 里的 `dominant session / hotspot json` 这层 metadata 也已并入 support helper，主文件进一步收敛到“execution 参数改写 + penalty merge”这类核心差异逻辑。
- execution overlay 里最后那段 `adv/session` 参数改写与 `execution_hotspot_penalties` merge 也已抽成 support helper，`Supervisor` 这条链现在更接近“读配置 -> 组装 metadata -> 应用差异参数 -> 落盘”的薄 orchestrator 结构。
- investment overlay 的 `scoring/plan + signal/execution penalties`，以及 paper overlay 的 `risk budget delta` 也已开始统一到 support helper；三条 weekly-feedback overlay 现在正收敛到一致的 `support builder + thin orchestrator` 结构。
- `previous execution penalties` 的读取/legacy fallback、`feedback_reason` 拼装、以及 `DECAY action` 的回退规则也已开始统一到 support helper，overlay 分支判断正在从主文件里继续后撤。
- `shadow_ml_*` 的 execution 参数改写和 `risk_feedback` metadata 也已并入 support helper；`Supervisor` 主文件里这三条 overlay 现在更多只剩 orchestration，而不是字段级组装。
- 三条 overlay 共用的 `existing feedback / auto-apply mode / patch metadata` 也已开始收口到 shared context builder，`Supervisor` 里的 `investment / execution / paper` 路径正在转成更一致的 `load base -> derive context -> apply delta -> write overlay` 结构。
- `row gating / should_write / weekly_feedback payload merge` 这层模板逻辑也已开始统一到 support helper，三条 `_effective_*_config_path()` 现在进一步靠近“只保留各自 delta builder”的薄函数形态。
- `dashboard control portfolios` 里的 `patch governance / market profile review / calibration review` 三组字段簇也已开始抽成 support builder，主文件这段开始从“大字典拼装”后撤到“拿 bundle -> merge sections”。
- `dashboard_control_portfolio_row()` 里的基础字段（identity / control flags / feedback status）也已开始统一到 support helper，`Supervisor` 这条 dashboard-control 支线正在从“整行手拼”转成“基础 row + patch sections”的组合结构。
- `dashboard control` 相关的 investment-report 遍历也已开始统一成 walker，并复用到 `portfolios / overrides / toggle execution mode` 路径，主文件里这条支线的市场/组合筛选逻辑开始集中而不是分散复制。
- `_apply_dashboard_control_overrides()` 里那组 `weekly_feedback_*` state 回填字段也已开始统一到 override-applier helper，dashboard-control 这条支线已经逐步形成 `walker + row builder + state applier` 的完整结构。
- `dashboard control` 的 `review/apply` handler 也已开始改成“定位组合 -> 组装 patch bundle -> 写回 review state / history / artifact”的分层结构，减少审批入口里重复的市场遍历、状态写回和返回值拼装。
- patch review 的 `state/history` 这组重复逻辑也已参数化，开始统一到 support helper，而不是继续分别维护 `market profile` 和 `calibration` 两套近似实现。
- `src/tools/review_investment_weekly.py` 也已开始第一轮收口，先把 `weekly_tuning_dataset` 拆成 `lookup map + row builder`，避免一个函数同时承担数据索引、上下文拼接和最终输出拼装。
- `weekly_tuning_dataset` 这整簇 helper 现在也已并入 `review_weekly_feedback_support.py`，主文件进一步从“内嵌 lookup/context join/summary”收敛为 orchestration 调用。
- `weekly_tuning_history / patch_governance / control_timeseries` 这组 history-overview builder 也已并入同一 support 模块，主文件继续从“分析聚合实现”收敛为 orchestration 与 persistence 调用。
- `broker summary / broker-local diff / CLI summary payload` 这条尾部支线也已开始并入 output support，`review_investment_weekly.py` 进一步减少对 artifact 包装与 broker 展示拼装的内嵌实现。
- `summary_rows` 的组合收益摘要构建，以及 `market_profile tuning/readiness` 对 `summary_rows + strategy_context_rows` 的回填，也已开始抽成 support builder，`main()` 正在从长循环收敛为“准备输入 -> 调 builder -> 串主流程”。
- `filtered fill/commission` 的 execution 过滤，以及 `broker_summary` 的 effect/planned/realized augment，也已开始统一到 support helper，`main()` 里 execution-analysis 前置编排继续从手工拼接收敛为 builder 调用。
- `execution_effect / planned_cost / gate / parent / outcome / edge / hotspot / session` 这整段 execution-analysis 前置编排，现在也已开始收成 bundle builder；同时 `calibration patch / runtime config` 这簇纯 helper 也已并入 support 模块，主文件只保留 orchestration 调用和兼容导入面。
- `strategy_context / attribution / decision evidence / risk-execution feedback / market-profile tuning` 这段主流程，现在也已开始收成 `strategy-feedback bundle`；`feedback automation / threshold history` 这条后续治理链也同步收成 `automation bundle`，`main()` 继续向“少量 orchestration + 少量 persistence”收缩。
- `weekly_tuning_history / decision_history / calibration / patch governance / control timeseries` 这段 persistence 与派生输出，现在也已开始收成 `history-calibration bundle`，主流程里这组连续的 `persist + build + patch suggest` 调用已经后撤成一次 bundle 调用。
- `csv/json/markdown/cli summary` 这条输出装配链，现在也已开始统一到 `output bundle`，`main()` 尾部从“多段 payload/kwargs 手工拼装”继续收敛为“调 output builder -> 写产物 -> emit summary”。
- `report-data warning / market-data gate` 这组 report + preflight 读取链现在也已并入 `review_weekly_feedback_support.py`，主文件只保留 thin wrapper，不再内嵌市场数据 gate 的文件读取与状态判断。
- `feedback calibration / automation-threshold / weekly tuning / decision evidence` 这组低层 history-persist 与效果快照 helper 也已后撤到 support 模块，`review_investment_weekly.py` 进一步收敛到 orchestration + compatibility wrapper。
- `shadow feedback / risk feedback` 这两条仍留在主文件里的核心 row builder 也已并入 support 模块，`review_investment_weekly.py` 这块从“剩余真逻辑”继续收敛成 thin wrapper + orchestration。
- `labeling summary resolve / shadow review order parsing / sqlite table-column probes` 这组零散但仍属真实逻辑的 helper 也已并入 support 模块，主文件继续向“CLI 入口 + orchestration + compatibility wrapper”收口。
- `strategy context / attribution / risk review / market-profile tuning` 这组 builder 现在也已真正迁到 `review_weekly_feedback_support.py`，主文件保留同名兼容 wrapper，对外接口不变，但实现已经后撤到 support 层。
- `risk_overlay_from_history / latest_risk_overlay / risk_driver_and_diagnosis` 这组 risk 低层 helper 也已后撤到 support，`risk review` 这条支线现在从低层解析到高层 builder 都开始在同一 support 模块里聚拢。
- `decision_evidence_history_overview / edge-slicing-risk calibration / market_profile patch readiness` 这组 history-calibration helper 现在也已后撤到 `review_weekly_feedback_support.py`，主文件只保留兼容 wrapper，不再内嵌这簇派生分析实现。
- `latest_report_dir / market_sentiment / report_json` 这组三方 report loader helper 也已后撤到 support；`broker summary / broker-local diff / cli summary payload` 则继续维持 thin wrapper + output support 的结构，`review_investment_weekly.py` 正在更明确地收敛到 orchestration + compatibility surface。
- `position snapshots / latest run positions / sector rows / holdings change / execution summary` 这组 weekly summary/output builder 现在也已并入 `review_weekly_output_support.py`，主文件保留兼容 wrapper 与 orchestration 调用，不再继续维护第二份实现。
- `summarize_changes / top_holdings / top_sector / market_from_portfolio_or_symbol` 这组面向 weekly summary 的小型 helper 也已统一到 output support，`review_investment_weekly.py` 里这条输出链正在收敛为“thin wrapper + shared builder”而不是散落的本地工具函数。
- `decision evidence / calibration` 这条支线也已开始复用同一层 helper，先把 `decision evidence row/summary`、按周 decision summary、以及 `risk calibration` 的入口做成共用构件，避免后续继续在同一文件里复制 weighted avg / bucket / weekly grouping 逻辑。
- `edge / slicing / risk calibration` 目前也已进入第二轮收口，开始统一成 `market/portfolio key extraction + per-portfolio builder + sorter` 的模式，后续继续拆其他周报聚合段时可以沿用同一套结构。
- `feedback threshold` 的 `history / effect / cohort` 这组 overview 也已开始共享统一的 threshold-history context，减少每个 overview 都重复做 market/kind 去重、历史读取、排序、current action 和 action chain 拼装。
- `feedback automation effect` 这条链也已开始共享 `market/portfolio/kind` history context，并把 market summary 的累计逻辑抽成单独 builder，后续可以继续把 threshold suggestion 和 automation alert 一并并入同一层聚合模式。
- `src/tools/review_weekly_thresholds.py` 这条已独立出来的阈值支线模块也开始按同一标准收口，`tuning summary / suggestion rows` 已改成单行 builder，避免主循环继续直接拼装长字典。
- `feedback automation` 主决策也已开始拆成 `maturity info / apply decision / market-data gate` 三层 helper，减少一个函数同时承担样本成熟度判断、自动化门槛决策和数据健康降级的复杂度。
- `feedback threshold / cohort / automation effect` 这簇周报逻辑也已从 `review_investment_weekly.py` 真正拆到独立 support 模块，主文件开始只保留 orchestration，而不是继续内嵌整段 history/effect/trial 逻辑。
- `feedback automation rows` 的构建也已并入同一 support 层，主文件不再内嵌 maturity map、自动化门槛决策和 market-data gate 降级逻辑，开始形成真正的 `orchestration + support module` 分层。
- `shadow review summary / shadow signal penalties / execution hotspot penalties` 这组稳定 builder 也已并入同一 support 模块，`review_investment_weekly.py` 继续减少内嵌聚合与惩罚逻辑。
- `execution effect rows / execution session rows / execution hotspot rows` 这簇 execution-feedback builder 也已并入同一 support 模块，连同共享的 session/cost helper 一起从主文件移出，`review_investment_weekly.py` 继续收敛为 orchestration 层。
- `execution_feedback_rows` 及其依赖的 `feedback confidence / calibration support / control-driver split` 这组共用决策 helper 也已并入 support 模块，execution-feedback 支线开始从“只拆行构建”推进到“连调参判断一起模块化”。
- `planned execution cost rows / execution gate rows` 这层 execution glue 也已并入同一 support 模块，主文件只保留更靠近 candidate linking / decision evidence 的编排逻辑，后续可以更干净地继续拆 decision-evidence 支线。
- `decision evidence row / summary / weekly map` 这组证据汇总 helper 也已并入同一 support 模块，`review_investment_weekly.py` 进一步收敛为 orchestration + history overview，开始为后续拆 candidate-linking 支线腾出边界。
- `candidate-linking / execution parent / outcome spread / edge realization / blocked edge attribution` 这条更靠前的 execution-analysis 支线也已并入同一 support 模块，`review_investment_weekly.py` 在 execution-feedback 之外继续减少对 candidate snapshot / order-edge / microstructure 细节的内嵌实现。
- `weekly review` 尾部的 `CSV/JSON artifact 写出 + weekly_tuning_dataset / weekly_review_summary payload + markdown kwargs` 这层输出装配也已抽到独立 support 模块，主文件开始把“分析/反馈编排”和“报告落盘”明确分层。

验收口径：

- 超大文件开始出现更清晰的逻辑分层
- 新增一个状态字段时，不再需要同时改多处拼装代码
- market/config 的差异能够通过 override 表达，而不是重复复制整份 YAML

#### June：强化生产化治理

- 补强 observability
- 把 live / paper 决策和 weekly feedback、patch governance 进一步打通
- 增加更真实的运行证据与审计闭环
- 为风险闸门和自动降级准备更明确的触发规则

验收口径：

- 能直接回答“为什么本周没有执行 / 为什么切到 REVIEW_ONLY / 为什么某个 patch 一直卡住”
- dashboard、weekly review、supervisor 三者对同一件事的解释保持一致
- live 侧关键动作具备更完整的 evidence 与 governance history

### 5.1 近 30 天：补工程基础

优先级最高的不是再加新策略，而是把现有系统的工程边界先稳住。

当前已完成：

- 项目级文档体系
  - 架构图
  - 运行路径说明
  - market/config 对照表
  - paper/live 切换说明
- `.env.example` 和更明确的环境变量入口
- `pyproject.toml`、console scripts、README 安装运行闭环
- GitHub 基础 CI
  - 自动跑测试
- `src.main` 收口为 CLI 入口，核心装配下沉到独立 bootstrap 模块

### Dashboard freshness / health helper 对齐（进行中）

- 已为 dashboard helper 补充 freshness / market-state / health-overview / market-data-health 的测试覆盖。
- 正在把 helper 从旧的静态字符串映射升级为：
  - freshness 支持 `market + report_date + latest_generated_at + as_of_date`
  - market state 支持 `None -> 市场状态: 暂无数据`
  - health overview 按 `degraded > warning > ready` 聚合，并合并摘要
  - market data health overview 在空输入时给出 `warning + 明确兜底摘要`
- 当前已把这些 helper 接到 dashboard card / JSON payload / simple mode / advanced mode 的统一字段上，避免“测试语义”和“页面文案”继续脱节。
- 当前还新增了 dashboard status rollout summary，开始把这层状态统一推进到 `ops overview / dashboard control`。
- 下一步会继续清理旧的同义字段，并把 status rollout summary 继续透到更多运维视图。

下一步建议：

- 为 lint / static check 增加统一命令
- 继续把“研究产物目录”和“源代码目录”的关系在文档中说明清楚
- 继续压缩 `main` 之外的大文件复杂度

预期目标：

- 新人或未来的自己能在较短时间内理解项目
- 提交后能自动知道有没有把核心功能跑坏

### 5.2 30-60 天：降复杂度

建议聚焦技术债治理。

建议事项：

- 拆分 `src/app/supervisor.py`
  - 调度
  - 配置解析
  - 控制模式
  - 报告任务
  - 执行任务
- 拆分 `src/tools/review_investment_weekly.py`
  - 数据采集
  - KPI 汇总
  - 风险反馈
  - threshold 建议
  - Markdown 渲染
- 为 `Storage` 引入更清晰的 schema 分层或 migration 机制
- 收敛配置
  - 用市场默认值 + override，替代重复 YAML
- 把“研究-only / paper / live”抽象成更清晰的 capability matrix

预期目标：

- 降低修改一处牵动全局的风险
- 提升后续加市场、加策略、加规则的速度

### 5.3 60-90 天：强化生产化能力

当工程边界更稳后，再加强真正影响实盘质量的能力。

建议事项：

- 加强 observability
  - 执行失败告警
  - 关键任务耗时
  - 每市场最新报告新鲜度
  - paper/live 状态看板
- 增加更真实的集成测试
  - mock IBKR
  - runtime_data 回放
  - 周复盘样本回归
- 引入更明确的风险闸门
  - 账户状态异常
  - 数据源缺失
  - preflight fail 时自动降级到 `REVIEW_ONLY`
- 做好 live 运行留痕
  - 谁在什么模式下提交
  - 为什么提交
  - 风险建议与最终动作是否一致

预期目标：

- 让系统从“能运行”升级成“可长期稳定运营”

## 6. 建议的产品路线

如果项目最终目标是“个人长期可用的投资运营系统”，我建议路线是：

1. 先把 `HK / US` 做成最稳定的双市场主战场
2. 再把 `ASX / XETRA` 视为框架复用验证市场
3. `CN` 继续保持 research-only，等数据和执行条件成熟再决定是否进入 paper/live
4. 新功能优先级排序建议：
   - 稳定性
   - 可观测性
   - 配置收敛
   - 策略增强

核心原因是：这个项目现在最稀缺的不是“再多一个信号”，而是“让已有闭环长期可靠地工作”。

## 7. 本次分析结论

截至 `2026-03-27`，该项目最值得肯定的地方有三点：

- 已经形成了清晰的产品主线，而不是功能碎片堆叠
- 研究、执行、风控、复盘已经基本连成闭环
- 测试与运行产物都说明它不是停留在概念阶段

当前最值得优先投入的地方也有三点：

- 文档化
- 降复杂度
- 生产化治理

如果按这个顺序推进，`ibkr_quant_system` 有机会从“个人量化工程项目”进一步演进成“可长期维护的个人投资操作平台”。
