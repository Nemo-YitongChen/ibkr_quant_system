# Evidence Focus Lifecycle 技术路径校准

## 结论

原路径方向正确：项目主线确实已经从 dashboard/control 基础设施，进入 `Evidence -> Evidence Action -> Control Audit -> Weekly Effectiveness -> Calibration` 的闭环阶段。

但执行顺序需要按当前代码状态调整。当前 `main` 已经完成原 Step 1、Step 2、Step 3，本轮已继续完成 Step 4、Step 5、Step 6 和 Step 7。下一步不应重复补 evidence/action/dashboard 基础链路，而应进入“weekly review 建议策略参数，但不自动生效”的治理层。

## 当前代码状态

已完成：

- Time-sensitive CI 修复：`tests/test_artifact_health.py` 与 `tests/test_governance_health_summary.py` 已显式传入 `now=`，并覆盖 stale warning / stale approved-not-applied degraded。
- Dashboard control audit linkage：`src/common/dashboard_control_audit.py` 已统一脱敏、错误分类、evidence action link 与 resolution summary。
- Evidence action resolution：`src/common/evidence_focus_actions.py` 已有 `apply_action_resolutions()`，dashboard 会用 control action history 回填 evidence action status。
- Weekly evidence artifacts：weekly review 已输出 `weekly_unified_evidence.json` 与 `weekly_blocked_vs_allowed_expost.json`，dashboard artifact health 已纳入这些产物。

仍需推进：

- Weekly review 可以基于 evidence 输出更具体的参数建议，但仍应只建议、不自动生效。
- 后续参数建议应一次只选一个 primary field，先 paper，再 shadow，再 limited live。

## 修正后的执行顺序

### P0 已完成

1. `test(ci): freeze time-sensitive health checks`

状态：已完成。PR #14 已合并。

2. `feat(dashboard): link control audit to evidence actions`

状态：已完成。PR #13 已合并。

3. `apply_action_resolutions`

状态：已完成。Dashboard payload 会使用 audit history 合成 action 当前状态。

### P1 已完成

4. `feat(review): summarize evidence focus effectiveness`

目标：

- weekly review summary JSON 增加 `evidence_focus_effectiveness`。
- weekly review markdown 增加 `## Evidence Focus Effectiveness`。
- 统计 new / urgent / resolved / stale urgent / avg resolution hours。
- `ACKNOWLEDGED` 不算 resolved，但不再计入 stale urgent。
- `sample_collection` 不计入 stale urgent。

本次执行状态：

- `src/common/evidence_focus_actions.py` 新增 `build_evidence_focus_effectiveness_summary()`。
- `src/tools/review_weekly_output_support.py` 会从 `blocked_vs_allowed_expost_rows` 生成 evidence focus actions，并写入 weekly summary/tuning dataset。
- `src/tools/review_weekly_markdown.py` 已渲染 Evidence Focus Effectiveness section。

5. `test(evidence): lock blocked-vs-allowed action mapping`

目标：

- `BLOCKED_OUTPERFORMED_ALLOWED -> review_gate_thresholds` 且 urgent。
- `INSUFFICIENT_SAMPLE -> collect_more_outcome_samples` 且不升级为 urgent warning。
- `GATE_OK / BLOCKING_HELPED -> keep_gate_monitor_post_cost`，不误触发调参。
- artifact missing 使用 fallback / monitor，不 crash。

本次执行状态：

- `src/common/evidence_focus_actions.py` 新增显式 `action_from_blocked_vs_allowed_review_label()`。
- missing `weekly_blocked_vs_allowed_expost` artifact 会生成缺 evidence fallback action；空 rows 不触发 fallback。
- `tests/test_evidence_focus_actions.py` 锁定 label 到 action/urgency 的映射契约。
- `tests/test_investment_evidence.py` 锁定 gate 有效、gate 过紧、outcome 缺失三类 review label。

### P2 已完成

6. `refactor(dashboard): organize v2 blocks by home and advanced categories`

目标：

- Home 只保留 Ops Health、Evidence Focus、Execution Quality、Governance / Control Actions。
- Advanced 展开 market views、waterfall、unified evidence、blocked-vs-allowed、control action history。
- block schema 增加 `category` 与 `advanced_only`。

本次执行状态：

- `dashboard_v2_blocks` 已新增 `category` 与 `advanced_only`。
- Home blocks 固定为 `ops_health / evidence_focus_actions / evidence_quality / dashboard_control_actions`。
- Advanced blocks 固定为 `market_views / weekly_attribution_waterfall / unified_evidence_overview / blocked_vs_allowed_expost / dashboard_control_action_history`。
- advanced HTML 会在首页展示 home blocks，并在 advanced mode 展开 advanced blocks 与 row previews。

### P3 已完成

7. `refactor(strategy): move primary signal weights into config`

目标：

- 只做低风险配置化和纯函数测试。
- 不在 evidence effectiveness 尚未稳定时做多字段自动调参。

本次执行状态：

- `StrategyConfig` 新增 `engine:` 参数入口，MR/BO 权重和 mid regime sizing 默认写入 `config/strategy_defaults*.yaml`。
- `EngineStrategy` 的短线 MR/BO 信号融合改为配置驱动，默认仍为 `0.60 / 0.40`。
- `fuse()` 支持配置化 short/long/mid-bias weights 和 risk-off chasing block 阈值。
- `strategy_parameter_registry.yaml` 已登记 `mr_weight / bo_weight / mid_soft_floor / mid_qty_min / mid_qty_max`。
- 纯策略测试覆盖 breakout 上下突破、short disabled、mid regime 高波动/深回撤收缩。

### 下一阶段

8. `feat(review): suggest one strategy parameter from evidence`

目标：

- Weekly review 只输出建议，不自动改配置。
- 每次只推荐一个 primary field。
- 参数建议必须引用 linked evidence、market、portfolio、acceptance rationale 和 rollback note。

## 治理原则

- Weekly review 先给 evidence 和建议，不自动应用策略参数。
- 每次只允许一个 primary field 进入人工 review。
- 从 paper 到 shadow 到 limited live 分阶段推进。
- 所有时间敏感测试必须显式传 `now=`。
