# Evidence Focus Lifecycle 路径评估与执行记录

日期：2026-05-09

## 结论

用户给出的 Step 1 到 Step 7 路径方向正确，优先级也合理：先稳 CI，再打通 evidence action 与 control audit，然后进入 weekly effectiveness、blocked-vs-allowed 映射、dashboard 信息架构和低风险策略配置化。

但按当前仓库状态，这条路径已经不是“待执行计划”，而是“已完成基线”。继续重复这些步骤会制造低价值改动。最合适的后续路径应调整为：

1. 保留 Step 1 到 Step 7 作为当前 evidence lifecycle 的完成基线。
2. 后续进入 Step 8：weekly review 基于 evidence 生成单一 primary strategy parameter 建议，但不自动生效。
3. 所有建议必须继续遵守 paper -> shadow -> limited live 的治理顺序。

## Step 1 到 Step 7 状态核对

### Step 1：冻结时间敏感测试

状态：已完成。

证据：

- `tests/test_artifact_health.py` 已在 weekly unified evidence freshness 测试中显式传入 `now=`。
- `tests/test_governance_health_summary.py` 已在 pending action health 测试中显式传入 `now=`。
- stale artifact warning 和 approved-not-applied degraded 的反向测试已存在。

判断：该步骤不需要重复改动。后续新增时间敏感测试必须继续显式传入 `now=`，不能依赖真实当前日期。

### Step 2：打通 Evidence Action 与 Control Audit

状态：已完成。

证据：

- `src/common/dashboard_control_audit.py` 已支持 `evidence_action_id`、market、portfolio、resolution status 和 resolution note。
- resolution status 已标准化为 `ACKNOWLEDGED / APPLIED / REJECTED / SUPERSEDED`。
- 未知 resolution status 会降级为 `ACKNOWLEDGED`，不会抛异常。
- audit payload 已做脱敏和截断。

判断：链路方向正确，当前实现符合治理要求。

### Step 3：合成 Evidence Action 最新状态

状态：已完成。

证据：

- `src/common/evidence_focus_actions.py` 已实现 `apply_action_resolutions()`。
- 多条 audit row 命中同一 action 时会使用最新 timestamp。
- unknown status 不会覆盖 action 当前状态。
- dashboard 已能用 control action history 回填 evidence action status。

判断：当前行为符合“dashboard 只读建议，不自动修改交易系统”的边界。

### Step 4：Evidence Focus Effectiveness 周度回看

状态：已完成。

证据：

- `build_evidence_focus_effectiveness_summary()` 已统计 new、urgent、resolved、stale urgent、sample collection 和平均处理耗时。
- weekly summary JSON 已输出 `evidence_focus_effectiveness`。
- weekly markdown 已渲染 Evidence Focus Effectiveness section。

判断：weekly review 已能回答 action 是否被处理、处理延迟和 stale urgent 风险。

### Step 5：锁住 Blocked-vs-Allowed 到 Action 的映射

状态：已完成。

证据：

- `BLOCKED_OUTPERFORMED_ALLOWED` 会生成 urgent `review_gate_thresholds`。
- `INSUFFICIENT_SAMPLE` 会生成 sample collection action。
- `GATE_OK` 和 `BLOCKING_HELPED` 不会生成 urgent 调参 action。
- 缺少 blocked-vs-allowed artifact 时会生成 fallback / monitor action，不 crash。

判断：blocked-vs-allowed review 已经从 artifact 输出推进到 action 驱动。

### Step 6：Dashboard 信息架构收敛

状态：已完成。

证据：

- dashboard v2 block schema 已增加 `category` 和 `advanced_only`。
- Home 聚焦 Ops Health、Evidence Focus、Execution Quality、Governance / Control Actions。
- Advanced 展开 market views、waterfall、unified evidence、blocked-vs-allowed 和 control action history。
- 旧 dashboard JSON key 保持兼容。

判断：信息架构已从“堆 summary 字段”推进到 home/advanced 分层。

### Step 7：策略技术债第一批

状态：已完成。

证据：

- `StrategyConfig` 已支持 `engine.mr_weight`、`engine.bo_weight`、`engine.mid_soft_floor`、`engine.mid_qty_min`、`engine.mid_qty_max`。
- `config/strategy_defaults*.yaml` 已写入默认配置。
- `config/strategy_parameter_registry.yaml` 已登记 `SIGNAL_FUSION` 和 `MID_REGIME_SIZING` 字段。
- 纯策略测试已覆盖 breakout 方向、short disabled 和高波动/深回撤收缩。

判断：第一批策略技术债已按低风险原则完成，默认行为保持不变。

## 调整后的后续路径

### Step 8：Weekly Review 生成单一策略参数建议

目标：weekly review 可以基于 evidence 输出更具体的策略参数建议，但仍然只建议、不自动应用。

最低验收标准：

- 每个 market / portfolio 每周最多推荐一个 primary field。
- 建议必须包含 linked evidence artifact、linked evidence key、market、portfolio、current value、suggested value、rationale、rollback note。
- 建议必须标记 `auto_apply=0` 或等价只读字段。
- 建议必须能进入 governance / control audit，而不是直接改 YAML。
- 如果 evidence 样本不足，只输出 sample collection 或 monitor，不输出参数调整。

推荐优先级：

1. `SIGNAL_RANKING_INVERTED`：优先建议复核 `mr_weight` 或 `bo_weight`，但一次只给一个 primary field。
2. `EXPECTED_EDGE_OVERSTATED`：优先建议复核 edge 映射或 cost buffer，不归入盲目策略权重调整。
3. `MIXED_SIGNAL` 或 `INSUFFICIENT_CANDIDATE_OUTCOME_SAMPLE`：只建议继续采样。
4. `SIGNAL_RANKING_WORKING`：不建议改参数，只记录继续观察。

### Step 9：建议效果追踪

目标：把 accepted / applied / rejected 的建议和后续 5/20/60d outcome 连接起来。

最低验收标准：

- weekly review 能显示上期 applied 建议的 outcome delta。
- rejected 建议不参与效果改善统计，但保留审计记录。
- superseded 建议必须指向替代 action 或替代 patch。

### Step 10：Strategy Config Patch Governance

目标：把“建议参数”推进到人工审批 patch，但仍不自动生效。

最低验收标准：

- 一个 patch 只允许一个 primary field。
- patch 必须带 evidence、approval、rollback、effect tracking 四件套。
- paper 观察期通过后，才允许 shadow；shadow 再通过，才允许 limited live。

## 执行原则

- 不重复实现已完成的 Step 1 到 Step 7。
- 不把 weekly review 建议直接写入运行配置。
- 不让 dashboard control 绕过 governance 直接改交易系统。
- 不接受只在单窗最好的参数。
- 不接受 pre-cost 好但 post-cost 变差的参数。
- 不接受只看全市场平均、忽略 market-specific 稳定性的参数。
