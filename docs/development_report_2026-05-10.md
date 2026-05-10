# 后续开发报告与技术路径 - 2026-05-10

## 1. 当前阶段判断

`ibkr_quant_system` 已经进入 Evidence-driven Operation 阶段。系统主链路已经不是“生成交易信号后下单”，而是：

`candidate -> strategy control -> risk overlay -> execution gate -> order/fill -> weekly evidence -> dashboard action -> control audit -> effectiveness review`

当前已经具备以下基础能力：

- weekly unified evidence、blocked-vs-allowed ex-post review、candidate model review 已成为周度复盘输入。
- evidence focus action 已接入 dashboard control audit，可以追踪 acknowledged/applied/rejected/superseded。
- strategy parameter suggestion 已由 weekly review 生成，并保持 `read_only` 与 `auto_apply=0`。
- strategy suggestion follow-up 已能比较后续 candidate model review，给出 `IMPROVED / DEGRADED / NO_CLEAR_CHANGE / INSUFFICIENT_FOLLOWUP_SAMPLE`。
- IBKR Gateway request telemetry 与 weekly budget status 已建立，dashboard ops health 能看到 Gateway 压力。
- walk-forward acceptance summary 与 market stability artifact 已存在，后续参数调整不能只看单窗最优。

因此，下一阶段不应该继续重复建设 evidence/action 基础设施，而应该把“建议是否被处理、处理后是否改善 post-cost edge”提升到 dashboard 和治理层。

## 2. 当前主要风险

### 2.1 大文件复杂度

高风险文件仍集中在：

- `src/tools/generate_dashboard.py`
- `src/app/supervisor.py`
- `src/tools/review_weekly_feedback_support.py`
- `src/tools/review_investment_weekly.py`
- `src/app/investment_engine.py`
- `src/common/storage.py`

后续任何新功能都应优先落在 common/support 模块或 dashboard block builder 中，避免继续扩大主 orchestration 文件。

### 2.2 Governance loop 可见性不足

weekly review 已能生成 strategy parameter suggestions 和 follow-up，但 dashboard v2 之前还没有一个专门 block 聚合：

- open suggestion
- handled/resolved suggestion
- stale suggestion
- auto-apply violation
- applied 后的 improved/degraded follow-up

这会导致 operator 必须翻 weekly artifacts 才能知道参数建议是否仍需要处理。

### 2.3 IBKR Gateway 压力仍需继续用数据治理

当前已经有 telemetry、cache、single-flight 和 request budget。后续不应马上做 long-lived Gateway broker，而应先用 weekly budget 与 dashboard health 验证：

- 哪些 market/tool 是 request hotspot。
- cache hit ratio 是否真的提升。
- 超预算是否来自历史数据重拉、候选列表过宽、还是 subprocess churn。

## 3. 推荐后续路径

### P0：把 strategy suggestion governance 接入 dashboard v2

目标：让 operator 在 advanced dashboard 里直接看到策略参数建议的处理状态和后续效果。

技术动作：

- `generate_dashboard.py` 加载 `weekly_strategy_parameter_suggestions.json/csv`。
- 加载 `weekly_strategy_parameter_suggestion_followup.json/csv`。
- 从 `weekly_review_summary.json` 加载 `strategy_parameter_suggestion_effectiveness`。
- `dashboard_blocks.py` 新增 `strategy_parameter_governance` advanced block。
- dashboard v2 HTML 继续通过通用 block renderer 展示，不新增手写 HTML section。

验收标准：

- dashboard JSON 保留旧 key，不破坏已有 consumers。
- new block 显示 suggestion/open/resolved/follow-up verdict metrics。
- `auto_apply_count > 0`、stale suggestion 或 degraded follow-up 会进入 warning。
- block 仍保持 read-only，不写配置。

### P1：dashboard 主文件继续拆分

目标：降低 `generate_dashboard.py` 风险。

建议顺序：

1. 先抽 `dashboard_weekly_artifacts.py`：集中 weekly JSON/CSV artifact loading。
2. 再抽 `dashboard_payload_builder.py`：只负责 payload key 聚合。
3. 再抽 advanced table/row preview helpers。

验收标准：

- `build_dashboard()` 行为不变。
- `dashboard.json` 旧 key 不删除。
- 每个新 support 模块有 import smoke 或 helper tests。

### P1：execution quality evidence 第二阶段

目标：从周度 summary 进一步推进到决策级 evidence。

建议统一字段：

- `signal_score`
- `expected_edge_bps`
- `expected_cost_bps`
- `edge_gate_threshold_bps`
- `dynamic_liquidity_bucket`
- `dynamic_order_adv_pct`
- `slice_count`
- `realized_slippage_bps`
- `realized_edge_bps`
- `outcome_5d / outcome_20d / outcome_60d`

验收标准：

- blocked vs allowed review 可以直接回答“被挡掉的单是否真是坏单”。
- slicing calibration 可以直接回答“当前切片是否过度保守”。
- weekly review 继续只建议，不自动改 live 参数。

### P2：配置 defaults + overrides 收敛

目标：减少多市场 YAML 复制，但不牺牲 live 安全字段的显式性。

建议先做：

- strategy defaults base layer。
- market-data cache/budget knobs base layer。
- report scoring 低风险字段。

暂缓：

- live execution safety fields。
- market-rule hard gates。
- account-specific live credentials 或 paths。

## 4. 本轮实现选择

本轮优先实现 P0：`Strategy Parameter Governance` dashboard v2 block。

原因：

- 已有 weekly suggestion/follow-up/effectiveness artifacts，不需要改变交易行为。
- 能把 strategy suggestion 从“周报产物”推进到“operator 可见治理状态”。
- 风险较低，只读 dashboard block 不会自动修改策略配置。
- 与当前 evidence lifecycle 主线最一致。

## 5. 完成后的下一步

完成 P0 后，下一步建议进入 P1：

1. 抽 `generate_dashboard.py` 中 weekly artifact loader 到独立 support 模块。
2. 把 strategy parameter governance block 的 payload loader 作为第一个迁移样例。
3. 再推进 execution quality evidence 决策级表。
4. 最后再做 config defaults + overrides，避免在 evidence 可见性不足时扩大配置自由度。
