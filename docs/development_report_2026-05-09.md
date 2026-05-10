# 后续开发报告与技术路径

日期：2026-05-09
分支：`main`
远端同步：已执行 `git pull --ff-only`，结果为 `Already up to date`。

## 1. 执行摘要

`ibkr_quant_system` 当前已经不是早期策略脚本，而是一个围绕 IB Gateway 的个人投资运营系统。主链路已经形成：

`research -> paper ledger -> broker execution -> guard/opportunity -> weekly review -> dashboard/supervisor governance`

当前最重要的判断是：项目已经从“补基础设施”进入“Evidence Focus Lifecycle + 参数建议治理”阶段。dashboard、weekly evidence、blocked-vs-allowed review、evidence action、control audit、weekly effectiveness 和第一批策略参数配置化都已具备基础。下一阶段不应该再重复补 dashboard/evidence/action 基础链路，而应把 weekly review 推进到“基于证据给出单一 primary strategy parameter 建议，但不自动生效”。

## 2. 当前仓库状态

### 2.1 Git 状态

- `origin/main` 已同步到本地，远端无新增提交需要拉取。
- 当前工作区有未提交改动，主要来自最近一轮 evidence lifecycle、dashboard v2 information architecture、blocked-vs-allowed action mapping 和 strategy config/pure signal regression 工作。
- `config/watchlists/resolved_hk_top100_bluechip.yaml` 存在既有本地 `generated_at` 变更，本报告不把它视为本轮开发内容。

### 2.2 工程规模

- Python 源文件：约 `140` 个。
- Python 源代码：约 `64k` 行。
- 测试文件：约 `58` 个。
- 测试代码：约 `24k` 行。
- 配置文件：约 `96` 个。

### 2.3 最大复杂度集中点

当前最需要控制复杂度的文件：

- `src/tools/generate_dashboard.py`：约 `10848` 行，是当前最大风险点。
- `src/app/supervisor.py`：约 `4687` 行，仍承担调度、状态、dashboard control 和 artifact 读取等多重职责。
- `src/tools/review_weekly_feedback_support.py`：约 `3064` 行，虽然已经从主文件拆出，但自身也开始变大。
- `src/app/investment_engine.py`：约 `2433` 行，后续 live execution 安全治理会继续给它施压。
- `src/tools/generate_investment_report.py`：约 `2318` 行，研究报告链路仍较集中。
- `src/common/storage.py`：约 `1910` 行，SQLite schema、audit writer 和兼容方法仍聚在一起。

## 3. 当前能力判断

### 3.1 已稳定成型的能力

- 多市场主路径：`HK / US` 最成熟，`ASX / XETRA` 已接入统一流程，`CN` 保持 research-only。
- 运行治理：已有 `AUTO / REVIEW_ONLY / PAUSED` 模式、preflight、dashboard control、weekly review 和 production governance 文档。
- CI 分层：`.github/workflows/python-tests.yml` 已按 `compile-check -> core-guardrails -> integration-suite -> full-suite` 分层运行，并包含 `pip check`、`compileall`、import smoke 和 pytest tiers。
- Evidence 基础：`weekly_unified_evidence.json`、`weekly_blocked_vs_allowed_expost.json`、artifact health、dashboard evidence blocks 已具备。
- Evidence Action：已具备 `action_id / status / urgency / linked_evidence_artifact / linked_evidence_key / read_only` 等稳定字段。
- Control Audit Linkage：dashboard control payload 可关联 `evidence_action_id`，audit record 可记录 resolution status/note，并做脱敏。
- Weekly Effectiveness：weekly review 已能统计 evidence action 的新增、紧急、处理、stale urgent 和平均处理耗时。
- Strategy Config 第一批：`mr_weight / bo_weight / mid_soft_floor / mid_qty_min / mid_qty_max` 已配置化，并补了纯策略测试。

### 3.2 当前主要缺口

- Evidence action 之后的“参数建议治理”仍未完全闭环：已有 action 和 audit，但 weekly review 还需要稳定输出单一参数建议、验收规则、回滚和效果追踪。
- Dashboard 信息量已够，但 `generate_dashboard.py` 仍过大，后续任何 dashboard 改动都容易扩大风险面。
- Weekly review 已拆出多组 support 模块，但 support 模块本身还需要继续按 evidence、calibration、governance、output 等领域收敛。
- 配置文件数量持续上升，多市场 YAML 复制仍会增加维护成本。
- Live 变更治理已有原则，但还需要更强的代码级 contract：证据、审批、回滚、效果追踪必须成为 patch/action schema 的一部分。

## 4. 技术路径总原则

后续开发应遵守四条原则：

1. 先闭环证据治理，再扩大策略自由度。
2. 所有参数建议先 paper，再 shadow，再 limited live。
3. 每次只建议或审批一个 primary field，避免多字段同时变更导致归因失效。
4. 大文件继续拆分，但每次拆分必须有 import/smoke/helper tests 防回归。

## 5. P0：先完成 Evidence-Driven Parameter Suggestion

### 目标

把 weekly review 从“展示 evidence/action”推进到“基于 evidence 生成一个可审计、可回滚、可追踪的单参数建议”。

### 当前进展

状态：已开始落地。

2026-05-09 已完成第一版 read-only 产物：

- `weekly_strategy_parameter_suggestions.csv`
- `weekly_strategy_parameter_suggestions.json`
- `weekly_review_summary.json -> strategy_parameter_suggestions`
- `weekly_tuning_dataset.json -> strategy_parameter_suggestions`
- `weekly_review.md -> Strategy Parameter Suggestions`

当前只对 `SIGNAL_RANKING_INVERTED` 且样本足够的 candidate model review 生成参数建议；`EXPECTED_EDGE_OVERSTATED`、`INSUFFICIENT_CANDIDATE_OUTCOME_SAMPLE` 等不会误触发 signal weight 调整。

### 建议新增产物

新增或扩展 weekly artifact：

- `weekly_strategy_parameter_suggestions.json`
- `weekly_strategy_parameter_suggestions.csv`

建议 schema：

```json
{
  "week_label": "2026-W19",
  "market": "US",
  "portfolio_id": "US:watchlist",
  "primary_field": "mr_weight",
  "config_scope": "STRATEGY_DEFAULTS",
  "config_path": "engine.mr_weight",
  "current_value": 0.6,
  "suggested_value": 0.55,
  "change_hint": "REDUCE",
  "linked_evidence_artifact": "weekly_candidate_model_review",
  "linked_evidence_key": "US:watchlist:SIGNAL_RANKING_INVERTED",
  "rationale": "Top-ranked candidates underperformed bottom-ranked candidates post-cost.",
  "acceptance_rule": "At least 3 validation windows without post-cost degradation.",
  "rollback_note": "Revert to previous strategy_defaults value if 20d post-cost edge deteriorates.",
  "effect_tracking_window_days": 60,
  "auto_apply": 0
}
```

### 输入信号优先级

1. `SIGNAL_RANKING_INVERTED`：优先建议调 `mr_weight` 或 `bo_weight`，一次只出一个 primary field。
2. `EXPECTED_EDGE_OVERSTATED`：优先建议复核 edge/cost 假设，不直接归因到策略权重。
3. `BLOCKED_OUTPERFORMED_ALLOWED`：优先建议复核 edge gate threshold，而不是策略信号权重。
4. `INSUFFICIENT_SAMPLE`：只输出 sample collection，不输出参数修改。
5. `SIGNAL_RANKING_WORKING` / `GATE_OK`：不建议改参数，只记录继续观察。

### 涉及文件

- `src/common/evidence_focus_actions.py`
- `src/common/strategy_parameter_registry.py`
- `src/tools/review_weekly_feedback_support.py`
- `src/tools/review_weekly_output_support.py`
- `src/tools/review_weekly_markdown.py`
- `tests/test_evidence_focus_actions.py`
- `tests/test_review_weekly_output_support.py`
- `tests/test_review_investment_weekly.py`

### 验收标准

- 一个 market/portfolio/week 最多一个 primary strategy parameter suggestion。
- `auto_apply` 必须为 `0`。
- 样本不足时不能生成参数调整建议。
- 建议必须引用 evidence artifact 和 evidence key。
- markdown、JSON、CSV 三个输出一致。

## 6. P0：把建议接入 Governance / Control Audit

### 目标

让参数建议可以被 dashboard/operator 处理，并能回答：

- 建议是否被 acknowledged？
- 是否被 applied/rejected/superseded？
- 由哪个 dashboard control audit record 处理？
- 处理后是否改善 post-cost outcome？

### 当前进展

状态：已开始落地。

2026-05-09 已完成 audit-only linkage：

- dashboard control payload 可携带 `strategy_parameter_suggestion_id`、`primary_field`、`config_path`、`resolution_status` 和 `resolution_note`。
- audit row 会持久化脱敏后的 `linked_strategy_parameter_suggestion_id`、`linked_strategy_parameter_field` 和 `linked_strategy_parameter_config_path`。
- dashboard v2 Governance / Control Actions block 会统计 linked strategy parameter suggestion history。
- advanced dashboard control audit table 会显示 linked strategy suggestion id 和 field。

当前仍保持治理边界：该 linkage 只做审计和状态追踪，不自动写策略配置。

2026-05-09 进一步补齐 strategy parameter suggestion effectiveness summary：

- `weekly_review_summary.json -> strategy_parameter_suggestion_effectiveness`
- `weekly_tuning_dataset.json -> strategy_parameter_suggestion_effectiveness`
- `weekly_review.md -> Strategy Parameter Suggestion Effectiveness`

该 summary 会统计 open、handled、resolved、stale、resolution mix、avg resolution hours 和 `auto_apply` 违规；dashboard control audit 可通过 `linked_strategy_parameter_suggestion_id` 回填 suggestion 状态。

### 技术动作

- 扩展 dashboard control payload，支持 `strategy_parameter_suggestion_id`。
- audit record 增加 `linked_strategy_parameter_suggestion_id`。
- resolution status 继续复用 `ACKNOWLEDGED / APPLIED / REJECTED / SUPERSEDED`。
- weekly effectiveness 增加 parameter suggestion 分组统计。

### 验收标准

- applied suggestion 会进入后续 weekly effect tracking。
- acknowledged 只表示已看过，不算 fully resolved。
- rejected/superseded 必须保留 reason/note。
- dashboard 不直接写配置文件。

## 7. P1：Dashboard v2 继续模块化

### 目标

把 `generate_dashboard.py` 从“巨型生成器”推进到“数据加载、block 构建、HTML 渲染、control payload”分层。

### 推荐拆分

- `src/tools/dashboard_data_loader.py`
- `src/tools/dashboard_html_renderer.py`
- `src/tools/dashboard_state_summary.py`
- `src/tools/dashboard_control_view.py`
- `src/tools/dashboard_advanced_blocks.py`

### 拆分顺序

1. 先迁移纯 HTML section renderer，不碰数据 contract。
2. 再迁移 artifact/data loading helper。
3. 再迁移 advanced block row preview。
4. 最后收口 dashboard control history/action view。

### 验收标准

- `dashboard.json` 旧 key 不删除。
- simple/advanced HTML snapshots 或 helper tests 不退化。
- `tests/test_generate_dashboard_helpers.py` 和 `tests/test_dashboard_rendering.py` 覆盖新增模块。

## 8. P1：Weekly Review 支撑模块二次收敛

### 目标

继续降低 `review_weekly_feedback_support.py` 和 `review_investment_weekly.py` 的认知负担。

### 推荐拆分

- `review_weekly_evidence_support.py`
- `review_weekly_calibration_support.py`
- `review_weekly_feedback_effect_support.py`
- `review_weekly_strategy_parameter_support.py`

### 验收标准

- `review_investment_weekly.py` 保持 CLI orchestration，不再承载新的业务聚合实现。
- 每个 support 模块可以冷启动 import。
- 原兼容 wrapper 保留一轮，避免测试和旧引用一次性大改。

## 9. P1：配置 defaults + overrides 收敛

### 目标

减少 `config/strategy_defaults*.yaml`、`investment_*`、`execution_*`、`paper_*` 的重复字段。

### 技术路径

- 保留市场专属 YAML，但把共同字段收敛到 base defaults。
- 统一使用 `config_layers` 读取：base -> market -> mode -> local override。
- 先从 strategy defaults 做，不直接大改 live execution config。

### 验收标准

- `US/HK/ASX/XETRA/CN` strategy defaults 输出等价。
- 缺失 override 时走 base default。
- 所有配置加载测试覆盖 source tracing，能说明某个字段来自 base 还是 market override。

## 10. P2：Walk-Forward Acceptance Rules

### 目标

把 walk-forward 从“有 review”推进到“有接受规则”。

### 必须硬规则

- 不接受只在单窗最好的参数。
- 必须看 post-cost，不看 pre-cost 幻觉收益。
- 必须看 market-specific 稳定性，不只看全市场平均。
- 至少连续 3 个验证窗不过度退化。
- turnover 降低后，post-cost edge 必须不变或更好。

### 产物

- `walk_forward_acceptance_summary.json`
- `walk_forward_parameter_candidates.csv`
- dashboard advanced block：Walk-Forward Acceptance

## 11. P2：Execution Quality Evidence 校准

### 目标

验证 execution gate、buffer、slicing、market-rule handling 是否真的改善 post-cost edge。

### 核心问题

- 被 edge gate 挡掉的订单，事后 outcome 是否真的更差？
- dynamic slicing 是否过度保守，导致 fill delay/slippage 变差？
- HK/CN 的 board lot、research-only、market rule block 是否有效？

### 输出

- `execution_quality_evidence.json`
- `blocked_vs_allowed_execution_delta.csv`
- `market_rule_expost_review.csv`

## 12. P2：Live 变更四件套

### 目标

所有 live 相关变更必须具备：

- evidence
- approval
- rollback
- effect tracking

### 技术动作

- 为 live patch/action 增加 mandatory schema。
- dashboard control 对 live scope 做更严格 validation。
- weekly review 追踪 live change 后 5/20/60d 效果。

### 验收标准

- 没有 evidence 的 live patch 无法进入 applied。
- 没有 rollback note 的 live patch 无法 approval。
- live applied 后必须出现在后续 weekly effectiveness。

## 13. P3：策略层后续增强

### 范围控制

策略增强应排在 evidence suggestion governance 之后，不能先扩大策略自由度。

### 推荐方向

- signal_score -> expected_edge -> realized_edge 的校准曲线。
- market-specific signal weight profile。
- outcome horizon 分层：5d / 20d / 60d。
- candidate-only evidence 与 actual fill evidence 分开建模。
- regime state 对 position sizing 的解释性归因。

### 不建议立即做

- 不建议直接引入复杂 ML 自动调参。
- 不建议同时调多个策略参数。
- 不建议把 live 参数变更自动化。

## 14. 推荐执行顺序

### 第一批：治理闭环

1. 归档并提交当前 evidence lifecycle / dashboard v2 / strategy config 改动。
2. 实现 weekly strategy parameter suggestion artifact。
3. 把 suggestion 接入 dashboard control audit resolution。
4. 在 weekly review 中增加 suggestion effectiveness summary。

### 第二批：降低复杂度

5. 拆 `generate_dashboard.py` 的 HTML renderer。
6. 拆 weekly calibration / strategy parameter support。
7. 补 import boundary 和 helper-level tests。

### 第三批：参数接受规则

8. 实现 walk-forward acceptance rules。
9. 接入 market-specific stability 和 post-cost checks。
10. dashboard advanced 展示 acceptance summary。

### 第四批：生产治理

11. live patch 四件套 schema。
12. live patch dashboard validation。
13. live effect tracking weekly review。

## 15. 下一步建议

最合理的下一步是：

`feat(review): add evidence-driven single strategy parameter suggestions`

这一步规模可控，能直接承接当前已经完成的 evidence lifecycle，也不会提前扩大 live 风险。完成后，系统才算真正从“能看见证据”进入“能用证据驱动受治理的参数调整”。
