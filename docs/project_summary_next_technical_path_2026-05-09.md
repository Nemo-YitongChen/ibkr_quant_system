# 项目总结与后续技术路径 - 2026-05-09

## 1. 执行摘要

`ibkr_quant_system` 当前已经不是单个策略脚本，而是一个围绕 IB Gateway 构建的个人投资运营系统。当前主链路是：

`research -> paper ledger -> broker execution -> guard/opportunity -> weekly evidence review -> dashboard/supervisor governance`

当前阶段可以概括为：

**Evidence-driven operation with controlled parameter governance**，也就是“基于证据运行，并通过受控流程调整参数”。

项目已经具备 dashboard、weekly evidence、evidence action、control audit、strategy parameter suggestion、IBKR request telemetry 等基础能力。后续不应该继续堆更多零散 dashboard 字段或策略开关，而应该把已有 evidence loop 推进到可量化、可审计、可回滚、可验证效果的治理闭环。

## 2. 当前项目结构

### 2.1 主要运行层

1. **IBKR / market data access**
   - `src/ibkr/market_data.py`
   - `src/ibkr/investment_orders.py`
   - `src/ibkr/account.py`
   - `src/ibkr/orders.py`
   - `src/offhours/ib_setup.py`

2. **Research and candidate generation**
   - `src/tools/generate_investment_report.py`
   - `src/tools/generate_trade_report.py`
   - `src/analysis/investment.py`
   - `src/analysis/investment_backtest.py`
   - `src/analysis/investment_shadow_ml.py`

3. **Strategy / risk / execution**
   - `src/strategies/engine_strategy.py`
   - `src/signals/fusion.py`
   - `src/app/investment_engine.py`
   - `src/app/investment_guard.py`
   - `src/app/investment_opportunity.py`
   - `src/portfolio/investment_allocator.py`

4. **Evidence / weekly review / calibration**
   - `src/common/investment_evidence.py`
   - `src/common/evidence_focus_actions.py`
   - `src/common/strategy_parameter_suggestions.py`
   - `src/tools/review_investment_weekly.py`
   - `src/tools/review_weekly_*_support.py`

5. **Dashboard / governance**
   - `src/tools/generate_dashboard.py`
   - `src/tools/dashboard_blocks.py`
   - `src/common/dashboard_rendering.py`
   - `src/common/dashboard_control_audit.py`
   - `src/app/dashboard_control_audit.py`

6. **Supervisor / operational orchestration**
   - `src/app/supervisor.py`
   - `src/app/supervisor_support.py`
   - `src/app/supervisor_patch_support.py`
   - `config/supervisor.yaml`

### 2.2 当前工程规模

当前大致规模：

- `src` 与 `tests` 下 Python 文件约 `203` 个。
- `config` 下配置文件约 `102` 个。
- `docs` 下文档约 `64` 个。
- `src` 代码约 `65.8k` 行。
- `tests` 代码约 `25.2k` 行。

当前最大复杂度集中点：

- `src/tools/generate_dashboard.py`：约 `10.8k` 行。
- `src/app/supervisor.py`：约 `4.9k` 行。
- `src/tools/review_weekly_feedback_support.py`：约 `3.1k` 行。
- `src/app/investment_engine.py`：约 `2.5k` 行。
- `src/tools/generate_investment_report.py`：约 `2.3k` 行。
- `src/common/storage.py`：约 `1.9k` 行。

这些文件已经是后续迭代的主要工程风险点。

## 3. 当前已经稳定成型的能力

### 3.1 Evidence Focus Lifecycle

此前规划的 Evidence Focus Lifecycle 基线已经基本完成：

- 时间敏感测试已显式传 `now=`，避免日期漂移。
- Evidence action 已接入 dashboard control audit。
- action 最新状态可以从 audit history 合成。
- Weekly review 已输出 evidence focus effectiveness。
- Blocked-vs-allowed review 已映射到 action 决策。
- Dashboard v2 已完成 home / advanced block 分类。
- 第一批策略技术债已完成：primary signal weights 和 mid-regime sizing 已配置化，并有 pure strategy tests 覆盖。

结论：Step 1-7 不应该重复实现。后续应该从“基础链路已完成”进入“效果追踪和治理闭环”。

### 3.2 Strategy Parameter Suggestions

项目已经开始具备受治理的参数建议能力：

- Weekly review 可以输出 `weekly_strategy_parameter_suggestions.json/csv`。
- Suggestion 是 read-only，必须保持 `auto_apply=0`。
- 每条 suggestion 都关联 evidence artifact 和 evidence key。
- Dashboard control audit 可以记录 strategy parameter suggestion 的 resolution status。
- Weekly review 可以汇总 suggestion effectiveness。

这是正确方向：evidence 可以产生建议，但不能静默改 live 配置。

### 3.3 IBKR Gateway Load Reduction

最近一轮 Gateway 优化已经降低了部分不必要请求：

- `src/ibkr/market_data.py` 已有 daily HMDS disk cache。
- `config/ibkr*.yaml` 已有 `market_data` cache/retry 配置。
- Labeling 会先使用 external/cache history，再 fallback 到 IBKR。
- Broker snapshot sync 已支持 supervisor cycle 内 single-flight 复用。
- Supervisor 会通过 `ibkr_task_min_gap_sec` 拉开 IBKR subprocess 启动间隔。
- `src/common/ibkr_telemetry.py` 已记录 lightweight request telemetry，并输出 weekly request summary。

结论：下一步不应该继续盲目加缓存，而应该建立 request budget 和 weekly/dashboard health 检查，验证这些优化是否真的降低 Gateway 压力。

### 3.4 CI / Packaging

项目已有较完整的工程入口：

- `pyproject.toml` 定义了 supervisor、dashboard、weekly review、execution、guard、opportunity、walk-forward、labeling、report 等 console scripts。
- GitHub Actions 已包含 compile、pip check、import smoke、guardrail tests、integration tests 和 full remaining suite。
- 另有 smoke / fill audit / order audit / structure check workflows。

这已经足够支撑小步、带测试的重构。

## 4. 当前核心瓶颈

### 4.1 Governance loop 还没有完全 outcome-driven

当前系统能生成 evidence、action、parameter suggestion，也能记录 audit resolution。剩下的问题是：这些被处理或应用的建议是否真的改善 post-cost outcome。

后续必须打通：

`suggestion/action -> resolution -> applied state -> future 5/20/60d outcome -> effectiveness verdict`

否则 weekly review 只能“提出建议”，但不能证明建议是否真的改善策略质量。

### 4.2 IBKR Gateway load 已可见，但还没有预算

IBKR request telemetry 已经存在，但还没有 budget policy。

后续需要：

- per-market weekly Gateway request budget。
- actual Gateway request count 超预算时给 soft warning。
- cache-hit ratio 可见。
- dashboard 顶部 ops health 纳入 Gateway load。

暂时不建议做 long-lived Gateway session broker。共享 Gateway broker 是更大的架构改动，应等 telemetry 证明 subprocess churn 仍然是主要瓶颈后再做。

### 4.3 Execution quality evidence 仍不完整

当前已有 unified weekly evidence 和 blocked-vs-allowed review，但 execution quality 还需要更强的决策级链路：

`candidate -> strategy control -> risk overlay -> execution gate -> order -> fill -> 5/20/60d outcome`

关键字段应包括 expected edge、required edge、dynamic liquidity bucket、ADV scale、slice count、realized slippage、realized edge 和 forward outcomes。

### 4.4 大文件复杂度已经影响安全迭代

`generate_dashboard.py`、`supervisor.py`、weekly support 模块都已经过大。后续新增能力必须继续拆分纯 helper。

拆分目标不是单纯减少行数，而是隔离 contract：

- data loading
- artifact normalization
- block building
- HTML rendering
- control payload handling
- audit/effectiveness aggregation

### 4.5 配置复制会继续扩大维护成本

市场级 YAML 数量已经很多。最近新增 `market_data` 配置块是必要的，但也说明复制问题会继续扩大。

后续应该推进：

`base defaults -> market override -> mode override -> local override`

但第一批应该只覆盖低风险字段，例如 strategy defaults、report scoring、market-data cache knobs。live execution safety fields 应继续显式配置，等 layering tests 足够稳定后再考虑。

## 5. 推荐后续优先级

### P0：归档当前批次并建立基线

#### 目标

继续做新功能前，先把当前未提交的大批改动归档、验证、提交并推送。

#### 原因

当前工作区包含 evidence lifecycle、strategy suggestions、Gateway telemetry/cache、dashboard、weekly review、strategy config、tests 等多组相关改动。继续叠加会让后续 regression triage 变困难。

#### 验收标准

- 每个完成能力都有 change archive。
- Targeted tests 通过。
- Full CI 交给 GitHub 跑。
- commit scope 可 bisect。

### P0：新增 IBKR Gateway request budgets

#### 目标

把 IBKR telemetry 从“有记录”推进到“有运行护栏”。

#### 技术路径

1. 在 `config/supervisor.yaml` 或新增 `config/ibkr_gateway_budgets.yaml` 中配置预算。
2. 使用 `weekly_ibkr_request_summary.json` 作为输入 artifact。
3. 计算每个 market 的：
   - gateway request count
   - cache-hit count
   - cache-hit ratio
   - request count by kind
   - top request-heavy tool
4. 在 dashboard ops health 中增加：
   - `ok`：低于预算
   - `warning`：超过 soft budget
   - `degraded`：连续超预算或 telemetry 缺失/过旧
5. 保持只读，不直接阻止交易任务。

#### 建议第一版配置

```yaml
ibkr_gateway_budgets:
  enabled: true
  default_weekly_gateway_request_budget: 1500
  stale_telemetry_warning_hours: 72
  markets:
    US:
      weekly_gateway_request_budget: 2000
    HK:
      weekly_gateway_request_budget: 1500
    XETRA:
      weekly_gateway_request_budget: 800
```

#### 验收标准

- Weekly review 输出 Gateway budget status。
- Dashboard Ops Health 展示 Gateway budget state。
- telemetry 缺失时可见但不 crash。
- 测试覆盖 under-budget、over-budget、missing telemetry、stale telemetry。

### P0：闭合 strategy suggestion effectiveness loop

#### 目标

让参数建议不只停留在“生成”和“acknowledge”，而是能追踪应用后的效果。

#### 技术路径

1. 扩展 suggestion effectiveness，追踪 later weekly windows。
2. 将 applied suggestion id 关联到后续 outcome evidence。
3. 生成效果 verdict：
   - `IMPROVED`
   - `NO_CLEAR_CHANGE`
   - `DEGRADED`
   - `INSUFFICIENT_FOLLOWUP_SAMPLE`
4. `ACKNOWLEDGED` 只代表已读，不等于 applied。
5. `REJECTED` 和 `SUPERSEDED` 保留 audit history，但不进入 improvement scoring。

#### 验收标准

- applied suggestion 能追溯到后续 5/20/60d outcome。
- weekly review 显示该 suggestion 是否改善 post-cost evidence。
- dashboard governance block 显示 open、handled、applied、stale、effectiveness。
- 不引入自动 YAML 写入。

### P1：建立 execution quality evidence

#### 目标

验证 edge gate、buffer、slicing、market-rule handling、risk throttle 是否真的改善 post-cost edge。

#### 技术路径

1. 新增或扩展 decision-level unified evidence table。
2. 至少包含：
   - `signal_score`
   - `expected_edge_bps`
   - `expected_cost_bps`
   - `edge_gate_threshold_bps`
   - `blocked_market_rule_order_count`
   - `blocked_edge_order_count`
   - `dynamic_liquidity_bucket`
   - `dynamic_order_adv_pct`
   - `slice_count`
   - `risk_market_profile_budget_weight_delta`
   - `risk_throttle_weight_delta`
   - `risk_recovery_weight_credit`
   - `realized_slippage_bps`
   - `realized_edge_bps`
   - `outcome_5d`
   - `outcome_20d`
   - `outcome_60d`
3. 生成 blocked-vs-allowed execution review：
   - blocked outperformed allowed
   - blocking helped
   - insufficient sample
   - market-rule false positive
4. Dashboard advanced 增加 execution quality evidence block。

#### 验收标准

- Weekly review 能回答被挡掉的订单是否真的更差。
- HK/CN market-rule block 与 edge-gate block 分开评价。
- slicing 保守程度可以通过 slippage 和 fill-delay evidence 评价。
- 不引入 live 自动改参。

### P1：继续大文件拆分

#### 目标

通过拆分纯 helper 降低变更风险。

#### 建议顺序

1. `src/tools/generate_dashboard.py`
   - 拆 HTML renderer
   - 拆 artifact/data loader
   - 拆 advanced block preview helper
   - 保持 JSON contract 不变

2. `src/tools/review_weekly_feedback_support.py`
   - 拆 evidence support
   - 拆 calibration support
   - 拆 strategy parameter suggestion support
   - 保留一轮兼容 wrapper

3. `src/app/supervisor.py`
   - 拆 IBKR task spacing 和 telemetry env helper
   - 拆 broker snapshot reuse helper
   - 拆 dashboard-control state helper

#### 验收标准

- 每个新 support 模块都有 import smoke test。
- 旧 JSON artifacts key 不删除。
- simple / advanced dashboard 不丢数据。
- helper extraction 不混入交易行为变更。

### P1：推进 config defaults + overrides

#### 目标

减少市场 YAML 复制，并让字段来源可解释。

#### 技术路径

1. 先覆盖非 live-critical config：
   - strategy defaults
   - report scoring
   - market-data cache knobs
2. 复用 `src/common/config_layers.py`，不要再造第二套 layering。
3. 增加 source tracing，能说明字段来自 base、market、mode 还是 local override。
4. live execution controls 暂时继续显式配置。

#### 验收标准

- US/HK/CN/ASX/XETRA 加载结果与当前行为等价。
- 缺少 market override 时自动走 base default。
- 测试覆盖 source tracing 和 market override precedence。

### P2：Walk-forward acceptance rules

#### 目标

把 walk-forward 从“有 review”推进到“有接受规则”。

#### 硬规则

- 不接受只在单窗最好的参数。
- 必须看 post-cost，不看 pre-cost 幻觉收益。
- 必须看 market-specific 稳定性，不只看全市场平均。
- 至少连续 3 个 validation windows 不过度退化。
- turnover 降低后，post-cost edge 必须不变或更好。
- top-ranked candidates 应在 5/20/60d 持续优于中位数。

#### 建议产物

- `walk_forward_acceptance_summary.json`
- `walk_forward_parameter_candidates.csv`
- `walk_forward_market_stability.json`

#### 验收标准

- 即使 global average 好，market-specific 不稳定也能拒绝参数。
- Dashboard advanced 显示 accepted / rejected reason。
- Strategy parameter suggestion 进入 paper 之后能引用 walk-forward acceptance status。

### P2：Live change 四件套治理

#### 目标

live 相关变更必须具备：

- evidence
- approval
- rollback
- effect tracking

#### 技术路径

1. 增加 live patch schema。
2. 校验 evidence artifact 和 evidence key。
3. 要求 approval identity / status。
4. 要求 rollback note 和 previous value。
5. 要求 follow-up effect tracking window。

#### 验收标准

- 没有 evidence 和 rollback 的 live change 不能进入 `APPLIED`。
- Dashboard 将 live change lifecycle 与 paper/shadow 分开展示。
- Weekly review 追踪 live change 后续 5/20/60d outcome。

## 6. 暂时不建议做的事

当前不建议：

- 不要自动应用 live 参数。
- 不要引入大范围 ML 自动调参。
- 不要在 telemetry 尚未证明必要前实现 long-lived shared Gateway broker。
- 不要一次调多个 primary strategy fields。
- 不要把 dashboard 展示改动和交易行为改动混在同一个 patch。

## 7. 推荐立即下一步

最高价值的下一步是：

**新增 IBKR Gateway request budgets，并接入 weekly review 与 dashboard ops health。**

原因：

- 最近已经完成 Gateway telemetry、cache、single-flight、task spacing。
- 现在缺的是衡量这些优化是否真的降低 Gateway 压力。
- 这一步是 read-only，风险低。
- 它能避免过早进入复杂的 long-lived Gateway broker 架构。

执行记录：

- 2026-05-09 已实现 Gateway request budget 基线。
- 归档见 `docs/change_archive_2026-05-09_ibkr_gateway_request_budgets.md`。
- 2026-05-10 已实现 strategy suggestion applied follow-up、weekly follow-up artifacts、walk-forward acceptance/stability aliases，并修复 weekly review `generated_at` smoke 问题。
- 2026-05-10 已修复 investment report `MarketDataService` NameError，weekly review 已接入 dashboard control audit history，dashboard advanced 已接入 walk-forward acceptance/stability block。
- 2026-05-10 已将 strategy suggestion follow-up 扩展到 5/20/60d outcome spread 与 realized/post-cost edge 字段。
- 归档见 `docs/change_archive_2026-05-10_followup_contracts.md`。

之后按顺序推进：

1. Dashboard / weekly / supervisor helper extraction。
2. Config defaults + overrides。
3. 将 live change 四件套治理继续保持 read-only 检查，不做自动应用。
4. 将 walk-forward acceptance status 反向引用到 strategy suggestion markdown/dashboard action 文案。
5. 在 dashboard control 中增加 strategy suggestion 专用 review/apply/reject 操作入口。

## 8. 下一阶段成功标准

下一阶段完成时，系统应该能直接从 weekly artifacts 和 dashboard JSON 回答：

- 哪些 market 正在给 IBKR Gateway 造成压力，原因是什么？
- 哪些 evidence actions 仍 open 或 stale？
- 哪些 strategy parameter suggestions 被 applied、rejected 或 superseded？
- applied suggestions 是否改善 5/20/60d post-cost outcome？
- edge gate 挡掉的订单是否真的更差？
- slicing / liquidity controls 是否降低 realized slippage？
- 哪一层 risk overlay 降低了 exposure，机会成本是否合理？
- live changes 是否具备 evidence、approval、rollback、effect tracking？

在这些问题能稳定回答之前，项目应该优先提升 evidence quality 和 governance，而不是增加策略复杂度。
