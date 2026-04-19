# 2026Q2 P1：Dashboard 信息架构 v2

## 目标

P1 只解决一个核心问题：

> 把已有复杂指标压缩成一眼能读懂的 dashboard 结构，让用户不需要自己在 JSON 和表格里拼上下文。

这一阶段不新增 execution gate，不扩新的 workflow 主入口，重点是把现有数据组织成可直接支持决策的界面结构。

---

## Why now

P0 之后，系统应该已经具备：

- artifact health / freshness / degraded-state visibility
- governance health summary
- 对缺失 artifact 的降级能力

在这个基础上，dashboard 才适合做信息架构重组。否则只是把旧的、不稳定的数据换一种摆法。

---

## 范围

### in scope

- dashboard 首页四块重组
- US / HK / CN 一等 market view
- weekly attribution waterfall
- signal / outcome / execution / governance 摘要块
- 部分输入缺失时的 block 级 fallback

### out of scope

- 新交易规则
- 新 execution gate
- 更细粒度的 ex-post calibration 逻辑
- 新的交易回测框架

---

## 交付清单

## P1-1：首页四块结构

首页只保留四块：

1. **组合健康**
2. **信号与结果**
3. **执行质量**
4. **治理与变更**

### 验收标准

- 首页不再是“多张散表堆叠”
- 每个 block 都回答一个明确问题
- 用户可以在 30 秒内读懂系统当前状态

---

## P1-2：组合健康块

### 要回答的问题

> 仓位为什么是现在这个样子

### 核心指标

- net exposure
- gross exposure
- short exposure
- 当前 risk budget
- throttle tightening
- recovery credit
- dominant throttle layer

### 建议文件

- `src/common/dashboard_market_views.py`
- `src/common/dashboard_blocks.py`

### 伪代码

```python
def build_portfolio_health_block(bundle: dict) -> dict:
    risk = bundle.get("risk_overlay_summary", {})
    return {
        "title": "组合健康",
        "status": infer_block_status(risk),
        "headline": {
            "net_exposure": risk.get("dynamic_net_exposure"),
            "gross_exposure": risk.get("dynamic_gross_exposure"),
            "short_exposure": risk.get("dynamic_short_exposure"),
            "risk_budget": risk.get("effective_risk_budget"),
            "throttle_tightening": risk.get("risk_throttle_weight_delta"),
            "recovery_credit": risk.get("risk_recovery_weight_credit"),
            "dominant_layer": risk.get("risk_dominant_throttle_layer"),
        },
        "summary": build_portfolio_health_explanation(risk),
    }
```

### 验收标准

- 缺少部分 risk overlay 字段时仍可渲染
- block 有人类可读 summary
- `dominant throttle layer` 能被单独显示

---

## P1-3：信号与结果块

### 要回答的问题

> 策略本身还有没有 edge

### 核心指标

- top-ranked 候选 5/20/60d outcome spread
- `signal_score -> expected_edge -> realized_edge` 偏差
- 每市场 post-cost alpha 是否仍为正

### 建议文件

- `src/common/dashboard_signal_quality.py`

### 伪代码

```python
def build_signal_outcome_block(bundle: dict) -> dict:
    outcome = bundle.get("signal_outcome_summary", {})
    return {
        "title": "信号与结果",
        "status": infer_signal_status(outcome),
        "headline": {
            "spread_5d": outcome.get("top_ranked_spread_5d"),
            "spread_20d": outcome.get("top_ranked_spread_20d"),
            "spread_60d": outcome.get("top_ranked_spread_60d"),
            "edge_gap": outcome.get("expected_realized_edge_gap"),
            "post_cost_alpha_sign": outcome.get("post_cost_alpha_sign"),
        },
        "summary": build_signal_summary(outcome),
    }
```

### 验收标准

- 至少显示 5d / 20d / 60d 三个 horizon
- 缺 outcome 样本时能 warning，而不是空白

---

## P1-4：执行质量块

### 要回答的问题

> 交易差是因为成本太高，还是 gate 太严

### 核心指标

- blocked by market rule
- blocked by edge
- avg realized slippage
- slice count
- fill delay
- adv participation
- 按 market + session + liquidity bucket 拆分

### 建议文件

- `src/common/dashboard_execution_quality.py`

### 伪代码

```python
def build_execution_quality_block(bundle: dict) -> dict:
    q = bundle.get("execution_quality_summary", {})
    return {
        "title": "执行质量",
        "status": infer_execution_status(q),
        "headline": {
            "blocked_market_rule": q.get("blocked_market_rule_order_count"),
            "blocked_edge": q.get("blocked_edge_order_count"),
            "avg_realized_slippage_bps": q.get("avg_realized_slippage_bps"),
            "avg_slice_count": q.get("avg_slice_count"),
            "avg_fill_delay_sec": q.get("avg_fill_delay_sec"),
            "avg_adv_participation": q.get("avg_adv_participation"),
        },
        "breakdown": q.get("by_market_session_liquidity_bucket", []),
        "summary": build_execution_quality_summary(q),
    }
```

### 验收标准

- block 缺 breakdown 时仍可显示 headline
- 至少能区分 `blocked by market rule` 和 `blocked by edge`

---

## P1-5：治理与变更块

### 要回答的问题

> 系统最近到底改了什么

### 核心指标

- 本周 tuning actions
- pending patch reviews
- 最近生效参数
- 生效后 1w / 2w / 4w 跟踪

### 建议文件

- `src/common/dashboard_governance_views.py`

### 伪代码

```python
def build_governance_change_block(bundle: dict) -> dict:
    gov = bundle.get("governance_health", {})
    return {
        "title": "治理与变更",
        "status": gov.get("status", "warning"),
        "headline": {
            "pending_tuning_actions": gov.get("pending_count"),
            "oldest_pending_days": gov.get("oldest_pending_age_days"),
            "approve_ratio_4w": gov.get("approve_ratio_4w"),
            "reject_ratio_4w": gov.get("reject_ratio_4w"),
            "superseded_ratio_4w": gov.get("superseded_ratio_4w"),
        },
        "recent_parameter_changes": bundle.get("recent_parameter_changes", []),
        "summary": gov.get("summary", "-"),
    }
```

### 验收标准

- 至少能列出最近变更与 pending 状态
- governance block 缺部分输入时仍然 warning 渲染

---

## P1-6：US / HK / CN 一等视图

### 目标

不要只在表里带一列 `market`，直接给 market card / tab：

- US
- HK
- CN

### 每个市场的固定上下文

- US：趋势优先 / 深流动性 / session 风险
- HK：board lot / odd lot / 成本 / sliced limit
- CN：research-only or staged / 低频 / 防守预算

### 建议文件

- `src/common/dashboard_market_views.py`

### 伪代码

```python
MARKET_CONTEXT = {
    "US": "趋势优先 / 深流动性 / session 风险",
    "HK": "board lot / odd lot / 成本 / sliced limit",
    "CN": "research-only / staged / 低频 / 防守预算",
}


def build_market_tabs(bundle: dict) -> list[dict]:
    tabs = []
    for market in ["US", "HK", "CN"]:
        tabs.append({
            "market": market,
            "context": MARKET_CONTEXT[market],
            "portfolio_health": build_portfolio_health_block(bundle.get(market, {})),
            "signal_outcome": build_signal_outcome_block(bundle.get(market, {})),
            "execution_quality": build_execution_quality_block(bundle.get(market, {})),
            "governance": build_governance_change_block(bundle.get(market, {})),
        })
    return tabs
```

### 验收标准

- US/HK/CN 均有默认 market tab/card
- market-specific context 在 UI 中可见

---

## P1-7：weekly attribution waterfall

### 目标

做一张简单但高价值的周度 waterfall：

- Strategy drag
- Risk budget drag
- Throttle drag
- Recovery credit
- Execution drag

### 要回答的问题

> 这周少赚，是策略问题、风险约束问题，还是执行问题

### 建议文件

- `src/common/dashboard_waterfall.py`

### 伪代码

```python
def build_weekly_attribution_waterfall(bundle: dict) -> list[dict]:
    attribution = bundle.get("weekly_attribution_summary", {})
    return [
        {"label": "Strategy drag", "value": attribution.get("strategy_drag", 0.0)},
        {"label": "Risk budget drag", "value": attribution.get("risk_budget_drag", 0.0)},
        {"label": "Throttle drag", "value": attribution.get("throttle_drag", 0.0)},
        {"label": "Recovery credit", "value": attribution.get("recovery_credit", 0.0)},
        {"label": "Execution drag", "value": attribution.get("execution_drag", 0.0)},
    ]
```

### 验收标准

- 缺某一项字段时默认为 0 或 warning
- waterfall 可用于 dashboard 首页或 market tab

---

## 建议执行顺序

### Step 1

先抽 dashboard block builder：

- 不改太多数据源
- 先把 layout 和 block 结构拉出来

### Step 2

接 signal / execution / governance 三块

### Step 3

接 market tabs/cards

### Step 4

最后接 weekly attribution waterfall

---

## 建议文件

- `src/common/dashboard_blocks.py`
- `src/common/dashboard_market_views.py`
- `src/common/dashboard_signal_quality.py`
- `src/common/dashboard_execution_quality.py`
- `src/common/dashboard_governance_views.py`
- `src/common/dashboard_waterfall.py`
- `tests/test_dashboard_blocks_v2.py`
- `tests/test_dashboard_market_tabs.py`
- `tests/test_dashboard_waterfall.py`

---

## 推荐 PR 拆分

### PR 1

`feat(dashboard): add homepage v2 block builders`

### PR 2

`feat(dashboard): add signal, execution, and governance summary blocks`

### PR 3

`feat(dashboard): add US/HK/CN market tabs`

### PR 4

`feat(dashboard): add weekly attribution waterfall`

---

## 测试计划

### 需要覆盖的场景

1. 组合健康块输入不完整
2. signal/outcome 样本缺失
3. execution breakdown 缺失
4. governance block 缺 recent changes
5. market tab 仅有部分市场数据
6. waterfall 缺若干字段

### 伪代码

```python
def test_portfolio_health_block_can_render_with_partial_risk_overlay():
    ...


def test_execution_quality_block_marks_warning_when_breakdown_missing():
    ...


def test_market_tabs_include_default_context_for_us_hk_cn():
    ...


def test_waterfall_can_render_with_partial_attribution_fields():
    ...
```

---

## 本阶段完成定义

P1 完成必须满足：

- dashboard 首页四块结构完成
- US/HK/CN 已成为一等视图
- weekly attribution waterfall 已可渲染
- 缺部分输入时 block 仍可 warning/fallback

---

## 本阶段提交 checklist

- [ ] 首页结构是否仍然围绕四个核心问题
- [ ] 新增指标是否明确回答一个用户问题
- [ ] market-specific context 是否可见
- [ ] 任一 block 缺部分输入时是否仍可渲染
- [ ] waterfall 是否能在字段缺失时降级
- [ ] 是否补了 block / market / waterfall 测试
- [ ] 若 dashboard 读法明显变化，是否更新 `README.md`
- [ ] 是否更新 `docs/current_status.md`
