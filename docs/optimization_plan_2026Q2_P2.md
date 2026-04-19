# 2026Q2 P2：交易质量证据化与分市场校准

## 目标

P2 只解决一个核心问题：

> 不再继续发明更多 gate，而是验证 4/18 之后已经上线的动态 gate 是否真的提高了 post-cost 交易质量。

这一阶段的关键不是新增规则，而是把已有规则串成可回看的证据链，并做 ex-post 对照。

---

## Why now

P0 之后，系统应该已经具备：

- artifact contract / health / degraded-state visibility
- governance health summary
- dashboard 对半坏状态的可见性

P1 之后，dashboard 应该已经具备：

- 组合健康
- 信号与结果
- 执行质量
- 治理与变更
- US/HK/CN 一等视图

到这一步，才适合认真回答：

- 被 `BLOCKED_EDGE` 的单事后真的更差吗
- 被 `BLOCKED_MARKET_RULE` 的单，是规则天然该挡，还是 sizing / rounding 太粗糙
- recovery 放出来的仓位，是否真的改善了恢复期表现
- 各市场 execution 约束到底该怎么校准

---

## 范围

### in scope

- unified evidence table
- blocked vs allowed ex-post review
- recovery release review
- US/HK/CN market-specific execution calibration
- outcome-based success metrics

### out of scope

- 新增交易入口
- 新增 execution gate 大类
- 全局统一优化所有市场
- 大规模策略框架重写

---

## 交付清单

## P2-1：统一证据表

### 目标

把这条链串成一张事实表：

`candidate -> strategy control -> risk overlay -> market rule gate -> edge gate -> order -> fill -> 5/20/60d outcome`

### 建议文件

- `src/common/trading_quality_evidence.py`
- `src/tools/review_trading_quality.py`

### 最小字段集

- market
- portfolio_id
- symbol
- trade_date
- session
- strategy_rank
- signal_score
- expected_edge_bps
- strategy_control_label
- risk_budget_delta
- throttle_delta
- recovery_credit
- dominant_throttle_layer
- blocked_market_rule
- blocked_edge
- block_reason
- dynamic_edge_floor_bps
- dynamic_edge_buffer_bps
- liquidity_bucket
- slice_count
- adv_participation
- order_submitted
- filled
- realized_slippage_bps
- fill_delay_sec
- outcome_5d
- outcome_20d
- outcome_60d

### 输出形式建议

至少二选一：

- SQLite view
- `reports_investment_weekly/trading_quality_evidence.csv`

更理想是二者都做：

- 数据源层可 query
- 报表层可直接消费

### 伪代码

```python
def build_evidence_row(
    candidate: dict,
    strategy_control: dict,
    risk_overlay: dict,
    gate_decision: dict,
    order_row: dict | None,
    fill_row: dict | None,
    outcome_row: dict | None,
) -> dict:
    return {
        "market": candidate.get("market"),
        "portfolio_id": candidate.get("portfolio_id"),
        "symbol": candidate.get("symbol"),
        "trade_date": candidate.get("trade_date"),
        "session": candidate.get("session"),
        "strategy_rank": candidate.get("strategy_rank"),
        "signal_score": candidate.get("signal_score"),
        "expected_edge_bps": candidate.get("expected_edge_bps"),
        "strategy_control_label": strategy_control.get("label"),
        "risk_budget_delta": risk_overlay.get("risk_market_profile_budget_weight_delta"),
        "throttle_delta": risk_overlay.get("risk_throttle_weight_delta"),
        "recovery_credit": risk_overlay.get("risk_recovery_weight_credit"),
        "dominant_throttle_layer": risk_overlay.get("risk_dominant_throttle_layer"),
        "blocked_market_rule": gate_decision.get("blocked_market_rule"),
        "blocked_edge": gate_decision.get("blocked_edge"),
        "block_reason": gate_decision.get("block_reason"),
        "dynamic_edge_floor_bps": gate_decision.get("dynamic_edge_floor_bps"),
        "dynamic_edge_buffer_bps": gate_decision.get("dynamic_edge_buffer_bps"),
        "liquidity_bucket": gate_decision.get("dynamic_liquidity_bucket"),
        "slice_count": gate_decision.get("slice_count"),
        "adv_participation": gate_decision.get("adv_participation"),
        "order_submitted": bool(order_row),
        "filled": bool(fill_row),
        "realized_slippage_bps": (fill_row or {}).get("realized_slippage_bps"),
        "fill_delay_sec": (fill_row or {}).get("fill_delay_sec"),
        "outcome_5d": (outcome_row or {}).get("outcome_5d"),
        "outcome_20d": (outcome_row or {}).get("outcome_20d"),
        "outcome_60d": (outcome_row or {}).get("outcome_60d"),
    }
```

### 验收标准

- evidence row 至少覆盖 candidate / gate / fill / outcome 四段链路
- evidence 输出可被 weekly review 或 dashboard 读取
- 字段命名稳定，不需要下游反复猜测语义

---

## P2-2：blocked vs allowed ex-post review

### 目标

最关键的问题不是 gate 多聪明，而是：

- 被挡掉的单事后真的更差吗
- 放行的单是否真的更优

### 建议文件

- `src/common/trading_quality_metrics.py`
- `src/tools/review_trading_quality.py`

### 伪代码

```python
def compare_blocked_vs_allowed(rows: list[dict], *, market: str | None = None) -> dict:
    scoped = [r for r in rows if not market or r.get("market") == market]

    blocked_edge = [r for r in scoped if r.get("blocked_edge")]
    blocked_market_rule = [r for r in scoped if r.get("blocked_market_rule")]
    allowed = [r for r in scoped if not r.get("blocked_edge") and not r.get("blocked_market_rule")]

    return {
        "blocked_edge_count": len(blocked_edge),
        "blocked_market_rule_count": len(blocked_market_rule),
        "allowed_count": len(allowed),
        "blocked_edge_avg_outcome_5d": avg(blocked_edge, "outcome_5d"),
        "allowed_avg_outcome_5d": avg(allowed, "outcome_5d"),
        "blocked_edge_avg_outcome_20d": avg(blocked_edge, "outcome_20d"),
        "allowed_avg_outcome_20d": avg(allowed, "outcome_20d"),
        "blocked_edge_avg_outcome_60d": avg(blocked_edge, "outcome_60d"),
        "allowed_avg_outcome_60d": avg(allowed, "outcome_60d"),
    }
```

### 第一批问题必须回答

- `BLOCKED_EDGE` 单 vs `allowed` 单
- `BLOCKED_MARKET_RULE` 单 vs `allowed` 单
- `blocked` 单是否在 5/20/60d 上持续更差

### 验收标准

- 至少在 US/HK 上有 review 输出
- review 结果能落到 markdown/csv/json，而不只是 helper 函数

---

## P2-3：recovery release review

### 目标

判断 recovery 放出来的仓位是否真的改善了恢复期表现。

### 建议文件

- `src/common/trading_quality_metrics.py`
- `src/tools/review_trading_quality.py`

### 伪代码

```python
def evaluate_recovery_release_effect(rows: list[dict]) -> dict:
    released = [r for r in rows if float(r.get("recovery_credit") or 0) > 0]
    baseline = [r for r in rows if float(r.get("recovery_credit") or 0) == 0]

    return {
        "released_count": len(released),
        "baseline_count": len(baseline),
        "released_avg_outcome_20d": avg(released, "outcome_20d"),
        "baseline_avg_outcome_20d": avg(baseline, "outcome_20d"),
        "released_avg_drawdown": avg(released, "drawdown_20d"),
        "baseline_avg_drawdown": avg(baseline, "drawdown_20d"),
    }
```

### 验收标准

- 至少能输出 released vs baseline 对照
- 能明确看到 recovery 放量是否带来更快恢复，且 drawdown 未明显恶化

---

## P2-4：分市场 execution 校准

## US

### 重点问题

- deep liquidity 名字是否被过度 edge gate
- open / close session buffer 是否过严
- 大票是否被不必要挡掉

### 推荐指标

- deep-liquidity blocked-edge rate
- open/close bucket outcome gap
- session-specific slippage

## HK

### 重点问题

- board lot / odd lot mismatch
- dynamic limit buffer 是否过大
- slice_count 是否过于保守
- market-rule block 中多少本可通过 sizing 解决

### 推荐指标

- board-lot mismatch frequency
- market-rule avoidable block rate
- avg slice_count vs realized slippage
- dynamic limit buffer vs fill success

## CN

### 重点问题

- research-only / staged rollout 下的候选排序
- defensive budget 是否合理
- 不追求 aggressive automation

### 推荐指标

- top-ranked candidate outcome spread
- defensive budget vs outcome relationship
- staged rollout conversion summary

### 建议文件

- `src/tools/review_trading_quality.py`
- `src/common/trading_quality_metrics.py`

### 伪代码

```python
def build_market_calibration_review(rows: list[dict]) -> list[dict]:
    output = []
    for market in ["US", "HK", "CN"]:
        scoped = [r for r in rows if r.get("market") == market]
        output.append({
            "market": market,
            "blocked_vs_allowed": compare_blocked_vs_allowed(scoped),
            "recovery_effect": evaluate_recovery_release_effect(scoped),
            "market_rule_mismatch_rate": compute_market_rule_mismatch_rate(scoped),
            "avg_realized_slippage_bps": avg(scoped, "realized_slippage_bps"),
            "notes": build_market_calibration_notes(market, scoped),
        })
    return output
```

### 验收标准

- 至少 US/HK/CN 三个市场都有独立 review 摘要
- 不是只输出 market 列，而是 market-specific 建议

---

## P2-5：把交易质量定义成结果

### 成功标准

不再用“成交更多”当成功标准，统一改成结果指标：

1. top-ranked 候选 5/20/60d 持续优于中位数
2. turnover 降了，但 post-cost 不恶化
3. blocked edge 单事后表现弱于 allowed 单
4. recovery 期间仓位恢复更快，但 drawdown 不明显恶化
5. HK/CN 的 market-rule block 更可解释，board lot mismatch 明显下降

### 建议文件

- `src/common/trading_quality_metrics.py`
- `src/tools/review_trading_quality.py`
- `docs/current_status.md`（若成功标准被正式采用）

### 伪代码

```python
def build_trading_quality_scorecard(rows: list[dict]) -> dict:
    return {
        "top_ranked_outperforms_median_5d": evaluate_top_ranked_outperformance(rows, horizon="5d"),
        "top_ranked_outperforms_median_20d": evaluate_top_ranked_outperformance(rows, horizon="20d"),
        "top_ranked_outperforms_median_60d": evaluate_top_ranked_outperformance(rows, horizon="60d"),
        "turnover_down_without_post_cost_decay": evaluate_turnover_vs_post_cost(rows),
        "blocked_edge_weaker_than_allowed": evaluate_blocked_vs_allowed_expectation(rows),
        "recovery_faster_without_drawdown_worse": evaluate_recovery_expectation(rows),
    }
```

### 验收标准

- scorecard 能出 markdown/json/csv
- 至少 3 条成功标准被自动计算，而不是只写文档

---

## 建议执行顺序

### Step 1

先做 evidence table v1：

- 先串 candidate / gate / fill / outcome
- 不追求一开始所有字段都齐

### Step 2

做 blocked vs allowed 对照：

- 先从 US/HK 开始
- CN 先聚焦 ranking / defensive budget

### Step 3

做 recovery release review

### Step 4

做 market-specific calibration notes / summary

### Step 5

固化 trading quality scorecard

---

## 建议文件

- `src/common/trading_quality_evidence.py`
- `src/common/trading_quality_metrics.py`
- `src/tools/review_trading_quality.py`
- `tests/test_trading_quality_evidence.py`
- `tests/test_trading_quality_metrics.py`
- `tests/test_trading_quality_market_calibration.py`

---

## 推荐 PR 拆分

### PR 1

`feat(trading-quality): add unified evidence table`

### PR 2

`feat(trading-quality): add blocked-vs-allowed ex-post review`

### PR 3

`feat(trading-quality): add recovery-release review`

### PR 4

`feat(trading-quality): add US/HK/CN market-specific calibration summary`

### PR 5

`feat(trading-quality): add outcome-based quality scorecard`

---

## 测试计划

### 需要覆盖的场景

1. evidence row builder 字段链完整
2. blocked vs allowed 统计
3. recovery release 对照
4. US/HK/CN calibration summary 输出
5. scorecard 成功标准计算

### 伪代码

```python
def test_build_evidence_row_preserves_candidate_gate_fill_outcome_chain():
    ...


def test_compare_blocked_vs_allowed_reports_outcome_gap():
    ...


def test_recovery_release_review_outputs_released_vs_baseline():
    ...


def test_market_calibration_review_contains_us_hk_cn_sections():
    ...


def test_trading_quality_scorecard_uses_outcome_based_metrics():
    ...
```

---

## 本阶段完成定义

P2 完成必须满足：

- unified evidence table 已可生成
- blocked vs allowed 至少在 US/HK 上可 review
- recovery release review 可输出摘要
- market-specific calibration 已有初版
- trading quality scorecard 至少实现 3 条 outcome-based success metrics

---

## 本阶段提交 checklist

- [ ] evidence table 字段链是否完整
- [ ] candidate -> gate -> fill -> outcome 是否能被串起来
- [ ] blocked vs allowed 是否可统计
- [ ] recovery review 是否不是只看仓位恢复，还看 drawdown
- [ ] market-specific 校准是否明确区分 US/HK/CN
- [ ] 成功标准是否仍以 post-cost 结果为准，而不是“成交更多”
- [ ] 是否补了 evidence / metrics / calibration 测试
- [ ] 若 review 输出进入 dashboard，是否同步更新 `docs/current_status.md`
- [ ] 若 dashboard 读法变化，是否同步更新 `README.md`
