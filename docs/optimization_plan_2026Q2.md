# 2026Q2 优化执行计划

本文把后续优化拆成三条主线，并进一步落成：

- 目标
- 交付顺序
- 建议模块/文件
- 数据结构
- 伪代码
- 测试策略
- 每次提交必须执行的 checklist

本文不再讨论“是否要做”，而是聚焦“如何做、先做什么、做到什么程度算完成”。

---

# 0. 总体原则

## 0.1 本季度不再优先扩新入口

当前项目主线已经有：

- workflow / contract / CI / smoke test
- outcome attribution
- walk-forward review
- layered risk overlay
- market-rule-aware execution
- governance / tuning action 方向

所以 2026Q2 的主目标不是继续堆新入口，而是把已有主线做稳、做清楚、做出证据。

## 0.2 先后顺序

本季度建议顺序：

1. **稳定性底座**
2. **dashboard 信息架构重组**
3. **交易质量证据化与校准**

原因很简单：

- 没有 artifact health / freshness / degraded-state visibility，dashboard 再漂亮也不可靠
- 没有统一 evidence table，交易质量优化容易凭感觉调整 gate

## 0.3 本季度的完成标准

到 2026Q2 结束时，项目应该能回答四个问题：

1. 当前系统是不是健康、是不是半坏
2. 当前仓位为什么长成这样
3. 当前策略是否还在产生正的 post-cost edge
4. 当前系统最近改了什么，改后效果如何

---

# 1. 线路 A：让系统更稳定

## 1.1 目标

让系统从“代码能跑”升级为“半坏状态可见、跨产物不一致可见、降级行为可见”。

当前最容易出问题的不是 Python exception，而是：

- 文件存在，但字段已经落后
- weekly review 是旧版本
- execution summary 和 risk history 不同步
- governance/tuning action 有产物，但 dashboard 没识别到
- artifact 缺字段时，页面静默变空白

## 1.2 本线交付物

本线最终至少交付：

1. artifact contract 定义层
2. artifact health / freshness 计算层
3. dashboard degraded-state 降级展示
4. governance health 纳入运行健康
5. degraded-input regression tests

## 1.3 建议模块拆分

建议新增或重构以下模块：

- `src/common/artifact_contracts.py`
- `src/common/artifact_health.py`
- `src/common/artifact_loader.py`
- `src/tools/generate_dashboard.py`（调用 health 层，不直接散落判断）
- `src/tools/review_investment_weekly.py`（输出 schema_version / generated_at）
- `tests/test_artifact_health.py`
- `tests/test_dashboard_degraded_inputs.py`
- `tests/test_weekly_review_backward_compat.py`
- `tests/test_governance_health_summary.py`

## 1.4 数据结构建议

每个主 artifact 在输出时都带统一头：

```json
{
  "schema_version": "2026Q2.v1",
  "generated_at": "2026-04-20T08:30:00+10:00",
  "producer": "review_investment_weekly",
  "market": "US",
  "portfolio_id": "US:watchlist",
  "payload": { ... }
}
```

CSV 产物如果不方便直接包头，至少在同目录增加 sidecar json：

- `weekly_execution_summary.csv`
- `weekly_execution_summary.meta.json`

sidecar 例子：

```json
{
  "schema_version": "2026Q2.v1",
  "generated_at": "2026-04-20T08:30:00+10:00",
  "required_columns": ["market", "portfolio_id", "submitted_order_rows", "filled_order_rows"]
}
```

## 1.5 统一健康状态

不要只用 fresh/stale 二元判断，统一成三层：

- `ready`
- `warning`
- `degraded`

建议定义：

- `ready`：文件存在、schema 匹配、关键字段齐全、时间新鲜
- `warning`：文件存在，但时间旧、缺非关键字段、部分产物没到齐
- `degraded`：关键字段缺失、schema 不兼容、跨 artifact 明显冲突

## 1.6 伪代码：artifact contract 注册表

```python
# src/common/artifact_contracts.py
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

@dataclass(frozen=True)
class ArtifactContract:
    artifact_key: str
    path_pattern: str
    schema_version: str
    required_fields: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    freshness_hours: int | None = None
    loader_kind: str = "json"  # json/csv/meta-json/custom
    compatibility_readers: tuple[str, ...] = ()

ARTIFACT_CONTRACTS = {
    "weekly_review_summary": ArtifactContract(
        artifact_key="weekly_review_summary",
        path_pattern="reports_investment_weekly/weekly_review_summary.json",
        schema_version="2026Q2.v1",
        required_fields=(
            "schema_version",
            "generated_at",
            "portfolio_summary",
        ),
        freshness_hours=72,
        loader_kind="json",
    ),
    "weekly_execution_summary": ArtifactContract(
        artifact_key="weekly_execution_summary",
        path_pattern="reports_investment_weekly/weekly_execution_summary.csv",
        schema_version="2026Q2.v1",
        required_columns=(
            "market",
            "portfolio_id",
            "submitted_order_rows",
            "filled_order_rows",
        ),
        freshness_hours=72,
        loader_kind="csv_meta",
    ),
}
```

## 1.7 伪代码：artifact health 计算

```python
# src/common/artifact_health.py
from dataclasses import dataclass
from datetime import datetime, timezone

@dataclass
class ArtifactHealth:
    artifact_key: str
    status: str              # ready/warning/degraded
    summary: str
    generated_at: str
    schema_version: str
    age_hours: float | None
    missing_fields: list[str]
    missing_columns: list[str]
    warnings: list[str]


def evaluate_artifact_health(contract: ArtifactContract, payload) -> ArtifactHealth:
    if payload is None:
        return ArtifactHealth(
            artifact_key=contract.artifact_key,
            status="degraded",
            summary="artifact missing",
            generated_at="",
            schema_version="",
            age_hours=None,
            missing_fields=list(contract.required_fields),
            missing_columns=list(contract.required_columns),
            warnings=["missing artifact"],
        )

    schema_version = read_schema_version(payload)
    generated_at = read_generated_at(payload)
    missing_fields = check_required_fields(payload, contract.required_fields)
    missing_columns = check_required_columns(payload, contract.required_columns)
    age_hours = compute_age_hours(generated_at)

    warnings = []
    status = "ready"

    if schema_version != contract.schema_version:
        status = "warning"
        warnings.append(f"schema mismatch: got={schema_version} expected={contract.schema_version}")

    if missing_fields or missing_columns:
        status = "degraded"
        warnings.append("required fields/columns missing")

    if contract.freshness_hours is not None and age_hours is not None and age_hours > contract.freshness_hours:
        if status == "ready":
            status = "warning"
        warnings.append(f"stale artifact: age_hours={age_hours:.1f}")

    return ArtifactHealth(
        artifact_key=contract.artifact_key,
        status=status,
        summary=build_summary(contract.artifact_key, status, warnings, age_hours),
        generated_at=generated_at,
        schema_version=schema_version,
        age_hours=age_hours,
        missing_fields=missing_fields,
        missing_columns=missing_columns,
        warnings=warnings,
    )
```

## 1.8 伪代码：跨 artifact 一致性检查

```python
# src/common/artifact_health.py

def evaluate_cross_artifact_consistency(bundle: dict[str, ArtifactHealth], loaded_payloads: dict[str, object]) -> list[dict]:
    rows = []

    exec_ts = read_generated_at(loaded_payloads.get("weekly_execution_summary"))
    risk_ts = read_generated_at(loaded_payloads.get("risk_history_summary"))
    gov_ts = read_generated_at(loaded_payloads.get("governance_actions"))

    if exec_ts and risk_ts and abs(hours_between(exec_ts, risk_ts)) > 24:
        rows.append({
            "status": "warning",
            "summary": "execution summary 与 risk history 时间差超过 24h",
        })

    if gov_ts and exec_ts and gov_ts > exec_ts:
        rows.append({
            "status": "warning",
            "summary": "governance action 晚于 execution summary，dashboard 可能未看到最新运行结果",
        })

    return rows
```

## 1.9 伪代码：dashboard 降级展示

```python
# src/tools/generate_dashboard.py

def build_dashboard_sections(...) -> dict:
    artifact_bundle = load_dashboard_artifacts(...)
    artifact_health_rows = build_artifact_health_rows(artifact_bundle)
    consistency_rows = evaluate_cross_artifact_consistency(...)

    dashboard = {
        "artifact_health": summarize_health(artifact_health_rows, consistency_rows),
        "sections": {}
    }

    weekly_review = artifact_bundle.get("weekly_review_summary")
    if weekly_review and health_is_usable("weekly_review_summary", artifact_health_rows):
        dashboard["sections"]["weekly_review"] = build_weekly_review_section(weekly_review)
    else:
        dashboard["sections"]["weekly_review"] = {
            "status": "warning",
            "title": "周度复盘",
            "summary": "周报缺失或版本不兼容，已降级展示 execution / paper / report 摘要",
            "fallback_section": build_weekly_fallback_section(...),
        }

    reconcile = artifact_bundle.get("broker_reconciliation")
    if not reconcile:
        dashboard["sections"]["reconcile"] = {
            "status": "warning",
            "summary": "对账产物缺失，保留 report / paper / execution 视图",
        }
```

## 1.10 治理状态纳入运行健康

新增治理健康指标：

- pending tuning actions 数量
- oldest pending action age
- 最近 4 周 approve / reject / superseded 比例
- 是否一次修改多个 primary field
- live 参数是否与最近 review evidence 一致

建议新增模块：

- `src/common/governance_health.py`

### 伪代码：governance health

```python
# src/common/governance_health.py

def build_governance_health(governance_rows: list[dict], applied_params: dict) -> dict:
    pending = [r for r in governance_rows if r.get("status") == "PENDING"]
    approved = [r for r in governance_rows if r.get("status") == "APPROVED"]
    rejected = [r for r in governance_rows if r.get("status") == "REJECTED"]
    superseded = [r for r in governance_rows if r.get("status") == "SUPERSEDED"]

    multi_primary = [r for r in governance_rows if len(set(r.get("primary_fields") or [])) > 1]
    evidence_mismatch = find_live_param_evidence_mismatch(governance_rows, applied_params)

    status = "ready"
    summaries = []

    if pending:
        status = "warning"
        summaries.append(f"pending actions={len(pending)}")
    if evidence_mismatch:
        status = "degraded"
        summaries.append("live 参数与最近 review evidence 不一致")
    if multi_primary:
        summaries.append(f"multi-primary-field changes={len(multi_primary)}")

    return {
        "status": status,
        "pending_count": len(pending),
        "oldest_pending_age_days": oldest_age_days(pending),
        "approve_ratio_4w": ratio(approved, governance_rows, 28),
        "reject_ratio_4w": ratio(rejected, governance_rows, 28),
        "superseded_ratio_4w": ratio(superseded, governance_rows, 28),
        "multi_primary_count": len(multi_primary),
        "evidence_mismatch": bool(evidence_mismatch),
        "summary": " | ".join(summaries) or "治理状态正常",
    }
```

## 1.11 测试计划

优先补异常路径测试，而不是再堆 happy path：

1. 缺 `weekly_review_summary.json`
2. 缺 `broker_reconciliation_summary.json`
3. execution summary 存在但 meta/version 缺失
4. 旧版 risk history 被新版 weekly review 兼容读取
5. governance artifact 缺关键字段时 dashboard 给出 clear warning
6. tuning action pending 太久时 governance health 显示 warning

### 伪代码：异常路径测试

```python
# tests/test_dashboard_degraded_inputs.py

def test_dashboard_shows_fallback_when_weekly_review_missing(tmp_path):
    artifacts = seed_minimal_dashboard_artifacts(tmp_path, include_weekly_review=False)
    dashboard = generate_dashboard_payload(artifacts)

    section = dashboard["sections"]["weekly_review"]
    assert section["status"] == "warning"
    assert "降级展示" in section["summary"]


def test_dashboard_marks_degraded_when_governance_fields_missing(tmp_path):
    artifacts = seed_governance_artifact(tmp_path, missing_fields=["primary_fields", "review_evidence"])
    dashboard = generate_dashboard_payload(artifacts)

    gov = dashboard["governance_health"]
    assert gov["status"] in {"warning", "degraded"}
    assert "字段" in gov["summary"] or "evidence" in gov["summary"]
```

## 1.12 完成定义

本线完成必须满足：

- dashboard 顶部存在 artifact health / freshness 概览
- 缺失主 artifact 时不再出现静默空白
- weekly review / execution / risk history / governance 都有 schema/freshness contract
- 至少 1 组 degraded-input 测试被纳入 CI

---

# 2. 线路 B：让 dashboard 更直观

## 2.1 目标

不是把 dashboard 做得更花，而是把已有复杂指标压缩成可以一眼理解的决策结构。

首页只回答四个问题：

1. 组合为什么长这样
2. 策略还有没有 edge
3. 交易差是成本问题还是 gate 问题
4. 最近系统改了什么

## 2.2 首页信息架构 v2

建议首页保留四块：

### Block A：组合健康

核心指标：

- net / gross / short exposure
- 当前 risk budget
- throttle tightening
- recovery credit
- dominant throttle layer

回答问题：

> 仓位为什么是现在这个样子

### Block B：信号与结果

核心指标：

- top-ranked 候选 5/20/60d outcome spread
- `signal_score -> expected_edge -> realized_edge` 偏差
- 每市场当前策略的 post-cost alpha 状态

回答问题：

> 策略本身还有没有 edge

### Block C：执行质量

核心指标：

- blocked by market rule
- blocked by edge
- avg realized slippage
- slice count / fill delay / participation
- 按 market + session + liquidity bucket 拆分

回答问题：

> 交易差是因为成本太高，还是 gate 太严

### Block D：治理与变更

核心指标：

- 本周 tuning actions
- pending patch reviews
- 最近生效参数
- 生效后 1w / 2w / 4w 跟踪

回答问题：

> 系统最近到底改了什么

## 2.3 市场视图升级

不要只在表里留 `market` 一列，直接提升为一等视图：

- US card / tab
- HK card / tab
- CN card / tab

每个市场卡片顶部给一句固定市场上下文：

- US：趋势优先 / 深流动性 / session 风险
- HK：board lot / odd lot / 成本 / sliced limit
- CN：research-only or staged / 低频 / 防守预算

## 2.4 建议新增模块

- `src/common/dashboard_blocks.py`
- `src/common/dashboard_market_views.py`
- `src/common/dashboard_waterfall.py`
- `src/common/dashboard_signal_quality.py`
- `src/common/dashboard_execution_quality.py`
- `src/common/dashboard_governance_views.py`

## 2.5 伪代码：dashboard block builder

```python
# src/common/dashboard_blocks.py

def build_dashboard_v2(summary_bundle: dict) -> dict:
    return {
        "top_health": build_top_health_banner(summary_bundle),
        "market_tabs": build_market_tabs(summary_bundle),
        "blocks": {
            "portfolio_health": build_portfolio_health_block(summary_bundle),
            "signal_and_outcome": build_signal_outcome_block(summary_bundle),
            "execution_quality": build_execution_quality_block(summary_bundle),
            "governance_and_change": build_governance_change_block(summary_bundle),
        }
    }
```

## 2.6 伪代码：组合健康块

```python
# src/common/dashboard_market_views.py

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
        "human_summary": build_portfolio_health_explanation(risk),
    }


def build_portfolio_health_explanation(risk: dict) -> str:
    if risk.get("risk_dominant_throttle_layer") == "market":
        return "当前仓位主要受市场层 throttle 收紧影响。"
    if abs(float(risk.get("risk_recovery_weight_credit", 0) or 0)) > 0:
        return "当前已有 recovery credit，但整体风险预算仍未完全恢复。"
    return "当前仓位主要由基准风险预算与执行约束共同决定。"
```

## 2.7 伪代码：信号与结果块

```python
# src/common/dashboard_signal_quality.py

def build_signal_outcome_block(bundle: dict) -> dict:
    outcome = bundle.get("signal_outcome_summary", {})
    return {
        "title": "信号与结果",
        "status": infer_signal_status(outcome),
        "headline": {
            "outcome_spread_5d": outcome.get("top_ranked_spread_5d"),
            "outcome_spread_20d": outcome.get("top_ranked_spread_20d"),
            "outcome_spread_60d": outcome.get("top_ranked_spread_60d"),
            "expected_vs_realized_edge_gap": outcome.get("expected_realized_edge_gap"),
            "post_cost_alpha_sign": outcome.get("post_cost_alpha_sign"),
        },
        "summary": build_signal_summary(outcome),
    }
```

## 2.8 伪代码：执行质量块

```python
# src/common/dashboard_execution_quality.py

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

## 2.9 伪代码：治理与变更块

```python
# src/common/dashboard_governance_views.py

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

## 2.10 伪代码：waterfall

最值得补的一张图：

- Strategy drag
- Risk budget drag
- Throttle drag
- Recovery credit
- Execution drag

```python
# src/common/dashboard_waterfall.py

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

## 2.11 测试计划

优先保证：

- 缺某块数据时 block 仍可渲染 warning/fallback
- market tab 对 HK/US/CN 均有默认上下文
- waterfall 在字段缺失时仍给出零值或 warning
- signal/execution/governance 三块不会因为局部缺字段而整页失败

### 伪代码：dashboard v2 测试

```python
# tests/test_dashboard_blocks_v2.py

def test_portfolio_health_block_can_render_with_partial_risk_overlay():
    bundle = {"risk_overlay_summary": {"dynamic_net_exposure": 0.32}}
    block = build_portfolio_health_block(bundle)
    assert block["title"] == "组合健康"
    assert "human_summary" in block


def test_execution_quality_block_marks_warning_when_only_partial_metrics_exist():
    bundle = {"execution_quality_summary": {"blocked_market_rule_order_count": 3}}
    block = build_execution_quality_block(bundle)
    assert block["title"] == "执行质量"
    assert block["status"] in {"ready", "warning", "degraded"}
```

## 2.12 完成定义

本线完成至少满足：

- 首页四块完成
- US/HK/CN 已成为一等 market view
- weekly waterfall 已可渲染
- 任一 block 缺部分输入时，dashboard 仍然可用

---

# 3. 线路 C：让交易更优质

## 3.1 目标

下一步不该再继续发明更多 gate，而该验证 4/18 之后已经上线的动态 gate 是否真的提高了 post-cost 交易质量。

重点不是“规则多不多”，而是：

- 被挡掉的单，事后真的更差吗
- recovery 放出来的仓位，是否真的改善了恢复期表现
- 不同市场的 gate 是否需要不同校准

## 3.2 本线交付物

本线最终至少交付：

1. unified evidence table
2. blocked vs allowed ex-post review
3. market-specific calibration review
4. outcome-based trading quality KPI

## 3.3 建议模块拆分

- `src/common/trading_quality_evidence.py`
- `src/common/trading_quality_metrics.py`
- `src/tools/review_trading_quality.py`
- `tests/test_trading_quality_evidence.py`
- `tests/test_trading_quality_metrics.py`

## 3.4 统一证据表

把以下链路串成一张事实表：

`candidate -> strategy control -> risk overlay -> market rule gate -> edge gate -> order -> fill -> 5/20/60d outcome`

建议最小字段集：

- market / portfolio_id / symbol / trade_date / session
- signal_score / expected_edge / strategy_rank
- strategy_control_label
- risk_budget_delta / throttle_delta / recovery_credit
- dominant_throttle_layer
- blocked_market_rule / blocked_edge / block_reason
- dynamic_edge_floor_bps / dynamic_edge_buffer_bps
- liquidity_bucket / slice_count / adv_participation
- order_submitted / filled / realized_slippage_bps / fill_delay_sec
- outcome_5d / outcome_20d / outcome_60d

## 3.5 伪代码：evidence row builder

```python
# src/common/trading_quality_evidence.py

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

## 3.6 伪代码：blocked vs allowed 对照

```python
# src/common/trading_quality_metrics.py

def compare_blocked_vs_allowed(rows: list[dict], *, market: str | None = None) -> dict:
    scoped = [r for r in rows if not market or r.get("market") == market]

    blocked_edge = [r for r in scoped if r.get("blocked_edge")]
    allowed = [r for r in scoped if not r.get("blocked_edge") and not r.get("blocked_market_rule")]

    return {
        "blocked_edge_count": len(blocked_edge),
        "allowed_count": len(allowed),
        "blocked_edge_avg_outcome_5d": avg(blocked_edge, "outcome_5d"),
        "allowed_avg_outcome_5d": avg(allowed, "outcome_5d"),
        "blocked_edge_avg_outcome_20d": avg(blocked_edge, "outcome_20d"),
        "allowed_avg_outcome_20d": avg(allowed, "outcome_20d"),
        "blocked_edge_avg_outcome_60d": avg(blocked_edge, "outcome_60d"),
        "allowed_avg_outcome_60d": avg(allowed, "outcome_60d"),
    }
```

## 3.7 伪代码：recovery 释放效果

```python
# src/common/trading_quality_metrics.py

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

## 3.8 市场校准策略

### US

重点看：

- deep liquidity 是否被过度 edge gate
- open/close session buffer 是否过严
- 深流动性大票是否被不必要挡掉

### HK

重点看：

- board lot / odd lot mismatch
- dynamic limit buffer 是否过大
- slice_count 是否过于保守
- market-rule block 里多少是本可通过 sizing 解决的

### CN

重点看：

- research-only / staged rollout 下的候选排序
- defensive budget 是否合理
- 暂不追求 aggressive automation

## 3.9 伪代码：按市场校准摘要

```python
# src/tools/review_trading_quality.py

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
        })
    return output
```

## 3.10 成功标准

成功标准不再只是“成交更多”，而是结果指标：

1. top-ranked 候选 5/20/60d 持续优于中位数
2. turnover 降了，但 post-cost 不恶化
3. blocked edge 单事后表现弱于 allowed 单
4. recovery 期间仓位恢复更快，但 drawdown 不明显恶化
5. HK/CN 的 market-rule block 更可解释，board lot mismatch 明显下降

## 3.11 测试计划

优先补：

- evidence row builder 字段链完整性测试
- blocked vs allowed 统计测试
- recovery effect 测试
- market-specific calibration summary 测试

### 伪代码：evidence 测试

```python
# tests/test_trading_quality_evidence.py

def test_build_evidence_row_preserves_candidate_gate_fill_outcome_chain():
    row = build_evidence_row(
        candidate={"market": "US", "symbol": "AAPL", "signal_score": 0.92},
        strategy_control={"label": "trend_ok"},
        risk_overlay={"risk_throttle_weight_delta": -0.1},
        gate_decision={"blocked_edge": False, "dynamic_edge_floor_bps": 12.0},
        order_row={"order_id": "1"},
        fill_row={"realized_slippage_bps": 8.5},
        outcome_row={"outcome_20d": 0.04},
    )
    assert row["market"] == "US"
    assert row["symbol"] == "AAPL"
    assert row["dynamic_edge_floor_bps"] == 12.0
    assert row["realized_slippage_bps"] == 8.5
    assert row["outcome_20d"] == 0.04
```

## 3.12 完成定义

本线完成至少满足：

- 统一 evidence table 已生成并可被周报/dashboard 读取
- blocked vs allowed 至少在 US/HK 上可出统计结果
- recovery effect 已有最小 review 输出
- 交易质量成功标准被固化到报告里，而不是只写在文档里

---

# 4. 执行节奏（30 / 60 / 90 天）

## 4.1 未来 30 天

只做稳定性底座与证据底座：

- artifact contract
- health / freshness / degraded 状态
- governance health summary
- degraded-input tests
- unified evidence table v1

### 30 天完成标准

- dashboard 顶部有可用的 health 总览
- 缺 weekly review / reconcile / governance artifact 时系统可降级
- evidence table 至少能串 candidate -> gate -> fill -> outcome

## 4.2 30–60 天

开始做 dashboard v2：

- 四块首页
- market tabs/cards
- waterfall
- governance block

### 60 天完成标准

- dashboard 首页可以直接回答四个核心问题
- US/HK/CN 市场卡片上线
- waterfall 已能显示 Strategy/Risk/Throttle/Recovery/Execution 五类贡献

## 4.3 60–90 天

开始交易质量校准：

- blocked vs allowed 对照
- recovery release review
- US/HK/CN 分市场 execution 校准建议
- 固化成功标准 KPI

### 90 天完成标准

- 关键 gate 是否有效不再靠感觉，而有 review 输出支撑
- 可以明确判断：系统是在保护收益，还是在过度保守

---

# 5. 拆分成可执行 issue / PR 的建议

## Epic A：Artifact health and degraded-state visibility

建议拆成：

1. `feat: add artifact contract registry and health helpers`
2. `feat: add dashboard degraded-state rendering for missing weekly/reconcile/governance artifacts`
3. `test: add degraded-input regression coverage for dashboard and weekly review`
4. `feat: add governance health summary into dashboard operations overview`

## Epic B：Dashboard information architecture v2

建议拆成：

1. `feat: add dashboard homepage block builders`
2. `feat: add US/HK/CN market cards or tabs`
3. `feat: add weekly attribution waterfall`
4. `feat: add governance and recent-parameter-change block`

## Epic C：Execution quality evidence and gate calibration

建议拆成：

1. `feat: add unified trading quality evidence table`
2. `feat: add blocked-vs-allowed ex-post review summary`
3. `feat: add recovery-release quality review`
4. `feat: add market-specific execution calibration summary`

---

# 6. 每次提交都必须做的 checklist

这一节用于约束以后每次改动，避免继续出现“代码改了，但文档/测试/产物契约没同步”的情况。

## 6.1 通用提交 checklist

每个 PR / commit（至少每个 PR）都要检查：

- [ ] 改动是否明确属于三条线之一：稳定性 / dashboard / 交易质量
- [ ] 改动是否写清了目标文件与影响范围
- [ ] 若新增 artifact，是否定义了 `schema_version`
- [ ] 若新增 artifact，是否定义了 `generated_at`
- [ ] 若新增 artifact，是否定义了 required fields / required columns
- [ ] 若读取已有 artifact，是否处理了缺失/旧版/字段不全的 degraded 输入
- [ ] 若修改 dashboard helper，是否补了对应测试
- [ ] 若修改 weekly review 输出，是否补了 backward-compat 或 contract 测试
- [ ] 若修改 governance / tuning 产物，是否同步更新 health summary 逻辑
- [ ] 若修改 gate / execution / evidence 链，是否补了证据表字段或 review 输出
- [ ] 若改动影响 CLI / 入口参数，是否同步更新 README 或 runnable summary
- [ ] 若改动影响项目阶段判断或阅读顺序，是否同步更新 `docs/current_status.md`
- [ ] 若改动影响长期路线或阶段结论，是否同步更新 `docs/project_status_roadmap.md`
- [ ] 若改动影响运行方式 / preflight / dashboard control，是否同步更新 `docs/supervisor_runbook.md`
- [ ] 若改动影响治理模式 / live 安全边界 / CI 基线，是否同步更新 `docs/production_governance.md`
- [ ] 是否新增或更新了测试
- [ ] 本地是否至少跑了与本次改动直接相关的一组测试
- [ ] 若改动较大，是否把 TODO / follow-up 写入对应 doc，而不是只留在聊天里

## 6.2 稳定性相关提交 checklist

- [ ] artifact contract 已更新
- [ ] freshness 逻辑已更新
- [ ] degraded-state UI 或 summary 已更新
- [ ] 至少有一个缺失/旧版/缺字段测试
- [ ] 不会因为单个 artifact 缺失导致整页或整流程静默失败

## 6.3 dashboard 相关提交 checklist

- [ ] 首页四块结构没有被打散回“堆表格”
- [ ] market-specific 展示没有退化成只有 `market` 一列
- [ ] 每个 block 在部分输入缺失时仍可渲染
- [ ] 人类可读 summary 已同步更新
- [ ] 若新增关键指标，已说明它回答的是哪一个用户问题

## 6.4 交易质量相关提交 checklist

- [ ] evidence table 字段链是否完整
- [ ] blocked vs allowed 是否仍可统计
- [ ] outcome 口径是否明确（5/20/60d）
- [ ] 新 gate / 新参数是否有 ex-post review 计划
- [ ] 成功标准是否仍以 post-cost 结果而不是“成交更多”为准

## 6.5 文档同步 checklist

每次提交后至少问自己一次：

- [ ] 新接手的人还能够只看 README + current_status 就理解这次改了什么吗
- [ ] 本次改动是否影响推荐阅读顺序
- [ ] 本次改动是否影响命令行用法
- [ ] 本次改动是否影响 dashboard 的读法
- [ ] 本次改动是否影响 live/paper 风险边界

如果答案是“会影响”，对应文档必须一起改。

## 6.6 推荐最小本地验证命令

按改动类型至少跑一组：

```bash
# 稳定性 / helper / contract 改动
pytest -q -p no:cacheprovider tests/test_artifact_health.py tests/test_dashboard_degraded_inputs.py

# dashboard 改动
pytest -q -p no:cacheprovider tests/test_dashboard_blocks_v2.py tests/test_review_weekly_helpers.py

# 交易质量 / evidence 改动
pytest -q -p no:cacheprovider tests/test_trading_quality_evidence.py tests/test_trading_quality_metrics.py

# 守底线
pytest -q -p no:cacheprovider -m guardrail
```

如果本次提交跨越多条线，至少补一条说明：

- 为什么必须跨线改
- 哪些点已验证
- 哪些 follow-up 仍未完成

---

# 7. 推荐提交模板

## PR 标题建议

```text
feat(stability): add artifact health contracts and degraded dashboard fallback
feat(dashboard): add homepage v2 blocks and market tabs
feat(trading-quality): add unified evidence table and blocked-vs-allowed review
```

## PR 描述建议结构

```text
## Summary
- 本次改动属于哪条优化线
- 解决的具体问题

## What changed
- 新增/修改的模块
- 新增/修改的 artifact / schema / block / review

## Why
- 为什么这一步现在最值得做
- 它如何服务于稳定性 / dashboard / 交易质量

## Validation
- 跑了哪些测试
- 哪些 degraded-input 场景被覆盖

## Docs updated
- README.md
- docs/current_status.md
- docs/project_status_roadmap.md
- docs/supervisor_runbook.md
- docs/production_governance.md
```

---

# 8. 一句话结论

2026Q2 的最优路线不是继续扩功能面，而是：

1. 先把 artifact / health / evidence 底座补齐
2. 再把 dashboard 压成一眼可读的四块结构
3. 最后用统一证据表去校准现有 gate，判断系统是在保护收益，还是在过度保守

只要按这个顺序推进，项目会从“功能完整的个人系统”继续进化成“更稳定、更可解释、也更容易持续迭代的个人投资操作平台”。
