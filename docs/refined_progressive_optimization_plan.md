# ibkr_quant_system 精细化渐进式优化执行文件

## 结论

原《渐进式改进执行手册》的方向总体是合适的：它抓住了 dashboard 巨型模块、策略参数硬编码、核心策略测试、告警体系、执行质量、多策略扩展、插件化、LLM 报告、数据库和部署文档这些长期痛点。

但是，按当前 main 的真实状态，需要做三类调整：

1. **不要重复做已经完成的 dashboard evidence helper 初版**。
   当前 main 已经有 `src/common/dashboard_evidence.py`，并且已经包含：
   - `build_weekly_attribution_waterfall`
   - `build_market_views`
   - `build_unified_evidence_overview`

2. **不要一上来整体拆 8000+ 行 dashboard 文件**。
   `generate_dashboard.py` 确实需要继续拆，但应该按可审查的小 PR 进行，先抽纯函数和渲染函数，避免一次性搬迁 `build_dashboard()` 导致输出漂移。

3. **优先级要从“策略扩展”调整为“dashboard/evidence/交易质量闭环稳定”**。
   目前项目正在沿 dashboard control、action audit、evidence blocks、market views、weekly attribution waterfall、unified evidence overview 前进。下一步应先把这些新能力稳定化，再做多策略、插件、LLM、PostgreSQL 等远期扩展。

---

## 当前 main 基线

截至本文件编写时，main 已经具备：

- Dashboard control POST route table。
- Dashboard control handler exception 结构化 JSON 响应。
- Dashboard control action history 初版。
- Dashboard v2 blocks 初版。
- US/HK/CN market views 初版。
- Weekly attribution waterfall 初版。
- Unified evidence overview 初版。
- `src/common/dashboard_evidence.py` 已存在。

因此，本文件不再把这些列为“从零开始实现”，而是定义下一步如何把它们做稳、做细、做成可维护闭环。

---

# 总体优先级

## P0：稳定 dashboard / evidence 新能力

目标：把已经进入 main 的 dashboard evidence 能力从“能生成”推进到“有契约、有测试、有渲染、有 fallback”。

## P1：建立交易质量证据闭环

目标：把 candidate → gate → order → fill → outcome 串成统一 evidence table，支撑 blocked vs allowed ex-post review。

## P2：清理策略技术债

目标：配置化关键策略参数，补纯函数测试，建立 alert/error 分类。

## P3：能力扩展

目标：多策略框架、智能执行、插件化市场、LLM 报告、PostgreSQL、部署自动化。

---

# PR 0：先处理当前打开的文档 PR

## 状态

PR #10 仍打开，内容是 PR A dashboard evidence helper implementation guide。

## 建议

由于 main 已经包含 `src/common/dashboard_evidence.py`，PR #10 的“抽离 helper”指导文档已经部分过时。

处理方式二选一：

### 方案 A：关闭 PR #10

如果团队已经接受 main 中的实际实现，则关闭 PR #10，并在关闭说明中引用本文件。

### 方案 B：更新 PR #10

如果仍想保留 PR #10，则把它改成“验证和增强 dashboard_evidence.py 的测试文档”，而不是“从 generate_dashboard.py 抽离 helper 的实施文档”。

推荐：**方案 A**，因为本文件已经覆盖新的真实状态。

---

# P0-1：补强 dashboard_evidence.py 的契约测试

## 背景

`src/common/dashboard_evidence.py` 已存在，但后续开发应先确认其行为被独立测试锁住。

## 目标文件

新增或完善：

```text
tests/test_dashboard_evidence.py
```

涉及：

```text
src/common/dashboard_evidence.py
```

## 必测函数

```python
build_weekly_attribution_waterfall(cards)
build_market_views(cards)
build_unified_evidence_overview(rows)
```

## 测试 1：market views 必须始终包含 US/HK/CN

```python
def test_market_views_empty_input_returns_all_markets():
    views = build_market_views([])
    assert set(views) == {"US", "HK", "CN"}
    assert views["US"]["portfolio_count"] == 0
    assert views["HK"]["portfolios"] == []
    assert views["CN"]["market"] == "CN"
```

## 测试 2：market views 统计 execution mode / freshness / health

```python
def test_market_views_counts_modes_and_health():
    cards = [
        {
            "market": "US",
            "exchange_open_raw": True,
            "report_status": {"fresh": True},
            "ops_health_rows": [{"status": "warn"}],
            "dashboard_control": {"portfolio": {"execution_control_mode": "REVIEW_ONLY"}},
            "execution_summary": {"submit_orders": False},
        },
        {
            "market": "US",
            "exchange_open_raw": False,
            "report_status": {"fresh": False},
            "ops_health_rows": [],
            "dashboard_control": {"portfolio": {"execution_control_mode": "PAUSED"}},
            "execution_summary": {"submit_orders": True},
        },
    ]

    us = build_market_views(cards)["US"]
    assert us["portfolio_count"] == 2
    assert us["open_count"] == 1
    assert us["fresh_report_count"] == 1
    assert us["stale_report_count"] == 1
    assert us["degraded_health_count"] == 1
    assert us["review_only_count"] == 1
    assert us["paused_count"] == 1
    assert us["auto_submit_count"] == 1
```

## 测试 3：waterfall 组件顺序和 residual

```python
def test_waterfall_has_stable_components_and_residual():
    cards = [
        {
            "market": "US",
            "portfolio_id": "p1",
            "weekly_attribution": {
                "selection_contribution": 0.01,
                "execution_contribution": -0.002,
                "weekly_return": 0.02,
            },
        }
    ]

    rows = build_weekly_attribution_waterfall(cards)
    components = [row["component"] for row in rows]
    assert components[:8] == [
        "selection",
        "sizing",
        "sector",
        "market",
        "execution",
        "strategy_control",
        "risk_overlay",
        "execution_gate",
    ]
    assert components[-2:] == ["residual_to_reported_return", "reported_weekly_return"]
    assert rows[-1]["running_end"] == 0.02
```

## 测试 4：evidence overview flags

```python
def test_evidence_overview_counts_string_and_bool_flags():
    rows = [
        {"market": "US", "blocked_flag": "1", "allowed_flag": "0"},
        {"market": "US", "blocked_flag": False, "allowed_flag": True},
        {"market": "HK", "blocked_flag": "true", "allowed_flag": "false"},
    ]

    overview = build_unified_evidence_overview(rows)
    assert overview["row_count"] == 3
    assert overview["blocked_row_count"] == 2
    assert overview["allowed_row_count"] == 1
```

## 验收标准

- 新增测试覆盖空输入、缺字段、字符串 flag、bool flag。
- 不改变 dashboard JSON key。
- 不修改 `generate_dashboard.py` 的输出结构。

---

# P0-2：为 market views 加市场上下文

## 背景

当前 `build_market_views()` 返回 US/HK/CN 的统计，但缺少操作员友好的市场解释文案。

## 目标文件

新增：

```text
src/common/dashboard_market_context.py
tests/test_dashboard_market_context.py
```

修改：

```text
src/common/dashboard_evidence.py
```

## 实现

```python
MARKET_CONTEXT = {
    "US": {
        "label": "US",
        "summary": "趋势优先 / 深流动性 / session 风险",
        "primary_risks": ["open_close_slippage", "trend_regime_flip", "overtrading"],
    },
    "HK": {
        "label": "HK",
        "summary": "board lot / odd lot / 成本 / sliced limit",
        "primary_risks": ["board_lot_mismatch", "thin_liquidity", "cost_buffer"],
    },
    "CN": {
        "label": "CN",
        "summary": "research-only / staged / 低频 / 防守预算",
        "primary_risks": ["research_only", "turnover", "defensive_budget"],
    },
}


def market_context(market: str) -> dict:
    code = str(market or "").strip().upper()
    return dict(MARKET_CONTEXT.get(code, {"label": code or "UNKNOWN", "summary": "", "primary_risks": []}))
```

在 `build_market_views()` 的每个 market row 中增加：

```python
"context": market_context(market_code)["summary"],
"primary_risks": market_context(market_code)["primary_risks"],
```

## 测试

```python
def test_market_context_known_markets():
    assert market_context("US")["summary"]
    assert "board_lot_mismatch" in market_context("HK")["primary_risks"]
    assert "research_only" in market_context("CN")["primary_risks"]


def test_market_views_include_context():
    views = build_market_views([])
    assert views["US"]["context"]
    assert isinstance(views["CN"]["primary_risks"], list)
```

## 验收标准

- dashboard JSON 中 US/HK/CN market views 都带 context。
- 空输入仍返回完整 market views。
- 不影响已有统计字段。

---

# P0-3：把 dashboard v2 blocks 渲染到 HTML advanced view

## 背景

`dashboard_v2_blocks` 已进入 payload，但如果只存在于 JSON，操作者仍然需要打开 artifact 才能看懂。

## 目标文件

修改：

```text
src/tools/generate_dashboard.py
```

可选新增：

```text
src/common/dashboard_rendering.py
tests/test_dashboard_rendering.py
```

推荐新增渲染模块，避免 `generate_dashboard.py` 继续膨胀。

## 接口

```python
from html import escape
from typing import Any, Dict, List

STATUS_CLASS = {
    "ok": "ok",
    "ready": "ok",
    "warn": "warn",
    "warning": "warn",
    "degraded": "warn",
    "fail": "fail",
    "error": "fail",
}


def render_dashboard_v2_blocks(blocks: List[Dict[str, Any]]) -> str:
    if not blocks:
        return '<section class="panel"><h2>Dashboard v2 Evidence Blocks</h2><p>No dashboard v2 blocks available.</p></section>'
    ...
```

## 渲染字段

每个 block 显示：

- title
- status badge
- summary
- metrics 前 8 项
- rows 前 5 行，复杂 rows 可显示 JSON preview

## 安全要求

所有来自 payload 的字符串必须 `html.escape()`。

## 测试

```python
def test_render_dashboard_v2_blocks_handles_empty(): ...
def test_render_dashboard_v2_blocks_escapes_html(): ...
def test_render_dashboard_v2_blocks_includes_metric_names(): ...
```

## 验收标准

- dashboard HTML advanced 区出现 Dashboard v2 Evidence Blocks。
- 空 blocks 有 fallback。
- HTML 注入被 escape。
- 不影响原有页面区块。

---

# P1-1：dashboard control action audit 持久化与脱敏

## 背景

当前 action history 已进入 supervisor / payload，但还需要保证 payload 安全、可持久、可重启恢复。

## 目标文件

新增：

```text
src/common/dashboard_control_audit.py
tests/test_dashboard_control_audit.py
```

修改：

```text
src/app/supervisor.py
```

## 接口

```python
ACTION_HISTORY_LIMIT = 50
SENSITIVE_KEYS = {"password", "token", "secret", "api_key", "account", "account_id"}


def sanitize_payload(payload: dict, *, max_string_len: int = 160) -> dict:
    ...


def build_action_record(*, action: str, status: str, payload: dict | None = None, error: str = "", ts: str = "") -> dict:
    ...


def append_action_history(history: list[dict], record: dict, *, limit: int = ACTION_HISTORY_LIMIT) -> list[dict]:
    ...
```

## 持久化

可选 JSONL：

```text
runtime/dashboard_control_actions.jsonl
```

每行一个 action record。

## 验收标准

- 敏感字段被 mask。
- 长字符串被截断。
- history limit 固定为 50。
- 老状态没有 payload 字段时仍能读取。

---

# P1-2：统一 weekly evidence table

## 背景

`unified_evidence_overview` 现在只统计输入 rows。真正的交易质量闭环需要 weekly review 生成统一 evidence rows。

## 目标文件

新增：

```text
src/common/investment_evidence.py
tests/test_investment_evidence.py
```

修改：

```text
src/tools/review_investment_weekly.py
```

## Evidence schema

```python
EVIDENCE_COLUMNS = [
    "week",
    "market",
    "portfolio_id",
    "symbol",
    "decision_ts",
    "decision_source",
    "signal_score",
    "expected_edge_bps",
    "required_edge_bps",
    "expected_cost_bps",
    "gate_status",
    "blocked_flag",
    "allowed_flag",
    "blocked_reason",
    "planned_order_value",
    "filled_order_value",
    "realized_slippage_bps",
    "fill_delay_sec",
    "slice_count",
    "adv_participation_pct",
    "outcome_5d",
    "outcome_20d",
    "outcome_60d",
    "realized_edge_bps",
    "join_quality",
]
```

## 接口

```python
def normalize_evidence_row(raw: dict) -> dict:
    """Return a row with all EVIDENCE_COLUMNS populated."""


def build_unified_evidence_rows(*, candidate_rows: list[dict], execution_rows: list[dict], outcome_rows: list[dict]) -> list[dict]:
    """Join candidate/gate/execution/outcome data into weekly evidence rows."""


def write_evidence_artifacts(out_dir: Path, rows: list[dict]) -> None:
    """Write weekly_unified_evidence.csv and weekly_unified_evidence.json."""
```

## Join keys

按优先级：

1. `decision_id`
2. `portfolio_id + symbol + decision_ts`
3. `portfolio_id + symbol + week`

无法完整 join 时保留 row，并标记：

```python
"join_quality": "partial"
```

## 验收标准

- candidate rows 不因缺 execution/outcome 被丢弃。
- weekly review 输出 CSV 和 JSON。
- dashboard 可以读取 `weekly_unified_evidence.csv`。

---

# P1-3：blocked vs allowed ex-post review

## 背景

交易质量最关键问题：gate 挡掉的单，事后是不是更差？

## 目标文件

修改或新增：

```text
src/common/investment_evidence.py
src/tools/review_investment_weekly.py
tests/test_investment_evidence.py
```

## 接口

```python
def build_blocked_vs_allowed_expost_review(evidence_rows: list[dict]) -> list[dict]:
    """Aggregate outcomes by market, portfolio_id, and horizon."""
```

## Output schema

```python
{
    "market": "US",
    "portfolio_id": "paper-us",
    "horizon": "20d",
    "blocked_count": 12,
    "allowed_count": 30,
    "blocked_avg_outcome": -0.012,
    "allowed_avg_outcome": 0.018,
    "blocked_minus_allowed": -0.030,
    "review_label": "GATE_OK",
}
```

## Label rules

```python
if blocked_count < 5 or allowed_count < 5:
    review_label = "INSUFFICIENT_SAMPLE"
elif blocked_minus_allowed > 0:
    review_label = "BLOCKED_OUTPERFORMED_ALLOWED"
elif blocked_minus_allowed < 0:
    review_label = "GATE_OK"
else:
    review_label = "NEUTRAL"
```

## Artifact paths

```text
weekly_blocked_vs_allowed_expost.csv
weekly_blocked_vs_allowed_expost.json
```

## 验收标准

- weekly review 生成 blocked-vs-allowed artifacts。
- dashboard evidence quality block 能读取。
- labels deterministic。

---

# P2：策略参数与测试清理

## 调整原建议

原手册提出“消除所有魔法数字”，方向正确，但不能机械搜索所有数字后全部配置化。

正确做法：

1. 只配置影响策略/执行/风险行为的阈值。
2. 保留显然属于格式、窗口边界、测试 fixture 的数字。
3. 每个配置迁移必须保证默认行为不变。

## 第一批可迁移参数

```text
src/strategies/engine_strategy.py
src/signals/fusion.py
src/strategies/mid_regime.py
src/strategies/regime_adaptor.py
```

建议字段：

```yaml
engine:
  mr_weight: 0.6
  bo_weight: 0.4
  mid_qty_min: 0.25
  mid_qty_max: 1.25
  mid_soft_floor: 0.0
```

## 策略测试注意事项

原手册里的 MR 测试样例要谨慎：mean reversion 信号不一定在连续上涨后给强买入，实际语义要以代码为准。

推荐先写这些测试：

- BO 突破向上/向下。
- fusion 在 `can_trade_short=False` 时不允许空头信号。
- mid_regime 在高波动/深回撤时 scale 下降。
- risk model 在极端亏损时触发限制。

---

# P3：远期扩展降级处理

以下建议方向正确，但不应排在近期：

- 多策略并行框架。
- 智能订单路由高级优化。
- 插件化市场和数据源。
- LLM 自然语言周报。
- SQLite → PostgreSQL。
- Docker / CI/CD 全量部署。

原因：

1. 当前 dashboard/evidence/weekly review 闭环还在稳定阶段。
2. 交易质量证据表还没完全生成。
3. 过早引入多策略或 LLM 会增加解释成本。
4. PostgreSQL / 插件化属于规模化部署阶段，不是当前瓶颈。

这些工作应在 P0/P1 完成后重新评估。

---

# 最终推荐执行顺序

## 立即执行

1. 补 `tests/test_dashboard_evidence.py`。
2. 给 market views 加 US/HK/CN context。
3. 渲染 dashboard v2 blocks 到 advanced HTML。
4. 关闭或更新 PR #10，避免和已完成 helper refactor 冲突。

## 接着执行

5. action audit 持久化与脱敏。
6. weekly unified evidence table。
7. blocked vs allowed ex-post review。

## 然后执行

8. 策略参数配置化。
9. 纯策略逻辑测试。
10. alert/error 分类体系。

## 暂缓

11. 多策略框架。
12. 高级智能执行。
13. 插件化市场。
14. LLM 周报。
15. PostgreSQL。
16. Docker / 部署体系。

---

# 验收口径

近期优化完成的标准不是“新增功能更多”，而是：

- dashboard JSON schema 稳定。
- dashboard HTML 能直接看见 evidence blocks。
- market views 对 US/HK/CN 都有 fallback 和解释。
- weekly review 能生成 unified evidence artifacts。
- blocked vs allowed 能回答 gate 是否过紧。
- action audit 不泄露敏感字段。
- 新增 helper 都有独立测试。
- `generate_dashboard.py` 逐步变薄，但没有一次性大迁移。

---

# 给后续开发者的注意事项

- 每个 PR 只做一层，不要混合 dashboard 渲染、weekly review 和策略参数。
- 所有新 helper 优先放入 `src/common/`，并保持纯函数。
- 所有缺数据情况都返回 warning/fallback，不要抛异常中断 dashboard。
- 不要改变现有 dashboard JSON key，除非同步更新测试和文档。
- 不要在 P0/P1 引入新交易 gate。
- 不要把 LLM 放进决策链，只能用于总结展示。
