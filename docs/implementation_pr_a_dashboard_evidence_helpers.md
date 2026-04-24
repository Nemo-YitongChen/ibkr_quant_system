# PR A 实施文档：抽离 Dashboard Evidence Helpers

## 当前状态结论

PR #9 已完成并合入 main。它已经把 P2 后续优化方向写成总开发交接文档：

- `docs/optimization_plan_2026Q2_P2_developer_handoff.md`

因此，PR #9 的目标已经完成：

- 明确了 P2 的总体开发顺序。
- 标明了后续要做的模块、接口、测试和验收标准。
- 给出了 PR A-F 的拆分建议。

下一步不需要再补一份总体 handoff，而应该进入 **PR A：抽离 dashboard evidence helper builders**。

---

## PR A 目标

把 `src/tools/generate_dashboard.py` 中和 evidence / market view / attribution waterfall 相关的纯逻辑抽离到 `src/common/dashboard_evidence.py`，并补独立单元测试。

PR A 只做重构和测试，不改变 dashboard JSON 结构，不改变 HTML 输出，不改变交易逻辑。

---

## 为什么先做 PR A

当前 `generate_dashboard.py` 已经包含：

- weekly attribution waterfall 生成逻辑
- US/HK/CN market view 生成逻辑
- unified evidence overview 生成逻辑
- weekly evidence artifact loader
- blocked vs allowed artifact loader
- dashboard v2 blocks 的接入点

这些逻辑会继续增长。如果继续留在 `generate_dashboard.py`，后续 PR B-F 会越来越难审查，也更容易破坏 dashboard 输出。

PR A 的目的：

1. 先把纯函数抽出。
2. 用小测试锁住 schema。
3. 保持 dashboard 输出完全兼容。
4. 给后续 PR B-F 降低改动面。

---

## 目标文件

### 新增

```text
src/common/dashboard_evidence.py
tests/test_dashboard_evidence.py
```

### 修改

```text
src/tools/generate_dashboard.py
```

### 不要修改

```text
src/app/supervisor.py
src/app/dashboard_control.py
src/tools/review_investment_weekly.py
README.md
```

这些文件属于后续 PR。

---

## 从 generate_dashboard.py 抽离哪些函数

优先抽离这三个 builder：

```python
_build_weekly_attribution_waterfall(cards)
_build_market_views(cards)
_build_unified_evidence_overview(rows)
```

迁移后在 `src/common/dashboard_evidence.py` 中命名为：

```python
build_weekly_attribution_waterfall(cards)
build_market_views(cards)
build_unified_evidence_overview(rows)
```

`generate_dashboard.py` 中可以保留薄 wrapper，或者直接改调用点导入新函数。

推荐第一版保留薄 wrapper，降低风险：

```python
from ..common.dashboard_evidence import (
    build_market_views as _common_build_market_views,
    build_unified_evidence_overview as _common_build_unified_evidence_overview,
    build_weekly_attribution_waterfall as _common_build_weekly_attribution_waterfall,
)


def _build_market_views(cards):
    return _common_build_market_views(cards)
```

这样现有测试如果直接 import 私有 helper，不会立刻坏。

---

## 新模块接口

文件：`src/common/dashboard_evidence.py`

```python
from __future__ import annotations

from typing import Any, Dict, List

TARGET_MARKETS = ("US", "HK", "CN")


def safe_float(value: Any, default: float = 0.0) -> float:
    """Best-effort numeric coercion for dashboard evidence helpers."""


def truthy_flag(value: Any) -> bool:
    """Interpret bool/string/int evidence flags consistently."""


def build_weekly_attribution_waterfall(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build stable attribution waterfall rows from expanded dashboard cards."""


def build_market_views(cards: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build fixed US/HK/CN market view summaries from dashboard cards."""


def build_unified_evidence_overview(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize unified evidence rows by total, blocked/allowed, and market."""
```

---

## Required schema: build_weekly_attribution_waterfall

Input:

```python
cards = [
    {
        "market": "US",
        "watchlist": "core_us",
        "portfolio_id": "paper-us",
        "weekly_attribution": {
            "selection_contribution": 0.01,
            "sizing_contribution": -0.002,
            "sector_contribution": 0.003,
            "market_contribution": 0.004,
            "execution_contribution": -0.001,
            "strategy_control_weight_delta": -0.02,
            "risk_overlay_weight_delta": -0.03,
            "execution_gate_blocked_weight": 0.04,
            "weekly_return": 0.012,
        },
    }
]
```

Output rows must include these components in stable order:

1. `selection`
2. `sizing`
3. `sector`
4. `market`
5. `execution`
6. `strategy_control`
7. `risk_overlay`
8. `execution_gate`
9. `residual_to_reported_return`
10. `reported_weekly_return`

Each row schema:

```python
{
    "market": "US",
    "watchlist": "core_us",
    "portfolio_id": "paper-us",
    "component_order": 1,
    "component": "selection",
    "source_key": "selection_contribution",
    "component_role": "return_component",
    "contribution": 0.01,
    "running_start": 0.0,
    "running_end": 0.01,
}
```

Behavior requirements:

- Missing fields default to `0.0`.
- Cards without `weekly_attribution` are skipped.
- `residual_to_reported_return = weekly_return - sum(known components)`.
- Output sorting is deterministic by `(market, portfolio_id, component_order)`.

---

## Required schema: build_market_views

Input: expanded dashboard cards.

The function must always return keys:

```python
{
    "US": {...},
    "HK": {...},
    "CN": {...},
}
```

Each market value must contain:

```python
{
    "market": "US",
    "portfolio_count": 0,
    "open_count": 0,
    "fresh_report_count": 0,
    "stale_report_count": 0,
    "degraded_health_count": 0,
    "data_attention_count": 0,
    "auto_submit_count": 0,
    "review_only_count": 0,
    "paused_count": 0,
    "portfolios": [],
}
```

Counting rules:

- `open_count`: cards with `exchange_open_raw == True`.
- `fresh_report_count`: cards with `report_status.fresh == True`.
- `stale_report_count`: cards with `report_status.fresh != True`.
- `degraded_health_count`: cards with any `ops_health_rows[*].status` not in `ok/pass`.
- `data_attention_count`: cards with market-data row status indicating missing/mixed/fallback/warning/fail.
- `review_only_count`: control execution mode `REVIEW_ONLY`.
- `paused_count`: control execution mode `PAUSED`.
- `auto_submit_count`: dashboard control or execution summary indicates order submission enabled.

Keep `portfolios` limited to 12 rows for dashboard readability.

---

## Required schema: build_unified_evidence_overview

Input:

```python
rows = [
    {"market": "US", "blocked_flag": 1, "allowed_flag": 0},
    {"market": "HK", "blocked_flag": 0, "allowed_flag": 1},
]
```

Output:

```python
{
    "row_count": 2,
    "blocked_row_count": 1,
    "allowed_row_count": 1,
    "market_rows": [
        {"market": "HK", "row_count": 1, "blocked_row_count": 0, "allowed_row_count": 1},
        {"market": "US", "row_count": 1, "blocked_row_count": 1, "allowed_row_count": 0},
    ],
}
```

Behavior requirements:

- Missing market becomes `UNKNOWN`.
- Bool, int, float, and string flags should be interpreted consistently.
- `market_rows` sorted by market string.

---

## Tests to add

File: `tests/test_dashboard_evidence.py`

### 1. Empty market views

```python
def test_build_market_views_empty_cards_returns_all_markets():
    views = build_market_views([])
    assert set(views) == {"US", "HK", "CN"}
    assert views["US"]["portfolio_count"] == 0
    assert views["HK"]["portfolios"] == []
```

### 2. Market view mode counts

```python
def test_build_market_views_counts_execution_modes_and_health():
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
    views = build_market_views(cards)
    assert views["US"]["portfolio_count"] == 2
    assert views["US"]["open_count"] == 1
    assert views["US"]["fresh_report_count"] == 1
    assert views["US"]["stale_report_count"] == 1
    assert views["US"]["degraded_health_count"] == 1
    assert views["US"]["review_only_count"] == 1
    assert views["US"]["paused_count"] == 1
    assert views["US"]["auto_submit_count"] == 1
```

### 3. Waterfall order and residual

```python
def test_build_weekly_attribution_waterfall_includes_residual_and_total():
    cards = [{
        "market": "US",
        "portfolio_id": "p1",
        "weekly_attribution": {
            "selection_contribution": 0.01,
            "execution_contribution": -0.002,
            "weekly_return": 0.02,
        },
    }]
    rows = build_weekly_attribution_waterfall(cards)
    assert [row["component"] for row in rows][-2:] == [
        "residual_to_reported_return",
        "reported_weekly_return",
    ]
    total = rows[-1]
    assert total["running_end"] == 0.02
```

### 4. Evidence overview flags

```python
def test_build_unified_evidence_overview_groups_by_market_and_flags():
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

---

## generate_dashboard.py migration steps

1. Add import:

```python
from ..common.dashboard_evidence import (
    build_market_views,
    build_unified_evidence_overview,
    build_weekly_attribution_waterfall,
)
```

2. Replace calls:

```python
market_views = _build_market_views(trade_cards)
weekly_attribution_waterfall = _build_weekly_attribution_waterfall(trade_cards)
unified_evidence_overview = _build_unified_evidence_overview(weekly_unified_evidence_rows)
```

with:

```python
market_views = build_market_views(trade_cards)
weekly_attribution_waterfall = build_weekly_attribution_waterfall(trade_cards)
unified_evidence_overview = build_unified_evidence_overview(weekly_unified_evidence_rows)
```

3. Either remove old private helpers or keep wrappers:

Preferred for first PR:

```python
def _build_market_views(cards):
    return build_market_views(cards)
```

This avoids breaking direct helper tests.

4. Do not change output keys:

```python
"market_views"
"weekly_attribution_waterfall"
"unified_evidence_overview"
```

They must remain identical.

---

## Backward compatibility checklist

Before opening PR:

- [ ] `dashboard.json` still includes `market_views`.
- [ ] `dashboard.json` still includes `weekly_attribution_waterfall`.
- [ ] `dashboard.json` still includes `unified_evidence_overview`.
- [ ] `dashboard_v2_blocks` still builds using those payload keys.
- [ ] Empty input does not crash.
- [ ] Missing fields become zero/fallback, not exception.

---

## Test commands

Run targeted tests:

```bash
python -m pytest tests/test_dashboard_evidence.py -q
python -m pytest tests/test_generate_dashboard_helpers.py -q
```

Run dashboard control regression too, because dashboard payload/control areas are adjacent:

```bash
python -m pytest tests/test_dashboard_control_service.py -q
```

If time allows:

```bash
python -m compileall src tests
python -m pytest -q
```

---

## PR acceptance criteria

PR A is complete when:

1. `src/common/dashboard_evidence.py` exists.
2. `tests/test_dashboard_evidence.py` exists.
3. `generate_dashboard.py` delegates the three evidence builders to the common module.
4. Dashboard JSON output keys are unchanged.
5. Tests cover empty input, partial rows, waterfall residual, fixed US/HK/CN markets, and flag parsing.
6. No trading behavior changes.
7. No dashboard HTML redesign in this PR.

---

## Known risks

### Risk 1: Private helper tests import old names

Mitigation: keep thin wrappers in `generate_dashboard.py` for one PR.

### Risk 2: flag parsing drift

Mitigation: centralize `truthy_flag` in `dashboard_evidence.py` and test strings/int/bool values.

### Risk 3: output shape drift

Mitigation: keep existing dashboard key names and add regression tests around schemas.

### Risk 4: too-large PR

Mitigation: do not include action audit persistence, HTML rendering, or weekly evidence generation in PR A.

---

## Suggested PR title and body

Title:

```text
refactor(dashboard): extract evidence helper builders
```

Body:

```markdown
## Summary
- move market views, weekly attribution waterfall, and unified evidence overview builders into `src/common/dashboard_evidence.py`
- keep dashboard JSON output shape unchanged
- add unit tests for empty inputs, partial rows, waterfall residuals, fixed US/HK/CN market views, and evidence flag parsing

## Why
`generate_dashboard.py` now owns multiple pure evidence builders. Extracting them lowers dashboard complexity and gives PR B-F a stable helper layer to build on.

## Validation
- `python -m pytest tests/test_dashboard_evidence.py -q`
- `python -m pytest tests/test_generate_dashboard_helpers.py -q`
```
