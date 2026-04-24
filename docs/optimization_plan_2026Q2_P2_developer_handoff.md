# 2026Q2 P2：Dashboard Evidence / Trading Quality 开发交接文档

## 目标

本文件给后续接手开发者一份可直接执行的开发手册。

当前项目已经完成了 dashboard control 稳定性第一阶段：

- POST route dispatch 已集中到 route table。
- handler exception 已返回结构化 JSON。
- dashboard control action history 已进入 supervisor / dashboard payload。
- dashboard v2 blocks、market views、weekly attribution waterfall、unified evidence overview 已有初版。

P2 的目标不是继续堆新交易规则，而是把已经进入主线的 evidence / dashboard / control history 变成稳定、可测试、可解释、可持续扩展的闭环。

---

## 当前主线基线

以 main 最新提交为基线：

- `Add dashboard evidence blocks and action audit`
- `Merge PR #8 dashboard control error handling`
- `Merge PR #7 dashboard control route dispatch`

这些提交说明：

1. `src/app/dashboard_control.py` 已经完成 route table 化和 handler exception JSON 化。
2. supervisor 已经开始记录 dashboard control action history。
3. dashboard payload 已经开始包含：
   - `dashboard_v2_blocks`
   - `market_views`
   - `weekly_attribution_waterfall`
   - `unified_evidence_overview`
   - `blocked_vs_allowed_expost_review`

后续开发不要重复实现这些初版功能，而是继续把它们稳定化、模块化、可视化和测试化。

---

## 总体开发顺序

推荐按以下 PR 顺序推进：

1. **P2-1：抽离 dashboard evidence helpers**
2. **P2-2：增强 dashboard control action audit 持久化与脱敏**
3. **P2-3：将 dashboard v2 blocks 接入 HTML/advanced view**
4. **P2-4：完善 US/HK/CN market views 的 fallback 和文案**
5. **P2-5：把 weekly attribution waterfall 做成可渲染表/图的数据结构**
6. **P2-6：建立 unified evidence table 的生成与 schema 测试**
7. **P2-7：实现 blocked vs allowed ex-post review 的 weekly review 输出**
8. **P2-8：清理旧 backlog / 过时 PR 文档并更新 README**

---

# P2-1：抽离 dashboard evidence helpers

## 背景

`src/tools/generate_dashboard.py` 目前承担太多职责。最新主线已经加入：

- `_build_weekly_attribution_waterfall`
- `_build_market_views`
- `_build_unified_evidence_overview`
- `_load_weekly_unified_evidence_rows`
- `_load_weekly_blocked_vs_allowed_expost_rows`

这些函数适合抽到 `src/common/`，降低 `generate_dashboard.py` 的复杂度。

## 目标文件

新增：

- `src/common/dashboard_evidence.py`
- `tests/test_dashboard_evidence.py`

移动或包装：

- `generate_dashboard._build_weekly_attribution_waterfall`
- `generate_dashboard._build_market_views`
- `generate_dashboard._build_unified_evidence_overview`

## 建议接口

```python
from __future__ import annotations

from typing import Any, Dict, List


def build_weekly_attribution_waterfall(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return ordered waterfall rows per market / portfolio.

    Required behavior:
    - tolerate missing attribution fields
    - output deterministic component_order
    - include residual_to_reported_return
    - include reported_weekly_return
    """


def build_market_views(cards: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Return fixed US/HK/CN market view payload.

    Required behavior:
    - always include US/HK/CN keys
    - each market row includes portfolio_count/open/fresh/stale/degraded/data_attention
    - tolerate empty input and return fallback rows
    """


def build_unified_evidence_overview(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize weekly unified evidence rows.

    Required behavior:
    - count total rows
    - count blocked rows
    - count allowed rows
    - group counts by market
    """
```

## Implementation notes

1. Keep thin wrappers in `generate_dashboard.py` if needed for backward-compatible tests.
2. `build_market_views` should not import `generate_dashboard.py`.
3. Keep all inputs as `Dict[str, Any]` / `List[Dict[str, Any]]` to avoid large refactor.
4. Do not change dashboard JSON output shape in this PR.

## Tests

Add tests:

```python

def test_build_market_views_always_returns_us_hk_cn(): ...

def test_build_market_views_tolerates_empty_cards(): ...

def test_build_weekly_attribution_waterfall_includes_residual_and_total(): ...

def test_build_unified_evidence_overview_groups_by_market(): ...
```

## Acceptance criteria

- `generate_dashboard.py` imports helpers from `src.common.dashboard_evidence`.
- Existing dashboard helper tests still pass.
- New helper tests cover empty input and partial rows.
- JSON output remains backward-compatible.

---

# P2-2：增强 dashboard control action audit 持久化与脱敏

## 背景

最新主线已经在 supervisor 内存和 dashboard control payload 里记录 action history。但下一步需要保证：

- action history 可长期追踪
- payload 不泄露敏感字段
- dashboard 重启后仍能看到最近动作
- action history 能独立被 dashboard / weekly review 消费

## 目标文件

新增：

- `src/common/dashboard_control_audit.py`
- `tests/test_dashboard_control_audit.py`

可能修改：

- `src/app/supervisor.py`
- `src/tools/generate_dashboard.py`

## 建议数据结构

```python
ACTION_HISTORY_LIMIT = 50
SENSITIVE_KEYS = {
    "password",
    "token",
    "secret",
    "api_key",
    "account",
    "account_id",
}


def sanitize_payload(payload: dict, *, max_string_len: int = 160) -> dict:
    """Mask sensitive keys and truncate long scalar values."""


def build_action_record(
    *,
    action: str,
    status: str,
    path: str = "",
    payload: dict | None = None,
    result: dict | None = None,
    error: str = "",
    exception_type: str = "",
    duration_ms: float | None = None,
    ts: str = "",
) -> dict:
    """Build a normalized dashboard control action record."""


def append_action_history(history: list[dict], record: dict, *, limit: int = ACTION_HISTORY_LIMIT) -> list[dict]:
    """Append and clamp action history."""
```

## Optional JSONL output

If persistent audit is needed beyond supervisor state, write JSONL:

```text
runtime/dashboard_control_actions.jsonl
```

Recommended helper:

```python

def append_action_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
```

## Integration points

In `Supervisor._record_dashboard_control_action(...)`:

1. Replace inline dict construction with `build_action_record(...)`.
2. Use `append_action_history(...)` for limit handling.
3. Optionally call `append_action_jsonl(...)` if a runtime path is available.

## Tests

```python

def test_sanitize_payload_masks_sensitive_keys(): ...

def test_sanitize_payload_truncates_long_strings(): ...

def test_build_action_record_normalizes_fields(): ...

def test_append_action_history_clamps_to_limit(): ...
```

## Acceptance criteria

- No raw token/password/secret/account_id appears in stored payload.
- action history limit remains deterministic.
- persisted rows are valid JSON lines.
- dashboard can still read old state without `payload` field.

---

# P2-3：将 dashboard v2 blocks 接入 HTML / advanced view

## 背景

`dashboard_v2_blocks` 已开始进入 payload，但如果只留在 JSON，操作者仍需要查看 raw artifact。下一步要把它渲染到 dashboard advanced view。

## 目标文件

修改：

- `src/tools/generate_dashboard.py`
- `tests/test_generate_dashboard_helpers.py`

可选新增：

- `src/common/dashboard_rendering.py`
- `tests/test_dashboard_rendering.py`

## 建议 HTML section

新增 Advanced section：

```text
Dashboard v2 Evidence Blocks
```

每个 block 渲染：

- title
- status badge
- summary
- top metrics
- first 5 rows if rows exist

## 建议渲染函数

```python

def _render_dashboard_v2_blocks(blocks: list[dict], *, advanced: bool = True) -> str:
    """Render dashboard v2 blocks as HTML cards."""
```

Card template fields:

```python
{
    "id": "ops_health",
    "title": "Ops Health",
    "status": "ok|warn|fail",
    "summary": "...",
    "metrics": {...},
    "rows": [...]
}
```

## Status mapping

```python
STATUS_CLASS = {
    "ok": "ok",
    "ready": "ok",
    "warn": "warn",
    "warning": "warn",
    "degraded": "warn",
    "fail": "fail",
    "error": "fail",
}
```

## Tests

```python

def test_render_dashboard_v2_blocks_handles_empty_blocks(): ...

def test_render_dashboard_v2_blocks_includes_status_and_summary(): ...

def test_render_dashboard_v2_blocks_escapes_html(): ...
```

## Acceptance criteria

- dashboard HTML shows v2 blocks in advanced mode.
- Empty blocks render fallback text.
- HTML escaping prevents injected markup.
- Existing dashboard sections remain unchanged.

---

# P2-4：完善 US / HK / CN market views fallback 和文案

## 背景

`market_views` 已有初版，但后续开发者需要让它变成一等 dashboard 信息结构，而不是隐藏 JSON 字段。

## 目标文件

建议新增或修改：

- `src/common/dashboard_evidence.py`
- `src/common/dashboard_market_context.py`
- `tests/test_dashboard_market_views.py`

## Market context 常量

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
```

## Required output

Each market view row should contain:

```python
{
    "market": "US",
    "context": "趋势优先 / 深流动性 / session 风险",
    "primary_risks": [...],
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

## Tests

```python

def test_market_views_include_context_for_all_markets(): ...

def test_market_views_empty_input_returns_fallback_context(): ...

def test_market_views_counts_execution_modes(): ...
```

## Acceptance criteria

- US/HK/CN always visible.
- Missing market cards do not remove the market tab.
- Operators can see market-specific context without reading config.

---

# P2-5：weekly attribution waterfall 渲染和字段契约

## 背景

waterfall 初版已经产生 rows。下一步要把它稳定成契约：字段固定、排序固定、HTML/JSON 消费一致。

## Target schema

```python
{
    "market": "US",
    "watchlist": "...",
    "portfolio_id": "...",
    "component_order": 1,
    "component": "selection",
    "source_key": "selection_contribution",
    "component_role": "return_component",
    "contribution": 0.0123,
    "running_start": 0.0,
    "running_end": 0.0123,
}
```

## Required components

At minimum include:

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

## Tests

```python

def test_waterfall_component_order_is_stable(): ...

def test_waterfall_residual_reconciles_to_reported_return(): ...

def test_waterfall_handles_missing_attribution_fields_as_zero(): ...
```

## Acceptance criteria

- Every portfolio gets deterministic component order.
- Missing fields default to zero.
- residual equals `reported_weekly_return - sum(known_components)`.
- dashboard can render rows without additional transformation.

---

# P2-6：建立 unified evidence table 的生成与 schema 测试

## 背景

`unified_evidence_overview` 现在只是统计输入 rows。下一步需要让 weekly review 明确生成统一 evidence rows。

## 目标文件

修改或新增：

- `src/tools/review_investment_weekly.py`
- `src/common/investment_evidence.py`
- `tests/test_investment_evidence.py`
- `tests/test_review_weekly_helpers.py`

## Evidence row schema

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
]
```

## Helper interfaces

```python

def normalize_evidence_row(raw: dict) -> dict:
    """Return a row with all EVIDENCE_COLUMNS populated."""


def build_unified_evidence_rows(
    *,
    candidate_rows: list[dict],
    execution_rows: list[dict],
    outcome_rows: list[dict],
) -> list[dict]:
    """Join candidate/gate/execution/outcome data into weekly evidence rows."""


def write_evidence_artifacts(out_dir: Path, rows: list[dict]) -> None:
    """Write CSV and JSON artifacts."""
```

## Suggested artifact paths

```text
weekly_unified_evidence.csv
weekly_unified_evidence.json
```

## Join keys

Preferred join key order:

1. `decision_id` if available
2. `portfolio_id + symbol + decision_ts`
3. `portfolio_id + symbol + week`

If a join fails, keep the row and mark:

```python
"join_quality": "partial"
```

## Tests

```python

def test_normalize_evidence_row_adds_all_columns(): ...

def test_build_unified_evidence_rows_keeps_partial_rows(): ...

def test_write_evidence_artifacts_outputs_csv_and_json(): ...
```

## Acceptance criteria

- Weekly review writes both CSV and JSON evidence artifacts.
- Missing execution/outcome data does not drop candidate rows.
- `generate_dashboard.py` can load the artifacts without fallback-only behavior.

---

# P2-7：实现 blocked vs allowed ex-post review

## 背景

交易质量的核心问题是：

> 被 gate 挡掉的单，事后是不是更差？

需要在 weekly review 中输出 blocked vs allowed 对照。

## 目标文件

新增或修改：

- `src/common/investment_evidence.py`
- `src/tools/review_investment_weekly.py`
- `tests/test_investment_evidence.py`

## Suggested function

```python

def build_blocked_vs_allowed_expost_review(evidence_rows: list[dict]) -> list[dict]:
    """Aggregate outcomes by market, gate_status, and horizon."""
```

## Output schema

```python
{
    "market": "US",
    "portfolio_id": "...",
    "horizon": "20d",
    "blocked_count": 12,
    "allowed_count": 30,
    "blocked_avg_outcome": -0.012,
    "allowed_avg_outcome": 0.018,
    "blocked_minus_allowed": -0.030,
    "review_label": "GATE_OK",
}
```

## Review labels

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

## Tests

```python

def test_blocked_vs_allowed_marks_gate_ok_when_blocked_underperforms(): ...

def test_blocked_vs_allowed_marks_too_restrictive_when_blocked_outperforms(): ...

def test_blocked_vs_allowed_marks_insufficient_sample(): ...
```

## Acceptance criteria

- Weekly review outputs blocked-vs-allowed artifacts.
- Dashboard evidence quality block can read and summarize them.
- Review labels are deterministic.

---

# P2-8：README / docs 清理

## 背景

Old backlog PRs and early roadmap docs may conflict with the current dashboard / evidence direction.

## Actions

1. Review `docs/github-issue-backlog.md` and decide whether to archive or replace it.
2. Add a short section to `README.md`:
   - dashboard control action history
   - dashboard v2 evidence blocks
   - unified evidence table
3. Update `docs/runnable_code_summary.md` if new modules are added.
4. Link this document from `docs/project_status_roadmap.md`.

## Acceptance criteria

- New developers can find this handoff from roadmap or README.
- Old stale PR references are not presented as current plan.

---

# Testing strategy

Run these levels after each PR:

```bash
python -m pytest tests/test_dashboard_control_service.py -q
python -m pytest tests/test_generate_dashboard_helpers.py -q
python -m pytest tests/test_dashboard_evidence.py -q
python -m pytest tests/test_investment_evidence.py -q
```

For full validation:

```bash
python -m pytest -q
```

If full test suite is too slow, at minimum run touched module tests plus import smoke:

```bash
python -m compileall src tests
```

---

# Definition of done for P2

P2 is done when:

- dashboard control action history is persisted, sanitized, and visible in dashboard payload.
- dashboard evidence helpers are isolated from `generate_dashboard.py`.
- dashboard v2 blocks render in advanced HTML view.
- US/HK/CN market views always exist with context and fallback.
- weekly attribution waterfall has stable schema and tests.
- weekly unified evidence artifacts are generated.
- blocked vs allowed ex-post review is generated and consumed by dashboard.
- docs link the new development path clearly.

---

# Recommended PR breakdown

## PR A

`refactor(dashboard): extract evidence helper builders`

Files:

- `src/common/dashboard_evidence.py`
- `src/tools/generate_dashboard.py`
- `tests/test_dashboard_evidence.py`

## PR B

`feat(dashboard): persist sanitized control action audit`

Files:

- `src/common/dashboard_control_audit.py`
- `src/app/supervisor.py`
- `tests/test_dashboard_control_audit.py`

## PR C

`feat(dashboard): render v2 evidence blocks`

Files:

- `src/tools/generate_dashboard.py`
- `tests/test_generate_dashboard_helpers.py`

## PR D

`feat(review): generate unified weekly evidence artifacts`

Files:

- `src/common/investment_evidence.py`
- `src/tools/review_investment_weekly.py`
- `tests/test_investment_evidence.py`

## PR E

`feat(review): add blocked-vs-allowed ex-post review`

Files:

- `src/common/investment_evidence.py`
- `src/tools/review_investment_weekly.py`
- `tests/test_investment_evidence.py`

## PR F

`docs: update dashboard evidence workflow documentation`

Files:

- `README.md`
- `docs/project_status_roadmap.md`
- `docs/runnable_code_summary.md`

---

# Developer notes

- Do not introduce new trading gates in P2.
- Do not rewrite `generate_dashboard.py` wholesale.
- Prefer pure helper functions under `src/common/` with tests.
- Keep dashboard payload backward-compatible.
- Treat empty/missing artifacts as warning/fallback, not hard failure.
- Keep market-specific behavior explicit for US/HK/CN.
- Avoid large PRs. Each PR should touch one conceptual layer.
