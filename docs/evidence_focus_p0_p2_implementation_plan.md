# Evidence Focus P0-P2 技术实施计划

## 背景与当前基线

截至最新主线 `6b29dab`，项目已经完成了一轮 dashboard evidence focus 可视化与 ops health 对齐：

- dashboard 已有 evidence focus actions。
- dashboard simple view 已能展示 market evidence actions。
- dashboard v2 已有 `Evidence Focus Actions` block。
- dashboard ops overview 已纳入 urgent evidence focus。
- dashboard v2 `Ops Health` block 已补充：
  - `evidence_focus_action_count`
  - `evidence_focus_urgent_count`
  - `evidence_focus_primary_market`
  - `evidence_focus_primary_action`
- `src/common/dashboard_evidence.py` 已存在，并已包含：
  - `build_weekly_attribution_waterfall`
  - `build_market_views`
  - `build_unified_evidence_overview`
- `src/common/investment_evidence.py` 已存在，并已包含 unified evidence normalization 与 blocked-vs-allowed ex-post aggregation。
- weekly review 已能输出 `weekly_unified_evidence` 与 `weekly_blocked_vs_allowed_expost` artifacts。

因此，下一阶段不再继续堆展示字段，而是把 evidence focus 从“dashboard 可见”推进到“行动闭环 + 周度回看 + 交易质量验证”。

## 本 PR 已落地的代码状态

本 PR 不再只是计划文档，已完成 P0-1 的第一层代码落地：

- 新增 `src/common/evidence_focus_actions.py`，集中维护 evidence focus action lifecycle schema。
- 新增稳定 `action_id`、`status`、`urgency`、`linked_evidence_artifact`、`linked_evidence_key`、`read_only` 字段。
- `src/tools/generate_dashboard.py` 的 `_build_evidence_focus_actions()` 与 `_build_evidence_focus_summary()` 已改为薄 wrapper，委托 common 模块。
- 现有 dashboard JSON 字段保持兼容：`market`、`action`、`primary_action`、`basis`、`detail`、`priority_order` 仍保留。
- 新增 `tests/test_evidence_focus_actions.py` 覆盖 lifecycle normalization、action id、ex-post action 生成与 summary priority。

---

## 总目标

后续 P0-P2 的核心目标是回答三个问题：

1. **Evidence action 有没有被处理？**
2. **处理后交易质量有没有改善？**
3. **dashboard 是否能把当前最重要的问题压缩成操作员一眼能懂的视图？**

---

# P0：Evidence Focus 行动闭环与统一证据表

P0 是下一阶段最优先的代码工作。目标不是新增 dashboard 字段，而是把当前 dashboard 上显示的 evidence focus action 与 weekly evidence artifact 绑定起来。

---

## P0-1：建立 Evidence Focus Action 生命周期模型

### 目标

让 evidence focus action 不只是 dashboard 上的一段建议文本，而是拥有可追踪状态、来源、证据 artifact 和后续 resolution 的结构化对象。

### 新增文件

```text
src/common/evidence_focus_actions.py
tests/test_evidence_focus_actions.py
```

### 修改文件

```text
src/tools/generate_dashboard.py
src/tools/review_investment_weekly.py
```

如果当前 evidence focus action 已在其他 helper 中生成，先保留原入口，在内部委托到新模块，避免一次性破坏 dashboard payload。

### Action 状态

```python
ACTION_STATUS_SUGGESTED = "SUGGESTED"
ACTION_STATUS_ACKNOWLEDGED = "ACKNOWLEDGED"
ACTION_STATUS_APPLIED = "APPLIED"
ACTION_STATUS_REJECTED = "REJECTED"
ACTION_STATUS_SUPERSEDED = "SUPERSEDED"
ACTION_STATUS_EXPIRED = "EXPIRED"
```

### Urgency 级别

```python
URGENCY_URGENT = "urgent"
URGENCY_NORMAL = "normal"
URGENCY_SAMPLE_COLLECTION = "sample_collection"
```

### Action schema

每条 action 必须至少包含：

```python
{
    "action_id": "2026W18-US-paper-us-review_gate_thresholds",
    "market": "US",
    "portfolio_id": "paper-us",
    "action_type": "review_gate_thresholds",
    "basis": "blocked_outperformed_allowed",
    "urgency": "urgent",
    "status": "SUGGESTED",
    "created_at": "2026-05-01T00:00:00Z",
    "updated_at": "2026-05-01T00:00:00Z",
    "owner": "",
    "linked_evidence_artifact": "weekly_blocked_vs_allowed_expost.csv",
    "linked_evidence_key": "US|paper-us|20d",
    "read_only": True,
    "summary": "US gate review needed: blocked orders outperformed allowed orders on 20d horizon.",
}
```

### 建议函数接口

```python
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha1
from typing import Any, Dict, Iterable, List


def normalize_action_status(status: Any) -> str:
    """Return a known action status, defaulting to SUGGESTED."""


def normalize_urgency(value: Any) -> str:
    """Return urgent/normal/sample_collection."""


def build_action_id(*, week: str, market: str, portfolio_id: str, action_type: str, basis: str) -> str:
    """Build deterministic action id. Use a short hash suffix if the raw id is too long."""


def normalize_evidence_focus_action(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Return a complete action dict with all required fields populated."""


def build_evidence_focus_actions_from_expost(rows: Iterable[Dict[str, Any]], *, week: str) -> List[Dict[str, Any]]:
    """Create actions from blocked-vs-allowed ex-post rows."""


def summarize_evidence_focus_actions(actions: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Return dashboard summary: counts, primary market/action, urgent count, basis counts."""
```

### Action 生成规则

从 blocked-vs-allowed review 行生成 action：

```python
if review_label == "BLOCKED_OUTPERFORMED_ALLOWED":
    action_type = "review_gate_thresholds"
    urgency = "urgent"
elif review_label == "INSUFFICIENT_SAMPLE":
    action_type = "continue_sample_collection"
    urgency = "sample_collection"
elif review_label == "GATE_OK":
    action_type = "keep_gate_policy"
    urgency = "normal"
else:
    action_type = "review_evidence"
    urgency = "normal"
```

### Summary schema

```python
{
    "action_count": 4,
    "urgent_count": 1,
    "sample_collection_count": 2,
    "primary_market": "US",
    "primary_action": "review_gate_thresholds",
    "primary_basis": "blocked_outperformed_allowed",
    "read_only": True,
    "summary_text": "1 urgent evidence action: US review_gate_thresholds.",
    "actions": [...],
}
```

### 测试用例

`tests/test_evidence_focus_actions.py`

```python
def test_normalize_evidence_focus_action_fills_defaults(): ...

def test_build_action_id_is_deterministic(): ...

def test_expost_blocked_outperformed_allowed_creates_urgent_gate_review(): ...

def test_insufficient_sample_creates_sample_collection_action(): ...

def test_summarize_evidence_focus_actions_prioritizes_urgent(): ...
```

### 验收标准

- dashboard 仍能读取原有 evidence focus payload。
- 新 action 有稳定 `action_id`。
- urgent action 可计数。
- sample collection 不进入 urgent warning。
- 所有 action 默认为 read-only，不自动改配置、不自动下单。

### 建议 PR 标题

```text
feat(evidence): add evidence focus action lifecycle model
```

---

## P0-2：生成 Unified Weekly Evidence Artifacts

### 目标

将 candidate → gate → order → fill → outcome 串成统一证据表，供 dashboard evidence focus、blocked-vs-allowed review、weekly review 共用。

### 新增文件

```text
src/common/investment_evidence.py
tests/test_investment_evidence.py
```

### 修改文件

```text
src/tools/review_investment_weekly.py
src/tools/generate_dashboard.py
```

### Artifact 输出

```text
weekly_unified_evidence.csv
weekly_unified_evidence.json
```

推荐输出目录沿用 weekly review 当前 out_dir。不要新增全局 runtime 路径，避免 dashboard 和 weekly review 找不到文件。

### Evidence columns

```python
EVIDENCE_COLUMNS = [
    "week",
    "market",
    "portfolio_id",
    "symbol",
    "decision_id",
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

### 建议函数接口

```python
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

EVIDENCE_COLUMNS = [...]


def normalize_evidence_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Return a row with all EVIDENCE_COLUMNS populated."""


def evidence_join_key(row: Dict[str, Any]) -> Tuple[str, ...]:
    """Preferred key order: decision_id, then portfolio_id/symbol/decision_ts, then portfolio_id/symbol/week."""


def build_unified_evidence_rows(
    *,
    candidate_rows: Iterable[Dict[str, Any]],
    execution_rows: Iterable[Dict[str, Any]],
    outcome_rows: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Join candidate/gate/execution/outcome data into weekly evidence rows.

    Candidate rows are the left side. Do not drop candidates if execution/outcome is missing.
    """


def write_evidence_artifacts(out_dir: Path, rows: Iterable[Dict[str, Any]]) -> Dict[str, str]:
    """Write CSV and JSON artifacts and return paths."""
```

### Join 规则

优先级：

1. `decision_id`
2. `portfolio_id + symbol + decision_ts`
3. `portfolio_id + symbol + week`

如果无法完整 join：

```python
row["join_quality"] = "partial"
```

如果成功 join：

```python
row["join_quality"] = "full"
```

### Gate flag 规则

```python
blocked_flag = gate_status in {
    "blocked_edge",
    "blocked_market_rule",
    "blocked_risk",
    "blocked_liquidity",
}
allowed_flag = gate_status in {"allowed", "submitted", "filled"}
```

### 测试用例

`tests/test_investment_evidence.py`

```python
def test_normalize_evidence_row_adds_all_columns(): ...

def test_build_unified_evidence_rows_keeps_candidate_without_execution(): ...

def test_build_unified_evidence_rows_joins_execution_and_outcome_by_decision_id(): ...

def test_write_evidence_artifacts_outputs_csv_and_json(tmp_path): ...
```

### 验收标准

- weekly review 生成 CSV 和 JSON。
- candidate rows 不因缺 execution/outcome 被丢弃。
- dashboard `unified_evidence_overview` 可读取 artifact。
- 所有输出列顺序稳定。

### 建议 PR 标题

```text
feat(review): generate unified weekly evidence artifacts
```

---

# P1：Blocked-vs-Allowed 回看与 Action/Audit 关联

P1 建立 “dashboard 建议 → 人工/控制动作 → 后续结果” 的解释链。

---

## P1-1：Blocked vs Allowed Ex-post Review

### 目标

验证现有 gate 是否过紧或过松。

核心问题：

> 被 gate 挡掉的订单，事后表现是否弱于 allowed 订单？

### 修改文件

```text
src/common/investment_evidence.py
src/tools/review_investment_weekly.py
tests/test_investment_evidence.py
```

### Artifact 输出

```text
weekly_blocked_vs_allowed_expost.csv
weekly_blocked_vs_allowed_expost.json
```

### 函数接口

```python
def build_blocked_vs_allowed_expost_review(evidence_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate outcomes by market, portfolio_id, and horizon."""
```

### Output schema

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

### Horizon

必须输出：

```python
["5d", "20d", "60d"]
```

对应 evidence columns：

```python
outcome_5d
outcome_20d
outcome_60d
```

### Label 规则

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

### 测试用例

```python
def test_blocked_vs_allowed_marks_gate_ok_when_blocked_underperforms(): ...

def test_blocked_vs_allowed_marks_restrictive_when_blocked_outperforms(): ...

def test_blocked_vs_allowed_marks_insufficient_sample(): ...
```

### 验收标准

- weekly review 输出 blocked-vs-allowed CSV/JSON。
- evidence focus actions 可从 review_label 生成。
- sample 不足只生成 sample_collection action。

### 建议 PR 标题

```text
feat(review): add blocked-vs-allowed ex-post review
```

---

## P1-2：Action Audit 与 Evidence Focus Action 关联

### 目标

把 dashboard control action history 和 evidence focus actions 打通，让 dashboard 能回答：

- 哪个 evidence action 被处理？
- 是谁/什么 control action 处理的？
- 处理结果是 applied、rejected 还是 acknowledged？

### 修改文件

```text
src/common/dashboard_control_audit.py
src/app/supervisor.py
src/tools/generate_dashboard.py
tests/test_dashboard_control_audit.py
```

如果 `src/common/dashboard_control_audit.py` 尚不存在，则新增。

### Action audit record 增加字段

```python
{
    "linked_evidence_action_id": "...",
    "linked_market": "US",
    "linked_portfolio_id": "paper-us",
    "resolution_status": "APPLIED",
}
```

### Payload 输入

dashboard control POST payload 可接受：

```python
{
    "evidence_action_id": "...",
    "market": "US",
    "portfolio_id": "paper-us",
    "resolution_status": "APPLIED"
}
```

### 安全要求

- 不允许 action audit payload 保存 token/password/account_id。
- 长字符串截断。
- 无 `evidence_action_id` 时保持向后兼容。

### 测试用例

```python
def test_action_record_links_evidence_action_id(): ...

def test_action_audit_sanitizes_linked_payload(): ...

def test_action_audit_accepts_missing_evidence_action_id(): ...
```

### 验收标准

- dashboard control history 可显示 linked evidence action。
- 不影响旧 action history。
- 敏感字段脱敏。

### 建议 PR 标题

```text
feat(dashboard): link control audit to evidence actions
```

---

## P1-3：Evidence Focus Effectiveness 周度回看

### 目标

在 weekly review 中加入 evidence focus action 的效果追踪。

### 修改文件

```text
src/tools/review_investment_weekly.py
src/common/evidence_focus_actions.py
tests/test_evidence_focus_actions.py
tests/test_review_weekly_helpers.py
```

### Review section

新增 markdown section：

```text
## Evidence Focus Effectiveness
```

### 指标

```python
{
    "new_action_count": 4,
    "urgent_action_count": 1,
    "resolved_action_count": 2,
    "applied_action_count": 1,
    "rejected_action_count": 1,
    "sample_collection_count": 2,
    "avg_resolution_hours": 18.5,
    "stale_urgent_action_count": 1,
}
```

### 规则

- urgent action 超过 7 天未处理 → stale urgent。
- sample_collection action 不计入 stale urgent。
- applied/rejected/superseded 都算 resolved。

### 测试用例

```python
def test_evidence_focus_effectiveness_counts_resolved_actions(): ...

def test_sample_collection_does_not_count_as_stale_urgent(): ...

def test_weekly_review_renders_evidence_focus_effectiveness_section(): ...
```

### 验收标准

- weekly review markdown 有新 section。
- dashboard 可读取 summary artifact。
- stale urgent action 进入 ops health warning。

### 建议 PR 标题

```text
feat(review): summarize evidence focus effectiveness
```

---

# P2：Dashboard 信息架构收敛与策略技术债

P2 的目标是减少 dashboard 噪音，并为后续策略优化做准备。

---

## P2-1：Dashboard V2 首页收敛

### 目标

dashboard v2 block 越来越多，需要信息架构收敛。

首页只保留四组：

1. `Ops Health`
2. `Evidence Focus`
3. `Execution Quality`
4. `Governance / Control Actions`

Advanced 再展开：

- market views
- waterfall
- unified evidence overview
- blocked vs allowed
- action history

### 修改文件

```text
src/tools/dashboard_blocks.py
src/tools/generate_dashboard.py
tests/test_dashboard_blocks.py
tests/test_generate_dashboard_helpers.py
```

### 建议实现

新增 block 分类字段：

```python
{
    "id": "evidence_focus_actions",
    "title": "Evidence Focus Actions",
    "category": "home",
    "advanced_only": False,
}
```

Advanced-only 示例：

```python
{
    "id": "weekly_attribution_waterfall",
    "category": "advanced",
    "advanced_only": True,
}
```

### 测试

```python
def test_dashboard_home_blocks_are_limited_to_core_four_categories(): ...

def test_advanced_blocks_still_include_market_views_and_waterfall(): ...
```

### 验收标准

- 首页不再堆所有 evidence 细节。
- advanced 仍能看到完整证据链。
- 现有 dashboard JSON key 不删除。

### 建议 PR 标题

```text
refactor(dashboard): organize v2 blocks by home and advanced categories
```

---

## P2-2：策略参数配置化第一批

### 目标

把影响交易行为的策略参数从代码硬编码迁移到配置，但不做大规模策略框架改造。

### 修改文件

```text
src/strategies/engine_strategy.py
src/signals/fusion.py
src/strategies/mid_regime.py
src/strategies/regime_adaptor.py
config/strategy_defaults.yaml
tests/test_engine_execution_boundary.py
```

### 不要做

- 不要机械迁移所有数字。
- 不要迁移测试 fixture 里的数字。
- 不要改变默认行为。

### 第一批字段

```yaml
engine:
  mr_weight: 0.6
  bo_weight: 0.4
  mid_qty_min: 0.25
  mid_qty_max: 1.25
  mid_soft_floor: 0.0
```

### 验收标准

- 默认配置下测试行为不变。
- 缺字段时使用 dataclass 默认值。
- YAML 覆盖值能被读取。

### 建议 PR 标题

```text
refactor(strategy): move primary signal weights into config
```

---

## P2-3：核心策略纯函数测试

### 目标

为策略纯函数补低成本回归测试。

### 新增测试

```text
tests/unit/test_short_breakout_signal.py
tests/unit/test_signal_fusion.py
tests/unit/test_mid_regime.py
```

### 先测这些

- breakout 向上/向下。
- `can_trade_short=False` 时负向 short signal 被置零。
- 高波动/深回撤时 mid regime scale 下降。

### 注意

不要用“连续上涨后 mean reversion 应强买入”这种未经代码语义验证的断言。先读当前函数定义，再构造测试。

### 建议 PR 标题

```text
test(strategy): add pure signal regression coverage
```

---

# 推荐执行顺序

## 第一批：必须先做

1. `feat(review): generate unified weekly evidence artifacts` - 已在 main 具备基础实现。
2. `feat(review): add blocked-vs-allowed ex-post review` - 已在 main 具备基础实现。
3. `feat(evidence): add evidence focus action lifecycle model` - 本 PR 已完成第一层 common 模块与 dashboard 接入。

## 第二批：闭环增强

4. `feat(dashboard): link control audit to evidence actions`
5. `feat(review): summarize evidence focus effectiveness`

## 第三批：dashboard 收敛

6. `refactor(dashboard): organize v2 blocks by home and advanced categories`

## 第四批：策略技术债

7. `refactor(strategy): move primary signal weights into config`
8. `test(strategy): add pure signal regression coverage`

---

# 明确暂缓

以下工作方向合理，但暂缓：

- 多策略并行框架。
- 插件化市场。
- LLM 周报。
- PostgreSQL 迁移。
- Docker / 部署体系。
- 新增自动调参。
- 新增交易 gate。

原因：当前系统已经能提出 evidence focus action，但还没有完整验证这些 action 的处理效果。先把 evidence loop 做闭环，再扩展能力。

---

# 当前最小下一步

本 PR 完成后，下一步建议直接实现：

```text
feat(dashboard): link control audit to evidence actions
```

最小范围：

```text
src/common/dashboard_control_audit.py
src/app/supervisor.py
src/tools/generate_dashboard.py
tests/test_dashboard_control_audit.py
```

最小验收：

```bash
python -m pytest tests/test_dashboard_control_audit.py tests/test_evidence_focus_actions.py -q
python -m py_compile src/common/dashboard_control_audit.py src/common/evidence_focus_actions.py src/app/supervisor.py src/tools/generate_dashboard.py
```

这一步完成后，dashboard control action history 才能回答“哪个 evidence action 被处理、处理结果是什么”，而不仅是展示建议队列。
