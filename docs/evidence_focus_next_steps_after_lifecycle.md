# Evidence Focus Lifecycle 后续实施计划

## 当前基线

截至 `b9f4767`，Evidence Focus 已完成从 dashboard 展示到 action lifecycle 的第一轮落地。

已进入 main 的关键能力：

- `src/common/evidence_focus_actions.py` 已新增。
- dashboard evidence focus action 已拥有稳定字段：
  - `action_id`
  - `status`
  - `urgency`
  - `linked_evidence_artifact`
  - `linked_evidence_key`
  - `read_only`
- 原 dashboard 兼容字段仍保留：
  - `market`
  - `action`
  - `primary_action`
  - `basis`
  - `detail`
  - `priority_order`
- `generate_dashboard.py` 中的 evidence focus action / summary 构造已改为 common helper wrapper。
- `tests/test_evidence_focus_actions.py` 已覆盖 lifecycle normalization、action id、ex-post action 生成和 summary priority。
- `src/common/investment_evidence.py` 已存在，并已有 unified evidence normalization 与 blocked-vs-allowed aggregation 基础实现。
- weekly review 已具备输出 `weekly_unified_evidence` 与 `weekly_blocked_vs_allowed_expost` artifacts 的基础能力。

因此，下一阶段不应再把重点放在“生成 action”本身，而应推进：

1. dashboard control action 与 evidence action 的关联。
2. evidence action 的处理结果与周度效果回看。
3. dashboard 首页/advanced 信息架构收敛。
4. 后续策略配置和纯函数测试债务。

---

# 下一步优先级

## P0：Control Audit 关联 Evidence Action

### 目标

让 dashboard 能回答：

- 哪个 evidence action 被处理了？
- 是通过哪个 dashboard control action 处理的？
- 处理结果是 `ACKNOWLEDGED`、`APPLIED`、`REJECTED` 还是其他状态？
- 是否还有 urgent action 没有处理？

### 目标文件

新增或修改：

```text
src/common/dashboard_control_audit.py
src/app/supervisor.py
src/tools/generate_dashboard.py
tests/test_dashboard_control_audit.py
tests/test_evidence_focus_actions.py
```

如果 `src/common/dashboard_control_audit.py` 已存在，则扩展现有 helper；不要在 `supervisor.py` 里继续堆 inline dict。

### Dashboard control payload 扩展

dashboard control POST payload 可选支持：

```python
{
    "evidence_action_id": "2026W18-US-paper-review_gate_thresholds-blocked_outperformed_allowed",
    "market": "US",
    "portfolio_id": "paper-us",
    "resolution_status": "ACKNOWLEDGED",
    "resolution_note": "Reviewed threshold drift; keeping settings for one more week.",
}
```

### Action audit record 扩展字段

在 action audit record 中增加：

```python
{
    "linked_evidence_action_id": "...",
    "linked_market": "US",
    "linked_portfolio_id": "paper-us",
    "resolution_status": "ACKNOWLEDGED",
    "resolution_note": "...",
}
```

### Resolution status 映射

允许的 resolution status：

```python
ACKNOWLEDGED
APPLIED
REJECTED
SUPERSEDED
```

映射到 evidence action lifecycle：

```python
ACKNOWLEDGED -> ACTION_STATUS_ACKNOWLEDGED
APPLIED -> ACTION_STATUS_APPLIED
REJECTED -> ACTION_STATUS_REJECTED
SUPERSEDED -> ACTION_STATUS_SUPERSEDED
```

未知值不应抛异常，统一降级为 `ACKNOWLEDGED`。

### 建议 helper 接口

```python
from __future__ import annotations

from typing import Any, Dict


def normalize_resolution_status(value: Any) -> str:
    """Normalize dashboard control resolution status."""


def extract_evidence_action_link(payload: Dict[str, Any]) -> Dict[str, str]:
    """Extract evidence action linkage fields from a control payload."""


def attach_evidence_action_link(record: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return an audit record enriched with optional evidence action linkage."""
```

### 脱敏要求

继续沿用已有 action audit 脱敏规则：

- `token`
- `password`
- `secret`
- `api_key`
- `account`
- `account_id`

`resolution_note` 需要截断，建议最大 240 字符。

### 测试用例

```python
def test_extract_evidence_action_link_accepts_missing_fields(): ...

def test_extract_evidence_action_link_normalizes_resolution_status(): ...

def test_attach_evidence_action_link_preserves_existing_audit_fields(): ...

def test_attach_evidence_action_link_truncates_resolution_note(): ...
```

### Dashboard 输出要求

`generate_dashboard.py` 应在 dashboard control history / evidence focus area 中能展示：

```python
{
    "last_linked_evidence_action_id": "...",
    "last_resolution_status": "ACKNOWLEDGED",
    "open_urgent_action_count": 2,
    "linked_action_history_count": 5,
}
```

不要在这个 PR 中修改策略参数或自动执行任何 action。

### 验收命令

```bash
python -m pytest tests/test_dashboard_control_audit.py tests/test_evidence_focus_actions.py -q
python -m pytest tests/test_generate_dashboard_helpers.py -q
python -m py_compile src/common/dashboard_control_audit.py src/common/evidence_focus_actions.py src/app/supervisor.py src/tools/generate_dashboard.py
```

### 推荐 PR 标题

```text
feat(dashboard): link control audit to evidence actions
```

---

## P0：Evidence Action 状态合成

### 目标

将原始 evidence actions 与 dashboard control audit history 合成，得到当前 action 的最新状态。

### 新增或修改文件

```text
src/common/evidence_focus_actions.py
tests/test_evidence_focus_actions.py
src/tools/generate_dashboard.py
```

### 建议接口

```python
from typing import Dict, Iterable, List


def apply_action_resolutions(
    actions: Iterable[Dict[str, Any]],
    audit_rows: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Apply latest dashboard control audit resolution to evidence actions."""
```

### 规则

- 用 `action_id` 与 `linked_evidence_action_id` 匹配。
- 如果多个 audit rows 命中同一 action，取最新 `timestamp`。
- 只接受 resolution status：
  - `ACKNOWLEDGED`
  - `APPLIED`
  - `REJECTED`
  - `SUPERSEDED`
- 未命中的 action 保持原状态。
- `read_only` 继续为 `True`，表示 dashboard 不自动修改交易系统。

### 输出字段扩展

被 resolution 命中的 action 增加：

```python
{
    "status": "ACKNOWLEDGED",
    "resolved_at": "2026-05-06T12:00:00Z",
    "resolution_source": "dashboard_control",
    "resolution_note": "...",
}
```

### 测试用例

```python
def test_apply_action_resolutions_keeps_unmatched_suggested(): ...

def test_apply_action_resolutions_uses_latest_audit_row(): ...

def test_apply_action_resolutions_ignores_unknown_status(): ...
```

### 验收标准

- dashboard evidence focus summary 使用合成后的 status。
- urgent action 被 acknowledged 后，不再计入 open urgent。
- sample collection action 仍不触发 urgent warning。

---

# P1：Evidence Focus Effectiveness 周度回看

## 目标

在 weekly review 中回答：

- 本周新增多少 evidence actions？
- 有多少 urgent？
- 有多少被处理？
- 平均处理耗时是多少？
- 是否存在 stale urgent action？
- applied / rejected 后，后续表现是否改善？

## 目标文件

```text
src/common/evidence_focus_actions.py
src/tools/review_investment_weekly.py
tests/test_evidence_focus_actions.py
tests/test_review_weekly_helpers.py
```

## Summary schema

```python
{
    "new_action_count": 4,
    "urgent_action_count": 1,
    "resolved_action_count": 2,
    "applied_action_count": 1,
    "rejected_action_count": 1,
    "sample_collection_count": 2,
    "stale_urgent_action_count": 1,
    "avg_resolution_hours": 18.5,
}
```

## 规则

- `APPLIED`、`REJECTED`、`SUPERSEDED` 视为 resolved。
- `ACKNOWLEDGED` 不算 resolved，但可从 open urgent 中移除，具体取决于 dashboard 文案。第一版建议：acknowledged 不算 stale urgent。
- urgent action 超过 7 天没有 acknowledged/applied/rejected/superseded，则计入 stale urgent。
- sample collection 不计入 stale urgent。

## 建议接口

```python
from typing import Any, Dict, Iterable


def build_evidence_focus_effectiveness_summary(
    actions: Iterable[Dict[str, Any]],
    *,
    now_iso: str,
    stale_after_days: int = 7,
) -> Dict[str, Any]:
    """Build weekly evidence focus effectiveness metrics."""
```

## Markdown section

在 weekly review markdown 中新增：

```md
## Evidence Focus Effectiveness

- New actions: 4
- Urgent actions: 1
- Resolved actions: 2
- Stale urgent actions: 1
- Avg resolution time: 18.5h
```

## Artifact 输出

建议在 weekly review summary JSON 中增加：

```python
"evidence_focus_effectiveness": {...}
```

如果已有 dashboard summary artifact，则同步写入相同 key。

## 测试用例

```python
def test_effectiveness_counts_resolved_actions(): ...

def test_acknowledged_urgent_is_not_stale(): ...

def test_sample_collection_is_not_stale_urgent(): ...

def test_weekly_review_renders_evidence_focus_effectiveness_section(): ...
```

## 验收命令

```bash
python -m pytest tests/test_evidence_focus_actions.py tests/test_review_weekly_helpers.py -q
python -m py_compile src/common/evidence_focus_actions.py src/tools/review_investment_weekly.py
```

## 推荐 PR 标题

```text
feat(review): summarize evidence focus effectiveness
```

---

# P1：Blocked-vs-Allowed 与 Action 生成质量检查

## 目标

确保 blocked-vs-allowed review 不只是生成 artifact，还能稳定驱动 evidence action。

## 目标文件

```text
src/common/investment_evidence.py
src/common/evidence_focus_actions.py
tests/test_investment_evidence.py
tests/test_evidence_focus_actions.py
```

## 检查点

1. `BLOCKED_OUTPERFORMED_ALLOWED` 必须生成 urgent `review_gate_thresholds`。
2. `INSUFFICIENT_SAMPLE` 必须生成 `sample_collection` urgency。
3. `GATE_OK` 不应生成 urgent。
4. `weekly_blocked_vs_allowed_expost.json` 缺失时应生成 `build_weekly_unified_evidence` 或 `monitor_evidence`，不能 dashboard crash。

## 新增测试建议

```python
def test_blocked_outperformed_allowed_drives_urgent_action(): ...

def test_gate_ok_does_not_create_urgent_action(): ...

def test_missing_expost_artifact_does_not_crash_evidence_focus(): ...
```

## 推荐 PR 标题

```text
test(evidence): lock blocked-vs-allowed action mapping
```

---

# P2：Dashboard 信息架构收敛

## 目标

当前 dashboard 已经展示 evidence focus、ops health、market views、waterfall、action history 等多层信息。下一步需要减少首页噪音。

首页保留四类：

1. `Ops Health`
2. `Evidence Focus`
3. `Execution Quality`
4. `Governance / Control Actions`

Advanced 展开：

- `market_views`
- `weekly_attribution_waterfall`
- `unified_evidence_overview`
- `blocked_vs_allowed_expost`
- `dashboard_control_action_history`

## 目标文件

```text
src/tools/dashboard_blocks.py
src/tools/generate_dashboard.py
tests/test_dashboard_blocks.py
tests/test_generate_dashboard_helpers.py
```

## Block schema 扩展

每个 v2 block 增加：

```python
{
    "category": "home" | "advanced",
    "advanced_only": True | False,
}
```

示例：

```python
{
    "id": "ops_health",
    "category": "home",
    "advanced_only": False,
}
```

```python
{
    "id": "weekly_attribution_waterfall",
    "category": "advanced",
    "advanced_only": True,
}
```

## 测试用例

```python
def test_dashboard_home_blocks_are_limited_to_core_categories(): ...

def test_advanced_blocks_include_evidence_detail_layers(): ...

def test_existing_block_ids_remain_available(): ...
```

## 验收标准

- dashboard JSON 仍保留所有旧 block。
- home view 只显示核心四类。
- advanced view 可展开完整证据链。
- 不删除任何旧 key。

## 推荐 PR 标题

```text
refactor(dashboard): organize v2 blocks by home and advanced categories
```

---

# P2：策略技术债第一批

## 目标

在 evidence loop 初步闭环后，再开始策略参数和纯函数测试的技术债，不要抢在 P0/P1 前面。

## 第一批配置化字段

新增或修改：

```text
config/strategy_defaults.yaml
src/strategies/engine_strategy.py
src/signals/fusion.py
src/strategies/mid_regime.py
src/strategies/regime_adaptor.py
```

建议配置：

```yaml
engine:
  mr_weight: 0.6
  bo_weight: 0.4
  mid_qty_min: 0.25
  mid_qty_max: 1.25
  mid_soft_floor: 0.0
```

## 测试范围

```text
tests/unit/test_short_breakout_signal.py
tests/unit/test_signal_fusion.py
tests/unit/test_mid_regime.py
```

测试重点：

- breakout 向上/向下。
- `can_trade_short=False` 时 short signal 被置零。
- 高波动/深回撤时 mid regime scale 下降。

## 注意

不要用未经代码语义验证的断言，例如“连续上涨后 mean reversion 必须强买入”。先按当前函数真实逻辑写测试。

## 推荐 PR 标题

```text
refactor(strategy): move primary signal weights into config
```

和：

```text
test(strategy): add pure signal regression coverage
```

---

# 推荐执行顺序

## 第一批

1. `feat(dashboard): link control audit to evidence actions`
2. `feat(review): summarize evidence focus effectiveness`
3. `test(evidence): lock blocked-vs-allowed action mapping`

## 第二批

4. `refactor(dashboard): organize v2 blocks by home and advanced categories`

## 第三批

5. `refactor(strategy): move primary signal weights into config`
6. `test(strategy): add pure signal regression coverage`

---

# 暂缓事项

以下方向继续暂缓：

- 新增交易 gate。
- 自动调参。
- LLM 周报。
- PostgreSQL 迁移。
- 插件化市场。
- 多策略并行框架。
- Docker / 全量部署体系。

原因：当前 evidence focus 已能生成 action，但 action 的处理与效果验证还没闭环。先把 action lifecycle → control audit → weekly effectiveness 做完整，再扩展系统能力。