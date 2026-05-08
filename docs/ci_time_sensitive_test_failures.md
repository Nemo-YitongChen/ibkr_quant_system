# CI 时间漂移测试失败排障与修复手册

## 背景

在执行：

```bash
pytest -q -p no:cacheprovider -m "not guardrail and not integration"
```

时出现 2 个失败：

```text
FAILED tests/test_artifact_health.py::test_weekly_unified_evidence_json_health_counts_rows
FAILED tests/test_governance_health_summary.py::test_governance_health_summary_warns_on_pending_actions
```

当前失败不是 dashboard evidence contract coverage 的业务逻辑错误，而是两个测试依赖真实当前时间，随着日期推进自然从通过变成失败。

这类问题称为 **time-sensitive / calendar-sensitive test drift**。

本 PR 已把这类问题从排障文档推进到测试护栏：

- `tests/test_governance_health_summary.py` 的 pending warning 测试已显式传入 `now=`，不会再随真实日期漂移。
- 已新增 stale approved-not-applied 的 degraded 测试，保留 14 天过期业务规则。
- `tests/test_artifact_health.py` 的 `weekly_unified_evidence` ready 测试已显式传入 `now=`，并补充了同类 freshness ready/stale 覆盖，避免 artifact health 测试随真实日期漂移。

---

## 失败 1：weekly_unified_evidence health 从 ready 变成 warning

### 失败现象

```text
assert row["status"] == "ready"
E AssertionError: assert 'warning' == 'ready'
```

失败测试：

```text
tests/test_artifact_health.py::test_weekly_unified_evidence_json_health_counts_rows
```

### 触发条件

测试写死了 artifact 时间：

```json
"generated_at": "2026-04-30T10:00:00+00:00"
```

而 `weekly_unified_evidence` 的 artifact contract 设置了 freshness 窗口：

```python
freshness_hours = 168
```

也就是 7 天。

`evaluate_artifact_health()` 会用真实当前时间计算 age。如果 `generated_at` 到当前时间超过 168 小时，就会添加 warning：

```text
stale artifact: age_hours=...
```

只要有 warning，status 就会从 `ready` 变成 `warning`。

### 根因

测试没有固定 `now`，而是让实现使用：

```python
datetime.now(timezone.utc)
```

所以测试结果会随日历日期漂移。

### 正确修复

在测试里传入固定 `now`，让 artifact 仍处于 freshness 窗口内。

修改：

```python
row = evaluate_artifact_health(contract, loaded, scope_label="GLOBAL")
```

为：

```python
from datetime import datetime, timezone

row = evaluate_artifact_health(
    contract,
    loaded,
    scope_label="GLOBAL",
    now=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
)
```

这样 `generated_at=2026-04-30T10:00:00+00:00` 到 `now=2026-05-01T10:00:00+00:00` 只有 24 小时，status 稳定为 `ready`。

### 建议补充测试

建议额外新增一个明确覆盖 stale 行为的测试：

```python
def test_weekly_unified_evidence_health_warns_when_stale(tmp_path: Path) -> None:
    contract = dashboard_artifact_contracts()["weekly_unified_evidence"]
    (tmp_path / "weekly_unified_evidence.json").write_text(
        (
            '{"generated_at":"2026-04-30T10:00:00+00:00",'
            '"schema_version":"2026Q2.p0.v1",'
            '"artifact_type":"weekly_unified_evidence",'
            '"row_count":1,'
            '"rows":[{"portfolio_id":"US:watchlist","market":"US","symbol":"AAPL"}]}'
        ),
        encoding="utf-8",
    )

    loaded = load_artifact(tmp_path, contract)
    row = evaluate_artifact_health(
        contract,
        loaded,
        scope_label="GLOBAL",
        now=datetime(2026, 5, 10, 10, 0, tzinfo=timezone.utc),
    )

    assert row["status"] == "warning"
    assert any(str(w).startswith("stale artifact") for w in row["warnings"])
```

这样 ready 行为和 stale warning 行为都会被显式锁住。

---

## 失败 2：governance health 从 warning 变成 degraded

### 失败现象

```text
assert summary["status"] == "warning"
E AssertionError: assert 'degraded' == 'warning'
```

失败测试：

```text
tests/test_governance_health_summary.py::test_governance_health_summary_warns_on_pending_actions
```

### 触发条件

测试写死 patch review history 时间：

```python
"ts": "2026-04-20T10:00:00+00:00"
```

`build_governance_health_summary()` 默认使用真实当前时间：

```python
now_dt = now or datetime.now(timezone.utc)
```

当前实现规则是：

```python
if approved_not_applied_count > 0 and oldest_pending_days >= 14.0:
    status = "degraded"
```

所以当真实日期距离 2026-04-20 超过 14 天后，这个测试就会从 `warning` 变成 `degraded`。

### 根因

测试名称和意图是验证“有 pending action 时给 warning”，但测试数据随着真实日期推进进入了“超过 14 天未应用，应 degraded”的业务分支。

### 正确修复

在 warning 测试里固定 `now` 到 14 天以内。

修改：

```python
summary = build_governance_health_summary(cards, overview_rows)
```

为：

```python
from datetime import datetime, timezone

summary = build_governance_health_summary(
    cards,
    overview_rows,
    now=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
)
```

这样 pending 只有 4 天，status 稳定为 `warning`。

### 建议补充测试

新增一个专门覆盖 14 天以上 pending 的 degraded 行为：

```python
def test_governance_health_summary_degrades_on_stale_approved_not_applied() -> None:
    cards = [
        {
            "market": "US",
            "watchlist": "watchlist",
            "dashboard_control": {
                "portfolio": {
                    "weekly_feedback_patch_governance_action_label": "优先处理已批准未应用 patch",
                    "weekly_feedback_market_profile_review_status": "APPROVED",
                    "weekly_feedback_market_profile_ready_for_manual_apply": True,
                }
            },
            "patch_review_history_rows": [
                {
                    "patch_kind": "market_profile",
                    "review_status": "APPROVED",
                    "ts": "2026-04-20T10:00:00+00:00",
                }
            ],
        }
    ]
    overview_rows = [
        {
            "rejection_rate": 0.0,
            "review_cycle_count": 1,
            "approved_not_applied_count": 1,
        }
    ]

    summary = build_governance_health_summary(
        cards,
        overview_rows,
        now=datetime(2026, 5, 8, 10, 0, tzinfo=timezone.utc),
    )

    assert summary["status"] == "degraded"
    assert summary["approved_not_applied_count"] == 1
    assert summary["oldest_pending_days"] >= 14.0
```

---

## 推荐补丁清单

### 1. 修改 `tests/test_artifact_health.py`

新增 import：

```python
from datetime import datetime, timezone
```

在 `test_weekly_unified_evidence_json_health_counts_rows` 中传固定 `now`：

```python
row = evaluate_artifact_health(
    contract,
    loaded,
    scope_label="GLOBAL",
    now=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
)
```

本 PR 同时新增了 `weekly_review_summary` 的 ready/stale 双路径测试，覆盖同一类 `evaluate_artifact_health(now=...)` 时间漂移风险。

---

### 2. 修改 `tests/test_governance_health_summary.py`

新增 import：

```python
from datetime import datetime, timezone
```

在 `test_governance_health_summary_warns_on_pending_actions` 中传固定 `now`：

```python
summary = build_governance_health_summary(
    cards,
    overview_rows,
    now=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
)
```

强烈建议新增 `test_governance_health_summary_degrades_on_stale_approved_not_applied`。

---

## 验收命令

先跑失败点：

```bash
python -m pytest tests/test_artifact_health.py::test_weekly_unified_evidence_json_health_counts_rows -q
python -m pytest tests/test_governance_health_summary.py::test_governance_health_summary_warns_on_pending_actions -q
```

再跑相关模块：

```bash
python -m pytest tests/test_artifact_health.py tests/test_governance_health_summary.py -q
```

最后跑 CI 对应选择器：

```bash
pytest -q -p no:cacheprovider -m "not guardrail and not integration"
```

---

## 后续测试规范

以后新增与时间相关的测试，必须遵守以下规则：

1. 如果被测函数支持 `now=` 参数，测试必须显式传 `now`。
2. 不要在断言 `ready/warning/degraded` 时依赖真实当前时间。
3. 如果测试 stale / overdue / expired 行为，应单独命名并传入明确的过期时间。
4. 同一个业务规则至少拆成两个测试：
   - 未过期路径
   - 已过期路径
5. 不要通过放宽业务规则来让测试通过。

---

## 结论

这次失败的正确处理方式是 **修测试，不改业务实现**。

原因：

- artifact 超过 freshness window 后变成 `warning` 是正确行为。
- approved-not-applied patch 超过 14 天后变成 `degraded` 是正确行为。
- 当前失败来自测试没有固定时间，导致测试随日历漂移。

修复后，测试既能稳定通过，也能保留现有健康规则的业务约束。
