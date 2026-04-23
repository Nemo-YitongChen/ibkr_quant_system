# 2026Q2 P0：稳定性底座与 artifact health

## 目标

P0 只解决一个核心问题：

> 让系统能识别“半坏状态”，并在 dashboard / weekly review / governance 之间给出可见、可降级、可测试的健康信号。

这一阶段不扩新入口，不做大规模 UI 重排，不调整新的 execution gate。

---

## Why now

当前主线已经有：

- workflow / contract / CI / smoke test
- governance / tuning action 方向
- dashboard / weekly review / preflight

现在最容易出问题的不是直接抛异常，而是：

- artifact 存在，但字段旧了
- weekly review 是旧版本
- execution summary 和 risk history 不同步
- governance 有产物，但 dashboard 没识别出来
- 页面静默空白，而不是明确 warning / degraded

所以 P0 是整个 2026Q2 的基础层。

---

## 范围

### in scope

- artifact contract registry
- artifact freshness / completeness / compatibility 检查
- dashboard degraded-state fallback
- governance health summary
- degraded-input regression tests

### out of scope

- dashboard 首页四块重组
- market tabs/cards
- weekly waterfall
- blocked vs allowed ex-post 质量评估
- 新 gate / 新 execution 规则

---

## 交付清单

## P0-1：artifact contract registry

### 目标

为主 artifact 建立统一 contract：

- `schema_version`
- `generated_at`
- `required_fields` / `required_columns`
- `freshness_hours`

### 建议文件

- `src/common/artifact_contracts.py`
- `src/common/artifact_loader.py`

### 第一批覆盖对象

- `weekly_review_summary.json`
- `weekly_execution_summary.csv`
- `investment_execution_summary.json`
- `risk_history_summary.json`
- `governance_actions.json`
- `supervisor_preflight_summary.json`

### 验收标准

- 至少 5 个主 artifact 已被 contract registry 描述
- contract 可在 dashboard 生成阶段统一读取
- 新 artifact 增加时不再写散落的 ad-hoc 判断

### 伪代码

```python
ARTIFACT_CONTRACTS = {
    "weekly_review_summary": ArtifactContract(...),
    "weekly_execution_summary": ArtifactContract(...),
    "risk_history_summary": ArtifactContract(...),
    "governance_actions": ArtifactContract(...),
}
```

---

## P0-2：artifact health / freshness 层

### 目标

统一输出：

- `ready`
- `warning`
- `degraded`

### 建议文件

- `src/common/artifact_health.py`

### 输出字段建议

```python
{
    "artifact_key": "weekly_review_summary",
    "status": "warning",
    "summary": "stale artifact: age_hours=98.2",
    "generated_at": "2026-04-20T08:30:00+10:00",
    "schema_version": "2026Q2.v1",
    "age_hours": 98.2,
    "missing_fields": [],
    "missing_columns": [],
    "warnings": ["stale artifact"]
}
```

### 验收标准

- health 逻辑不再散落在 `generate_dashboard.py`
- freshness / completeness / compatibility 都进入统一状态机
- 至少 1 个跨 artifact consistency 检查落地

### 伪代码

```python
def evaluate_artifact_health(contract, payload) -> ArtifactHealth:
    ...
```

---

## P0-3：dashboard degraded-state fallback

### 目标

dashboard 在主 artifact 缺失时不再静默空白，而是明确显示：

- 缺什么
- 当前降级到什么视图
- 哪些信息仍可用

### 建议文件

- `src/tools/generate_dashboard.py`
- `src/common/dashboard_blocks.py`（如果开始抽 block）

### 第一批需要降级的 section

- weekly review
- reconcile
- governance
- risk trail / risk history

### 降级策略

- 缺 weekly review：降级显示 report / paper / execution 摘要
- 缺 reconcile：保留 report / paper / execution，reconcile 卡 warning
- governance 缺字段：显示 governance warning summary
- risk history 旧版：兼容读取 + 标记 partial compatibility

### 验收标准

- 删除或缺失 `weekly_review_summary.json` 时 dashboard 仍可渲染
- `reconcile` 缺失时 dashboard 不崩
- 页面上能看到明确 warning/degraded 说明

---

## P0-4：governance health summary

### 目标

把治理状态变成系统健康的一部分，而不是孤立 artifact。

### 健康指标

- pending tuning actions 数量
- oldest pending action age
- 最近 4 周 approve / reject / superseded 比例
- 是否一次修改多个 primary field
- 当前 live 参数是否与最近 review evidence 一致

### 建议文件

- `src/common/governance_health.py`
- `src/tools/generate_dashboard.py`

### 验收标准

- dashboard 顶部或治理区块可显示 governance health
- evidence mismatch 时至少 warning，严重时 degraded
- 治理状态能参与整体健康汇总

---

## P0-5：degraded-input regression tests

### 目标

重点补异常路径，而不是继续堆 happy path。

### 建议文件

- `tests/test_artifact_health.py`
- `tests/test_dashboard_degraded_inputs.py`
- `tests/test_weekly_review_backward_compat.py`
- `tests/test_governance_health_summary.py`

### 第一批测试场景

1. 缺 `weekly_review_summary.json`
2. 缺 `broker_reconciliation_summary.json`
3. execution summary 缺 sidecar/meta/version
4. 旧版 risk history 与新版 dashboard 共存
5. governance 缺 `primary_fields`
6. governance 缺 `review_evidence`
7. pending action 太久

### 验收标准

- 至少 6 个 degraded-input case 纳入测试
- 至少 1 个测试进入 CI guardrail / integration 路径

---

## 推荐执行顺序

### Step 1

先做 contract registry + health helper：

- 不动 UI
- 不动大逻辑
- 先把底层 contract 固化

### Step 2

把 dashboard 顶部健康摘要改成读统一 health 层：

- 先替换 freshness/summary 判断
- 再接 degraded fallback

### Step 3

接 governance health：

- 让治理状态进入 health summary

### Step 4

最后补 tests：

- 缺失
- 旧版
- 缺字段
- 跨 artifact 时间不一致

---

## 推荐 PR 拆分

### PR 1

`feat(stability): add artifact contracts and health helpers`

### PR 2

`feat(dashboard): add degraded-state fallback for missing weekly/reconcile/governance artifacts`

### PR 3

`feat(governance): add governance health summary into dashboard`

### PR 4

`test(stability): add degraded-input regression coverage`

---

## 本阶段完成定义

P0 完成必须满足：

- dashboard 顶部存在 health/freshness 总览
- 缺失主 artifact 不再出现静默空白
- governance health 已纳入系统运行健康
- degraded-input regression tests 已进入项目测试体系

---

## 本阶段提交 checklist

- [ ] 新增/修改 artifact 是否定义了 `schema_version`
- [ ] 新增/修改 artifact 是否定义了 `generated_at`
- [ ] 是否定义了 required fields / columns
- [ ] dashboard 缺失输入时是否明确降级，而非静默空白
- [ ] governance 状态是否能进入健康概览
- [ ] 是否新增异常路径测试
- [ ] 是否更新 `docs/current_status.md`
- [ ] 若用户读法变化，是否更新 `README.md`
