# Change Archive - IBKR Gateway Request Budgets

日期：2026-05-09

## 背景

此前已经完成 IBKR Gateway 负载优化的基础能力：

- daily HMDS cache
- labeling 优先 external/cache history
- broker snapshot single-flight
- supervisor IBKR task spacing
- IBKR request telemetry

但 telemetry 只提供“有记录”，还没有形成运行预算和 dashboard/weekly health 结论。

## 本次改动

### 新增 Gateway budget 计算模块

新增：

- `src/common/ibkr_gateway_budget.py`

核心能力：

- 读取并标准化 Gateway budget config。
- 按 market 聚合 Gateway request count、cache hit、cache hit ratio。
- 识别 under budget、over budget、degraded over budget、missing telemetry、stale telemetry。
- 输出 weekly budget rows 和 summary payload。

### 新增 supervisor 配置

`config/supervisor.yaml` 新增：

```yaml
ibkr_gateway_budgets:
  enabled: true
  default_weekly_gateway_request_budget: 1500
  stale_telemetry_warning_hours: 72
  over_budget_degraded_ratio: 1.5
  missing_telemetry_status: "warning"
  markets:
    US:
      weekly_gateway_request_budget: 2000
    HK:
      weekly_gateway_request_budget: 1500
    ASX:
      weekly_gateway_request_budget: 1000
    XETRA:
      weekly_gateway_request_budget: 800
    CN:
      weekly_gateway_request_budget: 500
```

### Weekly review artifacts

`src/tools/review_investment_weekly.py` 现在会输出：

- `weekly_ibkr_gateway_budget_status.csv`
- `weekly_ibkr_gateway_budget_status.json`

并把以下字段写入 `weekly_review_summary.json`：

- `ibkr_request_summary`
- `ibkr_gateway_budget`
- `ibkr_gateway_budget_rows`

CLI summary 新增：

- `ibkr_gateway_budget_status`
- `ibkr_gateway_over_budget_market_count`
- `ibkr_gateway_budget_json`
- `ibkr_gateway_budget_csv`

### Weekly markdown

`weekly_review.md` 新增 `IBKR Gateway Request Budget` section，显示：

- summary status
- Gateway request count
- cache hit count / ratio
- over-budget market count
- stale / missing telemetry count
- market-level budget usage

### Dashboard ops health

`src/tools/generate_dashboard.py` 会读取 `weekly_ibkr_gateway_budget_status.json`，并把 summary 接入 `ops_overview`。

`src/tools/dashboard_blocks.py` 的 `Ops Health` block 新增 metrics：

- `ibkr_gateway_budget_status`
- `ibkr_gateway_budget_gateway_request_count`
- `ibkr_gateway_budget_cache_hit_count`
- `ibkr_gateway_budget_cache_hit_ratio`
- `ibkr_gateway_budget_max_usage_pct`
- `ibkr_gateway_budget_over_budget_market_count`
- `ibkr_gateway_budget_stale_telemetry_market_count`
- `ibkr_gateway_budget_missing_telemetry_market_count`

当 Gateway budget status 为 `warning` 或 `degraded` 时，dashboard ops alert rows 会出现 `IBKR_GATEWAY / request_budget`。

## 治理边界

本次改动只读：

- 不阻断 supervisor task。
- 不改交易参数。
- 不改 order / execution 行为。
- 不引入 long-lived Gateway broker。

## 测试

新增：

- `tests/test_ibkr_gateway_budget.py`

覆盖：

- under-budget -> ok
- over-budget -> warning
- far over-budget -> degraded
- missing telemetry -> warning
- stale telemetry -> warning

更新：

- `tests/test_dashboard_blocks.py`
- `tests/test_generate_dashboard_helpers.py`

目标验证：

- `py_compile` 目标文件通过。
- `tests/test_ibkr_gateway_budget.py tests/test_ibkr_request_telemetry.py tests/test_dashboard_blocks.py tests/test_generate_dashboard_helpers.py` 目标筛选：`13 passed, 35 deselected`
- `tests/test_review_weekly_output_support.py tests/test_review_weekly_helpers.py tests/test_review_investment_weekly.py`：`63 passed`
- `git diff --check` 通过。

## 后续

下一步应进入：

1. Strategy suggestion follow-up effectiveness。
2. Execution quality evidence。
3. Dashboard / weekly / supervisor helper extraction。
