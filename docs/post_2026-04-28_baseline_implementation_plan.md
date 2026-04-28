# 2026-04-28 New Baseline Implementation Plan

本文件记录 `3c6f0c5` 之后的后续实施计划。该基线已经完成 PR / 分支收口，GitHub open PR 为 `0`，有效 backlog 已保留，当前全量测试基线为 `401 passed`。

## 1. Current Baseline

最新基线已经包含：

- dashboard control route table
- dashboard control handler exception JSON responses
- dashboard action history 初版
- dashboard evidence helpers
- dashboard v2 blocks 初版
- US / HK / CN market views 初版
- weekly attribution waterfall 初版
- unified evidence overview 初版
- backlog 文档
- smoke tests
- structure-check
- fill audit tests
- order audit tests
- dashboard freshness tests
- CI workflows for the above guardrails

`src/common/dashboard_evidence.py` 已实现：

- `build_weekly_attribution_waterfall`
- `build_market_views`
- `build_unified_evidence_overview`

因此后续任务不是“抽离 dashboard evidence helpers”，而是在现有 evidence / dashboard 基础上继续加固。

## 2. Execution Principles

1. 每个 PR 只做一个层级，不把 dashboard 渲染、weekly review、策略参数和交易执行混在同一批改动中。
2. 优先补测试和契约，再加功能。
3. dashboard 缺数据时降级，不中断。
4. 不新增 trading gate，先验证现有 gate。
5. CI 只增强，不打散；后续新增 workflow 应尽量并入现有分层 CI。

## 3. Recommended PR Order

### PR 1: Dashboard Evidence Contract Coverage

目标：锁住 `src/common/dashboard_evidence.py` 的输出契约，防止 dashboard v2、market view、waterfall、unified evidence 迭代时发生 schema 漂移。

覆盖：

- `build_weekly_attribution_waterfall(cards)`
- `build_market_views(cards)`
- `build_unified_evidence_overview(rows)`

验收命令：

```bash
python -m pytest tests/test_dashboard_evidence.py -q
python -m pytest tests/test_generate_dashboard_helpers.py -q
python -m compileall src tests
```

### PR 2: US / HK / CN Market Context

目标：让 dashboard market views 不只是统计表，而是能直接解释每个市场的操作重点和主要风险。

建议新增：

- `src/common/dashboard_market_context.py`
- `tests/test_dashboard_market_context.py`

建议修改：

- `src/common/dashboard_evidence.py`
- `tests/test_dashboard_evidence.py`

### PR 3: Render Dashboard v2 Blocks in Advanced HTML

目标：当前 dashboard v2 blocks 已进入 payload，下一步渲染到 HTML dashboard 的 advanced 区域，避免操作者只能看 JSON。

建议新增：

- `src/common/dashboard_rendering.py`
- `tests/test_dashboard_rendering.py`

建议修改：

- `src/tools/generate_dashboard.py`
- `tests/test_generate_dashboard_helpers.py`

### PR 4: Persist Sanitized Dashboard Control Action Audit

目标：保证 dashboard action history 可持久化、可脱敏、可截断，并能在 dashboard 重启后读取最近操作记录。

建议新增或调整：

- `src/common/dashboard_control_audit.py`
- `tests/test_dashboard_control_audit.py`
- `src/app/supervisor.py`
- `src/tools/generate_dashboard.py`

### PR 5: Weekly Unified Evidence Table

目标：把 `candidate -> gate -> order -> fill -> outcome` 串成统一证据表，为交易质量分析提供数据基础。

建议新增：

- `src/common/investment_evidence.py`
- `tests/test_investment_evidence.py`

建议输出 artifacts：

- `weekly_unified_evidence.csv`
- `weekly_unified_evidence.json`

### PR 6: Blocked vs Allowed Ex-Post Review

目标：回答核心问题：被 gate 挡掉的单，事后是不是更差。

建议输出 artifacts：

- `weekly_blocked_vs_allowed_expost.csv`
- `weekly_blocked_vs_allowed_expost.json`

## 4. Deferred Work

以下方向合理，但应在 unified evidence table 和 blocked-vs-allowed review 稳定后再推进：

- 多策略并行框架
- 插件化市场与研究数据源
- LLM 周报
- SQLite -> PostgreSQL
- Docker / 全量部署体系
- 大规模策略参数迁移

当前最重要的工作是把 dashboard / evidence / weekly review 的闭环稳定下来，并能持续回答“交易质量是否改善”。

## 5. Immediate Next Step

最小、最安全、最有价值的一步是：

`test(dashboard): add dashboard evidence contract coverage`

需要覆盖：

- `test_market_views_empty_input_returns_all_markets`
- `test_market_views_counts_modes_and_health`
- `test_waterfall_has_stable_components_and_residual`
- `test_unified_evidence_overview_counts_bool_and_string_flags`
