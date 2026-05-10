# 2026-05-08 Dashboard v2 Information Architecture

## 背景

Dashboard v2 已经同时承载 ops health、evidence focus、market views、waterfall、action history、unified evidence 和 blocked-vs-allowed review。继续平铺会让首页变成“什么都有但没有主次”，因此需要把首页和高级证据层拆开。

## 已完成

- `dashboard_v2_blocks` schema 新增 `category` 与 `advanced_only`。
- Home blocks 固定为四类：
  - `ops_health`
  - `evidence_focus_actions`
  - `evidence_quality`
  - `dashboard_control_actions`
- Advanced blocks 固定为五类：
  - `market_views`
  - `weekly_attribution_waterfall`
  - `unified_evidence_overview`
  - `blocked_vs_allowed_expost`
  - `dashboard_control_action_history`
- 新增独立 advanced block builders，避免 market/evidence/waterfall/control history 只能塞在一个大 block 的 nested rows 里。
- dashboard HTML 现在在首页渲染 v2 home blocks，并只在 advanced mode 展开 advanced blocks 和 rows 预览。

## 验证

- `tests/test_dashboard_blocks.py` 锁定 home / advanced block 顺序、category 和 advanced_only。
- `tests/test_dashboard_rendering.py` 覆盖 home/advanced 分组渲染。
- `tests/test_investment_workflow_smoke.py` 更新 dashboard v2 block 数量和 home block 契约。

## 下一步

进入策略技术债第一批：把 primary signal weights / mid regime 参数低风险配置化，并补纯策略 regression coverage。
