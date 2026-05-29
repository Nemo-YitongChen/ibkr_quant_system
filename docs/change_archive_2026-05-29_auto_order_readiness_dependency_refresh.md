# 2026-05-29 Auto Order Readiness Dependency Refresh

## 背景

Dashboard 已经能识别 `auto_order_readiness.json` 过旧，但如果 supervisor 长时间运行且 readiness 内容签名不变，旧 artifact 可能不会被重写。这样会让自动下单前的证据链停在旧时间点，尤其是在 weekly Gateway budget、preflight 或 market readiness 已刷新之后。

## 改动

- Supervisor 的 auto-order readiness 写入新增依赖感知刷新：
  - artifact 超过 `auto_order_readiness.max_artifact_age_hours` 会重写；
  - preflight summary、weekly review summary、weekly Gateway budget、market readiness 比 readiness artifact 新时会重写；
  - payload 新增 `rewrite_reason`，区分 `content_changed`、`existing_artifact_stale`、`dependency_newer_than_artifact` 等原因。
- Auto-order readiness 的 weekly 输入优先消费轻量 `weekly_ibkr_gateway_budget_status.json`，再 fallback 到大 `weekly_review_summary.json`，减少对超大 weekly summary 的依赖。
- 新增 supervisor tests，锁住同签名 stale rewrite、依赖新于 artifact 的 rewrite，以及轻量 Gateway budget artifact 覆盖 weekly summary 的行为。

## 交易含义

这一步不放宽自动提交门。它让自动下单证据更及时：当 Gateway budget 从 degraded 恢复、preflight 刷新、或 market readiness 更新后，下一轮 supervisor 更容易自动生成当前 readiness，而不是继续让 dashboard 消费旧 artifact。

## 验证

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/app/supervisor.py tests/test_supervisor_cli.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_write_auto_order_readiness_summary_uses_summary_out_dir tests/test_supervisor_cli.py::SupervisorCliTests::test_write_auto_order_readiness_summary_refreshes_stale_same_signature tests/test_supervisor_cli.py::SupervisorCliTests::test_write_auto_order_readiness_summary_refreshes_when_dependency_is_newer tests/test_supervisor_cli.py::SupervisorCliTests::test_auto_order_weekly_summary_prefers_lightweight_gateway_budget_artifact`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py tests/test_market_readiness.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_opportunity_gateway_budget_skip_blocks_degraded_market tests/test_supervisor_cli.py::SupervisorCliTests::test_dashboard_market_data_health_overview_marks_nonresearch_fallback_market_as_attention`
