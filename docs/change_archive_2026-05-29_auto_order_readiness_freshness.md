# 2026-05-29 Auto Order Readiness Freshness

## 背景

`auto_order_readiness.json` 是 dashboard 判断 paper 自动提交是否安全的核心证据。当前运行状态里，weekly gateway budget 已经在 2026-05-28 更新，但 `auto_order_readiness.json` 仍停留在 2026-05-27，导致 dashboard 可能用旧的 submit gate 证据解释当前开市市场。

## 改动

- 新增 `auto_order_readiness_health`，检查 readiness artifact 是否缺失、缺少 `generated_at`、超过最大年龄，或早于 weekly gateway budget。
- `ops_overview` 新增 `auto_order_health=<status>` 摘要字段，并在 freshness 异常时输出 `AUTO_ORDER/readiness_freshness` 告警。
- dashboard v2 `auto_order_readiness` block 新增 readiness health 指标，ready submit plan 如果证据过旧会降为 warning。
- `config/supervisor.yaml` 新增 `auto_order_readiness.max_artifact_age_hours: 24`，默认要求自动下单证据 24 小时内刷新。

## 交易含义

本次不放宽风险门、edge gate 或 submit gate。它只防止系统在自动提交前消费过期证据；当 readiness 过旧时，正确动作是先刷新 preflight / paper execution / auto-order readiness，再判断是否存在当前 READY 的小额整股候选。

## 验证

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/generate_dashboard.py src/tools/dashboard_blocks.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`
