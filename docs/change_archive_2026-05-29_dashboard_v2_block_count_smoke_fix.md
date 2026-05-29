# 2026-05-29 Dashboard v2 Block Count Smoke Fix

## 背景

`watchlist_expansion` advanced block 接入 dashboard v2 后，`dashboard_v2_blocks` 从 13 个增加到 14 个。`tests/test_investment_workflow_smoke.py` 仍硬编码旧数量，导致 CI workflow smoke 失败。

## 改动

- 将 workflow smoke 中的固定数量断言改为 block id 契约：
  - 确认 block id 不重复；
  - 保持 home block 顺序断言；
  - 确认 `watchlist_expansion` block 存在。

## 验证

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile tests/test_investment_workflow_smoke.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_investment_workflow_smoke.py::test_investment_workflow_cli_smoke_generates_contract_artifacts`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_dashboard_blocks.py tests/test_generate_dashboard_helpers.py`
