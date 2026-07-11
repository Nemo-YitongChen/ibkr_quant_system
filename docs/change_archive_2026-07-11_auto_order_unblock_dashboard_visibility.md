# 2026-07-11 Auto-order unblock dashboard visibility

## Summary

- `generate_dashboard` now loads `reports_supervisor/auto_order_unblock/auto_order_unblock_plan.json` into `payload["auto_order_unblock_plan"]`.
- `dashboard_blocks.build_auto_order_readiness_block()` now surfaces the unblock dry-run artifact inside the existing `auto_order_readiness` block rather than adding another dashboard block.
- New block metrics include artifact presence, status, reason, apply flag, submit flag, command count, Gateway command count, failed command count, target market, target portfolio, and target symbols.
- Block rows now include the full `unblock_plan_artifact`, `unblock_plan_commands`, and `unblock_plan_command_results`.

## Runtime Evidence

- Real dashboard refresh completed against `runtime_data/paper_investment_only_duq152001/reports_supervisor`.
- The refreshed `dashboard.json` still has `dashboard_v2_blocks` count `14`.
- The `auto_order_readiness` block now shows:
  - `unblock_plan_artifact_present=1`
  - `unblock_plan_artifact_status=ready`
  - `unblock_plan_artifact_command_count=5`
  - `unblock_plan_artifact_gateway_command_count=2`
  - `unblock_plan_artifact_submit_orders=0`
  - `unblock_plan_artifact_target_market=US`
  - `unblock_plan_artifact_target_portfolio_id=US:watchlist`

## Trading Impact

- This does not connect to IBKR, does not submit orders, and does not relax any submit gate.
- It makes the next safe no-submit recovery step visible in the dashboard, reducing the risk that stale execution recovery depends on manually inspecting filesystem artifacts.
- Operator still must explicitly run `ibkr-quant-auto-order-unblock --apply` to consume IBKR Gateway request budget for the no-submit refresh.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/dashboard_blocks.py src/tools/generate_dashboard.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_dashboard_blocks.py tests/test_apply_auto_order_unblock_plan.py` -> `18 passed`.
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_dashboard_loads_auto_order_unblock_plan` -> `1 passed`.
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor` completed.
