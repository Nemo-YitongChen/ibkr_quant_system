# 2026-07-11 Auto-order unblock plan tool

## Summary

- Added `src/tools/apply_auto_order_unblock_plan.py` and console script `ibkr-quant-auto-order-unblock`.
- The tool reads `auto_order_readiness.json -> summary.unblock_plan` and only supports the current safe recovery action: `stale_execution_refresh_required / refresh_stale_execution_target_no_submit`.
- Default mode is dry-run. It writes `auto_order_unblock_plan.json` and `auto_order_unblock_plan.md` with exact commands, but does not connect to IBKR and does not execute subprocesses.
- `--apply` is explicit and remains no-submit: generated execution commands include `--recovery_evidence_only` and never include `--submit`.
- If the source unblock plan has `submit_orders=true`, unsupported status/action, missing readiness, or an unknown target portfolio, the tool writes a blocked plan with no commands.

## Runtime Evidence

- Real runtime dry-run wrote:
  - `runtime_data/paper_investment_only_duq152001/reports_supervisor/auto_order_unblock/auto_order_unblock_plan.json`
  - `runtime_data/paper_investment_only_duq152001/reports_supervisor/auto_order_unblock/auto_order_unblock_plan.md`
- Current target is `US / US:watchlist`, symbol `SCHX`.
- The generated command sequence is:
  - refresh US investment report
  - refresh US execution evidence with `--recovery_evidence_only`
  - refresh market readiness
  - refresh auto-order readiness
  - refresh dashboard
- All generated command specs have `submit_orders=false`, `paper_only=true`, and `does_not_relax_submit_gates=true`.

## Trading Impact

- This change does not submit orders, does not relax risk/edge/cost/liquidity/market-rule/Gateway/submit-quality gates, and does not modify strategy parameters.
- It reduces manual recovery risk by converting the strongest current blocker (`market_readiness_not_ready` via stale execution artifact) into a repeatable, auditable, no-submit recovery plan.
- Actual IBKR-backed refresh still requires operator intent via `--apply`; the first two generated steps require Gateway availability and will consume bounded IBKR request budget.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_apply_auto_order_unblock_plan.py tests/test_review_opportunity_outcomes.py tests/test_dashboard_blocks.py` -> `24 passed`.
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.apply_auto_order_unblock_plan --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001` -> `status=ready`, `target_market=US`, `target_portfolio_id=US:watchlist`, `submit_orders=false`, `command_count=5`.
