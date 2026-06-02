# Change Archive: Auto-order Frequency Plan

Date: 2026-06-03

## Context

The auto-order submit plan already prevents unsafe paper submission through preflight, Gateway budget, market readiness, submit-quality, edge/cost, order-count, and order-value gates. The next gap was diagnostic: the dashboard could show that no paper order is currently allowed, but it did not clearly separate two different paths:

- The current frontier order is blocked by stale or degraded execution evidence.
- The candidate supply is too narrow and needs ETF-first seed work before more high-quality paper orders can appear.

For a small account, increasing order frequency should come from more whole-share, low-cost, post-cost-positive candidates, not from relaxing risk or execution gates.

## Changes

- Added `build_auto_order_frequency_plan()` in `src/common/auto_order_readiness.py`.
- `build_auto_order_readiness_summary()` now includes:
  - `frequency_plan`
  - `candidate_supply_status`
  - `candidate_supply_reason`
  - `candidate_supply_primary_action`
- `review_auto_order_readiness` now loads the watchlist expansion summary and renders a `Frequency Plan` section in markdown.
- Supervisor auto-order readiness now treats `watchlist_expansion/watchlist_expansion_summary.json` as a dependency, so seed proposal changes can refresh readiness evidence.
- Dashboard v2 `auto_order_readiness` block now exposes frequency/candidate-supply metrics and the raw frequency plan row.
- The frequency plan explicitly records:
  - `does_not_change_submit_decision=true`
  - `submit_gate_policy=do_not_relax_submit_gates`

## Current Local Diagnosis

The latest local refresh generated:

- `frequency_plan.status=frontier_blocked`
- `frequency_plan.reason=preflight_stale`
- `primary_action=Refresh supervisor preflight before automated submit.`
- `seed_proposal_count=3`
- `manual_seed_proposal_count=3`
- `seed_proposal_markets=ASX,HK,XETRA`

The current paper submit plan remains blocked. The strongest operational blockers are:

- `ibkr_gateway_unavailable`
- `preflight_stale`
- `gateway_budget_degraded`
- `market_readiness_not_ready`

This change did not submit paper orders and did not weaken any risk, edge, cost, market-rule, Gateway-budget, or submit-quality gate.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/auto_order_readiness.py src/tools/review_auto_order_readiness.py src/tools/dashboard_blocks.py src/app/supervisor.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_dashboard_blocks.py tests/test_supervisor_cli.py::SupervisorCliTests::test_write_auto_order_readiness_summary_uses_summary_out_dir tests/test_supervisor_cli.py::SupervisorCliTests::test_write_auto_order_readiness_summary_refreshes_when_dependency_is_newer tests/test_investment_workflow_smoke.py::test_investment_workflow_cli_smoke_generates_contract_artifacts`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.expand_investment_watchlists --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir /tmp/ibkr_quant_auto_expanded_verify --analysis_dir reports_supervisor/watchlist_expansion --account_profile small --account_equity 1000`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_auto_order_readiness --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`

## Next Technical Path

1. Restore Gateway/preflight freshness first; current frontier is blocked before candidate supply becomes the primary limiting factor.
2. Keep high-request scans throttled until Gateway budget recovers.
3. Review ASX/HK/XETRA seed proposals manually, then add only verified ETF-first, whole-share, low-cost candidates.
4. Regenerate reports, paper execution dry-run, market readiness, and auto-order readiness.
5. Submit paper only when the submit plan returns a current READY candidate under the existing small-account limits.
