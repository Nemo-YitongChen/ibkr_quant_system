# 2026-06-18 Outcome Trial Gate Plan

## Context

HK positive post-cost candidates and close `WAIT_PULLBACK` groups have supportive 5/20d outcome evidence, but the current auto-order state is still blocked by stale execution artifacts, missing BUY plans, submit-quality failures, stale strategy suggestions, and Gateway budget pressure. The dashboard needed a direct bridge from outcome-supported trial contracts to current submit gates.

## Changes

- Added a read-only outcome trial gate plan inside the existing Auto Order dashboard block.
- The plan maps each `READY_FOR_MANUAL_REVIEW` calibration trial to the current portfolio readiness row.
- It reports current blockers including:
  - `fresh_buy_plan_required`
  - `buy_plan_missing`
  - `submit_quality_not_pass`
  - `strategy_suggestion_stale`
  - `gateway_budget_degraded`
- It only marks a trial as `READY_FOR_OPERATOR_PAPER_TRIAL` when the current artifact is fresh, a BUY plan exists, submit quality passes, and Gateway budget gates are not blocking.
- It always carries `paper_only=true`, `auto_apply=false`, `submit_orders=false`, and `does_not_relax_submit_gates=true`.

## Runtime Check

Regenerated local readiness and dashboard artifacts:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_auto_order_readiness \
  --config config/supervisor.yaml \
  --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json \
  --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor

PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard \
  --config config/supervisor.yaml \
  --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor
```

Current dashboard result:

- `outcome_trial_gate_status=BLOCKED_BY_CURRENT_GATES`
- `outcome_trial_gate_trial_count=8`
- `outcome_trial_gate_ready_count=0`
- `outcome_trial_gate_blocked_count=8`
- `outcome_trial_gate_primary_market=HK`
- `outcome_trial_gate_primary_portfolio_id=HK:resolved_hk_top100_bluechip`
- `outcome_trial_gate_primary_trial_type=HK_POST_COST_THRESHOLD_PAPER_TRIAL`
- `outcome_trial_gate_primary_blocker=fresh_buy_plan_required`
- `outcome_trial_gate_submit_orders=0`

Current blocker counts across the 8 trial rows:

- `fresh_buy_plan_required`: 7
- `buy_plan_missing`: 8
- `submit_quality_not_pass`: 8
- `strategy_suggestion_stale`: 4
- `gateway_budget_degraded`: 2

## Trading Impact

- No IBKR requests are added.
- No order is submitted.
- No YAML config is changed.
- No risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gate is relaxed.
- This prevents outcome-supported candidates from being mistaken for immediately executable orders.
