# 2026-06-18 Stale Execution Refresh Priority Plan

## Context

Auto-order readiness remains blocked mainly by Gateway budget pressure, stale execution artifacts, no current BUY plan, and stale strategy suggestions. The system already reports the blockers, but it did not rank which stale execution artifact should be refreshed first once request budget allows a no-submit evidence refresh.

## Changes

- Added read-only `stale_execution_refresh_plan` to auto-order readiness summary.
- The plan ranks stale execution artifacts by:
  - stale execution artifact status or stale offline-recovery reason,
  - positive post-cost candidate count,
  - high-cost-but-positive post-cost candidate count,
  - close `WAIT_PULLBACK` candidate count,
  - artifact stale gap,
  - existing order evidence.
- The plan excludes disabled and research-only rows.
- The plan always carries:
  - `paper_only=true`
  - `submit_orders=false`
  - `does_not_relax_submit_gates=true`
  - `request_policy=one_stale_execution_portfolio_after_gateway_budget_ok`
- If the top ranked target is blocked by Gateway budget, the plan status is `WAIT_GATEWAY_BUDGET` rather than refresh-ready.
- `auto_order_readiness.md` now includes a `Stale Execution Refresh Plan` section.
- Dashboard Auto Order block now exposes stale refresh plan metrics and rows.
- Dashboard Ops Health now checks Supervisor PID liveness when shutdown status says `running`; a dead PID is surfaced as a degraded supervisor state instead of being treated as healthy.
- Added a HK-only opportunity outcome validation refresh for the current post-cost and close `WAIT_PULLBACK` groups without overwriting the all-market dashboard artifact.

## Runtime check

Regenerated readiness and dashboard from local artifacts only:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_auto_order_readiness \
  --config config/supervisor.yaml \
  --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json \
  --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor

PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard \
  --config config/supervisor.yaml \
  --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor
```

Current runtime result:

- `stale_execution_refresh_plan.status=WAIT_GATEWAY_BUDGET`
- `primary_market=US`
- `primary_portfolio_id=US:watchlist`
- `target_count=4`
- `submit_orders=false`
- top rows include `US:watchlist`, `HK:resolved_hk_top100_tech_growth`, `HK:resolved_hk_top100_bluechip`, and `ASX:asx_top_quality`.

HK-only outcome validation was regenerated into:

```text
runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/opportunity_outcome_validation.json
```

Current HK outcome result:

- Positive post-cost candidates remain outcome-supported:
  - `HK:resolved_hk_top100_bluechip`: 5d `+138.32bps`, 20d `+307.90bps`
  - `HK:resolved_hk_top100_tech_growth`: 5d `+138.69bps`, 20d `+313.60bps`
- Close `WAIT_PULLBACK` candidates remain outcome-supported:
  - `HK:resolved_hk_top100_bluechip`: 5d `+125.96bps`, 20d `+212.07bps`
  - `HK:resolved_hk_top100_tech_growth`: 5d `+126.74bps`, 20d `+222.09bps`

Supervisor status after dashboard refresh:

- `supervisor_shutdown_status=running`
- `supervisor_shutdown_reason=ignored_signal:SIGHUP`
- `supervisor_shutdown_pid=77976`
- `supervisor_shutdown_liveness_status=unknown`
- `supervisor_shutdown_health_status=ready`

`unknown` means this local environment did not allow a reliable PID liveness probe. A definite dead PID now produces a `SUPERVISOR` alert and degraded Ops Health status.

## Trading impact

- No IBKR requests are added.
- No order is submitted.
- No YAML config is changed.
- No risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gate is relaxed.
- This only makes the next no-submit evidence refresh target explicit once request budget allows it.
