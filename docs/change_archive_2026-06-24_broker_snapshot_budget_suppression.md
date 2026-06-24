# 2026-06-24 Broker Snapshot Budget Suppression

## Context

HK outcome validation still supports paper-only calibration at the group level:

- Positive post-cost candidates: bluechip 5d `+122.89bps`, 20d `+253.84bps`; tech growth 5d `+125.19bps`, 20d `+264.69bps`.
- Close `WAIT_PULLBACK`: bluechip 5d `+125.96bps`, 20d `+212.07bps`; tech growth 5d `+126.74bps`, 20d `+222.09bps`.

The actionable boundary remains symbol-aware. Negative or missing single-symbol outcome names stay excluded from trial candidates.

The current auto-order blocker is not HK outcome evidence. The dominant blocker is Gateway budget degradation, with routine broker snapshot `positions` requests contributing most of the request load.

## Change

- Added Supervisor budget suppression for routine broker snapshot sync when `ibkr_gateway_budgets.suppress_broker_snapshot_when_degraded` is enabled.
- Added `suppress_broker_snapshot_statuses`, defaulting to `["degraded"]`.
- Added cycle summary visibility for skipped broker snapshots via `broker_snapshot_gateway_budget_degraded`.
- Added top-level `trade_engine` status in `supervisor_cycle_summary.json/md` so scheduled `src.main` stops caused by no active live market are visible as `reason=no_active_live_market`.

## Safety Boundary

This does not submit orders and does not relax any gate:

- risk gate unchanged
- edge gate unchanged
- cost gate unchanged
- liquidity gate unchanged
- market-rule gate unchanged
- Gateway budget gate unchanged
- submit-quality gate unchanged

The suppression applies to routine broker snapshot sync only. It does not suppress guard or execution safety paths.

## Validation

Targeted tests:

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py -k 'broker_snapshot_gateway_budget_skip or skips_broker_snapshot_when_gateway_budget_degraded or opportunity_gateway_budget_skip or reuses_broker_snapshot'
6 passed, 121 deselected
```

Full test suite:

```text
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider
754 passed
```

## Next Step

After Gateway budget recovers, run one no-submit stale execution refresh for the ranked target from `auto_order_readiness.summary.stale_execution_refresh_plan`. Do not submit until fresh BUY plan, submit quality, market rule, and Gateway budget all pass.
