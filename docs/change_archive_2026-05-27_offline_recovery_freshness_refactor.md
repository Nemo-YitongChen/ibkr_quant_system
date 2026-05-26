# 2026-05-27 Offline Recovery Freshness Refactor

## Scope

This change hardens the paper auto-order path after a network or IBKR Gateway outage longer than one day. It does not loosen risk gates, edge gates, Gateway request budget gates, market-rule gates, or paper-submit quality gates.

## Assessment

After an outage longer than 24 hours, the system should remain safe but should not assume old artifacts are still tradeable:

- Consistency: preflight, weekly review, market readiness, execution artifact health, and Gateway request-budget telemetry now share a common UTC timestamp/age helper instead of each module parsing freshness independently.
- Availability: supervisor/dashboard can still run and show the blocked state; stale/missing artifacts become visible as `offline_recovery_required` rather than failing silently or being hidden inside separate sections.
- Effectiveness: paper submit remains blocked until preflight and execution/market readiness artifacts are refreshed. This prevents old no-submit plans from being submitted after reconnect.

## Implementation

- Added `src/common/freshness.py` for UTC timestamp parsing, timestamp age, file age, and freshness labeling.
- Reused the common freshness helpers in:
  - `src/common/auto_order_readiness.py`
  - `src/common/market_readiness.py`
  - `src/common/ibkr_gateway_budget.py`
- Added auto-order recovery fields:
  - `offline_recovery_required`
  - `offline_recovery_reason`
  - `offline_recovery_reasons`
  - `offline_recovery_next_action`
  - `offline_recovery_gap_hours`
  - `offline_recovery_max_gap_hours`
- Added summary-level fields:
  - `offline_recovery_required_count`
  - `offline_recovery_markets`
  - `offline_recovery_portfolios`
  - `offline_recovery_reason_counts`
  - `offline_recovery_summary_text`
- Surfaced offline recovery in dashboard ops overview and Dashboard v2 auto-order block.

## Trading Policy Impact

The paper-submit behavior remains conservative:

- A stale preflight still blocks automated submit.
- A stale execution artifact still forces market readiness to `NEEDS_REFRESH`.
- A degraded Gateway budget still blocks submit.
- A candidate must still pass post-cost submit quality before it can be selected.

The new output improves operator sequencing after reconnect:

1. Refresh supervisor preflight.
2. Refresh investment report and paper execution dry-run.
3. Regenerate market readiness and auto-order readiness.
4. Only then review whether a single or multi-market small paper plan is still eligible.

## Validation

Targeted validation passed:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/freshness.py src/common/auto_order_readiness.py src/common/market_readiness.py src/common/ibkr_gateway_budget.py src/tools/dashboard_blocks.py src/tools/generate_dashboard.py tests/test_freshness.py tests/test_auto_order_readiness.py tests/test_dashboard_blocks.py tests/test_generate_dashboard_helpers.py
PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_freshness.py tests/test_auto_order_readiness.py tests/test_dashboard_blocks.py tests/test_generate_dashboard_helpers.py tests/test_market_readiness.py tests/test_ibkr_gateway_budget.py
```

Result: `91 passed`.

## Next Steps

- Refresh real preflight, market readiness, and paper execution artifacts after IBKR Gateway reconnect.
- Continue expanding non-CN watchlists through local execution-ready, whole-share, post-cost candidate evidence rather than hardcoding discretionary ticker picks.
- Once Gateway budget is no longer degraded, allow only the selected paper submit plan to proceed, then immediately review fill, slippage, broker ack, and no-order diagnostics.
