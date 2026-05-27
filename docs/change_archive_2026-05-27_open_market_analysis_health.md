# Change Archive: Open Market Analysis Health

Date: 2026-05-27

## Context

The scoped runtime dashboard was current, but it could still say that markets were open and reports were fresh while the auto-order gate evidence was missing from the same scoped summary directory. That made it harder to answer whether currently-open markets were actually ready for paper submission.

## Changes

- Added `src/common/open_market_analysis.py` to summarize open-market readiness from dashboard cards plus auto-order readiness rows.
- Added `open_market_analysis_summary` to dashboard JSON and dashboard v2 blocks.
- Added open-market metrics to ops overview and simple dashboard rows.
- Added an ops alert when open markets have stale reports, missing market state, missing auto-order gate evidence, blocked submit-gate rows, or market data attention.
- Made `Supervisor.run_cycle()` write a scoped `auto_order_readiness.json` artifact whenever the readiness signature changes, so the active runtime dashboard can consume the same gate evidence used by execution skips.

## Operating Notes

- This does not loosen risk, edge, market-rule, or submit gates.
- The change is diagnostic and governance-focused: open markets are only considered actionable when report freshness and auto-order readiness evidence agree.
- Current non-submit blockers should still be removed by priority: IBKR gateway availability, gateway request budget recovery, then per-portfolio submit-quality and market-readiness reasons.

## Validation

- `tests/test_open_market_analysis.py`
- `tests/test_generate_dashboard_helpers.py`
- `tests/test_dashboard_blocks.py`
- `tests/test_supervisor_cli.py`
