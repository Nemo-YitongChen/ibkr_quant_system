# Change Archive - 2026-05-10 Strategy Parameter Dashboard Governance

## Summary

Added a read-only dashboard v2 advanced block for strategy parameter governance.

## Changes

- Added `strategy_parameter_governance` to dashboard v2 advanced blocks.
- Dashboard now loads weekly strategy parameter suggestions, suggestion follow-up rows, and suggestion effectiveness summary.
- The new block reports open, handled, resolved, stale, applied, auto-apply violation, and follow-up verdict counts.
- Advanced HTML consumes the new block through the existing generic v2 renderer.

## Safety Boundary

This change is read-only. It does not write strategy configuration and does not auto-apply weekly suggestions.

## Validation

- `tests/test_dashboard_blocks.py`
- `tests/test_investment_workflow_smoke.py`
