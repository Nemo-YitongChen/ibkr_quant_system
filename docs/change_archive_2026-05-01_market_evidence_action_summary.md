# Change Archive: 2026-05-01 Market Evidence Action Summary

## Context

Dashboard evidence actions were available globally and at the portfolio/card level. The missing layer was market-level comparison: operators could not quickly see whether US, HK, or CN needed more samples, gate review, or signal calibration.

## Changes

- Added `market_evidence_action_summary` to `dashboard.json`.
- Aggregated evidence actions by market using the same evidence block contract as the portfolio-scoped action.
- Attached each market's evidence action, basis, rationale, and row count to `market_views`.
- Rendered these evidence fields in the US/HK/CN market view table.
- Added focused tests and smoke assertions for market-level evidence action output.

## Operational Impact

The dashboard can now compare evidence readiness and next action across markets without inspecting raw weekly artifacts or individual cards.
