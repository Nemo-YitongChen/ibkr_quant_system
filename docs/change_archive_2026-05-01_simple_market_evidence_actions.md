# Change Archive: 2026-05-01 Simple Market Evidence Actions

## Context

Market-level evidence action was available in `market_evidence_action_summary`, advanced market views, and dashboard v2 block metrics. The simple dashboard home view still required operators to drill into advanced sections to compare US/HK/CN evidence next steps.

## Changes

- Added a simple dashboard market evidence table with:
  - market
  - evidence next action
  - decision basis
  - evidence row count
  - short note/rationale
- Added `_simple_market_evidence_action_rows()` to render rows from `market_evidence_action_summary`, with fallback to legacy `market_views` fields.
- Persisted `evidence_primary_action`, `evidence_decision_basis`, and `evidence_action_note` onto each market view row when building dashboard payloads.
- Added focused tests for market-summary precedence and legacy market-view fallback.

## Operational Impact

The simple dashboard now exposes the market-level evidence loop directly on the landing view. Operators can quickly distinguish "collect more samples" from actual gate or signal calibration work without switching into advanced JSON-heavy views.
