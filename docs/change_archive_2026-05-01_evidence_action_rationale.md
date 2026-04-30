# Change Archive: 2026-05-01 Evidence Action Rationale

## Context

Dashboard evidence actions were scoped to each portfolio, but the payload still mainly said what to do next. It did not expose a stable reason field explaining why that action was selected.

## Changes

- Added `decision_basis`, `basis_label`, and `rationale` to `evidence_action_summary`.
- Preserved `blocked_label_summary` so consumers can audit blocked-vs-allowed label distribution.
- Added the rationale to the simple dashboard strategy explanation table.
- Added tests covering rationale extraction, blocked label summary propagation, and smoke coverage.

## Operational Impact

Operators and future automation can now distinguish:

- no unified evidence yet
- blocked rows outperformed allowed rows
- candidate model warning dominates
- blocked-vs-allowed evidence is sample-starved
- evidence is mixed
- blocking helped post-cost outcomes

This makes the evidence-first optimization path auditable instead of only action-oriented.
