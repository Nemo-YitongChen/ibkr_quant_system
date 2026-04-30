# Change Archive: 2026-05-01 Portfolio Scoped Evidence Action

## Context

The dashboard exposed a top-level evidence action and then attached that global action to each simple dashboard card. That was useful, but potentially misleading when the action came from one market or portfolio and was shown on every card.

## Changes

- Added portfolio/market scoped evidence action summaries for dashboard cards.
- Filtered unified evidence, blocked-vs-allowed reviews, candidate model reviews, and attribution waterfall rows by `portfolio_id` first, then by market when portfolio is unavailable.
- Preserved the global `evidence_action_summary` at the dashboard payload level.
- Added `global_evidence_action_summary` on cards for traceability.
- Added focused tests and smoke coverage for card-level scoping.

## Operational Impact

Simple dashboard cards now show evidence guidance that belongs to that card's portfolio or market. This prevents a HK/CN gate or sample warning from being shown as the next step for an unrelated US card.
