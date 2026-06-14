# 2026-06-14 Bounded Seed Evidence Queue

## Goal

Close candidate-report gaps for small-account ETF review seeds without spending
IBKR Gateway request budget, widening the production universe, or weakening
execution gates.

## Changes

- Added a bounded seed evidence queue that only schedules
  `CANDIDATE_REPORT_REQUIRED` review seeds.
- Queue jobs are ordered by whole-share affordability and constrained by the
  current account growth-tier order-value cap.
- Added a Supervisor path that runs at most one market-level seed evidence job
  after the local watchlist expansion refresh.
- Seed evidence mode is yfinance-only and explicitly disables:
  - IBKR connections and scanners.
  - symbol-master and recent-symbol expansion.
  - regime benchmark refresh.
  - market-wide macro, news, and snapshot enrichment.
  - short-book analysis, fundamentals enrichment, backtests, and candidate
    snapshot persistence.
- Dedicated seed evidence artifacts are stored under
  `reports_investment_seed_review` and consumed by the next local watchlist
  expansion run.
- Auto-order readiness and dashboard v2 now expose queue count, ready jobs,
  primary market, symbols, and evidence mode.

## Real Artifact Result

The bounded ASX job reviewed only:

- `DHHF.AX`
- `BGBL.AX`

The run produced:

- candidates: `2`
- ranked candidates: `2`
- plans: `2`
- short candidates: `0`
- IBKR requests: `0`
- scanner requests: `0`
- market-wide enrichment requests: `0`

After the report was consumed, both candidates moved from
`CANDIDATE_REPORT_REQUIRED` to `QUALITY_REJECTED`. The seed evidence queue then
returned to zero. Neither candidate was promoted to the symbol master or made
eligible for submission.

## Current Trading State

Automated paper submission remains blocked. The current primary blocker is
`ibkr_gateway_unavailable`, followed by stale preflight and market-readiness
evidence. The best existing US frontier remains below the configured net-edge
and edge-margin thresholds.

This change improves evidence throughput and candidate-supply diagnostics. It
does not establish profitable edge, increase current submit capacity, or
guarantee asset growth.

## Validation

- Focused seed/watchlist/Supervisor/dashboard tests: `181 passed`
- Integration tier: `123 passed`
- Guardrail tier: `26 passed`
- Python compile checks passed
- `pip check` reported no broken requirements
- Real scoped watchlist expansion, auto-order readiness, and dashboard artifacts
  were refreshed
