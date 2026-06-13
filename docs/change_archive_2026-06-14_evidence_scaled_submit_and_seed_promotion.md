# 2026-06-14 Evidence-Scaled Submit Capacity and Seed Promotion Review

## Goal

Improve paper auto-order quality and candidate supply without increasing risk
before fill, slippage, and post-cost edge evidence exists.

## Changes

- Added an evidence-scaled paper submit capacity plan.
- Kept the configured future ceiling at four portfolios / 400 AUD, but made the
  current effective ceiling one portfolio / 100 AUD until execution evidence
  passes.
- Required at least five fills and five matured 5-day realized-edge samples
  before capacity can scale.
- Required non-negative matured realized edge, absolute realized slippage no
  worse than 15 bps, and execution error rate no worse than 5%.
- Added a structured seed promotion lifecycle:
  - `SOURCE_REFRESH_REQUIRED`
  - `CANDIDATE_REPORT_REQUIRED`
  - `QUALITY_REJECTED`
  - `BROKER_MAPPING_REQUIRED`
  - `PROMOTION_REVIEW_READY`
- Reused the canonical watchlist quality policy for promotion review instead of
  duplicating cost, liquidity, whole-share, and edge rules.
- Added promotion review evidence to seed review YAML, watchlist expansion JSON,
  auto-order frequency plan, and dashboard v2.
- Refactored repeated report-file and submit-policy construction helpers.

## Candidate Supply

Added two ASX review-only ETF seeds:

- `BGBL.AX`: official NAV 83.42 AUD on 2026-06-11 and published management
  cost 0.08% p.a.
- `DHHF.AX`: official NAV 40.90 AUD on 2026-06-11 and published management
  cost 0.19% p.a.

Official sources:

- https://www.betashares.com.au/fund/global-shares-etf/
- https://www.betashares.com.au/fund/diversified-all-growth-etf/

These are research inputs, not trade recommendations. Both remain
`broker_mapping_status=TO_VERIFY`, `review_only=true`, and `auto_apply=false`.

## Current Evidence

The local read-only refresh on 2026-06-14 reported:

- selected expansion candidates: `0`
- seed source candidates: `8`
- submit capacity status: `BASELINE_INSUFFICIENT_EVIDENCE`
- effective submit capacity: `1 portfolio / 100 AUD`
- fills: `0`
- matured 5-day realized-edge samples: `0`
- nearest quality-passing frontier: `US:watchlist / SPLG`
- current leading blocker: stale preflight

The seed lifecycle now avoids broker mapping for locally rejected candidates.
Existing `A200.AX` and `VAS.AX` remain quality-rejected for the current
small-account constraints, while new seeds first require candidate-report
evidence.

## Safety Boundary

- Paper only.
- No live-submit enablement.
- No automatic symbol-master promotion.
- No relaxation of risk, edge, cost, liquidity, market-rule, or submit-quality
  gates.
- Higher submit capacity is conditional on realized evidence, not expected
  profit.

## Validation

- Python compile checks passed.
- Focused watchlist, auto-order, dashboard, and Supervisor tests passed.
- Guardrail tests and the investment workflow integration smoke passed.
- `pip check` reported no broken requirements.
- Real local watchlist expansion and auto-order readiness CLIs completed.

The full integration tier still contains an unrelated time-sensitive Supervisor
test that enters an existing retry sleep path. It was stopped after isolating
`test_closed_market_can_rerun_when_macro_signature_changes`; this change set
does not alter that scheduling path.
