# 2026-06-14 Staged Submit Capacity and Local Seed Refresh

## Goal

Increase paper-order frequency only after realized execution quality supports it,
while keeping new-market exploration small and making candidate-supply evidence
refresh automatically without additional IBKR requests.

## Changes

- Replaced binary submit-capacity scaling with three stages:
  - `baseline`: one portfolio / 100 AUD.
  - `trial`: two portfolios / 200 AUD.
  - `full`: configured ceiling of four portfolios / 400 AUD.
- Trial capacity requires at least five fills and five matured 5-day realized
  edge samples, non-negative post-cost edge, absolute slippage no worse than
  15 bps, and execution error rate no worse than 5%.
- Full capacity requires at least 20 fills, 15 matured 5-day samples, at least
  two markets with valid evidence, realized edge of at least 5 bps, absolute
  slippage no worse than 10 bps, and execution error rate no worse than 2%.
- Trial/full runs allow at most one market without its own realized evidence.
  This permits controlled market exploration without allowing one market's
  results to expand several untested markets simultaneously.
- Realized slippage aggregation is now fill-notional weighted, with fill count
  as a fallback when notional is unavailable.
- Missing or non-finite slippage and realized-edge values do not count as
  capacity evidence. A run cannot scale merely because fill/sample counters
  exist without usable quality measurements.
- Shared execution-evidence aggregation removes duplicate fill, slippage,
  error-rate, and realized-edge calculations.
- Supervisor now refreshes watchlist expansion and seed-promotion evidence from
  local report artifacts every three hours when all markets are closed.
- The standalone auto-order readiness CLI now resolves `summary_out_dir`
  through the configured runtime scope, matching the long-running Supervisor.
  This prevents an old repository-root artifact from hiding current scoped
  seed reviews or market readiness.
- The local refresh writes generated watchlists under the scoped runtime
  reports directory. It does not connect to IBKR and does not modify symbol
  master watchlists.
- Non-finite candidate values such as `NaN` are normalized before JSON output.

## Current Evidence

The local refresh on 2026-06-14 reported:

- selected expansion candidates: `0`
- seed source candidates: `8`
- seed promotion reviews: `8`
- promotion ready: `0`
- candidate report required: `2` (`BGBL.AX`, `DHHF.AX`)
- quality rejected: `6`
- paper fills: `0`
- matured 5-day realized-edge samples: `0`
- current capacity stage: `baseline`
- current effective capacity: one portfolio / 100 AUD

The latest US frontier is `VEA`, but it is not submit-quality eligible:

- planned gross value: about 71.31
- net edge: about 7.80 bps
- edge margin: about 1.80 bps
- submit-quality reason: `net_edge_below_min,edge_margin_below_min`

The current primary operational blockers remain stale preflight evidence,
unavailable/degraded Gateway evidence for some markets, and market-readiness
artifacts that require refresh. No order was submitted in this change.

## Process Consistency

A Supervisor process started before this code change was still running and
overwriting the new readiness artifact with the old schema. The old process was
stopped before final validation. The Supervisor must be restarted after the
commit so the long-running process loads the staged-capacity schema.

## Safety Boundary

- Paper only; live submit remains disabled.
- CN remains excluded from automatic submit.
- No relaxation of risk, edge, cost, liquidity, market-rule, Gateway budget,
  limit-order, or submit-quality gates.
- No automatic promotion of review seeds into production symbol masters.
- Capacity growth is based on realized evidence, not expected returns or an
  asset-growth target.

## Validation

- Focused auto-order, watchlist, dashboard, Supervisor, and workflow tests
  passed (`74 passed` in the final focused run).
- The full integration tier passed (`123 passed`).
- Guardrail tests passed.
- Python compile checks and `pip check` passed.
- Real local watchlist expansion, auto-order readiness, and dashboard builds
  completed.
- Dashboard displays `scale_stage=baseline`, zero evidence markets, and the
  effective one-portfolio / 100 AUD capacity.
