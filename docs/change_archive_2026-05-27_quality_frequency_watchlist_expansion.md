# 2026-05-27 Quality Frequency And Watchlist Expansion Archive

## Goal

Advance paper auto-ordering toward capital growth without bypassing risk, cost, market-rule, or Gateway-budget controls.

The operating principle remains:

- Increase order frequency only when order quality is higher.
- Expand the candidate universe only through auditable evidence.
- Keep CN research-only and keep live auto-submit disabled.
- Do not submit paper orders while Gateway budget or market readiness hard blocks are active.

## Completed Changes

### Submit Quality Tiering

- `market_readiness` now reads `investment_execution_plan.csv` and emits submit quality fields:
  - `submit_quality_status`
  - `submit_quality_tier`
  - post-cost net edge
  - edge margin
  - max expected cost
  - order ADV percentage
  - order type
  - edge, quality, market-rule, shadow, and manual review pass/fail counts
- `auto_order_readiness` now ranks eligible submit candidates by quality:
  - `HIGH` tier first
  - normal `PASS` second
  - lower planned gross only after quality sorting
- Supervisor policy now includes high-quality thresholds:
  - `high_quality_min_net_edge_bps: 16.0`
  - `high_quality_min_edge_margin_bps: 8.0`
  - `high_quality_max_expected_cost_bps: 25.0`

### Frequency Control

- Non-CN multi-market paper submit remains capped:
  - max 4 portfolios per run
  - max 1 portfolio per market
  - max 1 order per portfolio
  - max 100 AUD gross per portfolio
  - max 400 AUD gross total
  - BUY leg required for owner-growth submit
- Frequency can improve only by finding more high-quality candidates across markets, not by relaxing gates.

### Watchlist Expansion

- Added `src/common/watchlist_expansion.py`.
- Added `src/tools/expand_investment_watchlists.py`.
- Generated auto-expanded watchlists:
  - `config/watchlists/auto_expanded/us_quality_growth.yaml`
  - `config/watchlists/auto_expanded/hk_quality_growth.yaml`
  - `config/watchlists/auto_expanded/asx_quality_growth.yaml`
  - `config/watchlists/auto_expanded/xetra_quality_growth.yaml`
- US/HK/ASX/XETRA universe configs now include the auto-expanded quality-growth watchlists in `symbol_master_watchlists`.
- CN remains excluded from auto-order submit policy.

### Candidate Breadth

- Non-CN regular investment reports now output more ranked candidates:
  - US/HK/ASX/XETRA regular `top_n: 15`
  - US overnight `top_n: 10`
- This expands the analysis surface without increasing automatic submit exposure.

### Dashboard And Review

- Dashboard auto-order block now surfaces:
  - frontier quality pass count
  - high-quality frontier count
  - top frontier quality status/tier
  - top frontier net edge and margin
- Auto-order readiness markdown now shows submit frontier quality tier, net edge, and margin.

## Local Evidence

Latest generated watchlist expansion selected:

- US: `SPLG`, `SPTM`, `SCHB`
- ASX: none passed current whole-share/cost/liquidity filters
- HK: none passed current whole-share/cost/liquidity filters
- XETRA: none passed current whole-share/cost/liquidity filters

Latest auto-order readiness frontier:

- `US:watchlist`
- Planned order: `SPLG BUY 1`
- `submit_quality_status=PASS`
- `submit_quality_tier=PASS`
- min net edge about `10.84bps`
- min edge margin about `4.84bps`
- order type `LMT`

Current remaining hard block:

- `gateway_budget_degraded`

This means the current system is correctly refusing to submit despite a valid small US ETF plan.

## Validation

- `pytest -q -p no:cacheprovider tests/test_dashboard_blocks.py tests/test_auto_order_readiness.py tests/test_market_readiness.py tests/test_watchlist_expansion.py`
- `pytest -q -p no:cacheprovider tests/test_generate_dashboard_helpers.py -k auto_order`
- `python3 -m py_compile` for the changed readiness, dashboard, and expansion modules
- YAML parse check for supervisor and market universe configs
- `git diff --check`

## Next Steps

1. Wait for Gateway budget recovery or regenerate weekly review after the telemetry window rolls.
2. Refresh ASX/HK/XETRA report + paper execution dry-run using the expanded `top_n`.
3. Re-run watchlist expansion and symbol master.
4. Only consider paper `--submit` when at least one candidate is:
   - non-CN
   - READY_FOR_PAPER_REVIEW
   - BUY-side
   - LMT
   - under size caps
   - `submit_quality_status=PASS`
   - no Gateway budget hard block
5. Prefer `HIGH` tier candidates for any increase in submit frequency.
