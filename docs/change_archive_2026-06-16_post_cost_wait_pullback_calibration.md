# 2026-06-16 Post-Cost Buy Quality and WAIT_PULLBACK Calibration

## Context

The auto-order path was treating exit-only execution plans as failed buy candidates. Recent HK/US recovery evidence contained only small `SELL` rebalance-exit rows, but market readiness still evaluated their empty edge-gate fields as buy-quality failures. That made the strongest blocker look like post-cost edge degradation when the real issue was that no fresh buy plan existed.

At the same time, ASX/HK/US/XETRA opportunity scans were mostly blocked by `WAIT_PULLBACK`. The next useful optimization is to verify whether entry anchors are too conservative, not to relax risk, edge, or market-rule gates.

## Changes

- `market_readiness` submit-quality now evaluates only planned `BUY` / `ACCUMULATE` orders.
- Exit-only planned rows now produce `submit_quality_status=NO_BUY_ORDERS`, with separate buy/non-buy order counts.
- Added `src/common/opportunity_calibration.py` to summarize `WAIT_PULLBACK` rows by market/portfolio.
- `market_readiness.json` now includes per-portfolio WAIT_PULLBACK calibration fields plus a top-level summary.
- `auto_order_readiness` rows now carry WAIT_PULLBACK calibration fields through to dashboard v2.
- Dashboard v2 keeps the existing 14-block information architecture and adds WAIT_PULLBACK metrics inside the existing `auto_order_readiness` block.
- Recovery checkpoint creation now returns the targeted refresh context immediately instead of completing against older marker files in the same call.

## Current Evidence

- HK/US exit-only `SCHX` / `SCHX.HK` recovery evidence is now classified as `NO_BUY_ORDERS` instead of failed post-cost buy quality.
- `market_readiness` currently reports 7 portfolios across 5 markets, with no ready submit candidate.
- WAIT_PULLBACK calibration reports 6 portfolios needing anchor review, 31 close wait rows, and 0 near-entry candidates.
- The dashboard v2 block count remains 14.
- Current auto-order submit plan remains `BLOCKED/no_single_safe_submit_candidate`.

## Trading Interpretation

This change does not loosen automated submit gates. It makes the diagnostics more precise:

- Post-cost edge calibration should only inspect actual buy-side candidates.
- Exit/rebalance rows should not be used to decide whether expected edge is too low.
- The main current opportunity blocker is anchor placement for `WAIT_PULLBACK`, not a reason to lower risk, edge, cost, liquidity, or market-rule controls.

## Next Technical Path

1. Refresh preflight and stale execution artifacts for US overnight, XETRA, HK tech-growth, and ASX during their relevant market windows.
2. For markets with repeated `REVIEW_ANCHOR`, compare `WAIT_PULLBACK` close rows against next 5/20d outcomes before changing entry-band parameters.
3. Only if outcome evidence supports it, test paper-only NEAR_ENTRY limit trials with stricter size and limit-buffer rules.
4. Continue expanding ETF-first whole-share candidates, but require fresh buy plans with positive post-cost edge before submit.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_dashboard_blocks.py tests/test_market_readiness.py tests/test_opportunity_calibration.py`
- `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m src.tools.review_market_readiness --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`
- `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m src.tools.review_auto_order_readiness --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor --preflight_summary runtime_data/paper_investment_only_duq152001/reports_preflight/supervisor_preflight_summary.json --weekly_summary reports_investment_weekly/weekly_review_summary.json --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --runtime_root runtime_data/paper_investment_only_duq152001`
- `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`
