# Change Archive: Watchlist Expansion Gap Plan

Date: 2026-06-02

## Context

The small-account growth path should increase paper-order frequency only when candidate quality improves. The previous watchlist expansion view showed zero-selected markets and top reject reasons, but it did not distinguish between:

- a market with good ETF-first coverage but strict gates blocking candidates
- a market whose candidate source is mostly single-name or untagged symbols and therefore unsuitable for the current small-account ETF-first path

## Changes

- Extended `src/common/watchlist_expansion.py` with structured market gap fields:
  - `asset_class_summary`
  - `preferred_asset_class_count`
  - `preferred_asset_class_gap`
  - `expansion_target`
  - `near_miss_candidates`
  - `do_not_relax_submit_gates`
- Passed account-profile watchlist expansion policy into CLI, dashboard payload loading, and dashboard v2 fallback summaries.
- Added dashboard v2 metrics for primary expansion target and preferred asset-class gap count.
- Added tests for common summary logic, dashboard loader, dashboard v2 block, and workflow smoke coverage.

## Current Local Diagnosis

Latest local dashboard generation shows:

- candidate rows: `65`
- selected rows: `2`
- zero-selected markets: `3`
- primary market: `ASX`
- primary action: `calibrate_cost_or_expand_lower_cost_etfs`

Market-level findings:

- `ASX`: `preferred_asset_class_gap=true`, `expansion_target=seed_preferred_asset_class_candidates`, near-miss examples: `BHP.AX`, `RIO.AX`, `FMG.AX`
- `HK`: `preferred_asset_class_gap=true`, `expansion_target=seed_preferred_asset_class_candidates`, near-miss examples: `3988.HK`, `2388.HK`, `0939.HK`
- `XETRA`: `preferred_asset_class_gap=true`, `expansion_target=seed_preferred_asset_class_candidates`, near-miss examples: `IFX.DE`, `ALV.DE`, `CBK.DE`

These are not automatic-submit candidates. They are evidence for the next expansion step: add or tag lower-cost, high-liquidity, whole-share tradable ETF-first candidates before considering broader single-name stock expansion.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/watchlist_expansion.py src/tools/generate_dashboard.py src/tools/dashboard_blocks.py src/tools/expand_investment_watchlists.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_watchlist_expansion.py tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py tests/test_investment_workflow_smoke.py::test_investment_workflow_cli_smoke_generates_contract_artifacts`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`

## Next Technical Path

1. Add market-specific ETF-first seed lists for ASX, HK, and XETRA from verified IBKR-tradable symbols.
2. Regenerate candidate reports and watchlist expansion diagnostics.
3. Accept candidates only when whole-share tradability, cost, liquidity, data quality, and edge gates all pass.
4. Keep paper auto-submit capped to one small high-quality order until fill/slippage/post-cost edge evidence improves.
