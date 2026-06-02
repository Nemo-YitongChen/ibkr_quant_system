# Change Archive: Watchlist Seed Proposals

Date: 2026-06-03

## Context

The previous watchlist expansion gap plan identified that ASX, HK, and XETRA have zero selected small-account candidates because the current candidate source does not provide enough ETF-first, low-cost, whole-share tradable coverage. The next step should improve candidate coverage without automatically adding symbols to trading watchlists or weakening execution gates.

## Changes

- Added `build_watchlist_seed_proposals()` in `src/common/watchlist_expansion.py`.
- `summarize_watchlist_expansion()` now emits:
  - `seed_proposals`
  - `seed_proposal_count`
  - `manual_seed_proposal_count`
- Dashboard watchlist expansion payload now forwards seed proposal fields.
- Dashboard v2 `watchlist_expansion` block now exposes:
  - `seed_proposal_count`
  - `manual_seed_proposal_count`
  - `primary_seed_proposal_action`
  - `rows.seed_proposals`
- Seed proposals are always manual review by default:
  - `proposal_status=MANUAL_REVIEW_REQUIRED`
  - `auto_apply=false`
  - `submit_gate_policy=do_not_relax_submit_gates`

## Current Local Diagnosis

Latest local dashboard generation shows:

- candidate rows: `65`
- selected rows: `2`
- zero-selected markets: `3`
- seed proposals: `3`
- manual seed proposals: `3`

Generated seed proposals:

- `ASX`: `create_or_refresh_preferred_asset_seed_watchlist`, near-miss evidence symbols: `BHP.AX`, `RIO.AX`, `FMG.AX`, `MIN.AX`, `QBE.AX`
- `HK`: `create_or_refresh_preferred_asset_seed_watchlist`, near-miss evidence symbols: `3988.HK`, `2388.HK`, `0939.HK`, `1398.HK`, `0005.HK`
- `XETRA`: `create_or_refresh_preferred_asset_seed_watchlist`, near-miss evidence symbols: `IFX.DE`, `ALV.DE`, `CBK.DE`, `RWE.DE`, `DHL.DE`

These near-miss symbols are not automatic additions. They only explain the current evidence gap. Actual seed additions still require verified IBKR tradability, account-profile fit, whole-share support, cost, liquidity, data quality, and expected-edge pass in the next candidate report.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/watchlist_expansion.py src/tools/generate_dashboard.py src/tools/dashboard_blocks.py src/tools/expand_investment_watchlists.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_watchlist_expansion.py tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py tests/test_investment_workflow_smoke.py::test_investment_workflow_cli_smoke_generates_contract_artifacts`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_supervisor_cli.py::SupervisorCliTests::test_dashboard_market_data_health_overview_marks_nonresearch_fallback_market_as_attention`

## Next Technical Path

1. Create market-specific ETF-first seed watchlists for ASX, HK, and XETRA from verified symbols.
2. Regenerate investment candidate reports and watchlist expansion diagnostics.
3. Only move symbols into auto-expanded watchlists after the normal selection policy passes.
4. Keep paper auto-submit capped until fill/slippage/post-cost edge evidence supports higher frequency.
