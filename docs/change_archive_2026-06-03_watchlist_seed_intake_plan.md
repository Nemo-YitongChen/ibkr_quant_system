# Change Archive: Watchlist Seed Intake Plan

Date: 2026-06-03

## Context

The previous seed proposal layer identified that ASX, HK, and XETRA need better small-account candidate supply. However, the current near-miss symbols in those markets are mostly not preferred ETF-first candidates for the small-account profile. Treating them as automatic additions would weaken the candidate-quality boundary.

The next step was to make seed expansion actionable without wiring unverified symbols into execution.

## Changes

- Added `build_watchlist_seed_intake_plan()` in `src/common/watchlist_expansion.py`.
- `summarize_watchlist_expansion()` now emits:
  - `seed_intake_plan`
  - `seed_intake_plan_count`
  - `seed_intake_manual_review_count`
  - `seed_intake_external_source_count`
- `expand_investment_watchlists` now writes review-only seed intake YAML files under:
  - `reports_supervisor/watchlist_expansion/seed_review/`
- Dashboard v2 `watchlist_expansion` block now exposes:
  - `seed_intake_plan_count`
  - `seed_intake_external_source_count`
  - `seed_intake_manual_review_count`
  - `primary_seed_intake_status`
  - `primary_seed_intake_next_action`
  - `rows.seed_intake_plan`
- Watchlist expansion block status now becomes `warn` when zero-selected markets or external seed-source gaps remain.

## Current Local Diagnosis

Latest local refresh:

- selected candidates: `2`
- zero-selected markets: `3`
- seed intake plan rows: `3`
- external preferred-asset source gaps: `3`
- primary intake status: `NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE`

Generated review-only files:

- `reports_supervisor/watchlist_expansion/seed_review/asx_preferred_asset_seed_review.yaml`
- `reports_supervisor/watchlist_expansion/seed_review/hk_preferred_asset_seed_review.yaml`
- `reports_supervisor/watchlist_expansion/seed_review/xetra_preferred_asset_seed_review.yaml`

For ASX/HK/XETRA, `symbols` remains empty and near-miss stocks are stored only as `evidence_symbols`. The correct next step is to source verified low-cost ETF candidates, not to promote those equity near-misses into the symbol master.

## Guardrails

- `auto_apply=false`
- `review_only=true`
- `does_not_change_symbol_master=true`
- `submit_gate_policy=do_not_relax_submit_gates`

This change does not submit paper orders and does not relax risk, edge, cost, market-rule, Gateway-budget, or submit-quality gates.

## Validation

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/common/watchlist_expansion.py src/tools/expand_investment_watchlists.py src/tools/dashboard_blocks.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_watchlist_expansion.py tests/test_dashboard_blocks.py tests/test_generate_dashboard_helpers.py::test_load_watchlist_expansion_payload_counts_selected_and_reject_reasons tests/test_investment_workflow_smoke.py::test_investment_workflow_cli_smoke_generates_contract_artifacts`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.expand_investment_watchlists --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir /tmp/ibkr_quant_auto_expanded_verify --analysis_dir reports_supervisor/watchlist_expansion --account_profile small --account_equity 1000`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`

## Next Technical Path

1. Source verified ETF-first candidate lists for ASX, HK, and XETRA.
2. Put them through normal candidate report generation instead of directly editing symbol master.
3. Promote only symbols that pass account-profile fit, whole-share tradability, expected cost, liquidity, data quality, expected edge, and submit quality.
4. Re-run paper execution dry-run, market readiness, auto-order readiness, and dashboard before considering any paper submit.
