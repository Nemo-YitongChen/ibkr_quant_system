# 2026-06-26 Seed quality rejection surfaced in auto-order frequency plan

## Scope

This change improves the auto-order governance path for small-account universe expansion. It does not submit orders, change trading configuration, add symbols to symbol master, or relax risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gates.

## Runtime evidence

Commands run locally:

- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.expand_investment_watchlists --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor/watchlist_expansion/generated_watchlists --analysis_dir runtime_data/paper_investment_only_duq152001/reports_supervisor/watchlist_expansion --account_profile_config config/account_profiles.yaml --seed_source_registry config/watchlist_seed_sources.yaml --seed_evidence_root runtime_data/paper_investment_only_duq152001/reports_investment_seed_review --account_profile small --account_equity 1000`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_auto_order_readiness --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001 --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`

Observed current state:

- `watchlist_expansion_summary.json` generated at `2026-06-26T02:01:14.781568+00:00`.
- `seed_promotion_review_count=8`.
- `seed_promotion_quality_rejected_count=8`.
- `seed_evidence_queue_count=0`.
- `auto_order_readiness.summary.primary_block_reason=weekly_review_stale`.
- `auto_order_readiness.summary.frequency_plan.status=seed_evidence_quality_rejected`.
- `auto_order_readiness.summary.frequency_plan.primary_action=source_higher_quality_lower_cost_seed_candidates`.

Seed quality rejection reason counts from the refreshed frequency plan:

| Reason | Count |
|---|---:|
| expected_edge_below_min | 8 |
| score_below_min | 8 |
| whole_share_edge_margin_below_min | 8 |
| whole_share_not_tradable | 8 |
| liquidity_below_min | 5 |
| expected_cost_above_max | 4 |
| action_not_allowed | 2 |
| execution_not_ready | 2 |
| last_close_above_account_cap | 2 |
| data_quality_below_min | 1 |

## Code change

- `build_auto_order_frequency_plan` now treats a ready `seed_evidence_queue` as a first-class frequency-plan state:
  - `status=seed_evidence_queue_ready`
  - `primary_action=run_seed_candidate_evidence_review`
- If the seed evidence has already been produced and every seed candidate is `QUALITY_REJECTED`, the frequency plan now reports:
  - `status=seed_evidence_quality_rejected`
  - `primary_action=source_higher_quality_lower_cost_seed_candidates`
  - `seed_promotion_quality_reason_counts`
  - `seed_promotion_primary_quality_reason`
- Dashboard v2 `auto_order_readiness` metrics now expose seed evidence queue counts and seed quality rejection counts/reasons.
- Legacy dashboard fallback now backfills seed evidence metrics from `watchlist_expansion_summary` without overwriting existing modern seed source metrics.
- `review_auto_order_readiness.md` frequency table now includes ready seed evidence jobs and primary seed target.

## Trading interpretation

- The current small-account expansion blocker is not lack of seed review execution; the ASX seed review has already generated candidate evidence for `DHHF.AX` and `BGBL.AX`, but both still fail quality gates.
- The next safe expansion action is to source stronger low-price, whole-share-tradable ETF candidates with better expected edge, score, liquidity, and whole-share edge margin.
- Automatic submit should remain blocked until weekly review freshness, market readiness, stale execution evidence, Gateway budget, and submit-quality gates pass.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_auto_order_readiness.py tests/test_dashboard_blocks.py tests/test_watchlist_expansion.py tests/test_watchlist_seed_evidence.py`
- Result: `90 passed`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider`
- Result: `765 passed`
