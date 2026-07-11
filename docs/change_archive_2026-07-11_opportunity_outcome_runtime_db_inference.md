# 2026-07-11 Opportunity Outcome Runtime DB Inference

## Scope

- Reduced manual recovery steps for opportunity outcome validation.
- Made `review_opportunity_outcomes` infer the runtime `audit.db` from standard artifact paths when `--db` is not passed.
- Refreshed HK-only and root opportunity outcome validation artifacts without explicitly passing `--db`.
- Rebuilt dashboard JSON/HTML.

## Change

- `review_opportunity_outcomes` keeps the explicit `--db` override.
- If `--db` is omitted, the CLI now searches upward from `market_readiness.json` and `weekly_unified_evidence.csv` for `audit.db`.
- The DB fallback is still only used when the matched weekly evidence rows have no 5d/20d outcome values.
- Existing behavior is unchanged when weekly evidence already contains usable outcome values.

## Trading Impact

- Bounded weekly evidence can keep weekly refresh fast without causing HK or other dense-snapshot markets to appear falsely `OUTCOME_PENDING`.
- Dashboard/outcome refresh commands are less fragile because the operator no longer has to remember the extra `--db` argument in the standard runtime layout.
- No orders were submitted.
- No risk, edge, cost, liquidity, market-rule, Gateway, or submit-quality gate was relaxed.

## Runtime Verification

- HK-only refresh without `--db` inferred `runtime_data/paper_investment_only_duq152001/audit.db`.
- HK-only outcome source: `investment_candidate_outcomes`.
- HK-only summary: 4 validations, 6 matched symbols, 943 mature 5d samples, 499 mature 20d samples.
- Root dashboard was rebuilt after root opportunity outcome validation refresh.

## Tests

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/review_opportunity_outcomes.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_review_opportunity_outcomes.py` -> `6 passed`.
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes --market HK --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence runtime_data/paper_investment_only_duq152001/reports_investment_weekly/weekly_unified_evidence.csv --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.review_opportunity_outcomes --market_readiness runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json --weekly_unified_evidence runtime_data/paper_investment_only_duq152001/reports_investment_weekly/weekly_unified_evidence.csv --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`
