# 2026-06-26 Seed evidence source-only queue and HK/shutdown verification

## Scope

This change keeps the account-growth path focused on whole-share tradable, source-verified ETF candidates without relaxing trading gates. It does not submit orders, change watchlists, modify symbol master files, or increase IBKR request volume.

## HK outcome verification

Source artifact:

- `runtime_data/paper_investment_only_duq152001/reports_supervisor/hk_opportunity_outcome_validation/opportunity_outcome_validation.json`

Latest artifact timestamp:

- `2026-06-24T12:19:04.112359+00:00`

Validated HK rows:

| Portfolio | Group | Status | 5d avg bps | 20d avg bps | Candidate symbols |
|---|---|---|---:|---:|---|
| HK:resolved_hk_top100_bluechip | positive_post_cost_candidates | OUTCOME_SUPPORTS_GROUP | 122.893992 | 253.844811 | 3988.HK,0005.HK,0939.HK,2359.HK,0992.HK,2388.HK |
| HK:resolved_hk_top100_tech_growth | positive_post_cost_candidates | OUTCOME_SUPPORTS_GROUP | 125.190715 | 264.687094 | 3988.HK,0005.HK,0939.HK,2359.HK,0992.HK,2388.HK |
| HK:resolved_hk_top100_bluechip | close_wait_pullback | OUTCOME_SUPPORTS_GROUP | 125.961554 | 212.066284 | 3988.HK,2388.HK,1398.HK,0939.HK,0005.HK,3328.HK,1288.HK,2359.HK |
| HK:resolved_hk_top100_tech_growth | close_wait_pullback | OUTCOME_SUPPORTS_GROUP | 126.738345 | 222.088142 | 3988.HK,2388.HK,1398.HK,0939.HK,0005.HK,3328.HK,1288.HK,2359.HK |

Interpretation:

- HK has supportive 5/20d outcome evidence for both positive post-cost candidates and close WAIT_PULLBACK candidates.
- The existing trial plan remains correct: prepare paper-only HK post-cost threshold and WAIT_PULLBACK near-entry limit trials only when fresh BUY plans, submit quality, Gateway budget, and whole-share/post-cost checks pass.
- The evidence does not justify bypassing risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gates.

## Seed evidence queue fix

Observed artifact:

- `runtime_data/paper_investment_only_duq152001/reports_supervisor/watchlist_expansion/watchlist_expansion_summary.json`
- `generated_at=2026-06-26T00:04:26.743782+00:00`
- `seed_promotion_review_count=8`
- `seed_promotion_ready_count=0`
- `seed_evidence_queue=[]`

Problem:

- Source-registry rows for ETF candidates such as `BGBL.AX` and `DHHF.AX` had source evidence and reference prices, but not candidate-report fields such as score, expected edge/cost, data quality, liquidity, whole-share tradability, or action.
- The seed promotion review treated those source-only rows as candidate evidence and then applied quality gates to missing fields.
- That incorrectly produced `QUALITY_REJECTED` instead of `CANDIDATE_REPORT_REQUIRED`, so the downstream seed evidence queue stayed empty.

Code change:

- `src/common/watchlist_expansion.py` now separates source-registry evidence from candidate-report evidence.
- Only rows with candidate-report fields are eligible for quality-gate evaluation.
- Source-only rows now produce `promotion_status=CANDIDATE_REPORT_REQUIRED` and `next_action=run_candidate_report_for_seed`.
- Review rows include `candidate_report_evidence_present` so dashboard/report consumers can distinguish source evidence from full candidate evidence.

Expected trading impact:

- The next watchlist expansion refresh can prioritize source-verified, small-account-compatible ETFs for candidate report generation instead of discarding them as quality failures.
- This improves universe expansion for ASX/HK/XETRA without automatically adding symbols or weakening submit controls.

## Supervisor shutdown analysis

Current evidence:

- `supervisor_shutdown_status.json` reports `status=running`, `reason=ignored_signal:SIGHUP`, and PID `77976`.
- No `supervisor_shutdown_events.jsonl` exists in the current runtime folder, so there is no recent local event trail showing a crash.

Code path analysis:

- `SIGINT` or `SIGTERM` sets `_stopping=true`, writes `status=stopping`, raises `KeyboardInterrupt`, and finally records `stopped`.
- `SIGHUP` is explicitly ignored and writes `status=running`, `reason=ignored_signal:SIGHUP`.
- Unhandled exceptions write `status=crashed` and re-raise.
- A second `python -m src.app.supervisor` exits with code 2 when `supervisor.lock` is held by an existing instance.
- `--once` exits by design after one cycle.
- The intraday child trade engine can be stopped while Supervisor remains running when all trading windows are disabled or closed.

Practical diagnosis:

- If the top-level Supervisor appears to shut down, first check `supervisor_shutdown_status.json`, `supervisor.lock`, and whether the process was launched with `--once`.
- If only the trade engine child stopped, check `supervisor_cycle_summary.json -> trade_engine.reason`; this is usually a market-window/config state, not a Supervisor crash.

## Verification

- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_watchlist_expansion.py tests/test_watchlist_seed_evidence.py`
- Result: `18 passed`
