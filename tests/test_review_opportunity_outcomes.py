from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from src.tools.review_opportunity_outcomes import build_opportunity_outcome_validation_payload


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_opportunity_outcome_validation_payload_filters_hk_groups(tmp_path: Path) -> None:
    readiness_path = tmp_path / "market_readiness.json"
    weekly_path = tmp_path / "weekly_unified_evidence.csv"
    readiness_path.write_text(
        json.dumps(
            {
                "opportunity_calibration": {
                    "post_cost_rows": [
                        {
                            "market": "HK",
                            "portfolio_id": "HK:resolved_hk_top100_bluechip",
                            "positive_post_cost_rows": [
                                {"symbol": "3988.HK"},
                                {"symbol": "0939.HK"},
                            ],
                        },
                        {
                            "market": "US",
                            "portfolio_id": "US:watchlist",
                            "positive_post_cost_rows": [{"symbol": "SPLG"}],
                        },
                    ],
                    "wait_pullback_rows": [
                        {
                            "market": "HK",
                            "portfolio_id": "HK:resolved_hk_top100_bluechip",
                            "close_wait_pullback_count": 1,
                            "close_wait_pullback_rows": [{"symbol": "3988.HK"}],
                        }
                    ],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_csv(
        weekly_path,
        (
            [
                {
                    "market": "HK",
                    "portfolio_id": "HK:resolved_hk_top100_bluechip",
                    "symbol": "3988.HK",
                    "outcome_5d_bps": "120",
                    "outcome_20d_bps": "220",
                },
                {
                    "market": "HK",
                    "portfolio_id": "HK:resolved_hk_top100_bluechip",
                    "symbol": "0939.HK",
                    "outcome_5d_bps": "80",
                    "outcome_20d_bps": "180",
                },
            ]
            * 5
        )
        + [
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "symbol": "SPLG",
                "outcome_5d_bps": "-20",
                "outcome_20d_bps": "-30",
            },
        ],
    )

    payload = build_opportunity_outcome_validation_payload(
        market_readiness_path=readiness_path,
        weekly_unified_evidence_path=weekly_path,
        market="HK",
    )

    assert payload["market"] == "HK"
    assert payload["summary"]["validation_count"] == 2
    rows = payload["rows"]
    assert [row["group_name"] for row in rows] == [
        "positive_post_cost_candidates",
        "close_wait_pullback",
    ]
    assert rows[0]["candidate_symbols"] == "3988.HK,0939.HK"
    assert rows[0]["avg_outcome_5d_bps"] == 100.0
    assert rows[0]["avg_outcome_20d_bps"] == 200.0
    suggestions = payload["calibration_suggestions"]
    assert [row["suggestion_type"] for row in suggestions] == [
        "WAIT_PULLBACK_ANCHOR_REVIEW",
        "POST_COST_MONITOR",
    ]
    assert all(row["auto_apply"] is False for row in suggestions)
    assert all(row["read_only"] is True for row in suggestions)
    trial_plan = payload["calibration_trial_plan"]
    assert trial_plan[0]["trial_type"] == "WAIT_PULLBACK_NEAR_ENTRY_LIMIT_TRIAL"
    assert trial_plan[0]["primary_field"] == "opportunity_entry.near_entry_gap_pct"
    assert trial_plan[0]["suggested_value"] == 1.5
    assert trial_plan[0]["candidate_symbols"] == "3988.HK"
    assert trial_plan[0]["source_candidate_symbols"] == "3988.HK"
    assert trial_plan[0]["outcome_qualified_symbols"] == "3988.HK"
    assert trial_plan[0]["auto_apply"] is False
    assert payload["calibration_trial_plan_summary"]["trial_count"] == 1


def test_wait_pullback_trial_filters_symbol_level_negative_outcomes(tmp_path: Path) -> None:
    readiness_path = tmp_path / "market_readiness.json"
    weekly_path = tmp_path / "weekly_unified_evidence.csv"
    readiness_path.write_text(
        json.dumps(
            {
                "opportunity_calibration": {
                    "post_cost_rows": [],
                    "wait_pullback_rows": [
                        {
                            "market": "HK",
                            "portfolio_id": "HK:resolved_hk_top100_bluechip",
                            "close_wait_pullback_count": 2,
                            "close_wait_pullback_rows": [
                                {"symbol": "3988.HK"},
                                {"symbol": "1288.HK"},
                            ],
                        }
                    ],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_csv(
        weekly_path,
        (
            [
                {
                    "market": "HK",
                    "portfolio_id": "HK:resolved_hk_top100_bluechip",
                    "symbol": "3988.HK",
                    "outcome_5d_bps": "120",
                    "outcome_20d_bps": "220",
                },
                {
                    "market": "HK",
                    "portfolio_id": "HK:resolved_hk_top100_bluechip",
                    "symbol": "1288.HK",
                    "outcome_5d_bps": "-30",
                    "outcome_20d_bps": "-80",
                },
            ]
            * 5
        ),
    )

    payload = build_opportunity_outcome_validation_payload(
        market_readiness_path=readiness_path,
        weekly_unified_evidence_path=weekly_path,
        market="HK",
    )

    suggestion = payload["calibration_suggestions"][0]
    assert suggestion["suggestion_type"] == "WAIT_PULLBACK_ANCHOR_REVIEW"
    assert suggestion["candidate_symbols"] == "3988.HK,1288.HK"
    assert suggestion["outcome_qualified_symbols"] == "3988.HK"
    assert suggestion["outcome_excluded_symbols"] == "1288.HK"
    trial = payload["calibration_trial_plan"][0]
    assert trial["trial_type"] == "WAIT_PULLBACK_NEAR_ENTRY_LIMIT_TRIAL"
    assert trial["candidate_symbols"] == "3988.HK"
    assert trial["source_candidate_symbols"] == "3988.HK,1288.HK"
    assert trial["outcome_qualified_symbols"] == "3988.HK"
    assert trial["outcome_excluded_symbols"] == "1288.HK"


def test_wait_pullback_trial_blocks_when_no_symbol_level_outcome_support(tmp_path: Path) -> None:
    readiness_path = tmp_path / "market_readiness.json"
    weekly_path = tmp_path / "weekly_unified_evidence.csv"
    readiness_path.write_text(
        json.dumps(
            {
                "opportunity_calibration": {
                    "post_cost_rows": [],
                    "wait_pullback_rows": [
                        {
                            "market": "HK",
                            "portfolio_id": "HK:resolved_hk_top100_bluechip",
                            "close_wait_pullback_count": 2,
                            "close_wait_pullback_rows": [
                                {"symbol": "1288.HK"},
                                {"symbol": "2359.HK"},
                            ],
                        }
                    ],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_csv(
        weekly_path,
        (
            [
                {
                    "market": "HK",
                    "portfolio_id": "HK:resolved_hk_top100_bluechip",
                    "symbol": "1288.HK",
                    "outcome_5d_bps": "-30",
                    "outcome_20d_bps": "-80",
                },
                {
                    "market": "HK",
                    "portfolio_id": "HK:resolved_hk_top100_bluechip",
                    "symbol": "2359.HK",
                    "outcome_5d_bps": "-60",
                    "outcome_20d_bps": "-120",
                },
            ]
            * 5
        ),
    )

    payload = build_opportunity_outcome_validation_payload(
        market_readiness_path=readiness_path,
        weekly_unified_evidence_path=weekly_path,
        market="HK",
    )

    suggestion = payload["calibration_suggestions"][0]
    assert suggestion["suggestion_type"] == "COLLECT_MORE_OUTCOMES"
    assert suggestion["outcome_qualified_symbols"] == ""
    assert suggestion["outcome_excluded_symbols"] == "1288.HK,2359.HK"
    assert payload["calibration_trial_plan"] == []


def test_opportunity_outcome_validation_suggests_hk_post_cost_review(tmp_path: Path) -> None:
    readiness_path = tmp_path / "market_readiness.json"
    weekly_path = tmp_path / "weekly_unified_evidence.csv"
    readiness_path.write_text(
        json.dumps(
            {
                "opportunity_calibration": {
                    "post_cost_rows": [
                        {
                            "market": "HK",
                            "portfolio_id": "HK:resolved_hk_top100_bluechip",
                            "status": "COST_THRESHOLD_REVIEW",
                            "high_cost_positive_edge_count": 1,
                            "avg_post_cost_edge_bps": -10.0,
                            "positive_post_cost_rows": [{"symbol": "3988.HK"}],
                        }
                    ],
                    "wait_pullback_rows": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_csv(
        weekly_path,
        [
            {
                "market": "HK",
                "portfolio_id": "HK:resolved_hk_top100_bluechip",
                "symbol": "3988.HK",
                "outcome_5d_bps": "120",
                "outcome_20d_bps": "220",
            }
            for _ in range(5)
        ],
    )

    payload = build_opportunity_outcome_validation_payload(
        market_readiness_path=readiness_path,
        weekly_unified_evidence_path=weekly_path,
        market="HK",
    )

    suggestion = payload["calibration_suggestions"][0]
    assert suggestion["suggestion_type"] == "HK_POST_COST_THRESHOLD_REVIEW"
    assert suggestion["primary_field"] == "submit_quality.max_expected_cost_bps"
    assert suggestion["auto_apply"] is False
    assert suggestion["paper_only"] is True
    assert "Do not auto-apply" in suggestion["acceptance_rule"]
    trial = payload["calibration_trial_plan"][0]
    assert trial["trial_type"] == "HK_POST_COST_THRESHOLD_PAPER_TRIAL"
    assert trial["primary_field"] == "auto_order_readiness.max_submit_expected_cost_bps"
    assert trial["current_value"] == 35.0
    assert trial["suggested_value"] == 55.0
    assert trial["requires_submit_quality_pass"] is True
    assert trial["auto_apply"] is False
    assert payload["calibration_trial_plan_summary"]["p1_ready_for_manual_review_count"] == 1


def test_opportunity_outcome_validation_backfills_from_candidate_outcomes_db_when_weekly_is_pending(
    tmp_path: Path,
) -> None:
    readiness_path = tmp_path / "market_readiness.json"
    weekly_path = tmp_path / "weekly_unified_evidence.csv"
    db_path = tmp_path / "audit.db"
    readiness_path.write_text(
        json.dumps(
            {
                "opportunity_calibration": {
                    "post_cost_rows": [
                        {
                            "market": "HK",
                            "portfolio_id": "HK:resolved_hk_top100_bluechip",
                            "positive_post_cost_rows": [{"symbol": "0005.HK"}],
                        }
                    ],
                    "wait_pullback_rows": [],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_csv(
        weekly_path,
        [
            {
                "market": "HK",
                "portfolio_id": "HK:resolved_hk_top100_bluechip",
                "symbol": "0005.HK",
                "outcome_5d_bps": "",
                "outcome_20d_bps": "",
            }
        ],
    )
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE investment_candidate_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market TEXT,
                portfolio_id TEXT,
                symbol TEXT,
                horizon_days INTEGER,
                snapshot_ts TEXT,
                outcome_ts TEXT,
                future_return REAL
            )
            """
        )
        rows = []
        for idx in range(5):
            rows.append(
                (
                    "HK",
                    "HK:resolved_hk_top100_bluechip",
                    "0005.HK",
                    5,
                    f"2026-05-{idx + 1:02d}T00:00:00+00:00",
                    f"2026-05-{idx + 8:02d}T00:00:00+00:00",
                    0.012,
                )
            )
            rows.append(
                (
                    "HK",
                    "HK:resolved_hk_top100_bluechip",
                    "0005.HK",
                    20,
                    f"2026-05-{idx + 1:02d}T00:00:00+00:00",
                    f"2026-05-{idx + 25:02d}T00:00:00+00:00",
                    0.021,
                )
            )
        conn.executemany(
            """
            INSERT INTO investment_candidate_outcomes (
                market, portfolio_id, symbol, horizon_days, snapshot_ts, outcome_ts, future_return
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    payload = build_opportunity_outcome_validation_payload(
        market_readiness_path=readiness_path,
        weekly_unified_evidence_path=weekly_path,
        candidate_outcomes_db_path=db_path,
        market="HK",
    )

    assert payload["outcome_source"] == "investment_candidate_outcomes"
    row = payload["rows"][0]
    assert row["status"] == "OUTCOME_SUPPORTS_GROUP"
    assert row["matched_symbol_count"] == 1
    assert row["matured_5d_sample_count"] == 5
    assert row["matured_20d_sample_count"] == 5
    assert row["avg_outcome_5d_bps"] == 120.0
    assert row["avg_outcome_20d_bps"] == 210.0
