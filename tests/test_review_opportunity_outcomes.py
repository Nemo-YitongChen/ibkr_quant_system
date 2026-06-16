from __future__ import annotations

import csv
import json
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
