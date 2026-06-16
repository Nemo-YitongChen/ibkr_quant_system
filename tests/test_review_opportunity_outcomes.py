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
