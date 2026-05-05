from __future__ import annotations

import os
from pathlib import Path

from src.common.artifact_contracts import dashboard_artifact_contracts
from src.common.artifact_health import (
    build_artifact_consistency_rows,
    build_artifact_health_overview,
    evaluate_artifact_health,
)
from src.common.artifact_loader import load_artifact, load_artifact_set


def test_evaluate_artifact_health_marks_missing_weekly_review_degraded(tmp_path: Path) -> None:
    contract = dashboard_artifact_contracts()["weekly_review_summary"]
    loaded = load_artifact(tmp_path, contract)

    row = evaluate_artifact_health(contract, loaded, scope_label="GLOBAL")

    assert row["status"] == "degraded"
    assert "缺失 weekly_review_summary.json" in row["summary"]


def test_load_artifact_uses_weekly_review_section_fallback_for_governance_summary(tmp_path: Path) -> None:
    weekly_summary = tmp_path / "weekly_review_summary.json"
    weekly_summary.write_text(
        (
            '{"generated_at":"2026-04-23T10:00:00+00:00",'
            '"window_start":"2026-04-14","window_end":"2026-04-20","portfolio_count":1,'
            '"patch_governance_summary":[{"market":"US","field":"min_expected_edge_bps","latest_status_label":"已批准"}]}'
        ),
        encoding="utf-8",
    )

    contracts = dashboard_artifact_contracts()
    loaded = load_artifact_set(
        tmp_path,
        {
            "weekly_review_summary": contracts["weekly_review_summary"],
            "weekly_patch_governance_summary": contracts["weekly_patch_governance_summary"],
        },
    )

    row = evaluate_artifact_health(
        contracts["weekly_patch_governance_summary"],
        loaded["weekly_patch_governance_summary"],
        scope_label="GLOBAL",
    )

    assert row["status"] == "warning"
    assert row["source"] == "fallback:patch_governance_summary"
    assert any("partial compatibility" in warning for warning in row["warnings"])


def test_weekly_evidence_json_contracts_are_registered() -> None:
    contracts = dashboard_artifact_contracts()

    assert contracts["weekly_unified_evidence"].filename == "weekly_unified_evidence.json"
    assert contracts["weekly_blocked_vs_allowed_expost"].filename == "weekly_blocked_vs_allowed_expost.json"
    assert contracts["weekly_unified_evidence"].missing_status == "warning"
    assert contracts["weekly_blocked_vs_allowed_expost"].fallback_section == "blocked_vs_allowed_expost_review"


def test_weekly_quality_review_and_attribution_contracts_are_registered() -> None:
    contracts = dashboard_artifact_contracts()

    assert contracts["weekly_trading_quality_evidence"].filename == "weekly_trading_quality_evidence.csv"
    assert contracts["weekly_trading_quality_evidence"].fallback_section == "trading_quality_evidence"
    assert contracts["weekly_candidate_model_review"].filename == "weekly_candidate_model_review.csv"
    assert contracts["weekly_candidate_model_review"].fallback_section == "candidate_model_review"
    assert contracts["weekly_attribution_summary"].filename == "weekly_attribution_summary.csv"
    assert contracts["weekly_attribution_summary"].fallback_section == "attribution_summary"
    assert contracts["weekly_attribution_summary"].missing_status == "warning"


def test_weekly_unified_evidence_json_health_counts_rows(tmp_path: Path) -> None:
    contract = dashboard_artifact_contracts()["weekly_unified_evidence"]
    (tmp_path / "weekly_unified_evidence.json").write_text(
        (
            '{"generated_at":"2026-04-30T10:00:00+00:00","schema_version":"2026Q2.p0.v1",'
            '"artifact_type":"weekly_unified_evidence","row_count":2,'
            '"rows":[{"portfolio_id":"US:watchlist","market":"US","symbol":"AAPL"},'
            '{"portfolio_id":"HK:watchlist","market":"HK","symbol":"0700.HK"}]}'
        ),
        encoding="utf-8",
    )

    loaded = load_artifact(tmp_path, contract)
    row = evaluate_artifact_health(contract, loaded, scope_label="GLOBAL")

    assert loaded.row_count == 2
    assert "portfolio_id" in loaded.columns
    assert row["status"] == "ready"


def test_weekly_unified_evidence_falls_back_to_summary_rows(tmp_path: Path) -> None:
    weekly_summary = tmp_path / "weekly_review_summary.json"
    weekly_summary.write_text(
        (
            '{"generated_at":"2026-04-30T10:00:00+00:00","schema_version":"2026Q2.p0.v1",'
            '"window_start":"2026-04-24","window_end":"2026-04-30","portfolio_count":1,'
            '"unified_evidence_rows":[{"portfolio_id":"US:watchlist","market":"US","symbol":"AAPL"}]}'
        ),
        encoding="utf-8",
    )

    contracts = dashboard_artifact_contracts()
    loaded = load_artifact_set(
        tmp_path,
        {
            "weekly_review_summary": contracts["weekly_review_summary"],
            "weekly_unified_evidence": contracts["weekly_unified_evidence"],
        },
    )
    row = evaluate_artifact_health(
        contracts["weekly_unified_evidence"],
        loaded["weekly_unified_evidence"],
        scope_label="GLOBAL",
    )

    assert loaded["weekly_unified_evidence"].row_count == 1
    assert loaded["weekly_unified_evidence"].source == "fallback:unified_evidence_rows"
    assert row["status"] == "warning"
    assert row["schema_version"] == "2026Q2.p0.v1"
    assert any("partial compatibility" in warning for warning in row["warnings"])


def test_load_artifact_uses_weekly_review_section_fallback_for_quality_review_and_attribution(
    tmp_path: Path,
) -> None:
    weekly_summary = tmp_path / "weekly_review_summary.json"
    weekly_summary.write_text(
        (
            '{"generated_at":"2026-04-30T10:00:00+00:00","schema_version":"2026Q2.p0.v1",'
            '"window_start":"2026-04-24","window_end":"2026-04-30","portfolio_count":1,'
            '"trading_quality_evidence":[{"portfolio_id":"US:watchlist","market":"US",'
            '"evidence_layer":"EDGE_GATE","sample_count":3}],'
            '"candidate_model_review":[{"portfolio_id":"US:watchlist","market":"US",'
            '"review_label":"SIGNAL_RANKING_WORKING"}],'
            '"attribution_summary":[{"portfolio_id":"US:watchlist","market":"US","weekly_return":0.01}]}'
        ),
        encoding="utf-8",
    )

    contracts = dashboard_artifact_contracts()
    loaded = load_artifact_set(
        tmp_path,
        {
            "weekly_review_summary": contracts["weekly_review_summary"],
            "weekly_trading_quality_evidence": contracts["weekly_trading_quality_evidence"],
            "weekly_candidate_model_review": contracts["weekly_candidate_model_review"],
            "weekly_attribution_summary": contracts["weekly_attribution_summary"],
        },
    )

    for key, fallback_section in (
        ("weekly_trading_quality_evidence", "trading_quality_evidence"),
        ("weekly_candidate_model_review", "candidate_model_review"),
        ("weekly_attribution_summary", "attribution_summary"),
    ):
        row = evaluate_artifact_health(contracts[key], loaded[key], scope_label="GLOBAL")
        assert row["status"] == "warning"
        assert row["source"] == f"fallback:{fallback_section}"
        assert row["row_count"] == 1
        assert row["missing_columns"] == []
        assert row["schema_version"] == "2026Q2.p0.v1"
        assert any("partial compatibility" in warning for warning in row["warnings"])


def test_load_artifact_uses_weekly_review_section_fallback_for_broker_positions(tmp_path: Path) -> None:
    weekly_summary = tmp_path / "weekly_review_summary.json"
    weekly_summary.write_text(
        (
            '{"generated_at":"2026-04-23T10:00:00+00:00","schema_version":"2026Q2.p0.v1",'
            '"window_start":"2026-04-14","window_end":"2026-04-20","portfolio_count":1,'
            '"broker_snapshot_rows":[{"portfolio_id":"US:watchlist","market":"US","symbol":"AAPL"}]}'
        ),
        encoding="utf-8",
    )

    contracts = dashboard_artifact_contracts()
    loaded = load_artifact_set(
        tmp_path,
        {
            "weekly_review_summary": contracts["weekly_review_summary"],
            "weekly_broker_positions": contracts["weekly_broker_positions"],
        },
    )

    row = evaluate_artifact_health(
        contracts["weekly_broker_positions"],
        loaded["weekly_broker_positions"],
        scope_label="GLOBAL",
    )

    assert row["status"] == "warning"
    assert row["source"] == "fallback:broker_snapshot_rows"
    assert row["schema_version"] == "2026Q2.p0.v1"
    assert row["missing_columns"] == []
    assert any("partial compatibility" in warning for warning in row["warnings"])


def test_missing_broker_reconciliation_summary_is_warning_contract(tmp_path: Path) -> None:
    contract = dashboard_artifact_contracts()["broker_reconciliation_summary"]
    loaded = load_artifact(tmp_path, contract)

    row = evaluate_artifact_health(contract, loaded, scope_label="GLOBAL")

    assert row["status"] == "warning"
    assert "缺失 broker_reconciliation_summary.json" in row["summary"]


def test_build_artifact_consistency_rows_warns_when_weekly_bundle_drifts(tmp_path: Path) -> None:
    contracts = dashboard_artifact_contracts()
    (tmp_path / "weekly_review_summary.json").write_text(
        '{"generated_at":"2026-04-23T10:00:00+00:00","schema_version":"2026Q2.p0.v1","window_start":"2026-04-14","window_end":"2026-04-20","portfolio_count":1}',
        encoding="utf-8",
    )
    weekly_exec = tmp_path / "weekly_execution_summary.csv"
    weekly_exec.write_text(
        "portfolio_id,market,execution_runs,submitted_order_rows\nUS:watchlist,US,1,1\n",
        encoding="utf-8",
    )
    os.utime(weekly_exec, (weekly_exec.stat().st_atime, weekly_exec.stat().st_mtime - 60 * 60 * 12))

    loaded = load_artifact_set(
        tmp_path,
        {
            "weekly_review_summary": contracts["weekly_review_summary"],
            "weekly_execution_summary": contracts["weekly_execution_summary"],
        },
    )
    rows = [
        evaluate_artifact_health(contracts[key], loaded[key], scope_label="GLOBAL")
        for key in ("weekly_review_summary", "weekly_execution_summary")
    ]

    consistency_rows = build_artifact_consistency_rows(rows)
    overview = build_artifact_health_overview(rows, consistency_rows=consistency_rows)

    assert consistency_rows
    assert consistency_rows[0]["status"] == "warning"
    assert overview["consistency_warning_count"] == 1
    assert overview["status"] == "warning"
