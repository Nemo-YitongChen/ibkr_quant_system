from __future__ import annotations

import json
from pathlib import Path

from src.tools.generate_dashboard import build_dashboard, write_dashboard


def _write_base_report_files(report_dir: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "investment_paper_summary.json",
        "investment_execution_summary.json",
        "investment_guard_summary.json",
        "investment_opportunity_summary.json",
    ):
        if not (report_dir / name).exists():
            (report_dir / name).write_text("{}", encoding="utf-8")


def _write_config(
    cfg_path: Path,
    *,
    summary_dir: Path,
    report_root: Path,
    weekly_dir: Path,
    preflight_dir: Path,
    reconcile_dir: Path | None = None,
) -> None:
    lines = [
        'timezone: "Australia/Sydney"',
        f'summary_out_dir: "{summary_dir}"',
        f'dashboard_weekly_review_dir: "{weekly_dir}"',
        f'dashboard_preflight_dir: "{preflight_dir}"',
    ]
    if reconcile_dir is not None:
        lines.append(f'dashboard_reconcile_dir: "{reconcile_dir}"')
    lines.extend(
        [
            "poll_sec: 30",
            "markets:",
            '  - name: "us"',
            '    market: "US"',
            "    enabled: true",
            "    reports:",
            '      - kind: "investment"',
            f'        out_dir: "{report_root}"',
            '        watchlist_yaml: "config/watchlist.yaml"',
        ]
    )
    cfg_path.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def test_dashboard_degraded_inputs_missing_weekly_review_is_visible(tmp_path: Path) -> None:
    cfg_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
    weekly_dir = tmp_path / "reports_investment_weekly"
    preflight_dir = tmp_path / "reports_preflight"
    report_root = tmp_path / "reports_investment"
    report_dir = report_root / "watchlist"
    _write_base_report_files(report_dir)
    preflight_dir.mkdir(parents=True, exist_ok=True)
    (preflight_dir / "supervisor_preflight_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-23T10:00:00+00:00",
                "schema_version": "2026Q2.p0.v1",
                "pass_count": 3,
                "warn_count": 0,
                "fail_count": 0,
                "checks": [],
            }
        ),
        encoding="utf-8",
    )
    _write_config(cfg_path, summary_dir=summary_dir, report_root=report_root, weekly_dir=weekly_dir, preflight_dir=preflight_dir)

    payload = build_dashboard(str(cfg_path), str(summary_dir))
    write_dashboard(payload, str(summary_dir))
    html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")

    assert payload["artifact_health_overview"]["status"] == "degraded"
    assert "Artifact 健康" in html_text
    assert "缺失 weekly_review_summary.json" in html_text


def test_dashboard_degraded_inputs_supports_legacy_weekly_review_fallback(tmp_path: Path) -> None:
    cfg_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
    weekly_dir = tmp_path / "reports_investment_weekly"
    preflight_dir = tmp_path / "reports_preflight"
    report_root = tmp_path / "reports_investment"
    report_dir = report_root / "watchlist"
    _write_base_report_files(report_dir)
    (report_dir / "investment_execution_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-23T10:00:00+00:00",
                "schema_version": "2026Q2.p0.v1",
                "portfolio_id": "US:watchlist",
                "market": "US",
                "order_count": 1,
                "blocked_order_count": 0,
            }
        ),
        encoding="utf-8",
    )
    weekly_dir.mkdir(parents=True, exist_ok=True)
    preflight_dir.mkdir(parents=True, exist_ok=True)
    (weekly_dir / "weekly_review_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-23T10:00:00+00:00",
                "window_start": "2026-04-14",
                "window_end": "2026-04-20",
                "portfolio_count": 1,
                "risk_review_summary": [{"portfolio_id": "US:watchlist", "market": "US"}],
                "patch_governance_summary": [
                    {"market": "US", "field": "min_expected_edge_bps", "latest_status_label": "已批准"}
                ],
            }
        ),
        encoding="utf-8",
    )
    (weekly_dir / "weekly_execution_summary.csv").write_text(
        "portfolio_id,market,execution_runs,submitted_order_rows\nUS:watchlist,US,1,1\n",
        encoding="utf-8",
    )
    (preflight_dir / "supervisor_preflight_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-23T10:00:00+00:00",
                "schema_version": "2026Q2.p0.v1",
                "pass_count": 3,
                "warn_count": 0,
                "fail_count": 0,
                "checks": [],
            }
        ),
        encoding="utf-8",
    )
    _write_config(cfg_path, summary_dir=summary_dir, report_root=report_root, weekly_dir=weekly_dir, preflight_dir=preflight_dir)

    payload = build_dashboard(str(cfg_path), str(summary_dir))
    rows = list(payload["artifact_health_overview"]["rows"])
    governance_row = next(row for row in rows if row["artifact_key"] == "weekly_patch_governance_summary")

    assert payload["artifact_health_overview"]["status"] == "warning"
    assert governance_row["source"] == "fallback:patch_governance_summary"
    assert any("partial compatibility" in warning for warning in governance_row["warnings"])


def test_dashboard_degraded_inputs_surfaces_weekly_quality_review_fallbacks(tmp_path: Path) -> None:
    cfg_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
    weekly_dir = tmp_path / "reports_investment_weekly"
    preflight_dir = tmp_path / "reports_preflight"
    report_root = tmp_path / "reports_investment"
    report_dir = report_root / "watchlist"
    _write_base_report_files(report_dir)
    (report_dir / "investment_execution_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-30T10:00:00+00:00",
                "schema_version": "2026Q2.p0.v1",
                "portfolio_id": "US:watchlist",
                "market": "US",
                "order_count": 1,
                "blocked_order_count": 0,
            }
        ),
        encoding="utf-8",
    )
    weekly_dir.mkdir(parents=True, exist_ok=True)
    preflight_dir.mkdir(parents=True, exist_ok=True)
    (weekly_dir / "weekly_review_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-30T10:00:00+00:00",
                "schema_version": "2026Q2.p0.v1",
                "window_start": "2026-04-24",
                "window_end": "2026-04-30",
                "portfolio_count": 1,
                "trading_quality_evidence": [
                    {
                        "portfolio_id": "US:watchlist",
                        "market": "US",
                        "evidence_layer": "EDGE_GATE",
                        "sample_count": 3,
                    }
                ],
                "candidate_model_review": [
                    {
                        "portfolio_id": "US:watchlist",
                        "market": "US",
                        "review_label": "SIGNAL_RANKING_WORKING",
                    }
                ],
                "attribution_summary": [
                    {"portfolio_id": "US:watchlist", "market": "US", "weekly_return": 0.01}
                ],
            }
        ),
        encoding="utf-8",
    )
    (weekly_dir / "weekly_execution_summary.csv").write_text(
        "portfolio_id,market,execution_runs,submitted_order_rows\nUS:watchlist,US,1,1\n",
        encoding="utf-8",
    )
    (weekly_dir / "weekly_risk_review_summary.csv").write_text(
        "portfolio_id,market\nUS:watchlist,US\n",
        encoding="utf-8",
    )
    (weekly_dir / "weekly_patch_governance_summary.csv").write_text(
        "market,field,latest_status_label\nUS,min_expected_edge_bps,已批准\n",
        encoding="utf-8",
    )
    (preflight_dir / "supervisor_preflight_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-30T10:00:00+00:00",
                "schema_version": "2026Q2.p0.v1",
                "pass_count": 3,
                "warn_count": 0,
                "fail_count": 0,
                "checks": [],
            }
        ),
        encoding="utf-8",
    )
    _write_config(cfg_path, summary_dir=summary_dir, report_root=report_root, weekly_dir=weekly_dir, preflight_dir=preflight_dir)

    payload = build_dashboard(str(cfg_path), str(summary_dir))
    write_dashboard(payload, str(summary_dir))
    rows = {row["artifact_key"]: row for row in payload["artifact_health_overview"]["rows"]}
    html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")

    assert rows["weekly_trading_quality_evidence"]["source"] == "fallback:trading_quality_evidence"
    assert rows["weekly_candidate_model_review"]["source"] == "fallback:candidate_model_review"
    assert rows["weekly_attribution_summary"]["source"] == "fallback:attribution_summary"
    assert rows["weekly_trading_quality_evidence"]["summary"] == "兼容模式读取"
    assert rows["weekly_candidate_model_review"]["status"] == "warning"
    assert rows["weekly_attribution_summary"]["row_count"] == 1
    assert payload["artifact_health_overview"]["compatibility_warning_count"] >= 3
    assert "Weekly Trading Quality Evidence" in html_text
    assert "Weekly Candidate Model Review" in html_text
    assert "Weekly Attribution Summary" in html_text


def test_dashboard_degraded_inputs_surfaces_reconcile_contract_health(tmp_path: Path) -> None:
    cfg_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
    weekly_dir = tmp_path / "reports_investment_weekly"
    preflight_dir = tmp_path / "reports_preflight"
    reconcile_dir = tmp_path / "reports_investment_reconcile"
    report_root = tmp_path / "reports_investment"
    report_dir = report_root / "watchlist"
    _write_base_report_files(report_dir)
    weekly_dir.mkdir(parents=True, exist_ok=True)
    preflight_dir.mkdir(parents=True, exist_ok=True)
    reconcile_dir.mkdir(parents=True, exist_ok=True)
    (weekly_dir / "weekly_review_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-23T10:00:00+00:00",
                "schema_version": "2026Q2.p0.v1",
                "window_start": "2026-04-14",
                "window_end": "2026-04-20",
                "portfolio_count": 1,
                "broker_snapshot_rows": [{"portfolio_id": "US:watchlist", "market": "US", "symbol": "AAPL"}],
                "broker_local_diff_rows": [
                    {
                        "portfolio_id": "US:watchlist",
                        "market": "US",
                        "local_holdings_count": 1,
                        "broker_holdings_count": 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (weekly_dir / "weekly_execution_summary.csv").write_text(
        "portfolio_id,market,execution_runs,submitted_order_rows\nUS:watchlist,US,1,1\n",
        encoding="utf-8",
    )
    (weekly_dir / "weekly_risk_review_summary.csv").write_text(
        "portfolio_id,market\nUS:watchlist,US\n",
        encoding="utf-8",
    )
    (weekly_dir / "weekly_patch_governance_summary.csv").write_text(
        "market,field,latest_status_label\nUS,min_expected_edge_bps,已批准\n",
        encoding="utf-8",
    )
    (preflight_dir / "supervisor_preflight_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-23T10:00:00+00:00",
                "schema_version": "2026Q2.p0.v1",
                "pass_count": 3,
                "warn_count": 0,
                "fail_count": 0,
                "checks": [],
            }
        ),
        encoding="utf-8",
    )
    _write_config(
        cfg_path,
        summary_dir=summary_dir,
        report_root=report_root,
        weekly_dir=weekly_dir,
        preflight_dir=preflight_dir,
        reconcile_dir=reconcile_dir,
    )

    payload = build_dashboard(str(cfg_path), str(summary_dir))
    write_dashboard(payload, str(summary_dir))
    rows = list(payload["artifact_health_overview"]["rows"])
    reconcile_row = next(row for row in rows if row["artifact_key"] == "broker_reconciliation_summary")
    html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")

    assert reconcile_row["status"] == "warning"
    assert "缺失 broker_reconciliation_summary.json" in reconcile_row["summary"]
    assert payload["reconcile_overview"]["configured"] is True
    assert payload["reconcile_overview"]["available"] is False
    assert "Broker / Reconcile" in html_text


def test_dashboard_degraded_inputs_marks_broken_execution_summary_per_portfolio(tmp_path: Path) -> None:
    cfg_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
    weekly_dir = tmp_path / "reports_investment_weekly"
    preflight_dir = tmp_path / "reports_preflight"
    report_root = tmp_path / "reports_investment"
    report_dir = report_root / "watchlist"
    _write_base_report_files(report_dir)
    (report_dir / "investment_execution_summary.json").write_text("{}", encoding="utf-8")
    weekly_dir.mkdir(parents=True, exist_ok=True)
    preflight_dir.mkdir(parents=True, exist_ok=True)
    (weekly_dir / "weekly_review_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-23T10:00:00+00:00",
                "schema_version": "2026Q2.p0.v1",
                "window_start": "2026-04-14",
                "window_end": "2026-04-20",
                "portfolio_count": 1,
            }
        ),
        encoding="utf-8",
    )
    (weekly_dir / "weekly_execution_summary.csv").write_text(
        "portfolio_id,market,execution_runs,submitted_order_rows\nUS:watchlist,US,1,1\n",
        encoding="utf-8",
    )
    (weekly_dir / "weekly_risk_review_summary.csv").write_text(
        "portfolio_id,market\nUS:watchlist,US\n",
        encoding="utf-8",
    )
    (weekly_dir / "weekly_patch_governance_summary.csv").write_text(
        "market,field,latest_status_label\nUS,min_expected_edge_bps,已批准\n",
        encoding="utf-8",
    )
    (preflight_dir / "supervisor_preflight_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-23T10:00:00+00:00",
                "schema_version": "2026Q2.p0.v1",
                "pass_count": 3,
                "warn_count": 0,
                "fail_count": 0,
                "checks": [],
            }
        ),
        encoding="utf-8",
    )
    _write_config(cfg_path, summary_dir=summary_dir, report_root=report_root, weekly_dir=weekly_dir, preflight_dir=preflight_dir)

    payload = build_dashboard(str(cfg_path), str(summary_dir))
    rows = list(payload["artifact_health_overview"]["rows"])
    execution_row = next(
        row
        for row in rows
        if row["artifact_key"] == "investment_execution_summary" and row["portfolio_id"] == "US:watchlist"
    )

    assert execution_row["status"] == "degraded"
    assert "缺字段" in execution_row["summary"]
