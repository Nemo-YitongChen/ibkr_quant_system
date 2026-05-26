from __future__ import annotations

import json

from src.tools import run_investment_execution


def test_run_investment_execution_writes_degraded_artifacts_when_gateway_unavailable(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    report_dir = tmp_path / "reports"
    ibkr_cfg = tmp_path / "ibkr.yaml"
    paper_cfg = tmp_path / "paper.yaml"
    execution_cfg = tmp_path / "execution.yaml"
    db_path = tmp_path / "audit.db"
    paper_cfg.write_text("paper: {}\n", encoding="utf-8")
    execution_cfg.write_text("execution: {}\n", encoding="utf-8")
    ibkr_cfg.write_text(
        "\n".join(
            [
                "mode: paper",
                "host: 127.0.0.1",
                "port: 4002",
                "client_id: 101",
                "account_id: DUQ152001",
                f"investment_paper_config: {paper_cfg}",
                f"investment_execution_config: {execution_cfg}",
            ]
        ),
        encoding="utf-8",
    )

    def _raise_connection_refused(*_args, **_kwargs):
        raise ConnectionRefusedError(61, "connection refused")

    monkeypatch.setattr(run_investment_execution, "connect_ib", _raise_connection_refused)

    run_investment_execution.main(
        [
            "--market",
            "US",
            "--db",
            str(db_path),
            "--report_dir",
            str(report_dir),
            "--ibkr_config",
            str(ibkr_cfg),
            "--portfolio_id",
            "US:watchlist",
        ]
    )

    stdout = capsys.readouterr().out
    assert "investment execution run degraded: ibkr gateway unavailable" in stdout
    summary = json.loads((report_dir / "investment_execution_summary.json").read_text(encoding="utf-8"))
    diagnostics = json.loads((report_dir / "investment_no_order_diagnostics.json").read_text(encoding="utf-8"))
    owner = json.loads((report_dir / "investment_owner_progression_assessment.json").read_text(encoding="utf-8"))

    assert summary["ibkr_connection_status"] == "FAILED"
    assert summary["primary_no_order_reason"] == "IBKR_GATEWAY_UNAVAILABLE"
    assert summary["submit_effective"] is False
    assert summary["paper_submit_ready"] is False
    assert diagnostics["primary_action"] == "start_or_unlock_ib_gateway_paper_api"
    assert diagnostics["paper_submit_readiness_status"] == "BLOCKED"
    assert owner["overall_status"] == "PAPER_BLOCKED"
    assert (report_dir / "investment_execution_report.md").exists()
