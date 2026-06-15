from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from src.app.supervisor import Supervisor


def _supervisor(tmp_path: Path) -> Supervisor:
    cfg_path = tmp_path / "supervisor.yaml"
    cfg_path.write_text(
        "\n".join(
            [
                'timezone: "Australia/Sydney"',
                f'summary_out_dir: "{tmp_path / "reports_supervisor"}"',
                "scope_summary_out_dir: false",
                "auto_order_readiness:",
                "  enabled: true",
                "  execution_evidence_maintenance_enabled: true",
                "  execution_evidence_maintenance_interval_min: 60",
                "  execution_evidence_maintenance_only_when_all_markets_closed: false",
                "markets:",
                '  - name: "asx"',
                '    market: "ASX"',
                '    local_timezone: "Australia/Sydney"',
                "    enabled: true",
                "    watchlists: []",
                "    reports:",
                '      - kind: "investment"',
                '        watchlist_yaml: "config/watchlists/asx_top_quality.yaml"',
                "        run_investment_execution: true",
                "        submit_investment_execution: true",
                "    short_safety_sync:",
                "      enabled: false",
                "    trading:",
                "      enabled: false",
            ]
        ),
        encoding="utf-8",
    )
    return Supervisor(str(cfg_path))


def _plan() -> dict:
    return {
        "schema_version": "test",
        "generated_at": "2026-06-15T12:00:00+00:00",
        "status": "READY",
        "reason": "single_paper_no_submit_execution_refresh_ready",
        "target_market": "ASX",
        "target_portfolio_id": "ASX:asx_top_quality",
        "paper_only": True,
        "submit_orders": False,
        "recovery_evidence_only": True,
    }


def test_supervisor_maintenance_forces_no_submit_without_mutating_config(
    tmp_path: Path,
) -> None:
    supervisor = _supervisor(tmp_path)
    item = supervisor.markets[0].reports[0]
    cycle_summary = [
        {
            "market": "ASX",
            "execution_run": 0,
            "notable_actions": [],
        }
    ]
    now = datetime(2026, 6, 15, 22, 0, tzinfo=supervisor.tz)

    with patch.object(
        supervisor,
        "_execution_evidence_maintenance_due",
        return_value=True,
    ), patch.object(
        supervisor,
        "_execution_evidence_maintenance_plan",
        return_value=_plan(),
    ), patch.object(
        supervisor,
        "_run_investment_execution",
        return_value=True,
    ) as run_execution:
        ran = supervisor._run_execution_evidence_maintenance(
            now,
            cycle_summary,
            recovery_context={},
        )

    assert ran is True
    assert item["submit_investment_execution"] is True
    execution_item = run_execution.call_args.args[1]
    assert execution_item is not item
    assert execution_item["submit_investment_execution"] is False
    assert execution_item["_recovery_evidence_only"] is True
    assert cycle_summary[0]["execution_run"] == 1
    assert cycle_summary[0]["notable_actions"] == [
        "execution_evidence_maintenance:asx_top_quality:dry_run"
    ]
    state = json.loads(
        supervisor._execution_evidence_maintenance_state_path().read_text(
            encoding="utf-8"
        )
    )
    assert state["status"] == "COMPLETE"
    assert state["submit_orders"] is False


def test_supervisor_maintenance_skips_existing_execution_and_active_recovery(
    tmp_path: Path,
) -> None:
    supervisor = _supervisor(tmp_path)
    now = datetime(2026, 6, 15, 22, 0, tzinfo=supervisor.tz)

    with patch.object(
        supervisor,
        "_execution_evidence_maintenance_due",
        return_value=True,
    ), patch.object(
        supervisor,
        "_execution_evidence_maintenance_plan",
        return_value=_plan(),
    ) as build_plan, patch.object(
        supervisor,
        "_run_investment_execution",
    ) as run_execution:
        assert (
            supervisor._run_execution_evidence_maintenance(
                now,
                [{"market": "ASX", "execution_run": 1}],
                recovery_context={},
            )
            is False
        )
        assert (
            supervisor._run_execution_evidence_maintenance(
                now,
                [{"market": "ASX", "execution_run": 0}],
                recovery_context={"eligibility": {"active": True}},
            )
            is False
        )

    build_plan.assert_not_called()
    run_execution.assert_not_called()
