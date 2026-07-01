from __future__ import annotations

import json
from pathlib import Path

from src.tools.generate_dashboard import build_dashboard


def test_dashboard_surfaces_supervisor_shutdown_history(tmp_path: Path) -> None:
    summary_dir = tmp_path / "reports_supervisor"
    summary_dir.mkdir(parents=True)
    cfg_path = tmp_path / "supervisor.yaml"
    cfg_path.write_text(
        "\n".join(
            [
                'timezone: "Australia/Sydney"',
                f'summary_out_dir: "{summary_dir}"',
                "markets: []",
            ]
        ),
        encoding="utf-8",
    )
    shutdown_status = {
        "schema_version": "2026Q2.supervisor_shutdown_status.v1",
        "status": "crashed",
        "reason": "exception:RuntimeError",
        "pid": 1234,
        "config_path": str(cfg_path),
        "last_signal_name": "",
        "written_at": "2026-06-18T10:00:00+00:00",
    }
    (summary_dir / "supervisor_shutdown_status.json").write_text(
        json.dumps(shutdown_status),
        encoding="utf-8",
    )
    events = [
        {**shutdown_status, "status": "running", "reason": "started"},
        shutdown_status,
    ]
    (summary_dir / "supervisor_shutdown_events.jsonl").write_text(
        json.dumps(events[0]) + "\nnot-json\n" + json.dumps(events[1]) + "\n",
        encoding="utf-8",
    )

    payload = build_dashboard(str(cfg_path), str(summary_dir))

    assert payload["supervisor_runtime_status"]["supervisor_status"] == "crashed"
    assert payload["supervisor_runtime_status"]["health_status"] == "degraded"
    assert payload["supervisor_runtime_status"]["blocks_recovery_refresh"] is True
    assert payload["supervisor_runtime_status"]["submit_orders"] is False
    assert payload["supervisor_shutdown_status"]["status"] == "crashed"
    assert len(payload["supervisor_shutdown_events"]) == 2
    assert payload["ops_overview"]["supervisor_shutdown_status"] == "crashed"
    assert payload["ops_overview"]["supervisor_shutdown_health_status"] == "degraded"
    assert payload["ops_overview"]["supervisor_shutdown_reason"] == "exception:RuntimeError"
    assert payload["ops_overview"]["supervisor_runtime_blocks_recovery_refresh"] is True
    assert payload["ops_overview"]["supervisor_shutdown_event_count"] == 2
    assert any(row.get("category") == "SUPERVISOR" for row in payload["ops_overview"]["alert_rows"])
    ops_block = next(block for block in payload["dashboard_v2_blocks"] if block.get("id") == "ops_health")
    assert ops_block["metrics"]["supervisor_shutdown_status"] == "crashed"
    assert ops_block["metrics"]["supervisor_shutdown_event_count"] == 2
