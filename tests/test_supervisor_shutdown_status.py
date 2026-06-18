from __future__ import annotations

import json
from pathlib import Path

from src.app.supervisor import Supervisor


def test_supervisor_writes_shutdown_status(tmp_path: Path) -> None:
    cfg_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
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
    supervisor = Supervisor(str(cfg_path))

    supervisor._last_signal_name = "SIGTERM"
    supervisor._write_shutdown_status(status="stopping", reason="signal:SIGTERM")

    payload = json.loads((summary_dir / "supervisor_shutdown_status.json").read_text(encoding="utf-8"))
    assert payload["status"] == "stopping"
    assert payload["reason"] == "signal:SIGTERM"
    assert payload["last_signal_name"] == "SIGTERM"

    events = [
        json.loads(line)
        for line in (summary_dir / "supervisor_shutdown_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(events) == 1
    assert events[0]["status"] == "stopping"
    assert events[0]["reason"] == "signal:SIGTERM"
    assert events[0]["last_signal_name"] == "SIGTERM"


def test_supervisor_final_shutdown_status_preserves_crash(tmp_path: Path) -> None:
    cfg_path = tmp_path / "supervisor.yaml"
    cfg_path.write_text(
        "\n".join(
            [
                'timezone: "Australia/Sydney"',
                f'summary_out_dir: "{tmp_path / "reports_supervisor"}"',
                "markets: []",
            ]
        ),
        encoding="utf-8",
    )
    supervisor = Supervisor(str(cfg_path))

    supervisor._shutdown_reason = "exception:RuntimeError"
    assert supervisor._final_shutdown_status() == "crashed"

    supervisor._shutdown_reason = "signal:SIGTERM"
    assert supervisor._final_shutdown_status() == "stopped"
