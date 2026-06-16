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
