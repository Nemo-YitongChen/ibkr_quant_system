from __future__ import annotations

import json
from pathlib import Path

import pytest

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
    supervisor._runtime_code_revision = "abc123"
    supervisor._code_revision = lambda: "current123"

    supervisor._last_signal_name = "SIGTERM"
    supervisor._write_shutdown_status(status="stopping", reason="signal:SIGTERM")

    payload = json.loads((summary_dir / "supervisor_shutdown_status.json").read_text(encoding="utf-8"))
    assert payload["status"] == "stopping"
    assert payload["reason"] == "signal:SIGTERM"
    assert payload["code_revision"] == "abc123"
    assert payload["current_code_revision"] == "current123"
    assert payload["last_signal_name"] == "SIGTERM"

    events = [
        json.loads(line)
        for line in (summary_dir / "supervisor_shutdown_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(events) == 1
    assert events[0]["status"] == "stopping"
    assert events[0]["reason"] == "signal:SIGTERM"
    assert events[0]["code_revision"] == "abc123"
    assert events[0]["current_code_revision"] == "current123"
    assert events[0]["last_signal_name"] == "SIGTERM"


def test_supervisor_heartbeat_updates_status_without_event_spam(tmp_path: Path) -> None:
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
    supervisor._runtime_code_revision = "abc123"
    supervisor._code_revision = lambda: "current123"

    supervisor._write_shutdown_status(status="running", reason="started")
    supervisor._write_shutdown_status(
        status="running",
        reason="cycle_complete",
        append_event=False,
        extra={"consecutive_cycle_error_count": 0},
    )

    payload = json.loads((summary_dir / "supervisor_shutdown_status.json").read_text(encoding="utf-8"))
    assert payload["status"] == "running"
    assert payload["reason"] == "cycle_complete"
    assert payload["code_revision"] == "abc123"
    assert payload["current_code_revision"] == "current123"
    assert payload["consecutive_cycle_error_count"] == 0

    events = [
        json.loads(line)
        for line in (summary_dir / "supervisor_shutdown_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(events) == 1
    assert events[0]["reason"] == "started"


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


def test_supervisor_run_forever_continues_after_transient_cycle_error(tmp_path: Path) -> None:
    cfg_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
    cfg_path.write_text(
        "\n".join(
            [
                'timezone: "Australia/Sydney"',
                "poll_sec: 0",
                f'summary_out_dir: "{summary_dir}"',
                "max_consecutive_cycle_errors_before_shutdown: 3",
                "markets: []",
            ]
        ),
        encoding="utf-8",
    )
    supervisor = Supervisor(str(cfg_path))
    supervisor._code_revision = lambda: "abc123"
    supervisor._setup_signal_handlers = lambda: None
    supervisor._start_dashboard_control_service = lambda: None
    supervisor._stop_dashboard_control_service = lambda: None

    calls = {"count": 0}

    def _run_cycle() -> None:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("temporary db lock")
        supervisor._stopping = True

    supervisor.run_cycle = _run_cycle  # type: ignore[method-assign]

    supervisor.run_forever()

    assert calls["count"] == 2
    payload = json.loads((summary_dir / "supervisor_shutdown_status.json").read_text(encoding="utf-8"))
    assert payload["status"] == "stopped"
    assert payload["reason"] == "running"

    events = [
        json.loads(line)
        for line in (summary_dir / "supervisor_shutdown_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(row["status"] == "running_degraded" and row["reason"] == "cycle_exception:RuntimeError" for row in events)


def test_supervisor_run_forever_crashes_after_cycle_error_budget(tmp_path: Path) -> None:
    cfg_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
    cfg_path.write_text(
        "\n".join(
            [
                'timezone: "Australia/Sydney"',
                "poll_sec: 0",
                f'summary_out_dir: "{summary_dir}"',
                "max_consecutive_cycle_errors_before_shutdown: 2",
                "markets: []",
            ]
        ),
        encoding="utf-8",
    )
    supervisor = Supervisor(str(cfg_path))
    supervisor._code_revision = lambda: "abc123"
    supervisor._setup_signal_handlers = lambda: None
    supervisor._start_dashboard_control_service = lambda: None
    supervisor._stop_dashboard_control_service = lambda: None
    supervisor.run_cycle = lambda: (_ for _ in ()).throw(RuntimeError("persistent failure"))  # type: ignore[method-assign]

    with pytest.raises(RuntimeError):
        supervisor.run_forever()

    payload = json.loads((summary_dir / "supervisor_shutdown_status.json").read_text(encoding="utf-8"))
    assert payload["status"] == "crashed"
    assert payload["reason"] == "exception:RuntimeError"
    assert payload["consecutive_cycle_error_count"] == 2
