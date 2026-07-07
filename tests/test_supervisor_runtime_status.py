from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.common import supervisor_runtime_status
from src.common.supervisor_runtime_status import (
    build_supervisor_runtime_status,
    build_supervisor_runtime_status_from_payloads,
    pid_alive,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_supervisor_runtime_status_requires_restart_when_revision_missing(tmp_path: Path) -> None:
    summary_dir = tmp_path / "reports_supervisor"
    _write_json(
        summary_dir / "supervisor.lock",
        {"pid": 123, "config_path": "config/supervisor.yaml"},
    )
    _write_json(
        summary_dir / "supervisor_shutdown_status.json",
        {
            "status": "running",
            "reason": "ignored_signal:SIGHUP",
            "pid": 123,
        },
    )

    payload = build_supervisor_runtime_status(
        summary_dir=summary_dir,
        current_revision="abc",
        now=datetime(2026, 7, 2, 1, 0, tzinfo=timezone.utc),
        pid_alive_func=lambda pid: True,
    )

    assert payload["health_status"] == "warning"
    assert payload["supervisor_code_revision_status"] == "missing"
    assert payload["restart_required"] is True
    assert payload["blocks_recovery_refresh"] is True
    assert payload["next_action"] == "restart_supervisor_current_code"
    assert payload["request_policy"] == "no_ibkr_requests_until_supervisor_runtime_current"
    assert payload["submit_orders"] is False


def test_supervisor_runtime_status_marks_current_running_runtime_ready(tmp_path: Path) -> None:
    summary_dir = tmp_path / "reports_supervisor"
    _write_json(summary_dir / "supervisor.lock", {"pid": 123})
    _write_json(
        summary_dir / "supervisor_shutdown_status.json",
        {
            "status": "running",
            "reason": "cycle_complete",
            "pid": 123,
            "code_revision": "abc",
            "written_at": "2026-07-02T00:58:00+00:00",
        },
    )

    payload = build_supervisor_runtime_status(
        summary_dir=summary_dir,
        current_revision="abc",
        now=datetime(2026, 7, 2, 1, 0, tzinfo=timezone.utc),
        pid_alive_func=lambda pid: True,
    )

    assert payload["health_status"] == "ready"
    assert payload["supervisor_code_revision_status"] == "match"
    assert payload["supervisor_heartbeat_status"] == "fresh"
    assert payload["restart_required"] is False
    assert payload["blocks_recovery_refresh"] is False
    assert payload["next_action"] == "continue_monitoring_supervisor_runtime"


def test_supervisor_runtime_status_degrades_stale_running_heartbeat(tmp_path: Path) -> None:
    summary_dir = tmp_path / "reports_supervisor"
    _write_json(summary_dir / "supervisor.lock", {"pid": 123})
    _write_json(
        summary_dir / "supervisor_shutdown_status.json",
        {
            "status": "running",
            "reason": "cycle_complete",
            "pid": 123,
            "code_revision": "abc",
            "written_at": "2026-07-01T12:00:00+00:00",
        },
    )

    payload = build_supervisor_runtime_status(
        summary_dir=summary_dir,
        current_revision="abc",
        now=datetime(2026, 7, 2, 1, 0, tzinfo=timezone.utc),
        pid_alive_func=lambda pid: True,
    )

    assert payload["health_status"] == "degraded"
    assert payload["supervisor_heartbeat_status"] == "stale"
    assert payload["supervisor_heartbeat_age_hours"] == 13.0
    assert payload["restart_required"] is True
    assert payload["blocks_recovery_refresh"] is True
    assert payload["next_action"] == "restart_stale_supervisor_heartbeat_current_code"


def test_supervisor_runtime_status_from_payloads_handles_running_degraded_revision_mismatch(tmp_path: Path) -> None:
    payload = build_supervisor_runtime_status_from_payloads(
        summary_dir=tmp_path / "reports_supervisor",
        lock_owner={"pid": 123},
        shutdown_status={
            "status": "running_degraded",
            "reason": "cycle_exception:RuntimeError",
            "pid": 123,
            "code_revision": "old",
        },
        current_revision="new",
        now=datetime(2026, 7, 2, 1, 0, tzinfo=timezone.utc),
        pid_alive_func=lambda pid: True,
    )

    assert payload["supervisor_status"] == "running_degraded"
    assert payload["health_status"] == "degraded"
    assert payload["supervisor_code_revision_status"] == "mismatch"
    assert payload["restart_required"] is True
    assert payload["blocks_recovery_refresh"] is True
    assert payload["next_action"] == "restart_supervisor_current_code"


def test_supervisor_runtime_status_blocks_on_stale_lock(tmp_path: Path) -> None:
    summary_dir = tmp_path / "reports_supervisor"
    _write_json(summary_dir / "supervisor.lock", {"pid": 123})

    payload = build_supervisor_runtime_status(
        summary_dir=summary_dir,
        current_revision="abc",
        now=datetime(2026, 7, 2, 1, 0, tzinfo=timezone.utc),
        pid_alive_func=lambda pid: False,
    )

    assert payload["health_status"] == "degraded"
    assert payload["lock_status"] == "stale_lock"
    assert payload["restart_required"] is True
    assert payload["blocks_recovery_refresh"] is True
    assert payload["next_action"] == "remove_stale_lock_then_restart_supervisor"


def test_pid_alive_falls_back_to_ps_when_kill_is_not_permitted(monkeypatch) -> None:
    class Result:
        returncode = 0

    def _deny_kill(pid: int, sig: int) -> None:
        raise PermissionError("sandbox denied kill probe")

    def _run(cmd, **kwargs):
        assert cmd == ["ps", "-p", "123"]
        return Result()

    monkeypatch.setattr(supervisor_runtime_status.os, "kill", _deny_kill)
    monkeypatch.setattr(supervisor_runtime_status.subprocess, "run", _run)

    assert pid_alive(123) is True
