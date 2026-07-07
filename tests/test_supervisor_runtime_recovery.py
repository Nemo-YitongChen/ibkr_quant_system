from __future__ import annotations

import json
from pathlib import Path

from src.common.supervisor_runtime_recovery import build_supervisor_runtime_recovery_plan
from src.tools import recover_supervisor_runtime


def test_recovery_plan_allows_stale_supervisor_heartbeat_with_matching_command() -> None:
    plan = build_supervisor_runtime_recovery_plan(
        {
            "restart_required": True,
            "next_action": "restart_stale_supervisor_heartbeat_current_code",
            "lock_status": "held",
            "supervisor_pid": 123,
            "config_path": "config/supervisor.yaml",
        },
        process_command="/usr/bin/python -m src.app.supervisor --config config/supervisor.yaml",
    )

    assert plan["status"] == "ready"
    assert plan["allowed"] is True
    assert plan["reason"] == "restart_stale_supervisor_heartbeat_current_code"
    assert plan["terminate_pid"] == 123
    assert plan["remove_lock_path"] == ""
    assert plan["submit_orders"] is False
    assert plan["connects_to_ibkr"] is False


def test_recovery_plan_blocks_supervisor_restart_when_command_does_not_match() -> None:
    plan = build_supervisor_runtime_recovery_plan(
        {
            "restart_required": True,
            "next_action": "restart_stale_supervisor_heartbeat_current_code",
            "lock_status": "held",
            "supervisor_pid": 123,
        },
        process_command="/usr/bin/python -m unrelated.service",
    )

    assert plan["status"] == "blocked"
    assert plan["allowed"] is False
    assert plan["reason"] == "supervisor_process_command_mismatch"
    assert plan["terminate_pid"] == 0


def test_recovery_plan_blocks_when_process_command_is_unavailable() -> None:
    plan = build_supervisor_runtime_recovery_plan(
        {
            "restart_required": True,
            "next_action": "restart_stale_supervisor_heartbeat_current_code",
            "lock_status": "held",
            "supervisor_pid": 123,
        },
        process_command="",
    )

    assert plan["status"] == "blocked"
    assert plan["allowed"] is False
    assert plan["reason"] == "supervisor_process_command_unavailable"
    assert plan["terminate_pid"] == 0


def test_recovery_plan_allows_stale_lock_removal_without_terminating_pid(tmp_path: Path) -> None:
    lock_path = tmp_path / "reports_supervisor" / "supervisor.lock"
    plan = build_supervisor_runtime_recovery_plan(
        {
            "restart_required": True,
            "next_action": "remove_stale_lock_then_restart_supervisor",
            "lock_status": "stale_lock",
            "supervisor_pid": 123,
            "lock_path": str(lock_path),
        },
        process_command="",
    )

    assert plan["status"] == "ready"
    assert plan["allowed"] is True
    assert plan["terminate_pid"] == 0
    assert plan["remove_lock_path"] == str(lock_path)


def test_recovery_plan_can_prepare_explicit_start_command() -> None:
    plan = build_supervisor_runtime_recovery_plan(
        {
            "restart_required": True,
            "next_action": "restart_supervisor_current_code",
            "lock_status": "held",
            "supervisor_pid": 123,
            "config_path": "config/supervisor.yaml",
        },
        process_command="python -m src.app.supervisor",
        start_after_apply=True,
    )

    assert plan["status"] == "ready"
    assert plan["allowed"] is True
    assert plan["start_after_apply"] is True
    assert plan["start_command"] == "python -m src.app.supervisor --config config/supervisor.yaml"
    assert plan["connects_to_ibkr"] is True
    assert plan["submit_orders"] is False


def test_recover_supervisor_runtime_cli_defaults_to_dry_run(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "supervisor.yaml"
    summary_dir = tmp_path / "reports_supervisor"
    out_dir = tmp_path / "out"
    config_path.write_text('timezone: "Australia/Sydney"\nmarkets: []\n', encoding="utf-8")
    summary_dir.mkdir(parents=True)
    monkeypatch.setattr(
        recover_supervisor_runtime,
        "build_supervisor_runtime_status",
        lambda **kwargs: {
            "restart_required": True,
            "next_action": "restart_stale_supervisor_heartbeat_current_code",
            "lock_status": "held",
            "supervisor_pid": 123,
            "config_path": str(config_path),
        },
    )
    monkeypatch.setattr(
        recover_supervisor_runtime,
        "_process_command",
        lambda pid: "python -m src.app.supervisor --config config/supervisor.yaml",
    )

    recover_supervisor_runtime.main(
        [
            "--config",
            str(config_path),
            "--summary_dir",
            str(summary_dir),
            "--out_dir",
            str(out_dir),
        ]
    )

    payload = json.loads((out_dir / "supervisor_runtime_recovery_plan.json").read_text(encoding="utf-8"))
    assert payload["status"] == "ready"
    assert payload["allowed"] is True
    assert payload["applied"] is False
    assert payload["apply_result"]["status"] == "dry_run"
    assert payload["terminate_pid"] == 123
    assert payload["submit_orders"] is False
