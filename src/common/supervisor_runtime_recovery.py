from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Mapping


RESTART_ACTIONS = {
    "restart_stale_supervisor_heartbeat_current_code",
    "restart_supervisor_current_code",
    "remove_stale_lock_then_restart_supervisor",
    "start_supervisor_current_code",
}


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _command_matches_supervisor(command: str) -> bool:
    compact = " ".join(_text(command).split())
    if not compact:
        return False
    return "src.app.supervisor" in compact and ("python" in compact or "Python" in compact)


def build_supervisor_runtime_recovery_plan(
    runtime_status: Mapping[str, Any],
    *,
    process_command: str = "",
    start_after_apply: bool = False,
) -> Dict[str, Any]:
    """Create a safe Supervisor recovery plan without performing side effects."""
    runtime = dict(runtime_status or {})
    next_action = _text(runtime.get("next_action"))
    restart_required = bool(runtime.get("restart_required", False))
    lock_status = _text(runtime.get("lock_status"))
    pid = _int(runtime.get("supervisor_pid"), 0)
    command = _text(process_command)
    command_match = _command_matches_supervisor(command)
    has_restart_action = next_action in RESTART_ACTIONS

    status = "not_required"
    reason = "supervisor_runtime_current"
    allowed = False
    terminate_pid = 0
    remove_lock_path = ""
    start_command = ""
    if not restart_required and next_action != "start_supervisor_current_code":
        pass
    elif not has_restart_action:
        status = "blocked"
        reason = "unsupported_runtime_recovery_action"
    elif lock_status == "stale_lock":
        status = "ready"
        reason = "stale_lock_can_be_removed"
        allowed = True
        remove_lock_path = _text(runtime.get("lock_path"))
    elif next_action == "start_supervisor_current_code":
        status = "ready"
        reason = "supervisor_not_running"
        allowed = True
    elif pid <= 0:
        status = "blocked"
        reason = "missing_supervisor_pid"
    elif not command:
        status = "blocked"
        reason = "supervisor_process_command_unavailable"
    elif not command_match:
        status = "blocked"
        reason = "supervisor_process_command_mismatch"
    else:
        status = "ready"
        reason = next_action
        allowed = True
        terminate_pid = pid

    config_path = _text(runtime.get("config_path"))
    if start_after_apply and allowed:
        start_command = " ".join(
            part
            for part in [
                "python",
                "-m",
                "src.app.supervisor",
                "--config",
                config_path,
            ]
            if part
        )

    return {
        "schema_version": "2026Q3.supervisor_runtime_recovery_plan.v1",
        "status": status,
        "reason": reason,
        "allowed": bool(allowed),
        "next_action": next_action,
        "restart_required": bool(restart_required),
        "lock_status": lock_status,
        "supervisor_pid": int(pid),
        "process_command": command,
        "process_command_match": bool(command_match),
        "terminate_pid": int(terminate_pid),
        "remove_lock_path": remove_lock_path,
        "start_after_apply": bool(start_after_apply),
        "start_command": start_command,
        "submit_orders": False,
        "connects_to_ibkr": bool(start_after_apply),
        "does_not_change_submit_gates": True,
        "requires_explicit_apply": True,
        "dry_run_default": True,
    }


def supervisor_runtime_recovery_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Supervisor Runtime Recovery Plan",
        "",
        f"- Status: {payload.get('status', '')}",
        f"- Reason: {payload.get('reason', '')}",
        f"- Allowed: {bool(payload.get('allowed', False))}",
        f"- Next action: {payload.get('next_action', '')}",
        f"- PID: {payload.get('supervisor_pid', 0) or '-'}",
        f"- Command match: {bool(payload.get('process_command_match', False))}",
        f"- Terminate PID: {payload.get('terminate_pid', 0) or '-'}",
        f"- Remove lock: {payload.get('remove_lock_path', '') or '-'}",
        f"- Start after apply: {bool(payload.get('start_after_apply', False))}",
        f"- Start command: {payload.get('start_command', '') or '-'}",
        f"- Submit orders: {bool(payload.get('submit_orders', False))}",
        f"- Connects to IBKR: {bool(payload.get('connects_to_ibkr', False))}",
        "",
        "Dry-run is the default. Use explicit apply controls before terminating or starting Supervisor.",
    ]
    return "\n".join(lines) + "\n"


def write_supervisor_runtime_recovery_plan(
    payload: Mapping[str, Any],
    *,
    out_dir: Path,
) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "supervisor_runtime_recovery_plan.json"
    md_path = out_dir / "supervisor_runtime_recovery_plan.md"
    json_path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(supervisor_runtime_recovery_markdown(payload), encoding="utf-8")
    return {"summary_json": json_path, "markdown": md_path}


def enrich_recovery_plan_with_apply_result(
    plan: Mapping[str, Any],
    *,
    applied: bool,
    result: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    payload = dict(plan or {})
    payload["applied"] = bool(applied)
    payload["apply_result"] = dict(result or {})
    return payload
