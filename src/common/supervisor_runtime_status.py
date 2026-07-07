from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping


RUNNING_SUPERVISOR_STATES = {"running", "running_degraded"}
SUPERVISOR_HEARTBEAT_STALE_HOURS = 6.0


def load_json_file(path: Path) -> Dict[str, Any]:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def current_git_revision(repo_root: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            text=True,
            capture_output=True,
            timeout=2,
            check=False,
        )
    except Exception:
        return ""
    if completed.returncode != 0:
        return ""
    return str(completed.stdout or "").strip()


def pid_alive(pid_value: Any) -> bool | None:
    try:
        pid = int(pid_value or 0)
    except (TypeError, ValueError):
        return None
    if pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return _pid_alive_via_ps(pid)
    except OSError:
        return _pid_alive_via_ps(pid)
    return True


def _pid_alive_via_ps(pid: int) -> bool | None:
    try:
        completed = subprocess.run(
            ["ps", "-p", str(int(pid))],
            text=True,
            capture_output=True,
            timeout=2,
            check=False,
        )
    except Exception:
        return None
    if completed.returncode == 0:
        return True
    if completed.returncode == 1:
        return False
    return None


def _liveness_label(value: bool | None) -> str:
    if value is True:
        return "alive"
    if value is False:
        return "dead"
    return "unknown"


def _revision_status(status: str, supervisor_revision: str, current_revision: str) -> str:
    if status not in RUNNING_SUPERVISOR_STATES:
        return ""
    if supervisor_revision and current_revision:
        return "match" if supervisor_revision == current_revision else "mismatch"
    if not supervisor_revision:
        return "missing"
    return "unknown"


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _parse_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _heartbeat_age_hours(written_at: Any, now: datetime) -> float | None:
    written_dt = _parse_dt(written_at)
    if written_dt is None:
        return None
    return max(0.0, (now.astimezone(timezone.utc) - written_dt).total_seconds() / 3600.0)


def build_supervisor_runtime_status(
    *,
    summary_dir: Path,
    config_path: Path | str = "",
    repo_root: Path | None = None,
    now: datetime | None = None,
    current_revision: str | None = None,
    pid_alive_func: Callable[[Any], bool | None] = pid_alive,
) -> Dict[str, Any]:
    """Build a read-only Supervisor runtime contract from lock/status artifacts."""
    summary = Path(summary_dir).resolve()
    lock_path = summary / "supervisor.lock"
    status_path = summary / "supervisor_shutdown_status.json"
    return build_supervisor_runtime_status_from_payloads(
        summary_dir=summary,
        lock_owner=load_json_file(lock_path),
        shutdown_status=load_json_file(status_path),
        config_path=config_path,
        repo_root=repo_root,
        now=now,
        current_revision=current_revision,
        pid_alive_func=pid_alive_func,
    )


def build_supervisor_runtime_status_from_payloads(
    *,
    summary_dir: Path,
    lock_owner: Mapping[str, Any] | None = None,
    shutdown_status: Mapping[str, Any] | None = None,
    config_path: Path | str = "",
    repo_root: Path | None = None,
    now: datetime | None = None,
    current_revision: str | None = None,
    pid_alive_func: Callable[[Any], bool | None] = pid_alive,
) -> Dict[str, Any]:
    """Build a Supervisor runtime contract from already-loaded artifacts."""
    root = Path(repo_root or Path.cwd()).resolve()
    generated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    summary = Path(summary_dir).resolve()
    lock_path = summary / "supervisor.lock"
    status_path = summary / "supervisor_shutdown_status.json"
    lock = dict(lock_owner or {})
    shutdown = dict(shutdown_status or {})
    status = str(shutdown.get("status") or "").strip().lower()
    reason = str(shutdown.get("reason") or "").strip()
    status_pid = _int(shutdown.get("pid"), 0)
    lock_pid = _int(lock.get("pid"), 0)
    effective_pid = status_pid or lock_pid
    alive = pid_alive_func(effective_pid)
    liveness_status = _liveness_label(alive)
    current = str(current_revision if current_revision is not None else current_git_revision(root) or "").strip()
    supervisor_revision = str(shutdown.get("code_revision") or "").strip()
    revision_status = _revision_status(status, supervisor_revision, current)
    heartbeat_age_hours = _heartbeat_age_hours(shutdown.get("written_at"), generated_at)
    heartbeat_status = "missing"
    if status in RUNNING_SUPERVISOR_STATES:
        if heartbeat_age_hours is None:
            heartbeat_status = "unknown"
        elif heartbeat_age_hours > SUPERVISOR_HEARTBEAT_STALE_HOURS:
            heartbeat_status = "stale"
        else:
            heartbeat_status = "fresh"

    lock_status = "missing"
    if lock:
        lock_status = "held"
        if alive is False:
            lock_status = "stale_lock"
        elif alive is None:
            lock_status = "held_unknown_liveness"

    restart_required = False
    blocks_recovery_refresh = False
    next_action = "inspect_supervisor_runtime"
    health_status = "warning"
    if not lock and not shutdown:
        next_action = "start_supervisor_current_code"
        health_status = "warning"
    elif status == "crashed":
        next_action = "inspect_crash_then_restart_supervisor"
        health_status = "degraded"
        blocks_recovery_refresh = True
    elif lock_status == "stale_lock":
        next_action = "remove_stale_lock_then_restart_supervisor"
        health_status = "degraded"
        restart_required = True
        blocks_recovery_refresh = True
    elif status in RUNNING_SUPERVISOR_STATES and heartbeat_status == "stale":
        next_action = "restart_stale_supervisor_heartbeat_current_code"
        health_status = "degraded"
        restart_required = True
        blocks_recovery_refresh = True
    elif status in RUNNING_SUPERVISOR_STATES and revision_status in {"missing", "mismatch"}:
        next_action = "restart_supervisor_current_code"
        health_status = "degraded" if revision_status == "mismatch" else "warning"
        restart_required = True
        blocks_recovery_refresh = True
    elif status in {"stopping", "stopped"}:
        next_action = "start_supervisor_current_code"
        health_status = "warning"
        restart_required = True
        blocks_recovery_refresh = True
    elif status in RUNNING_SUPERVISOR_STATES:
        next_action = "continue_monitoring_supervisor_runtime"
        health_status = "ready" if revision_status in {"match", "unknown"} else "warning"
    else:
        next_action = "inspect_supervisor_runtime"

    return {
        "schema_version": "2026Q3.supervisor_runtime_status.v1",
        "generated_at": generated_at.isoformat(),
        "summary_dir": str(summary),
        "config_path": str(config_path or shutdown.get("config_path") or lock.get("config_path") or ""),
        "lock_path": str(lock_path),
        "status_path": str(status_path),
        "lock_status": lock_status,
        "lock_owner": lock,
        "shutdown_status": shutdown,
        "supervisor_status": status,
        "supervisor_reason": reason,
        "supervisor_pid": effective_pid,
        "supervisor_liveness_status": liveness_status,
        "supervisor_code_revision": supervisor_revision,
        "current_code_revision": current,
        "supervisor_code_revision_status": revision_status,
        "supervisor_heartbeat_status": heartbeat_status,
        "supervisor_heartbeat_age_hours": heartbeat_age_hours,
        "supervisor_heartbeat_stale_hours": SUPERVISOR_HEARTBEAT_STALE_HOURS,
        "health_status": health_status,
        "restart_required": bool(restart_required),
        "blocks_recovery_refresh": bool(blocks_recovery_refresh),
        "next_action": next_action,
        "request_policy": (
            "no_ibkr_requests_until_supervisor_runtime_current"
            if blocks_recovery_refresh
            else "normal_supervisor_request_policy"
        ),
        "submit_orders": False,
        "does_not_relax_submit_gates": True,
    }


def supervisor_runtime_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Supervisor Runtime Status",
        "",
        f"- Generated: {payload.get('generated_at', '')}",
        f"- Health: {payload.get('health_status', '')}",
        f"- Status: {payload.get('supervisor_status', '') or 'missing'}",
        f"- Reason: {payload.get('supervisor_reason', '') or '-'}",
        f"- PID: {payload.get('supervisor_pid', 0) or '-'}",
        f"- Liveness: {payload.get('supervisor_liveness_status', '')}",
        f"- Lock: {payload.get('lock_status', '')}",
        f"- Code revision status: {payload.get('supervisor_code_revision_status', '') or '-'}",
        f"- Heartbeat: {payload.get('supervisor_heartbeat_status', '') or '-'}"
        f" age_hours={payload.get('supervisor_heartbeat_age_hours', '')}",
        f"- Next action: {payload.get('next_action', '')}",
        f"- Blocks recovery refresh: {bool(payload.get('blocks_recovery_refresh', False))}",
        f"- Request policy: {payload.get('request_policy', '')}",
        "",
        "This artifact is read-only. It does not stop Supervisor, connect to IBKR, or submit orders.",
    ]
    return "\n".join(lines) + "\n"
