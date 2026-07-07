from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.runtime_paths import resolve_repo_path
from ..common.supervisor_runtime_recovery import (
    build_supervisor_runtime_recovery_plan,
    enrich_recovery_plan_with_apply_result,
    write_supervisor_runtime_recovery_plan,
)
from ..common.supervisor_runtime_status import build_supervisor_runtime_status, pid_alive

BASE_DIR = Path(__file__).resolve().parents[2]


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _summary_dir(cfg: Dict[str, Any], runtime_root: str) -> Path:
    raw = str(cfg.get("summary_out_dir", "reports_supervisor") or "reports_supervisor")
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    if runtime_root and bool(cfg.get("scope_summary_out_dir", False)):
        return (resolve_repo_path(BASE_DIR, runtime_root) / raw).resolve()
    return resolve_repo_path(BASE_DIR, raw)


def _process_command(pid: int) -> str:
    if pid <= 0:
        return ""
    try:
        completed = subprocess.run(
            ["ps", "-p", str(int(pid)), "-o", "command="],
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


def _terminate_pid(pid: int, *, timeout_sec: float = 10.0, kill_after_sec: float = 0.0) -> Dict[str, Any]:
    if pid <= 0:
        return {"status": "skipped", "reason": "missing_pid"}
    if pid_alive(pid) is False:
        return {"status": "already_dead", "pid": pid}
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return {"status": "already_dead", "pid": pid}
    except Exception as exc:
        return {"status": "failed", "pid": pid, "error": f"{type(exc).__name__}:{exc}"}

    deadline = time.monotonic() + max(0.0, float(timeout_sec))
    while time.monotonic() < deadline:
        if pid_alive(pid) is False:
            return {"status": "terminated", "pid": pid, "signal": "SIGTERM"}
        time.sleep(0.2)

    if kill_after_sec and kill_after_sec > 0:
        kill_deadline = time.monotonic() + float(kill_after_sec)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            return {"status": "terminated", "pid": pid, "signal": "SIGTERM"}
        except Exception as exc:
            return {"status": "failed", "pid": pid, "error": f"{type(exc).__name__}:{exc}"}
        while time.monotonic() < kill_deadline:
            if pid_alive(pid) is False:
                return {"status": "killed", "pid": pid, "signal": "SIGKILL"}
            time.sleep(0.2)

    return {"status": "timeout", "pid": pid, "signal": "SIGTERM"}


def _remove_lock(path: str, *, summary_dir: Path) -> Dict[str, Any]:
    raw = str(path or "").strip()
    if not raw:
        return {"status": "skipped", "reason": "missing_lock_path"}
    lock_path = Path(raw).resolve()
    summary_root = summary_dir.resolve()
    if summary_root not in lock_path.parents:
        return {"status": "blocked", "reason": "lock_path_outside_summary_dir", "lock_path": str(lock_path)}
    try:
        if lock_path.exists():
            lock_path.unlink()
            return {"status": "removed", "lock_path": str(lock_path)}
        return {"status": "already_missing", "lock_path": str(lock_path)}
    except Exception as exc:
        return {"status": "failed", "lock_path": str(lock_path), "error": f"{type(exc).__name__}:{exc}"}


def _start_supervisor(config_path: Path, *, out_dir: Path) -> Dict[str, Any]:
    log_path = out_dir / "supervisor_recovery_launch.log"
    handle = log_path.open("a", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "src.app.supervisor", "--config", str(config_path)],
            cwd=str(BASE_DIR),
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
    except Exception as exc:
        handle.close()
        return {"status": "failed", "error": f"{type(exc).__name__}:{exc}", "log_path": str(log_path)}
    handle.close()
    return {"status": "started", "pid": int(proc.pid), "log_path": str(log_path)}


def _apply_recovery_plan(
    plan: Dict[str, Any],
    *,
    summary_dir: Path,
    config_path: Path,
    out_dir: Path,
    terminate_timeout_sec: float,
    kill_after_sec: float,
) -> Dict[str, Any]:
    if not bool(plan.get("allowed", False)):
        return {"status": "blocked", "reason": str(plan.get("reason") or "plan_not_allowed")}
    result: Dict[str, Any] = {"status": "applied", "steps": []}
    terminate_pid = int(plan.get("terminate_pid", 0) or 0)
    if terminate_pid > 0:
        result["steps"].append(
            {
                "action": "terminate_supervisor",
                **_terminate_pid(
                    terminate_pid,
                    timeout_sec=terminate_timeout_sec,
                    kill_after_sec=kill_after_sec,
                ),
            }
        )
    lock_path = str(plan.get("remove_lock_path") or "")
    if lock_path:
        result["steps"].append({"action": "remove_stale_lock", **_remove_lock(lock_path, summary_dir=summary_dir)})
    if bool(plan.get("start_after_apply", False)):
        result["steps"].append({"action": "start_supervisor", **_start_supervisor(config_path, out_dir=out_dir)})
    if any(str(step.get("status") or "") in {"failed", "timeout", "blocked"} for step in result["steps"]):
        result["status"] = "partial_or_failed"
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = build_cli_parser(
        description="Build or apply a safe Supervisor runtime recovery plan.",
        command="ibkr-quant-supervisor-recovery",
        examples=[
            "python -m src.tools.recover_supervisor_runtime --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001",
            "python -m src.tools.recover_supervisor_runtime --apply --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001",
        ],
        notes=[
            "Dry-run is the default.",
            "Does not submit orders or change submit gates.",
            "Starting Supervisor is opt-in with --start and may let the scheduler resume normal configured tasks.",
        ],
    )
    parser.add_argument("--config", default="config/supervisor.yaml", help="Supervisor config path.")
    parser.add_argument("--runtime_root", default="", help="Optional scoped runtime root.")
    parser.add_argument("--summary_dir", default="", help="Optional explicit reports_supervisor directory.")
    parser.add_argument("--out_dir", default="", help="Output directory. Defaults to summary_dir.")
    parser.add_argument("--apply", action="store_true", default=False, help="Apply safe terminate/remove-lock actions.")
    parser.add_argument("--start", action="store_true", default=False, help="Start Supervisor after successful apply.")
    parser.add_argument("--terminate_timeout_sec", type=float, default=10.0, help="Seconds to wait after SIGTERM.")
    parser.add_argument("--kill_after_sec", type=float, default=0.0, help="Optional seconds to wait after SIGKILL fallback.")
    return parser


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    cfg_path = resolve_repo_path(BASE_DIR, str(args.config))
    cfg = _load_yaml(cfg_path)
    summary_dir = (
        resolve_repo_path(BASE_DIR, str(args.summary_dir))
        if str(args.summary_dir or "").strip()
        else _summary_dir(cfg, str(args.runtime_root or ""))
    )
    out_dir = resolve_repo_path(BASE_DIR, str(args.out_dir)) if str(args.out_dir or "").strip() else summary_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    runtime = build_supervisor_runtime_status(
        summary_dir=summary_dir,
        config_path=cfg_path,
        repo_root=BASE_DIR,
    )
    process_command = _process_command(int(runtime.get("supervisor_pid", 0) or 0))
    plan = build_supervisor_runtime_recovery_plan(
        runtime,
        process_command=process_command,
        start_after_apply=bool(args.start),
    )
    if bool(args.apply):
        plan = enrich_recovery_plan_with_apply_result(
            plan,
            applied=True,
            result=_apply_recovery_plan(
                plan,
                summary_dir=summary_dir,
                config_path=cfg_path,
                out_dir=out_dir,
                terminate_timeout_sec=float(args.terminate_timeout_sec),
                kill_after_sec=float(args.kill_after_sec),
            ),
        )
    else:
        plan = enrich_recovery_plan_with_apply_result(plan, applied=False, result={"status": "dry_run"})
    artifacts = write_supervisor_runtime_recovery_plan(plan, out_dir=out_dir)
    emit_cli_summary(
        command="ibkr-quant-supervisor-recovery",
        headline="supervisor runtime recovery plan complete",
        summary={
            "status": plan.get("status"),
            "reason": plan.get("reason"),
            "allowed": plan.get("allowed"),
            "applied": plan.get("applied"),
            "terminate_pid": plan.get("terminate_pid"),
            "start_after_apply": plan.get("start_after_apply"),
            "submit_orders": plan.get("submit_orders"),
            "connects_to_ibkr": plan.get("connects_to_ibkr"),
        },
        artifacts=artifacts,
    )


if __name__ == "__main__":
    main()
