from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import yaml

from ..analysis.report import write_json
from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.markets import market_config_path, resolve_market_code
from ..common.runtime_paths import resolve_repo_path

BASE_DIR = Path(__file__).resolve().parents[2]
SAFE_UNBLOCK_STATUS = "stale_execution_refresh_required"
SAFE_UNBLOCK_ACTION = "refresh_stale_execution_target_no_submit"


def build_parser() -> argparse.ArgumentParser:
    ap = build_cli_parser(
        description="Build or apply the next safe auto-order unblock step from readiness artifacts.",
        command="ibkr-quant-auto-order-unblock",
        examples=[
            "ibkr-quant-auto-order-unblock --config config/supervisor.yaml",
            "ibkr-quant-auto-order-unblock --config config/supervisor.yaml --apply",
        ],
        notes=[
            "Default mode is dry-run: it writes the exact recovery commands but does not run them.",
            "Apply mode is still strictly no-submit and forces --recovery_evidence_only for execution.",
        ],
    )
    ap.add_argument("--config", default="config/supervisor.yaml", help="Supervisor config path.")
    ap.add_argument(
        "--runtime_root",
        default="runtime_data/paper_investment_only_duq152001",
        help="Runtime artifact root used by supervisor-scoped artifacts.",
    )
    ap.add_argument(
        "--readiness",
        default="",
        help="Optional auto_order_readiness.json path. Defaults to supervisor summary_out_dir.",
    )
    ap.add_argument("--out_dir", default="", help="Output directory. Defaults to summary_out_dir/auto_order_unblock.")
    ap.add_argument("--python", default=sys.executable, help="Python executable used in generated commands.")
    ap.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Run the generated no-submit commands after writing the plan.",
    )
    return ap


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return dict(payload) if isinstance(payload, dict) else {}


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _resolve(raw_path: str | Path) -> Path:
    return resolve_repo_path(BASE_DIR, str(raw_path or ""))


def _runtime_root_path(runtime_root: str | Path) -> Path:
    return _resolve(str(runtime_root or "runtime_data/paper_investment_only_duq152001"))


def _runtime_path(runtime_root: Path, raw_path: str | Path, default: str) -> Path:
    path = Path(str(raw_path or default))
    if path.is_absolute():
        return path.resolve()
    return (runtime_root / path).resolve()


def _repo_config_path(raw_path: str | Path) -> Path:
    return _resolve(str(raw_path or ""))


def _summary_dir(cfg: Dict[str, Any], runtime_root: Path) -> Path:
    raw_path = Path(str(cfg.get("summary_out_dir", "reports_supervisor") or "reports_supervisor"))
    if raw_path.is_absolute():
        return raw_path.resolve()
    if bool(cfg.get("scope_summary_out_dir", False)):
        return (runtime_root / raw_path).resolve()
    return _resolve(raw_path)


def _portfolio_id(market: str, item: Dict[str, Any]) -> str:
    explicit = str(item.get("portfolio_id") or "").strip()
    if explicit:
        return explicit
    watchlist = str(item.get("watchlist_yaml") or "").strip()
    return f"{market}:{Path(watchlist).stem}"


def _iter_investment_reports(cfg: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for market_cfg_raw in list(cfg.get("markets") or []):
        market_cfg = dict(market_cfg_raw or {})
        market = resolve_market_code(str(market_cfg.get("market") or market_cfg.get("name") or ""))
        if not market:
            continue
        for item_raw in list(market_cfg.get("reports") or []):
            item = dict(item_raw or {})
            if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                continue
            row = dict(item)
            row["_market_cfg"] = market_cfg
            row["market"] = market
            row["portfolio_id"] = _portfolio_id(market, item)
            yield row


def _match_investment_report(cfg: Dict[str, Any], market: str, portfolio_id: str) -> Dict[str, Any]:
    target_market = resolve_market_code(market)
    target_portfolio = str(portfolio_id or "").strip()
    for row in _iter_investment_reports(cfg):
        if row.get("market") == target_market and str(row.get("portfolio_id") or "") == target_portfolio:
            return row
    return {}


def _market_specific_config(item: Dict[str, Any], key: str, fallback_name: str) -> str:
    raw = str(item.get(key) or "").strip()
    if raw:
        return str(_repo_config_path(raw))
    market = str(item.get("market") or "").strip()
    return str((BASE_DIR / "config" / f"{fallback_name}_{market.lower()}.yaml").resolve())


def _ibkr_config_path(item: Dict[str, Any]) -> Path:
    market_cfg = dict(item.get("_market_cfg") or {})
    explicit = str(item.get("ibkr_config") or market_cfg.get("ibkr_config") or "").strip() or None
    return Path(market_config_path(BASE_DIR, str(item.get("market") or ""), explicit)).resolve()


def _command_spec(
    *,
    step: str,
    argv: Sequence[str],
    requires_gateway: bool = False,
    timeout_sec: float | None = None,
) -> Dict[str, Any]:
    command = [str(part) for part in argv]
    return {
        "step": step,
        "argv": command,
        "command": shlex.join(command),
        "requires_gateway": bool(requires_gateway),
        "submit_orders": False,
        "paper_only": True,
        "timeout_sec": float(timeout_sec or 0.0),
    }


def _report_command(item: Dict[str, Any], *, runtime_root: Path, python_executable: str) -> Dict[str, Any]:
    market = str(item.get("market") or "").strip()
    out_dir = _runtime_path(runtime_root, item.get("out_dir", "reports_investment"), "reports_investment")
    db_path = _runtime_path(runtime_root, item.get("db", "audit.db"), "audit.db")
    command: list[str] = [
        python_executable,
        "-m",
        "src.tools.generate_investment_report",
        "--out_dir",
        str(out_dir),
        "--watchlist_yaml",
        str(_repo_config_path(str(item.get("watchlist_yaml") or ""))),
        "--market",
        market,
        "--max_universe",
        str(int(item.get("max_universe", 1000) or 1000)),
        "--top_n",
        str(int(item.get("top_n", 15) or 15)),
        "--db",
        str(db_path),
        "--audit_limit",
        str(int(item.get("audit_limit", 500) or 500)),
        "--investment_config",
        _market_specific_config(item, "investment_config", "investment"),
        "--ibkr_config",
        str(_ibkr_config_path(item)),
    ]
    for key, arg in (
        ("request_timeout_sec", "--request_timeout_sec"),
        ("backtest_top_k", "--backtest_top_k"),
        ("fundamentals_top_k", "--fundamentals_top_k"),
    ):
        if item.get(key) is not None:
            command.extend([arg, str(item.get(key))])
    if bool(item.get("use_audit_recent", False)):
        command.append("--use_audit_recent")
    return _command_spec(
        step="refresh_investment_report",
        argv=command,
        requires_gateway=True,
        timeout_sec=float(item.get("timeout_sec", 1200) or 1200),
    )


def _execution_command(item: Dict[str, Any], *, runtime_root: Path, python_executable: str) -> Dict[str, Any]:
    market = str(item.get("market") or "").strip()
    out_dir = _runtime_path(runtime_root, item.get("out_dir", "reports_investment"), "reports_investment")
    db_path = _runtime_path(runtime_root, item.get("db", "audit.db"), "audit.db")
    command: list[str] = [
        python_executable,
        "-m",
        "src.tools.run_investment_execution",
        "--market",
        market,
        "--reports_root",
        str(out_dir),
        "--watchlist_yaml",
        str(_repo_config_path(str(item.get("watchlist_yaml") or ""))),
        "--portfolio_id",
        str(item.get("portfolio_id") or ""),
        "--db",
        str(db_path),
        "--execution_config",
        _market_specific_config(item, "execution_config", "investment_execution"),
        "--paper_config",
        _market_specific_config(item, "paper_config", "investment_paper"),
        "--ibkr_config",
        str(_ibkr_config_path(item)),
        "--request_timeout_sec",
        str(float(item.get("request_timeout_sec", 10.0) or 10.0)),
        "--recovery_evidence_only",
    ]
    return _command_spec(
        step="refresh_execution_no_submit",
        argv=command,
        requires_gateway=True,
        timeout_sec=float(item.get("execution_timeout_sec", 300) or 300),
    )


def _local_refresh_commands(
    *,
    config_path: Path,
    runtime_root: Path,
    summary_dir: Path,
    python_executable: str,
) -> list[Dict[str, Any]]:
    return [
        _command_spec(
            step="refresh_market_readiness",
            argv=[
                python_executable,
                "-m",
                "src.tools.review_market_readiness",
                "--config",
                str(config_path),
                "--runtime_root",
                str(runtime_root),
                "--out_dir",
                str(summary_dir),
            ],
        ),
        _command_spec(
            step="refresh_auto_order_readiness",
            argv=[
                python_executable,
                "-m",
                "src.tools.review_auto_order_readiness",
                "--config",
                str(config_path),
                "--runtime_root",
                str(runtime_root),
                "--out_dir",
                str(summary_dir),
            ],
        ),
        _command_spec(
            step="refresh_dashboard",
            argv=[
                python_executable,
                "-m",
                "src.tools.generate_dashboard",
                "--config",
                str(config_path),
                "--out_dir",
                str(summary_dir),
            ],
        ),
    ]


def _blocked_payload(
    *,
    reason: str,
    cfg_path: Path,
    runtime_root: Path,
    readiness_path: Path,
    out_dir: Path,
    apply_requested: bool,
) -> Dict[str, Any]:
    return {
        "generated_at": _utc_now(),
        "status": "blocked",
        "reason": reason,
        "apply_requested": bool(apply_requested),
        "submit_orders": False,
        "paper_only": True,
        "does_not_relax_submit_gates": True,
        "config_path": str(cfg_path),
        "runtime_root": str(runtime_root),
        "readiness_path": str(readiness_path),
        "out_dir": str(out_dir),
        "commands": [],
        "command_results": [],
    }


def build_auto_order_unblock_payload(
    *,
    config_path: str = "config/supervisor.yaml",
    runtime_root: str = "runtime_data/paper_investment_only_duq152001",
    readiness_path: str = "",
    out_dir: str = "",
    python_executable: str = sys.executable,
    apply_requested: bool = False,
) -> Dict[str, Any]:
    cfg_path = _resolve(config_path)
    runtime_root_path = _runtime_root_path(runtime_root)
    cfg = _load_yaml(cfg_path)
    summary_dir = _summary_dir(cfg, runtime_root_path)
    readiness = _load_json(_resolve(readiness_path) if readiness_path else summary_dir / "auto_order_readiness.json")
    effective_readiness_path = _resolve(readiness_path) if readiness_path else summary_dir / "auto_order_readiness.json"
    effective_out_dir = _resolve(out_dir) if out_dir else summary_dir / "auto_order_unblock"
    summary = dict(readiness.get("summary") or {})
    unblock = dict(summary.get("unblock_plan") or {})
    if not readiness:
        return _blocked_payload(
            reason="auto_order_readiness_missing",
            cfg_path=cfg_path,
            runtime_root=runtime_root_path,
            readiness_path=effective_readiness_path,
            out_dir=effective_out_dir,
            apply_requested=apply_requested,
        )
    if bool(unblock.get("submit_orders", False)):
        return _blocked_payload(
            reason="unsafe_unblock_plan_submit_orders_true",
            cfg_path=cfg_path,
            runtime_root=runtime_root_path,
            readiness_path=effective_readiness_path,
            out_dir=effective_out_dir,
            apply_requested=apply_requested,
        )
    if str(unblock.get("status") or "") != SAFE_UNBLOCK_STATUS:
        return _blocked_payload(
            reason="unsupported_unblock_plan_status",
            cfg_path=cfg_path,
            runtime_root=runtime_root_path,
            readiness_path=effective_readiness_path,
            out_dir=effective_out_dir,
            apply_requested=apply_requested,
        )
    if str(unblock.get("primary_action") or "") != SAFE_UNBLOCK_ACTION:
        return _blocked_payload(
            reason="unsupported_unblock_plan_action",
            cfg_path=cfg_path,
            runtime_root=runtime_root_path,
            readiness_path=effective_readiness_path,
            out_dir=effective_out_dir,
            apply_requested=apply_requested,
        )
    target_market = resolve_market_code(str(unblock.get("target_market") or ""))
    target_portfolio = str(unblock.get("target_portfolio_id") or "").strip()
    item = _match_investment_report(cfg, target_market, target_portfolio)
    if not item:
        return _blocked_payload(
            reason="target_portfolio_not_found_in_supervisor_config",
            cfg_path=cfg_path,
            runtime_root=runtime_root_path,
            readiness_path=effective_readiness_path,
            out_dir=effective_out_dir,
            apply_requested=apply_requested,
        )
    commands = [
        _report_command(item, runtime_root=runtime_root_path, python_executable=python_executable),
        _execution_command(item, runtime_root=runtime_root_path, python_executable=python_executable),
        *_local_refresh_commands(
            config_path=cfg_path,
            runtime_root=runtime_root_path,
            summary_dir=summary_dir,
            python_executable=python_executable,
        ),
    ]
    return {
        "generated_at": _utc_now(),
        "status": "ready" if not apply_requested else "ready_to_apply",
        "reason": "safe_no_submit_unblock_plan_built",
        "apply_requested": bool(apply_requested),
        "submit_orders": False,
        "paper_only": True,
        "does_not_relax_submit_gates": True,
        "config_path": str(cfg_path),
        "runtime_root": str(runtime_root_path),
        "readiness_path": str(effective_readiness_path),
        "out_dir": str(effective_out_dir),
        "target_market": target_market,
        "target_portfolio_id": target_portfolio,
        "target_symbols": str(unblock.get("target_symbols") or ""),
        "requires_ibkr_gateway": bool(unblock.get("requires_ibkr_gateway", False)),
        "request_policy": str(unblock.get("request_policy") or ""),
        "source_unblock_plan": unblock,
        "commands": commands,
        "command_results": [],
    }


def _run_command_specs(commands: Sequence[Dict[str, Any]]) -> list[Dict[str, Any]]:
    results: list[Dict[str, Any]] = []
    for spec in commands:
        argv = [str(part) for part in list(spec.get("argv") or [])]
        timeout = float(spec.get("timeout_sec", 0.0) or 0.0) or None
        started_at = _utc_now()
        try:
            completed = subprocess.run(
                argv,
                cwd=str(BASE_DIR),
                text=True,
                capture_output=True,
                timeout=timeout,
                check=False,
            )
            results.append(
                {
                    "step": spec.get("step"),
                    "started_at": started_at,
                    "finished_at": _utc_now(),
                    "returncode": int(completed.returncode),
                    "ok": completed.returncode == 0,
                    "stdout_tail": str(completed.stdout or "")[-4000:],
                    "stderr_tail": str(completed.stderr or "")[-4000:],
                }
            )
        except subprocess.TimeoutExpired as exc:
            results.append(
                {
                    "step": spec.get("step"),
                    "started_at": started_at,
                    "finished_at": _utc_now(),
                    "returncode": 124,
                    "ok": False,
                    "stdout_tail": str(exc.stdout or "")[-4000:],
                    "stderr_tail": str(exc.stderr or "")[-4000:],
                    "error": f"timeout_after_{timeout}_sec",
                }
            )
            break
        except Exception as exc:
            results.append(
                {
                    "step": spec.get("step"),
                    "started_at": started_at,
                    "finished_at": _utc_now(),
                    "returncode": 1,
                    "ok": False,
                    "stdout_tail": "",
                    "stderr_tail": str(exc),
                    "error": type(exc).__name__,
                }
            )
            break
        if results[-1]["returncode"] != 0:
            break
    return results


def _write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    lines = [
        "# Auto Order Unblock Plan",
        "",
        f"- Generated at: {payload.get('generated_at', '')}",
        f"- Status: {payload.get('status', '-')}",
        f"- Reason: {payload.get('reason', '-')}",
        f"- Apply requested: {int(bool(payload.get('apply_requested', False)))}",
        f"- Submit orders: {int(bool(payload.get('submit_orders', False)))}",
        f"- Target: {payload.get('target_market', '-')}/{payload.get('target_portfolio_id', '-')}",
        f"- Target symbols: {payload.get('target_symbols', '-')}",
        f"- Request policy: {payload.get('request_policy', '-')}",
        "",
        "## Commands",
        "",
    ]
    for idx, command in enumerate(list(payload.get("commands") or []), start=1):
        row = dict(command or {})
        lines.extend(
            [
                f"### {idx}. {row.get('step', '-')}",
                "",
                f"- Requires Gateway: {int(bool(row.get('requires_gateway', False)))}",
                f"- Submit orders: {int(bool(row.get('submit_orders', False)))}",
                "",
                "```bash",
                str(row.get("command") or ""),
                "```",
                "",
            ]
        )
    results = [dict(row) for row in list(payload.get("command_results") or []) if isinstance(row, dict)]
    if results:
        lines.extend(["## Command Results", "", "| step | returncode | ok | error |", "| --- | ---: | ---: | --- |"])
        for row in results:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("step") or "-"),
                        str(int(row.get("returncode", 0) or 0)),
                        str(int(bool(row.get("ok", False)))),
                        str(row.get("error") or "-"),
                    ]
                )
                + " |"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    payload = build_auto_order_unblock_payload(
        config_path=str(args.config),
        runtime_root=str(args.runtime_root),
        readiness_path=str(args.readiness or ""),
        out_dir=str(args.out_dir or ""),
        python_executable=str(args.python or sys.executable),
        apply_requested=bool(args.apply),
    )
    out_dir = Path(str(payload.get("out_dir") or "")).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    if bool(args.apply) and payload.get("status") in {"ready", "ready_to_apply"}:
        payload["command_results"] = _run_command_specs(
            [dict(row) for row in list(payload.get("commands") or []) if isinstance(row, dict)]
        )
        payload["status"] = (
            "applied" if payload["command_results"] and all(row.get("ok") for row in payload["command_results"]) else "apply_failed"
        )
    json_path = out_dir / "auto_order_unblock_plan.json"
    md_path = out_dir / "auto_order_unblock_plan.md"
    write_json(str(json_path), payload)
    _write_markdown(md_path, payload)
    emit_cli_summary(
        command="ibkr-quant-auto-order-unblock",
        headline="auto-order unblock plan complete",
        summary={
            "status": payload.get("status", ""),
            "reason": payload.get("reason", ""),
            "target_market": payload.get("target_market", ""),
            "target_portfolio_id": payload.get("target_portfolio_id", ""),
            "submit_orders": bool(payload.get("submit_orders", False)),
            "command_count": len(list(payload.get("commands") or [])),
        },
        artifacts={"summary_json": json_path, "markdown": md_path},
    )


if __name__ == "__main__":
    main()
