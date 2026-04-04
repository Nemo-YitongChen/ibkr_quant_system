from __future__ import annotations

import argparse
import json
import socket
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ..common.logger import get_logger
from ..common.markets import market_config_path, resolve_market_code
from ..common.runtime_paths import resolve_repo_path, scope_from_ibkr_config

BASE_DIR = Path(__file__).resolve().parents[2]
log = get_logger("tools.preflight_supervisor")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run lightweight preflight checks for supervisor paper/live deployment.")
    ap.add_argument("--config", default="config/supervisor.yaml", help="Supervisor config path.")
    ap.add_argument("--runtime_root", default="", help="Optional scoped runtime root such as runtime_data/paper_... .")
    ap.add_argument("--out_dir", default="reports_preflight", help="Directory to write preflight summary files.")
    return ap.parse_args()


def _load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _status_for_exists(path: Path, *, should_exist: bool = True) -> str:
    exists = path.exists()
    if should_exist and exists:
        return "PASS"
    if should_exist and not exists:
        return "FAIL"
    return "PASS" if exists else "WARN"


def _make_check(name: str, status: str, detail: str, **extra: Any) -> Dict[str, Any]:
    row = {
        "name": str(name or "").strip(),
        "status": str(status or "").strip().upper() or "WARN",
        "detail": str(detail or "").strip(),
    }
    row.update(extra)
    return row


def _probe_port(host: str, port: int, timeout_sec: float = 0.25) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout_sec)
    try:
        if sock.connect_ex((host, int(port))) == 0:
            return True
    except Exception:
        pass
    finally:
        sock.close()
    # sandbox 内偶尔会把本地 socket connect 误判成失败，这里再用 lsof 确认一次是否真的在监听。
    try:
        proc = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{int(port)}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            timeout=1.0,
            check=False,
        )
    except Exception:
        return False
    return proc.returncode == 0 and bool(str(proc.stdout or "").strip())


def _resolve_dashboard_db_path(cfg: Dict[str, Any], runtime_root: Path | None) -> Path:
    dashboard_db = str(cfg.get("dashboard_db", "audit.db") or "audit.db")
    if runtime_root is not None and not Path(dashboard_db).is_absolute():
        return (runtime_root / dashboard_db).resolve()
    return resolve_repo_path(BASE_DIR, dashboard_db)


def _resolve_summary_out_dir(cfg: Dict[str, Any], runtime_root: Path | None) -> Path:
    summary_out_dir = str(cfg.get("summary_out_dir", "reports_supervisor") or "reports_supervisor")
    if runtime_root is not None and bool(cfg.get("scope_summary_out_dir", False)) and not Path(summary_out_dir).is_absolute():
        return (runtime_root / summary_out_dir).resolve()
    return resolve_repo_path(BASE_DIR, summary_out_dir)


def _report_items(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for market_cfg in list(cfg.get("markets", []) or []):
        market_cfg_dict = dict(market_cfg)
        market = str(market_cfg_dict.get("market") or market_cfg_dict.get("name") or "").strip().upper()
        for item in list(dict(market_cfg).get("reports", []) or []):
            row = dict(item)
            row["market"] = market
            row["market_ibkr_config"] = str(market_cfg_dict.get("ibkr_config") or "").strip()
            out.append(row)
    return out


def _resolve_ibkr_config_path(item: Dict[str, Any]) -> Path:
    market = resolve_market_code(str(item.get("market") or "").strip())
    override = str(item.get("ibkr_config") or item.get("market_ibkr_config") or "").strip() or None
    return Path(market_config_path(BASE_DIR, market, override)).resolve()


def _build_report_checks(cfg: Dict[str, Any], runtime_root: Path | None) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    for item in _report_items(cfg):
        market = str(item.get("market") or "").strip().upper()
        watchlist_yaml = str(item.get("watchlist_yaml") or "").strip()
        out_dir = str(item.get("out_dir") or "").strip()
        label = f"{market}:{Path(watchlist_yaml).stem or Path(out_dir).name or 'report'}"
        if watchlist_yaml:
            watchlist_path = resolve_repo_path(BASE_DIR, watchlist_yaml)
            checks.append(
                _make_check(
                    f"{label}:watchlist",
                    _status_for_exists(watchlist_path),
                    f"watchlist_yaml={watchlist_path}",
                    path=str(watchlist_path),
                )
            )
        if out_dir:
            report_root = resolve_repo_path(BASE_DIR, out_dir)
            checks.append(
                _make_check(
                    f"{label}:out_dir",
                    "PASS" if report_root.parent.exists() else "FAIL",
                    f"report_root={report_root}",
                    path=str(report_root),
                )
            )
        ibkr_config_path = _resolve_ibkr_config_path(item)
        if market:
            status = _status_for_exists(ibkr_config_path)
            detail = f"ibkr_config={ibkr_config_path}"
            scope_root = ""
            if ibkr_config_path.exists():
                ibkr_cfg = _load_yaml(ibkr_config_path)
                scope_root = str(scope_from_ibkr_config(ibkr_cfg).root(BASE_DIR))
                detail = f"{detail} scope_root={scope_root}"
            checks.append(
                _make_check(
                    f"{label}:ibkr_config",
                    status,
                    detail,
                    path=str(ibkr_config_path),
                    scope_root=scope_root,
                )
            )
    if runtime_root is not None:
        checks.append(
            _make_check(
                "runtime_root",
                _status_for_exists(runtime_root),
                f"runtime_root={runtime_root}",
                path=str(runtime_root),
            )
        )
    return checks


def _build_core_checks(cfg: Dict[str, Any], config_path: Path, runtime_root: Path | None) -> List[Dict[str, Any]]:
    summary_out_dir = _resolve_summary_out_dir(cfg, runtime_root)
    weekly_review_dir = resolve_repo_path(BASE_DIR, str(cfg.get("dashboard_weekly_review_dir", "reports_investment_weekly") or "reports_investment_weekly"))
    execution_kpi_dir = resolve_repo_path(BASE_DIR, str(cfg.get("dashboard_execution_kpi_dir", "reports_investment_execution") or "reports_investment_execution"))
    dashboard_db = _resolve_dashboard_db_path(cfg, runtime_root)
    dashboard_control_state = summary_out_dir / "dashboard_control_state.json"

    checks = [
        _make_check("config", _status_for_exists(config_path), f"config={config_path}", path=str(config_path)),
        _make_check("summary_out_dir", "PASS" if summary_out_dir.parent.exists() else "FAIL", f"summary_out_dir={summary_out_dir}", path=str(summary_out_dir)),
        _make_check("dashboard_weekly_review_dir", _status_for_exists(weekly_review_dir, should_exist=False), f"weekly_review_dir={weekly_review_dir}", path=str(weekly_review_dir)),
        _make_check("dashboard_execution_kpi_dir", _status_for_exists(execution_kpi_dir, should_exist=False), f"execution_kpi_dir={execution_kpi_dir}", path=str(execution_kpi_dir)),
        _make_check("dashboard_db", _status_for_exists(dashboard_db), f"dashboard_db={dashboard_db}", path=str(dashboard_db)),
        _make_check(
            "dashboard_control_state",
            _status_for_exists(dashboard_control_state, should_exist=False),
            f"dashboard_control_state={dashboard_control_state}",
            path=str(dashboard_control_state),
        ),
    ]
    return checks


def _configured_ibkr_endpoints(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows_by_key: Dict[tuple[str, int], Dict[str, Any]] = {}
    for item in _report_items(cfg):
        market = str(item.get("market") or "").strip().upper()
        if not market:
            continue
        ibkr_config_path = _resolve_ibkr_config_path(item)
        if not ibkr_config_path.exists():
            continue
        ibkr_cfg = _load_yaml(ibkr_config_path)
        host = str(ibkr_cfg.get("host", "127.0.0.1") or "127.0.0.1").strip() or "127.0.0.1"
        try:
            port = int(ibkr_cfg.get("port", 4002) or 4002)
        except Exception:
            port = 4002
        key = (host, port)
        row = rows_by_key.get(key)
        if row is None:
            row = {
                "markets": [],
                "host": host,
                "port": port,
                "ibkr_config_paths": [],
            }
            rows_by_key[key] = row
        if market not in row["markets"]:
            row["markets"].append(market)
        config_path_text = str(ibkr_config_path)
        if config_path_text not in row["ibkr_config_paths"]:
            row["ibkr_config_paths"].append(config_path_text)
    return list(rows_by_key.values())


def _build_port_checks(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    endpoints = _configured_ibkr_endpoints(cfg)
    if not endpoints:
        endpoints = [{"host": "127.0.0.1", "port": port, "market": "", "ibkr_config_path": ""} for port in (4001, 4002)]
    for endpoint in endpoints:
        host = str(endpoint.get("host", "127.0.0.1") or "127.0.0.1").strip() or "127.0.0.1"
        port = int(endpoint.get("port", 4002) or 4002)
        listening = _probe_port(host, port)
        markets = [str(value).strip().upper() for value in list(endpoint.get("markets", []) or []) if str(value).strip()]
        config_paths = [str(value).strip() for value in list(endpoint.get("ibkr_config_paths", []) or []) if str(value).strip()]
        detail = f"{host}:{port} {'listening' if listening else 'not_listening'}"
        if markets:
            detail = f"{detail} markets={','.join(markets)}"
        if config_paths:
            detail = f"{detail} ibkr_config={','.join(config_paths)}"
        checks.append(
            _make_check(
                f"ibkr_port:{host}:{port}",
                "PASS" if listening else "WARN",
                detail,
                host=host,
                port=port,
                markets=markets,
                ibkr_config_paths=config_paths,
            )
        )
    return checks


def _render_markdown(summary: Dict[str, Any]) -> str:
    lines = [
        "# Supervisor Preflight",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- config: {summary['config_path']}",
        f"- runtime_root: {summary['runtime_root'] or '-'}",
        f"- pass: {summary['pass_count']}",
        f"- warn: {summary['warn_count']}",
        f"- fail: {summary['fail_count']}",
        "",
        "## Checks",
        "",
    ]
    for row in list(summary.get("checks", []) or []):
        lines.append(f"- [{row.get('status', 'WARN')}] {row.get('name', '')}: {row.get('detail', '')}")
    return "\n".join(lines) + "\n"


def run_preflight(config_path: str, runtime_root: str = "", out_dir: str = "reports_preflight") -> Dict[str, Any]:
    resolved_config = resolve_repo_path(BASE_DIR, config_path)
    cfg = _load_yaml(resolved_config)
    resolved_runtime_root = resolve_repo_path(BASE_DIR, runtime_root) if str(runtime_root or "").strip() else None
    out_path = resolve_repo_path(BASE_DIR, out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    checks = []
    checks.extend(_build_core_checks(cfg, resolved_config, resolved_runtime_root))
    checks.extend(_build_report_checks(cfg, resolved_runtime_root))
    checks.extend(_build_port_checks(cfg))

    pass_count = sum(1 for row in checks if str(row.get("status") or "") == "PASS")
    warn_count = sum(1 for row in checks if str(row.get("status") or "") == "WARN")
    fail_count = sum(1 for row in checks if str(row.get("status") or "") == "FAIL")
    summary = {
        "generated_at": datetime.now().isoformat(),
        "config_path": str(resolved_config),
        "runtime_root": str(resolved_runtime_root) if resolved_runtime_root is not None else "",
        "pass_count": int(pass_count),
        "warn_count": int(warn_count),
        "fail_count": int(fail_count),
        "checks": checks,
    }
    (out_path / "supervisor_preflight_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out_path / "supervisor_preflight_report.md").write_text(
        _render_markdown(summary),
        encoding="utf-8",
    )
    log.info("Wrote supervisor preflight -> %s fails=%s warns=%s", out_path, fail_count, warn_count)
    return summary


def main() -> None:
    args = parse_args()
    run_preflight(args.config, runtime_root=args.runtime_root, out_dir=args.out_dir)


if __name__ == "__main__":
    main()
