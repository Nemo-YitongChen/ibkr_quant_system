from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

from ..app.investment_guard import InvestmentGuardConfig, InvestmentGuardEngine
from ..common.logger import get_logger
from ..common.markets import add_market_args, market_config_path, resolve_market_code
from ..common.storage import Storage
from ..offhours.ib_setup import connect_ib
from ..portfolio.investment_allocator import InvestmentExecutionConfig

log = get_logger("tools.run_investment_guard")
BASE_DIR = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run defensive investment guard checks and optional protective broker orders.")
    add_market_args(ap)
    ap.add_argument("--ibkr_config", default="", help="Path to the IBKR runtime config yaml.")
    ap.add_argument("--execution_config", default="", help="Path to the investment execution config yaml.")
    ap.add_argument("--guard_config", default="", help="Path to the investment guard config yaml.")
    ap.add_argument("--db", default="audit.db")
    ap.add_argument("--report_dir", default="", help="Explicit report directory used for output artifacts.")
    ap.add_argument("--reports_root", default="reports_investment", help="Root directory used by investment reports.")
    ap.add_argument("--watchlist_yaml", default="", help="Use the same watchlist stem as the report generator.")
    ap.add_argument("--portfolio_id", default="", help="Stable identifier for one investment guard portfolio.")
    ap.add_argument("--submit", action="store_true", default=False, help="Actually submit protective broker orders.")
    ap.add_argument("--request_timeout_sec", type=float, default=10.0)
    return ap.parse_args()


def _resolve_project_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    for candidate in (BASE_DIR / path, BASE_DIR / "config" / path, Path.cwd() / path, Path.cwd() / "config" / path):
        if candidate.exists():
            return candidate.resolve()
    return (BASE_DIR / path).resolve()


def _load_yaml(path_str: str) -> Dict[str, Any]:
    import yaml

    with _resolve_project_path(path_str).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _slugify_report_name(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "_" for ch in (name or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_") or "default"


def _infer_report_dir(args: argparse.Namespace, market: str) -> Path:
    if args.report_dir:
        return _resolve_project_path(args.report_dir)
    root = _resolve_project_path(args.reports_root)
    if args.watchlist_yaml:
        return root / _slugify_report_name(Path(str(args.watchlist_yaml)).stem)
    return root / f"market_{str(market or 'default').lower()}"


def main() -> None:
    args = parse_args()
    market = resolve_market_code(getattr(args, "market", ""))
    if not market:
        raise SystemExit("--market is required")

    ibkr_cfg_path = str(market_config_path(BASE_DIR, market, args.ibkr_config or None))
    ibkr_cfg = _load_yaml(ibkr_cfg_path)
    execution_cfg_path = str(
        _resolve_project_path(
            args.execution_config or str(ibkr_cfg.get("investment_execution_config", f"config/investment_execution_{market.lower()}.yaml"))
        )
    )
    guard_cfg_path = str(
        _resolve_project_path(
            args.guard_config or str(ibkr_cfg.get("investment_guard_config", f"config/investment_guard_{market.lower()}.yaml"))
        )
    )
    execution_cfg = InvestmentExecutionConfig.from_dict(_load_yaml(execution_cfg_path).get("execution"))
    guard_cfg = InvestmentGuardConfig.from_dict(_load_yaml(guard_cfg_path).get("guard"))

    report_dir = _infer_report_dir(args, market)
    portfolio_id = str(args.portfolio_id or f"{market}:{report_dir.name}")
    storage = Storage(str(_resolve_project_path(args.db)))
    ib = connect_ib(
        str(ibkr_cfg["host"]),
        int(ibkr_cfg["port"]),
        int(ibkr_cfg["client_id"]),
        request_timeout=float(args.request_timeout_sec),
    )
    try:
        engine = InvestmentGuardEngine(
            ib=ib,
            account_id=str(ibkr_cfg["account_id"]),
            storage=storage,
            market=market,
            portfolio_id=portfolio_id,
            execution_cfg=execution_cfg,
            guard_cfg=guard_cfg,
        )
        result = engine.run(report_dir=str(report_dir), submit=bool(args.submit))
        print(
            f"market={result.market} portfolio={result.portfolio_id} submitted={int(result.submitted)} "
            f"orders={result.order_count} stop_count={result.stop_count} take_profit_count={result.take_profit_count}"
        )
        print(f"summary_json={report_dir / 'investment_guard_summary.json'}")
        print(f"plan_csv={report_dir / 'investment_guard_plan.csv'}")
        print(f"markdown={report_dir / 'investment_guard_report.md'}")
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
