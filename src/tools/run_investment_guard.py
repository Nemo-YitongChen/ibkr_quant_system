from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

from ..app.investment_guard import InvestmentGuardConfig, InvestmentGuardEngine
from ..common.adaptive_strategy import load_adaptive_strategy
from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.logger import get_logger
from ..common.market_structure import load_market_structure
from ..common.markets import add_market_args, market_config_path, resolve_market_code
from ..common.runtime_paths import resolve_repo_path
from ..common.storage import Storage
from ..offhours.ib_setup import connect_ib
from ..portfolio.investment_allocator import InvestmentExecutionConfig

log = get_logger("tools.run_investment_guard")
BASE_DIR = Path(__file__).resolve().parents[2]


def build_parser() -> argparse.ArgumentParser:
    ap = build_cli_parser(
        description="Run defensive investment guard checks and optional protective broker orders.",
        command="ibkr-quant-guard",
        examples=[
            "ibkr-quant-guard --market HK --submit",
            "ibkr-quant-guard --market US --report_dir reports_investment_us/market_us",
        ],
        notes=[
            "Writes investment guard summary JSON, plan CSV, and investment_guard_report.md in the report directory.",
        ],
    )
    add_market_args(ap)
    ap.add_argument("--ibkr_config", default="", help="Path to the IBKR runtime config yaml.")
    ap.add_argument("--execution_config", default="", help="Path to the investment execution config yaml.")
    ap.add_argument("--guard_config", default="", help="Path to the investment guard config yaml.")
    ap.add_argument("--market_structure_config", default="", help="Path to market structure constraint yaml.")
    ap.add_argument("--adaptive_strategy_config", default="", help="Path to adaptive strategy framework yaml.")
    ap.add_argument("--db", default="audit.db", help="SQLite audit database used for guard state and broker snapshots.")
    ap.add_argument("--report_dir", default="", help="Explicit report directory used for output artifacts.")
    ap.add_argument("--reports_root", default="reports_investment", help="Root directory used by investment reports.")
    ap.add_argument("--watchlist_yaml", default="", help="Use the same watchlist stem as the report generator.")
    ap.add_argument("--portfolio_id", default="", help="Stable identifier for one investment guard portfolio.")
    ap.add_argument("--submit", action="store_true", default=False, help="Actually submit protective broker orders.")
    ap.add_argument("--request_timeout_sec", type=float, default=10.0, help="IBKR request timeout in seconds.")
    return ap


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _resolve_project_path(path_str: str) -> Path:
    return resolve_repo_path(BASE_DIR, path_str)


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


def _cli_summary_payload(result: Any, report_dir: Path) -> tuple[Dict[str, Any], Dict[str, Path]]:
    return (
        {
            "market": str(getattr(result, "market", "") or "DEFAULT"),
            "portfolio_id": str(getattr(result, "portfolio_id", "") or "-"),
            "submitted": bool(getattr(result, "submitted", False)),
            "order_count": int(getattr(result, "order_count", 0) or 0),
            "stop_count": int(getattr(result, "stop_count", 0) or 0),
            "take_profit_count": int(getattr(result, "take_profit_count", 0) or 0),
            "market_rules": str(getattr(result, "market_rules", "") or "-"),
        },
        {
            "summary_json": report_dir / "investment_guard_summary.json",
            "plan_csv": report_dir / "investment_guard_plan.csv",
            "report_md": report_dir / "investment_guard_report.md",
        },
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
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
    market_structure = load_market_structure(
        BASE_DIR,
        market,
        str(
            args.market_structure_config
            or ibkr_cfg.get("market_structure_config", f"config/market_structure_{market.lower()}.yaml")
        ),
    )
    adaptive_strategy = load_adaptive_strategy(
        BASE_DIR,
        str(
            args.adaptive_strategy_config
            or ibkr_cfg.get("adaptive_strategy_config", "config/adaptive_strategy_framework.yaml")
        ),
    )
    if not str(execution_cfg.lot_size_file or "").strip():
        execution_cfg.lot_size = max(int(execution_cfg.lot_size or 1), int(market_structure.order_rules.buy_lot_multiple or 1))

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
            market_structure=market_structure,
            adaptive_strategy=adaptive_strategy,
        )
        result = engine.run(report_dir=str(report_dir), submit=bool(args.submit))
        summary_fields, artifact_fields = _cli_summary_payload(result, report_dir)
        emit_cli_summary(
            command="ibkr-quant-guard",
            headline="investment guard run complete",
            summary=summary_fields,
            artifacts=artifact_fields,
        )
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
