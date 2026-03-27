from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

from ..analysis.investment_portfolio import (
    InvestmentPaperConfig,
    build_target_allocations,
    is_rebalance_due,
    simulate_rebalance,
)
from ..analysis.report import write_csv, write_json
from ..common.logger import get_logger
from ..common.markets import add_market_args, resolve_market_code
from ..common.storage import Storage, build_investment_risk_history_row

log = get_logger("tools.run_investment_paper")
BASE_DIR = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run the investment paper ledger and periodic rebalance.")
    add_market_args(ap)
    ap.add_argument("--db", default="audit.db")
    ap.add_argument("--report_dir", default="", help="Explicit report directory that contains investment_candidates.csv.")
    ap.add_argument("--reports_root", default="reports_investment", help="Root directory used by investment reports.")
    ap.add_argument("--watchlist_yaml", default="", help="Use the same watchlist stem as the report generator.")
    ap.add_argument("--paper_config", default="", help="Path to investment paper config yaml.")
    ap.add_argument("--portfolio_id", default="", help="Optional stable identifier for one investment paper portfolio.")
    ap.add_argument("--force", action="store_true", default=False)
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


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    return [dict(row) for row in rows]


def _read_report_books(report_dir: Path) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    candidates = _read_csv(report_dir / "investment_candidates.csv")
    candidates.extend(_read_csv(report_dir / "investment_short_candidates.csv"))
    plans = _read_csv(report_dir / "investment_plan.csv")
    plans.extend(_read_csv(report_dir / "investment_short_plan.csv"))
    return candidates, plans


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _write_md(path: Path, summary: Dict[str, Any], trades: List[Dict[str, Any]], positions: List[Dict[str, Any]]) -> None:
    risk_label = str(summary.get("risk_stress_worst_scenario_label", "") or "-")
    lines = [
        "# Investment Paper Report",
        "",
        f"- Generated: {summary.get('ts', '')}",
        f"- Market: {summary.get('market', '')}",
        f"- Portfolio: {summary.get('portfolio_id', '')}",
        f"- Rebalance due: {summary.get('rebalance_due', False)}",
        f"- Executed: {summary.get('executed', False)}",
        f"- Cash after: {float(summary.get('cash_after', 0.0) or 0.0):.2f}",
        f"- Equity after: {float(summary.get('equity_after', 0.0) or 0.0):.2f}",
        f"- Target invested weight: {float(summary.get('target_invested_weight', 0.0) or 0.0):.3f}",
        f"- Dynamic net exposure cap: {float(summary.get('risk_dynamic_net_exposure', 0.0) or 0.0):.3f}",
        f"- Dynamic gross exposure cap: {float(summary.get('risk_dynamic_gross_exposure', 0.0) or 0.0):.3f}",
        f"- Avg pair correlation: {float(summary.get('risk_avg_pair_correlation', 0.0) or 0.0):.2f}",
        f"- Worst stress: {risk_label} loss={float(summary.get('risk_stress_worst_loss', 0.0) or 0.0):.3f}",
        "",
        "## Trades",
    ]
    if not trades:
        lines.append("- (no trades)")
    else:
        for trade in trades:
            lines.append(
                f"- {trade['action']} {trade['symbol']} qty={float(trade['qty']):.0f} price={float(trade['price']):.2f} "
                f"value={float(trade['trade_value']):.2f} reason={trade['reason']}"
            )
    lines.append("")
    lines.append("## Portfolio")
    if not positions:
        lines.append("- (no positions)")
    else:
        for pos in positions:
            lines.append(
                f"- {pos['symbol']} qty={float(pos['qty']):.0f} last={float(pos['last_price']):.2f} "
                f"mv={float(pos['market_value']):.2f} weight={float(pos.get('weight', 0.0) or 0.0):.3f}"
            )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    market = resolve_market_code(getattr(args, "market", ""))
    if not market:
        raise SystemExit("--market is required for investment paper runs")

    default_paper_cfg = f"config/investment_paper_{market.lower()}.yaml"
    paper_cfg = InvestmentPaperConfig.from_dict(_load_yaml(args.paper_config or default_paper_cfg).get("paper"))
    report_dir = _infer_report_dir(args, market)
    portfolio_id = str(args.portfolio_id or f"{market}:{report_dir.name}")
    candidates, plans = _read_report_books(report_dir)
    if not candidates or not plans:
        raise SystemExit(f"investment report files not found or empty under {report_dir}")

    price_map = {
        str(row["symbol"]).upper(): _to_float(row.get("last_close", 0.0))
        for row in candidates
        if str(row.get("symbol", "")).strip()
    }

    storage = Storage(str(_resolve_project_path(args.db)))
    last_run = storage.get_latest_investment_run(market, portfolio_id=portfolio_id)
    positions = storage.get_latest_investment_positions(market, portfolio_id=portfolio_id)
    cash_before = _to_float(last_run.get("cash_after"), paper_cfg.initial_cash) if last_run else float(paper_cfg.initial_cash)
    equity_before = cash_before + sum(
        _to_float(pos.get("qty")) * _to_float(price_map.get(sym, pos.get("last_price", pos.get("cost_basis", 0.0))))
        for sym, pos in positions.items()
    )

    now = datetime.now(timezone.utc)
    rebalance_due = is_rebalance_due(
        str(last_run.get("ts", "") or "") if last_run else "",
        now,
        frequency=str(paper_cfg.rebalance_frequency),
        rebalance_weekday=int(paper_cfg.rebalance_weekday),
        force=bool(args.force),
    )
    target_weights, risk_overlay = build_target_allocations(candidates, plans, cfg=paper_cfg, return_details=True)

    executed = False
    trades: List[Dict[str, Any]] = []
    if rebalance_due:
        positions, trades, cash_after, equity_after = simulate_rebalance(
            positions,
            cash=cash_before,
            price_map=price_map,
            target_weights=target_weights,
            cfg=paper_cfg,
        )
        executed = True
    else:
        cash_after = cash_before
        equity_after = equity_before

    run_id = f"{market}-{now.strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    details = {
        "report_dir": str(report_dir),
        "target_weights": target_weights,
        "position_count": int(sum(1 for pos in positions.values() if abs(_to_float(pos.get("qty"))) > 1e-9)),
        "risk_overlay": risk_overlay,
    }
    storage.insert_investment_run(
        {
            "run_id": run_id,
            "market": market,
            "portfolio_id": portfolio_id,
            "report_dir": str(report_dir),
            "rebalance_due": int(rebalance_due),
            "executed": int(executed),
            "cash_before": float(cash_before),
            "cash_after": float(cash_after),
            "equity_before": float(equity_before),
            "equity_after": float(equity_after),
            "details": json.dumps(details, ensure_ascii=False),
        }
    )

    position_rows: List[Dict[str, Any]] = []
    for symbol, pos in positions.items():
        qty = _to_float(pos.get("qty"))
        if abs(qty) <= 1e-9:
            continue
        last_price = _to_float(price_map.get(symbol, pos.get("last_price", pos.get("cost_basis", 0.0))))
        market_value = qty * last_price
        weight = (market_value / equity_after) if equity_after > 0 else 0.0
        row = {
            "symbol": symbol,
            "qty": qty,
            "cost_basis": _to_float(pos.get("cost_basis", last_price)),
            "last_price": last_price,
            "market_value": market_value,
            "weight": weight,
            "status": "OPEN",
        }
        position_rows.append(row)
        storage.insert_investment_position(
            {
                "run_id": run_id,
                "market": market,
                "portfolio_id": portfolio_id,
                **row,
                "details": json.dumps({"report_dir": str(report_dir)}, ensure_ascii=False),
            }
        )

    for trade in trades:
        storage.insert_investment_trade(
            {
                "run_id": run_id,
                "market": market,
                "portfolio_id": portfolio_id,
                **trade,
                "details": json.dumps({"report_dir": str(report_dir)}, ensure_ascii=False),
            }
        )

    summary = {
        "ts": now.isoformat(),
        "market": market,
        "portfolio_id": portfolio_id,
        "report_dir": str(report_dir),
        "rebalance_due": bool(rebalance_due),
        "executed": bool(executed),
        "cash_before": float(cash_before),
        "cash_after": float(cash_after),
        "equity_before": float(equity_before),
        "equity_after": float(equity_after),
        "run_id": run_id,
        "target_weights": target_weights,
        "target_invested_weight": float(sum(abs(float(v)) for v in target_weights.values())),
        "target_net_weight": float(sum(float(v) for v in target_weights.values())),
        "risk_overlay_enabled": bool(risk_overlay.get("enabled", False)),
        "risk_dynamic_scale": float(risk_overlay.get("dynamic_scale", 1.0) or 1.0),
        "risk_dynamic_net_exposure": float(risk_overlay.get("dynamic_net_exposure", 0.0) or 0.0),
        "risk_dynamic_gross_exposure": float(risk_overlay.get("dynamic_gross_exposure", 0.0) or 0.0),
        "risk_dynamic_short_exposure": float(risk_overlay.get("dynamic_short_exposure", 0.0) or 0.0),
        "risk_applied_net_exposure": float(risk_overlay.get("applied_net_exposure", 0.0) or 0.0),
        "risk_applied_gross_exposure": float(risk_overlay.get("applied_gross_exposure", 0.0) or 0.0),
        "risk_avg_pair_correlation": float(risk_overlay.get("final_avg_pair_correlation", risk_overlay.get("avg_pair_correlation", 0.0)) or 0.0),
        "risk_max_pair_correlation": float(risk_overlay.get("final_max_pair_correlation", risk_overlay.get("max_pair_correlation", 0.0)) or 0.0),
        "risk_stress_index_drop_loss": float(
            dict(risk_overlay.get("final_stress_scenarios", {}) or risk_overlay.get("stress_scenarios", {})).get("index_drop", {}).get("loss", 0.0) or 0.0
        ),
        "risk_stress_volatility_spike_loss": float(
            dict(risk_overlay.get("final_stress_scenarios", {}) or risk_overlay.get("stress_scenarios", {})).get("volatility_spike", {}).get("loss", 0.0) or 0.0
        ),
        "risk_stress_liquidity_shock_loss": float(
            dict(risk_overlay.get("final_stress_scenarios", {}) or risk_overlay.get("stress_scenarios", {})).get("liquidity_shock", {}).get("loss", 0.0) or 0.0
        ),
        "risk_stress_worst_loss": float(risk_overlay.get("final_stress_worst_loss", risk_overlay.get("stress_worst_loss", 0.0)) or 0.0),
        "risk_stress_worst_scenario": str(risk_overlay.get("final_stress_worst_scenario", risk_overlay.get("stress_worst_scenario", "")) or ""),
        "risk_stress_worst_scenario_label": str(
            risk_overlay.get("final_stress_worst_scenario_label", risk_overlay.get("stress_worst_scenario_label", "")) or ""
        ),
        "risk_returns_based_enabled": bool(risk_overlay.get("final_returns_based_enabled", risk_overlay.get("returns_based_enabled", False))),
        "risk_returns_based_symbol_count": int(risk_overlay.get("final_returns_based_symbol_count", risk_overlay.get("returns_based_symbol_count", 0)) or 0),
        "risk_returns_based_sample_size": int(risk_overlay.get("final_returns_based_sample_size", risk_overlay.get("returns_based_sample_size", 0)) or 0),
        "risk_returns_based_var_95_1d": float(
            risk_overlay.get("final_returns_based_var_95_1d", risk_overlay.get("returns_based_var_95_1d", 0.0)) or 0.0
        ),
        "risk_returns_based_portfolio_vol_1d": float(
            risk_overlay.get("final_returns_based_portfolio_vol_1d", risk_overlay.get("returns_based_portfolio_vol_1d", 0.0)) or 0.0
        ),
        "risk_returns_based_downside_vol_1d": float(
            risk_overlay.get("final_returns_based_downside_vol_1d", risk_overlay.get("returns_based_downside_vol_1d", 0.0)) or 0.0
        ),
        "risk_correlation_source": str(risk_overlay.get("correlation_source", "") or ""),
        "risk_top_sector_share": float(risk_overlay.get("top_sector_share", 0.0) or 0.0),
        "risk_notes": list(risk_overlay.get("notes", []) or []),
        "risk_correlation_reduced_symbols": list(risk_overlay.get("correlation_reduced_symbols", []) or []),
    }
    # 风险轨迹单独落一份规范化记录，后续 dashboard / 周报优先读这里，
    # 不必每次再从 investment_runs.details 里回推。
    storage.insert_investment_risk_history(
        build_investment_risk_history_row(
            run_id=run_id,
            ts=summary["ts"],
            market=market,
            portfolio_id=portfolio_id,
            source_kind="paper",
            source_label="Dry Run",
            report_dir=str(report_dir),
            risk_overlay=risk_overlay,
            details={
                "rebalance_due": bool(rebalance_due),
                "executed": bool(executed),
                "equity_after": float(equity_after),
            },
        )
    )

    write_csv(str(report_dir / "investment_portfolio.csv"), position_rows)
    write_csv(str(report_dir / "investment_rebalance_trades.csv"), trades)
    write_json(str(report_dir / "investment_paper_summary.json"), summary)
    _write_md(report_dir / "investment_paper_report.md", summary, trades, position_rows)
    log.info(
        "Wrote investment paper -> %s executed=%s trades=%s positions=%s",
        report_dir / "investment_paper_report.md",
        executed,
        len(trades),
        len(position_rows),
    )


if __name__ == "__main__":
    main()
