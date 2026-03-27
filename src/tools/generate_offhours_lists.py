from __future__ import annotations

"""Generate offhours watchlists from historical market data.

Outputs:
- short_watchlist.csv
- mid_watchlist.csv
- long_watchlist.csv
- enrichment.json (includes macro/events/markets snapshots + mid param tuning suggestion)

Design principles:
- Best-effort / robust: missing web dependencies or partial data should not crash the batch.
- Do not place orders. This script is for planning and next-day preparation.
- Keep modules separated for easier iteration.
"""

import argparse
import os
import time
from pathlib import Path
from typing import Any, Dict, List

from ..common.logger import get_logger
from ..common.markets import add_market_args, market_config_path, resolve_market_code
from ..ibkr.market_data import MarketDataService
from ..enrichment.providers import EnrichmentProviders
from ..strategies.engine_strategy import StrategyConfig
from ..strategies.mid_regime import RegimeConfig
from ..strategies.regime_adaptor import RegimeAdaptConfig, RegimeAdaptor

from ..offhours.candidates import build_candidate_symbols
from ..offhours.ib_setup import connect_ib, set_delayed_frozen, register_contracts, get_net_liquidation
from ..offhours.compute_short import compute_short_for_symbol
from ..offhours.compute_mid import compute_mid_for_symbol
from ..offhours.compute_long import compute_long_for_symbol
from ..offhours.export import write_csv, write_json

log = get_logger("tools.generate_offhours_lists")
BASE_DIR = Path(__file__).resolve().parents[2]


class PermissiveGate:
    """Offline planning does not enforce live short-trading stops."""

    def can_trade_short(self) -> bool:
        return True


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    add_market_args(ap)
    ap.add_argument("--ibkr_config", default="config/ibkr.yaml")
    ap.add_argument("--db", default="audit.db")
    ap.add_argument("--out_dir", default=".")
    ap.add_argument("--watchlist_yaml", default="")
    ap.add_argument("--symbols", default="", help="Comma-separated symbols to override candidate pool")
    ap.add_argument("--audit_limit", type=int, default=500)
    ap.add_argument("--short_bars_need", type=int, default=600)
    ap.add_argument("--mid_lookback_days", type=int, default=180)
    ap.add_argument("--long_years", type=int, default=5)
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    # Load IB connection config (reuse project's yaml if present).
    # We keep this local import to avoid creating new config dependencies.
    import yaml
    market_code = resolve_market_code(getattr(args, "market", ""))
    explicit_cfg = str(args.ibkr_config) if str(args.ibkr_config) != "config/ibkr.yaml" or not market_code else ""
    cfg_path = market_config_path(BASE_DIR, market_code, explicit_cfg)
    with cfg_path.open("r", encoding="utf-8") as f:
        ibkr_cfg = yaml.safe_load(f)
    strategy_cfg_path = BASE_DIR / str(ibkr_cfg.get("strategy_config", "config/strategy_defaults.yaml"))
    with strategy_cfg_path.open("r", encoding="utf-8") as f:
        strategy_cfg_raw = yaml.safe_load(f) or {}
    regime_adaptor_cfg_path = BASE_DIR / str(ibkr_cfg.get("regime_adaptor_config", "config/regime_adaptor.yaml"))
    with regime_adaptor_cfg_path.open("r", encoding="utf-8") as f:
        regime_adaptor_cfg_raw = yaml.safe_load(f) or {}

    host = ibkr_cfg["host"]
    port = int(ibkr_cfg["port"])
    client_id = int(ibkr_cfg["client_id"])
    account_id = ibkr_cfg["account_id"]

    ib = connect_ib(host, port, client_id, request_timeout=5)
    set_delayed_frozen(ib)
    log.info("Connected.")

    # Candidate pool: from provided symbols OR db+watchlist+defaults.
    if args.symbols.strip():
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = build_candidate_symbols(
            db_path=args.db,
            watchlist_yaml=args.watchlist_yaml or None,
            default_symbols=["SPY"],
            audit_limit=args.audit_limit,
        )

    log.info(f"Offhours symbols n={len(symbols)}: {symbols}")

    md = MarketDataService(ib)
    register_contracts(ib, md, symbols)

    base_mid_cfg = RegimeConfig(**(strategy_cfg_raw.get("mid_regime", {}) or {}))
    regime_adaptor = RegimeAdaptor(
        market=market_code or "DEFAULT",
        base_cfg=base_mid_cfg,
        adapt_cfg=RegimeAdaptConfig.from_dict(regime_adaptor_cfg_raw.get("regime_adaptor")),
    )
    adapted_mid_cfg = regime_adaptor.refresh_if_due(md, force=True)

    # Offhours scoring only needs the gate interface, not live account-backed risk state.
    gate = PermissiveGate()
    strat_cfg = StrategyConfig(
        take_profit_pct=float(strategy_cfg_raw.get("orders", {}).get("default_take_profit_pct", 0.004)),
        stop_loss_pct=float(strategy_cfg_raw.get("orders", {}).get("default_stop_loss_pct", 0.006)),
        mid=adapted_mid_cfg,
    )

    # Compute watchlists
    short_rows: List[Dict[str, Any]] = []
    mid_rows: List[Dict[str, Any]] = []
    long_rows: List[Dict[str, Any]] = []

    for sym in symbols:
        # SHORT
        try:
            r = compute_short_for_symbol(symbol=sym, md=md, cfg=strat_cfg, gate=gate, bars_need=args.short_bars_need)
            if r is not None:
                short_rows.append(r)
        except Exception as e:
            log.warning(f"short compute failed for {sym}: {type(e).__name__} {e}")

        # MID
        try:
            r2 = compute_mid_for_symbol(sym, md, lookback_days=args.mid_lookback_days)
            if r2 is not None:
                mid_rows.append(r2)
        except Exception as e:
            log.warning(f"mid compute failed for {sym}: {type(e).__name__} {e}")

        # LONG
        try:
            r3 = compute_long_for_symbol(sym, md, years=args.long_years)
            if r3 is not None:
                long_rows.append(r3)
        except Exception as e:
            log.warning(f"long compute failed for {sym}: {type(e).__name__} {e}")

    # Sort
    short_rows.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    mid_rows.sort(key=lambda x: float(x.get("mid_scale", 0.0)), reverse=True)
    long_rows.sort(key=lambda x: float(x.get("long_score", 0.0)), reverse=True)

    out_dir = args.out_dir or "."
    short_csv = os.path.join(out_dir, "short_watchlist.csv")
    mid_csv = os.path.join(out_dir, "mid_watchlist.csv")
    long_csv = os.path.join(out_dir, "long_watchlist.csv")
    enrich_json = os.path.join(out_dir, "enrichment.json")

    write_csv(short_csv, short_rows, fieldnames=["symbol","score","direction","channel","short_sig","total_sig","mid_scale","stability","bars","bar_end_time","reason"])
    write_csv(mid_csv, mid_rows, fieldnames=["symbol","mid_scale","trend_slope_60d","last_close","bars"])
    write_csv(long_csv, long_rows, fieldnames=["symbol","long_score","trend_vs_ma200","mdd_1y","last_close","bars","rebalance_flag"])

    # Enrichment (web best-effort)
    netliq = get_net_liquidation(ib, account_id)
    providers = EnrichmentProviders()
    bundle = providers.collect(symbols=symbols, market=market_code)

    # Very lightweight mid param tuning suggestion based on VIX (if available)
    mid_tuning = {"mid_qty_min": 0.25, "mid_qty_max": 1.25, "note": "default"}
    try:
        vix = bundle.get("markets", {}).get("tickers", {}).get("^VIX", {})
        vix_close = float(vix.get("close", 0.0) or 0.0)
        if vix_close >= 25:
            mid_tuning = {"mid_qty_min": 0.15, "mid_qty_max": 0.9, "note": "VIX high -> reduce risk"}
        elif vix_close >= 18:
            mid_tuning = {"mid_qty_min": 0.20, "mid_qty_max": 1.0, "note": "VIX elevated -> slightly reduce risk"}
        else:
            mid_tuning = {"mid_qty_min": 0.25, "mid_qty_max": 1.25, "note": "VIX normal -> default risk"}
    except Exception:
        pass

    payload = {
        "ts": int(time.time()),
        "account": {"NetLiquidation": float(netliq)},
        "bundle": bundle,
        "regime_snapshot": regime_adaptor.snapshot.to_dict() if regime_adaptor.snapshot else {},
        "mid_param_tuning": mid_tuning,
    }
    write_json(enrich_json, payload)

    log.info(f"Wrote: {short_csv} ({len(short_rows)} rows)")
    log.info(f"Wrote: {mid_csv} ({len(mid_rows)} rows)")
    log.info(f"Wrote: {long_csv} ({len(long_rows)} rows)")
    log.info(f"Wrote: {enrich_json}")


if __name__ == "__main__":
    main()
