from __future__ import annotations

import argparse
import os
import sqlite3
import time
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Set

from ..analysis.features import FeatureConfig, compute_features_for_symbol
from ..analysis.plan import TradePlanConfig, make_trade_plan
from ..analysis.report import write_csv, write_json, write_md
from ..analysis.scoring import ReportScoringConfig, overlay_symbol
from ..analysis.universe import build_candidates
from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.logger import get_logger
from ..common.markets import (
    add_market_args,
    infer_market_from_config_path,
    load_symbols_from_symbol_master,
    load_market_universe_config,
    market_config_path,
    resolve_market_code,
    symbol_matches_market,
)
from ..common.runtime_paths import resolve_repo_path
from ..common.storage import Storage
from ..enrichment.providers import EnrichmentProviders
from ..ibkr.universe import UniverseService, UniverseConfig, scanner_location_codes_from_config
from ..ibkr.market_data import MarketDataService
from ..offhours.candidates import load_watchlist_symbols, read_recent_symbols_from_audit
from ..offhours.compute_short import compute_engine_signal_for_symbol
from ..offhours.ib_setup import connect_ib, get_net_liquidation, register_contracts, set_delayed_frozen
from ..risk.model import TradeRiskConfig
from ..risk.short_safety import ShortSafetyConfig, ShortSafetyGate, load_short_safety_rule_file, load_symbol_float_map
from ..strategies import StrategyConfig
from ..strategies.mid_regime import RegimeConfig
from ..strategies.regime_adaptor import RegimeAdaptConfig, RegimeAdaptor

log = get_logger("tools.generate_trade_report")
BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_SEED_SYMBOLS = "SPY,TSLA,AAPL,MSFT,NVDA"


def build_parser() -> argparse.ArgumentParser:
    ap = build_cli_parser(
        description="Automated analysis -> ranked candidates -> trade plan report.",
        command="ibkr-quant-trade-report",
        examples=[
            "ibkr-quant-trade-report --market US --ibkr_config config/ibkr_us.yaml --top_n 20",
            "ibkr-quant-trade-report --market HK --watchlist_yaml config/watchlists/resolved_hk_top100_bluechip.yaml",
        ],
        notes=[
            "Generates ranked candidate, scoring, and trade-plan artifacts under --out_dir.",
        ],
    )
    add_market_args(ap)
    ap.add_argument("--ibkr_config", default="config/ibkr.yaml", help="Path to the IBKR connection config yaml.")
    ap.add_argument("--report_config", default="", help="Path to report scoring/plan config yaml.")
    ap.add_argument("--out_dir", default="", help="Optional output directory override. Defaults to reports_<market>.")
    ap.add_argument("--top_n", type=int, default=10, help="Number of ranked trade ideas to emit.")
    ap.add_argument("--max_universe", type=int, default=1000, help="Maximum candidate universe size before scoring.")
    ap.add_argument("--symbols", default=DEFAULT_SEED_SYMBOLS, help="Comma-separated seed symbols.")
    ap.add_argument("--watchlist_yaml", default="", help="YAML with {symbols: [...]} used to expand candidates.")
    ap.add_argument("--db", default="audit.db", help="SQLite audit database used for recents and blacklist data.")
    ap.add_argument("--symbol_master_db", default="", help="SQLite symbol master database used for market universe candidates.")
    ap.add_argument("--use_seed", action="store_true", default=False, help="Include the explicit seed symbols in the candidate pool.")
    ap.add_argument("--no_seed", dest="use_seed", action="store_false", help="Exclude the explicit seed symbols from the candidate pool.")
    ap.add_argument("--use_audit_recent", action="store_true", default=True, help="Include recent symbols from signals_audit.")
    ap.add_argument("--audit_limit", type=int, default=500, help="Maximum recent audit symbols to pull into the candidate pool.")
    ap.add_argument("--use_scanner", action="store_true", default=False, help="Include scanner hotlist results via UniverseService.")
    ap.add_argument("--scanner_limit", type=int, default=None, help="Optional override for scanner result count.")
    ap.add_argument("--scanner_codes", default="", help="Optional comma-separated IBKR scanner codes.")
    ap.add_argument("--scanner_max_codes_per_run", type=int, default=None, help="Optional limit for scanner codes per refresh.")
    ap.add_argument("--scanner_refresh_sec", type=int, default=None, help="Optional scanner cache TTL override in seconds.")
    ap.add_argument("--exclude_blacklist", action="store_true", default=True, help="Drop blacklisted symbols from the candidate pool.")
    ap.add_argument("--include_blacklist", dest="exclude_blacklist", action="store_false", help="Keep blacklisted symbols in the candidate pool.")
    return ap


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _extract_vix(bundle: Dict[str, Any]) -> float:
    try:
        vix = bundle.get("markets", {}).get("tickers", {}).get("^VIX", {})
        return float(vix.get("close", 0.0) or 0.0)
    except Exception:
        return 0.0


def _macro_high_risk(bundle: Dict[str, Any]) -> bool:
    events = bundle.get("macro_events") or bundle.get("bundle", {}).get("macro_events") or []
    for e in events:
        imp = str(e.get("importance", "")).lower()
        if imp in ("high", "3"):
            return True
    return False


def _earnings_map(bundle: Dict[str, Any]) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    em = bundle.get("earnings") or bundle.get("bundle", {}).get("earnings") or {}
    if isinstance(em, dict):
        for sym, info in em.items():
            try:
                flag = bool(info.get("in_14d", info.get("in_window", False)))
                out[str(sym).upper()] = flag
            except Exception:
                out[str(sym).upper()] = False
    return out


def _event_risk_for_symbol(bundle: Dict[str, Any], symbol: str, macro_high_risk: bool) -> tuple[str, str]:
    sym = str(symbol).upper()
    reasons: List[str] = []
    earnings = bundle.get("earnings") or bundle.get("bundle", {}).get("earnings") or {}
    info = earnings.get(sym, {}) if isinstance(earnings, dict) else {}
    if bool(info.get("in_14d", info.get("in_window", False))):
        nxt = str(info.get("next_earnings_date") or "").strip()
        reasons.append(f"earnings:{nxt or 'window'}")
    if macro_high_risk:
        reasons.append("macro_calendar_high")
    return ("HIGH" if reasons else "NONE", ",".join(reasons))


def _parse_csv_list(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _is_hk_symbol(sym: str) -> bool:
    s = str(sym).upper().strip()
    return s.startswith("HK:") or s.endswith(".HK")


def _user_explicitly_set_symbols() -> bool:
    return "--symbols" in sys.argv


def _filter_symbols_by_market(symbols: List[str], hk_only: bool) -> List[str]:
    if not hk_only:
        return [str(sym).upper() for sym in symbols]
    return [str(sym).upper() for sym in symbols if _is_hk_symbol(sym)]


def _filter_symbols_for_market(symbols: List[str], market: str) -> List[str]:
    return [str(sym).upper() for sym in symbols if symbol_matches_market(str(sym), market)]


def _resolve_project_path(path_str: str) -> str:
    return str(resolve_repo_path(BASE_DIR, path_str))


def _load_yaml(path_str: str) -> Dict[str, Any]:
    path = Path(_resolve_project_path(path_str))
    import yaml

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _slugify_report_name(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "_" for ch in (name or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_") or "default"


class _ReportGate:
    def __init__(
        self,
        *,
        event_risk_by_symbol: Dict[str, str],
        event_reason_by_symbol: Dict[str, str],
        borrow_fee_bps_by_symbol: Dict[str, float],
        borrow_source_by_symbol: Dict[str, str],
    ):
        self._event_risk = {str(k).upper(): str(v).upper() for k, v in event_risk_by_symbol.items()}
        self._event_reason = {str(k).upper(): str(v) for k, v in event_reason_by_symbol.items()}
        self._borrow_fee = {str(k).upper(): float(v) for k, v in borrow_fee_bps_by_symbol.items()}
        self._borrow_source = {str(k).upper(): str(v) for k, v in borrow_source_by_symbol.items()}

    def can_trade_short(self) -> bool:
        return True

    def event_risk_for(self, symbol: str) -> str:
        return str(self._event_risk.get(str(symbol).upper(), "NONE"))

    def event_risk_reason_for(self, symbol: str) -> str:
        return str(self._event_reason.get(str(symbol).upper(), ""))

    def short_borrow_fee_bps_for(self, symbol: str) -> float:
        return float(self._borrow_fee.get(str(symbol).upper(), 0.0))

    def short_borrow_source_for(self, symbol: str) -> str:
        return str(self._borrow_source.get(str(symbol).upper(), "default"))


def _load_blacklist(db_path: str) -> Set[str]:
    try:
        st = Storage(db_path)
        now_ts = int(time.time())
        rows = st.get_md_blacklist_active(now_ts)
        return {str(r[0]).upper() for r in rows if r and r[0]}
    except Exception:
        return set()


def _scanner_cache_get(db_path: str, codes_key: str, ttl_sec: int) -> List[str]:
    try:
        c = sqlite3.connect(db_path)
        try:
            c.execute(
                """create table if not exists scanner_cache(
                    id integer primary key autoincrement,
                    ts integer not null,
                    codes_key text not null,
                    symbols_json text not null
                )"""
            )
            now = int(time.time())
            row = c.execute(
                "select ts, symbols_json from scanner_cache where codes_key=? and ts>=? order by ts desc limit 1",
                (codes_key, now - int(ttl_sec)),
            ).fetchone()
            if not row:
                return []
            _ts, symbols_json = row
            syms = json.loads(symbols_json) if symbols_json else []
            if isinstance(syms, list):
                return [str(x).upper() for x in syms if x]
            return []
        finally:
            c.close()
    except Exception:
        return []


def _scanner_cache_put(db_path: str, codes_key: str, symbols: List[str]) -> None:
    try:
        c = sqlite3.connect(db_path)
        try:
            c.execute(
                """create table if not exists scanner_cache(
                    id integer primary key autoincrement,
                    ts integer not null,
                    codes_key text not null,
                    symbols_json text not null
                )"""
            )
            c.execute(
                "insert into scanner_cache(ts, codes_key, symbols_json) values(?,?,?)",
                (int(time.time()), codes_key, json.dumps(symbols)),
            )
            c.commit()
        finally:
            c.close()
    except Exception:
        pass


def _scanner_symbols(ib, md, db_path: str, args: argparse.Namespace, cache_namespace: str, ibkr_cfg: Dict[str, Any]) -> List[str]:
    try:
        codes = _parse_csv_list(args.scanner_codes) or [str(x) for x in ibkr_cfg.get("scanner_codes", ["HOT_BY_VOLUME", "TOP_PERC_GAIN", "TOP_PERC_LOSE"])]
        scanner_limit = int(args.scanner_limit if args.scanner_limit is not None else ibkr_cfg.get("scanner_limit", 20))
        scanner_refresh_sec = int(args.scanner_refresh_sec if args.scanner_refresh_sec is not None else ibkr_cfg.get("scanner_refresh_sec", 120))
        scanner_max_codes_per_run = int(
            args.scanner_max_codes_per_run if args.scanner_max_codes_per_run is not None else ibkr_cfg.get("scanner_max_codes_per_run", 3)
        )
        scanner_instrument = str(ibkr_cfg.get("scanner_instrument", "STK"))
        scanner_location_codes = scanner_location_codes_from_config(ibkr_cfg, default="STK.US.MAJOR")

        codes_key = f"{cache_namespace}|{scanner_instrument}|{','.join(scanner_location_codes)}|{','.join(codes)}|limit={scanner_limit}"
        cached = _scanner_cache_get(db_path, codes_key=codes_key, ttl_sec=scanner_refresh_sec)
        if cached:
            return cached

        cfg = UniverseConfig(
            scanner_enabled=True,
            scanner_instrument=scanner_instrument,
            scanner_location_codes=scanner_location_codes,
            scanner_location_code=str(scanner_location_codes[0]),
            scanner_limit=scanner_limit,
            scanner_refresh_sec=scanner_refresh_sec,
            scanner_max_codes_per_run=scanner_max_codes_per_run,
            scanner_codes=codes,
            seed_symbols=[],
        )
        storage = Storage(db_path) if db_path else None
        uni = UniverseService(ib, cfg, storage=storage, md=md)
        res = uni.build()
        syms = [str(x).upper() for x in (res.get("hot", []) or []) if x]
        _scanner_cache_put(db_path, codes_key=codes_key, symbols=syms)
        return syms
    except Exception as e:
        log.warning(f"scanner failed: {type(e).__name__} {e}")
        return []


def _cli_summary_payload(
    *,
    market: str,
    out_dir: Path,
    candidate_count: int,
    ranked_count: int,
    plan_count: int,
    scanner_enabled: bool,
    watchlist_name: str = "",
) -> tuple[Dict[str, Any], Dict[str, Path]]:
    return (
        {
            "market": str(market or "DEFAULT"),
            "candidate_count": int(candidate_count),
            "ranked_count": int(ranked_count),
            "plan_count": int(plan_count),
            "scanner_enabled": bool(scanner_enabled),
            "watchlist": str(watchlist_name or "-"),
        },
        {
            "report_md": out_dir / "report.md",
            "ranked_csv": out_dir / "ranked_candidates.csv",
            "trade_plan_csv": out_dir / "trade_plan.csv",
            "universe_csv": out_dir / "universe_candidates.csv",
            "enrichment_json": out_dir / "enrichment.json",
        },
    )


def main() -> None:
    args = parse_args()

    market_code = resolve_market_code(getattr(args, "market", ""))
    explicit_cfg = str(args.ibkr_config) if str(args.ibkr_config) != "config/ibkr.yaml" or not market_code else ""
    ibkr_cfg_path = str(market_config_path(BASE_DIR, market_code, explicit_cfg))
    resolved_market = market_code or infer_market_from_config_path(ibkr_cfg_path) or "DEFAULT"
    cfg = _load_yaml(ibkr_cfg_path)
    market_universe_cfg = load_market_universe_config(BASE_DIR, resolved_market)
    report_cfg_path = _resolve_project_path(args.report_config or str(cfg.get("report_config", "config/report_scoring.yaml")))
    report_cfg = _load_yaml(report_cfg_path)
    strategy_cfg_path = _resolve_project_path(str(cfg.get("strategy_config", "config/strategy_defaults.yaml")))
    regime_adaptor_cfg_path = _resolve_project_path(str(cfg.get("regime_adaptor_config", "config/regime_adaptor.yaml")))
    risk_cfg_path = _resolve_project_path(str(cfg.get("risk_config", "config/risk.yaml")))
    strategy_cfg = _load_yaml(strategy_cfg_path)
    regime_adaptor_cfg_raw = _load_yaml(regime_adaptor_cfg_path)
    risk_cfg = _load_yaml(risk_cfg_path)
    feature_cfg = FeatureConfig.from_dict(report_cfg.get("features"))
    scoring_cfg = ReportScoringConfig.from_dict(report_cfg.get("scoring"))
    plan_cfg = TradePlanConfig.from_dict(report_cfg.get("trade_plan"))
    regime_cfg = RegimeConfig(**(strategy_cfg.get("mid_regime", {}) or {}))

    host = cfg["host"]
    port = int(cfg["port"])
    client_id = int(cfg["client_id"])
    account_id = cfg["account_id"]
    db_path = _resolve_project_path(args.db)
    symbol_master_db_path = _resolve_project_path(args.symbol_master_db or str(market_universe_cfg.get("symbol_master_db", "symbol_master.db")))
    default_watchlist_yaml = str(
        market_universe_cfg.get("report_watchlist_yaml", cfg.get("report_watchlist_yaml", cfg.get("seed_watchlist_yaml", ""))) or ""
    )
    watchlist_yaml = _resolve_project_path(args.watchlist_yaml or default_watchlist_yaml) if (args.watchlist_yaml or default_watchlist_yaml) else ""
    out_dir_arg = args.out_dir or f"reports_{(resolved_market or 'default').lower()}"
    log.info(f"Using market={resolved_market} IBKR config: {ibkr_cfg_path}")
    log.info(f"Using market universe config: {BASE_DIR / 'config' / 'markets' / resolved_market.lower() / 'universe.yaml' if resolved_market != 'DEFAULT' else ''}")
    log.info(f"Using report config: {report_cfg_path}")
    log.info(
        "Resolved defaults: out_dir=%s watchlist=%s symbol_master_db=%s top_n=%s max_universe=%s use_audit_recent=%s use_seed=%s",
        out_dir_arg,
        watchlist_yaml or "",
        symbol_master_db_path,
        args.top_n,
        args.max_universe,
        args.use_audit_recent,
        args.use_seed,
    )

    ib = connect_ib(host, port, client_id, request_timeout=5)
    try:
        set_delayed_frozen(ib)

        seed_symbols = [s.strip().upper() for s in _parse_csv_list(args.symbols)]
        symbol_master_symbols = load_symbols_from_symbol_master(symbol_master_db_path, resolved_market if resolved_market != "DEFAULT" else "")
        if not symbol_master_symbols and resolved_market != "DEFAULT":
            log.info(f"symbol_master load skipped/empty for market={resolved_market}: path={symbol_master_db_path}")
        if symbol_master_symbols:
            log.info(f"Loaded symbol master universe: market={resolved_market} path={symbol_master_db_path} symbols={len(symbol_master_symbols)}")
        yaml_symbols: List[str] = []
        if not symbol_master_symbols:
            yaml_symbols = load_watchlist_symbols(watchlist_yaml) if watchlist_yaml else []
            if resolved_market != "DEFAULT" and yaml_symbols:
                before = len(yaml_symbols)
                yaml_symbols = _filter_symbols_for_market(yaml_symbols, resolved_market)
                if len(yaml_symbols) != before:
                    log.info(f"Filtered watchlist for market={resolved_market}: before={before} after={len(yaml_symbols)}")
            if watchlist_yaml:
                if Path(watchlist_yaml).exists():
                    log.info(f"Loaded watchlist fallback: path={watchlist_yaml} symbols={len(yaml_symbols)}")
                else:
                    log.warning(f"Watchlist file not found: path={watchlist_yaml}")

        base_universe_symbols = symbol_master_symbols or yaml_symbols
        hk_watchlist = bool(base_universe_symbols) and all(_is_hk_symbol(sym) for sym in base_universe_symbols)
        explicit_symbols = _user_explicitly_set_symbols()
        if not explicit_symbols and base_universe_symbols and not args.use_seed:
            seed_symbols = []
        elif hk_watchlist and not explicit_symbols and args.use_seed and args.symbols == DEFAULT_SEED_SYMBOLS:
            log.info("HK watchlist detected and no explicit --symbols provided; disabling default US seed symbols")
            seed_symbols = []

        if base_universe_symbols:
            hk_count = sum(1 for sym in base_universe_symbols if _is_hk_symbol(sym))
            log.info(f"Universe market mix: hk={hk_count} non_hk={len(base_universe_symbols) - hk_count}")

        recent_symbols = read_recent_symbols_from_audit(db_path, limit=int(args.audit_limit)) if args.use_audit_recent else []
        if recent_symbols and resolved_market != "DEFAULT":
            before = len(recent_symbols)
            recent_symbols = _filter_symbols_for_market(recent_symbols, resolved_market)
            log.info(f"Filtered audit recent symbols for market={resolved_market}: before={before} after={len(recent_symbols)}")
        elif hk_watchlist and recent_symbols:
            before = len(recent_symbols)
            recent_symbols = _filter_symbols_by_market(recent_symbols, hk_only=True)
            log.info(f"Filtered audit recent symbols for HK watchlist: before={before} after={len(recent_symbols)}")

        md = MarketDataService(ib)
        regime_adaptor = RegimeAdaptor(
            market=resolved_market,
            base_cfg=regime_cfg,
            adapt_cfg=RegimeAdaptConfig.from_dict(regime_adaptor_cfg_raw.get("regime_adaptor")),
        )
        adapted_regime_cfg = regime_adaptor.refresh_if_due(md, force=True)
        blacklist: Set[str] = _load_blacklist(db_path) if args.exclude_blacklist else set()
        scanner_syms: List[str] = []
        if args.use_scanner:
            scanner_syms = _scanner_symbols(ib, md, db_path, args, cache_namespace=resolved_market, ibkr_cfg=cfg)
            if resolved_market != "DEFAULT" and scanner_syms:
                before = len(scanner_syms)
                scanner_syms = _filter_symbols_for_market(scanner_syms, resolved_market)
                if len(scanner_syms) != before:
                    log.info(f"Filtered scanner symbols for market={resolved_market}: before={before} after={len(scanner_syms)}")

        uni = build_candidates(
            seed_symbols=(seed_symbols if args.use_seed else []) + [str(x).upper() for x in base_universe_symbols],
            recent_symbols=[str(x).upper() for x in recent_symbols],
            scanner_symbols=[str(x).upper() for x in scanner_syms],
            blacklist=blacklist,
            max_n=int(args.max_universe),
        )
        candidates = uni.symbols
        log.info(
            f"Universe n={len(candidates)} "
            f"(seed={len(seed_symbols) if args.use_seed else 0}, symbol_master={len(symbol_master_symbols)}, yaml_fallback={len(yaml_symbols)}, "
            f"recent={len(recent_symbols) if args.use_audit_recent else 0}, scanner={len(scanner_syms) if args.use_scanner else 0}, "
            f"blacklist_active={len(blacklist) if args.exclude_blacklist else 0})"
        )

        register_contracts(ib, md, candidates)

        providers = EnrichmentProviders()
        bundle = providers.collect(symbols=candidates, market=resolved_market)

        vix = _extract_vix(bundle)
        macro_high_risk = _macro_high_risk(bundle)
        earnings_map = _earnings_map(bundle)
        risk_context_raw = dict(risk_cfg.get("risk_context") or {})
        short_borrow_fee_bps = {
            str(sym).upper(): float(val)
            for sym, val in dict(risk_context_raw.get("short_borrow_fee_bps", {}) or {}).items()
        }
        short_borrow_fee_sources = {
            str(sym).upper(): "config_inline"
            for sym in short_borrow_fee_bps
        }
        short_borrow_fee_file = str(risk_context_raw.get("short_borrow_fee_file", "") or "").strip()
        if short_borrow_fee_file:
            try:
                fee_path = Path(_resolve_project_path(short_borrow_fee_file))
                file_values, file_sources = load_symbol_float_map(
                    fee_path,
                    source_label=f"file:{fee_path.name}",
                    value_keys=("borrow_fee_bps", "short_borrow_fee_bps", "fee_bps", "value"),
                )
                short_borrow_fee_bps.update(file_values)
                for sym, source in file_sources.items():
                    if sym in file_values or sym not in short_borrow_fee_bps:
                        short_borrow_fee_sources[sym] = source
            except Exception as e:
                log.warning(f"borrow fee file load failed for report: path={short_borrow_fee_file} error={type(e).__name__} {e}")

        short_safety_raw = dict(risk_cfg.get("short_safety") or {})
        short_safety_file = str(short_safety_raw.get("short_safety_file", "") or "").strip()
        if short_safety_file:
            try:
                rules_path = Path(_resolve_project_path(short_safety_file))
                rule_payload = load_short_safety_rule_file(
                    rules_path,
                    source_label=f"file:{rules_path.name}",
                )
                for key in ("locate_status", "ssr_status", "spread_bps", "has_uptick_data", "sources"):
                    merged = dict(short_safety_raw.get(key, {}) or {})
                    merged.update(rule_payload.get(key, {}) or {})
                    short_safety_raw[key] = merged
            except Exception as e:
                log.warning(f"short safety file load failed for report: path={short_safety_file} error={type(e).__name__} {e}")
        short_safety_gate = ShortSafetyGate(
            ShortSafetyConfig.from_dict(short_safety_raw, market=resolved_market),
            context=None,
        )
        event_risk_by_symbol: Dict[str, str] = {}
        event_reason_by_symbol: Dict[str, str] = {}
        for sym in candidates:
            event_risk, event_reason = _event_risk_for_symbol(bundle, sym, macro_high_risk)
            event_risk_by_symbol[str(sym).upper()] = event_risk
            event_reason_by_symbol[str(sym).upper()] = event_reason
        report_gate = _ReportGate(
            event_risk_by_symbol=event_risk_by_symbol,
            event_reason_by_symbol=event_reason_by_symbol,
            borrow_fee_bps_by_symbol=short_borrow_fee_bps,
            borrow_source_by_symbol=short_borrow_fee_sources,
        )
        strat_cfg = StrategyConfig(
            take_profit_pct=float(strategy_cfg.get("orders", {}).get("default_take_profit_pct", 0.004)),
            stop_loss_pct=float(strategy_cfg.get("orders", {}).get("default_stop_loss_pct", 0.006)),
            mid=adapted_regime_cfg,
            risk=TradeRiskConfig.from_dict(risk_cfg.get("trade_risk")),
        )

        features: List[Dict[str, Any]] = []
        for sym in candidates:
            try:
                feat = compute_features_for_symbol(md, sym, cfg=feature_cfg, regime_cfg=adapted_regime_cfg)
                if feat:
                    features.append(feat)
            except Exception as e:
                log.warning(f"feature failed for {sym}: {type(e).__name__} {e}")

        feat_map = {str(f["symbol"]).upper(): f for f in features}
        engine_rows: List[Dict[str, Any]] = []
        for sym in feat_map:
            try:
                signal_row = compute_engine_signal_for_symbol(
                    symbol=sym,
                    md=md,
                    cfg=strat_cfg,
                    gate=report_gate,
                )
                if signal_row:
                    engine_rows.append(signal_row)
            except Exception as e:
                log.warning(f"engine replay failed for {sym}: {type(e).__name__} {e}")

        ranked: List[Dict[str, Any]] = []
        for signal_row in engine_rows:
            sym = str(signal_row["symbol"]).upper()
            feat = feat_map.get(sym)
            if feat is None:
                continue

            direction = str(signal_row.get("direction", "WAIT") or "WAIT").upper()
            tradable_status = "NOT_SHORT"
            blocked_reason = ""
            if direction == "SHORT":
                decision = short_safety_gate.evaluate(
                    sym,
                    avg_bar_volume=float(signal_row.get("avg_bar_volume", feat.get("short_vol", 0.0)) or 0.0),
                    action="SELL",
                    enforce_timing=False,
                    event_risk=str(signal_row.get("event_risk", event_risk_by_symbol.get(sym, "NONE")) or "NONE"),
                    event_risk_reason=str(signal_row.get("event_risk_reason", event_reason_by_symbol.get(sym, "")) or ""),
                    short_borrow_fee_bps=float(short_borrow_fee_bps.get(sym, signal_row.get("short_borrow_fee_bps", 0.0)) or 0.0),
                    short_borrow_source=str(short_borrow_fee_sources.get(sym, signal_row.get("short_borrow_source", "default")) or "default"),
                )
                tradable_status = decision.tradable_status
                blocked_reason = decision.blocked_reason_text()

            overlay = overlay_symbol(
                feat,
                vix=vix,
                earnings_in_14d=bool(earnings_map.get(sym, False)),
                macro_high_risk=macro_high_risk,
                tradable_status=tradable_status,
                blocked_reason=blocked_reason,
                short_borrow_fee_bps=float(short_borrow_fee_bps.get(sym, signal_row.get("short_borrow_fee_bps", 0.0)) or 0.0),
                cfg=scoring_cfg,
            )
            final_score = (
                float(scoring_cfg.engine_score_weight) * float(signal_row.get("engine_score", 0.0) or 0.0)
                + float(scoring_cfg.overlay_score_weight) * float(overlay.get("overlay_score", 0.0) or 0.0)
            )
            ranked.append(
                {
                    "symbol": sym,
                    "score": float(final_score),
                    "direction": direction,
                    "engine_score": float(signal_row.get("engine_score", 0.0) or 0.0),
                    "signal_strength": float(signal_row.get("signal_strength", 0.0) or 0.0),
                    "signal_value": float(signal_row.get("signal_value", 0.0) or 0.0),
                    "stability": float(signal_row.get("stability", 0.0) or 0.0),
                    "channel": str(signal_row.get("channel", "") or ""),
                    "short_sig": float(signal_row.get("short_sig", 0.0) or 0.0),
                    "total_sig": float(signal_row.get("total_sig", 0.0) or 0.0),
                    "alpha": float(overlay.get("overlay_alpha", 0.0) or 0.0),
                    "risk": float(overlay.get("overlay_risk", 0.0) or 0.0),
                    "overlay_score": float(overlay.get("overlay_score", 0.0) or 0.0),
                    "risk_on": bool(signal_row.get("risk_on", feat.get("risk_on", True))),
                    "mid_scale": float(signal_row.get("mid_scale", feat.get("mid_scale", 0.5)) or 0.5),
                    "regime_state": str(signal_row.get("regime_state", feat.get("regime_state", "")) or ""),
                    "regime_reason": str(signal_row.get("regime_reason", feat.get("regime_reason", "")) or ""),
                    "regime_composite": float(feat.get("regime_composite", 0.0) or 0.0),
                    "tradable_status": tradable_status,
                    "blocked_reason": blocked_reason,
                    "should_trade": bool(signal_row.get("should_trade", False)),
                    "action": str(signal_row.get("action", "") or ""),
                    "entry_price": float(signal_row.get("entry_price", feat.get("last", 0.0)) or 0.0),
                    "risk_allowed": bool(signal_row.get("risk_allowed", True)),
                    "risk_per_share": float(signal_row.get("risk_per_share", 0.0) or 0.0),
                    "stop_price": float(signal_row.get("stop_price", 0.0) or 0.0),
                    "take_profit_price": float(signal_row.get("take_profit_price", 0.0) or 0.0),
                    "stop_distance": float(signal_row.get("stop_distance", 0.0) or 0.0),
                    "take_profit_distance": float(signal_row.get("take_profit_distance", 0.0) or 0.0),
                    "liquidity_haircut": float(signal_row.get("liquidity_haircut", 0.0) or 0.0),
                    "avg_bar_volume": float(signal_row.get("avg_bar_volume", feat.get("short_vol", 0.0)) or 0.0),
                    "event_risk": str(signal_row.get("event_risk", event_risk_by_symbol.get(sym, "")) or ""),
                    "event_risk_reason": str(signal_row.get("event_risk_reason", event_reason_by_symbol.get(sym, "")) or ""),
                    "short_borrow_fee_bps": float(short_borrow_fee_bps.get(sym, signal_row.get("short_borrow_fee_bps", 0.0)) or 0.0),
                    "short_borrow_source": str(short_borrow_fee_sources.get(sym, signal_row.get("short_borrow_source", "default")) or "default"),
                    "vol_norm": float(feat.get("vol_norm", 0.0) or 0.0),
                    "dist_ma20": float(overlay.get("dist_ma20", 0.0) or 0.0),
                    "bar_end_time": str(signal_row.get("bar_end_time", "") or ""),
                    "reason": str(signal_row.get("reason", "") or ""),
                    "risk_snapshot": dict(signal_row.get("risk_snapshot", {}) or {}),
                }
            )

        ranked.sort(
            key=lambda row: (
                1 if bool(row.get("should_trade", False)) else 0,
                float(row.get("score", 0.0) or 0.0),
                float(row.get("stability", 0.0) or 0.0),
            ),
            reverse=True,
        )
        ranked = ranked[: int(args.top_n)]

        plans = []
        for row in ranked:
            sym = str(row["symbol"]).upper()
            feat = feat_map.get(sym)
            if feat is not None:
                plans.append(make_trade_plan(row, feat, vix=vix, cfg=plan_cfg))

        out_dir = Path(out_dir_arg)
        if not out_dir.is_absolute():
            out_dir = BASE_DIR / out_dir
        if watchlist_yaml and not symbol_master_symbols:
            watchlist_name = _slugify_report_name(Path(watchlist_yaml).stem)
            out_dir = out_dir / watchlist_name
        elif symbol_master_symbols:
            out_dir = out_dir / f"market_{resolved_market.lower()}"
        os.makedirs(out_dir, exist_ok=True)

        uni_rows = []
        for sym in candidates:
            meta = uni.meta.get(sym, {})
            reasons = ",".join(meta.get("reasons", [])) if isinstance(meta, dict) else ""
            uni_rows.append({"symbol": sym, "sources": reasons})
        write_csv(str(out_dir / "universe_candidates.csv"), uni_rows)
        write_csv(str(out_dir / "ranked_candidates.csv"), ranked)
        write_csv(str(out_dir / "trade_plan.csv"), plans)
        write_json(str(out_dir / "enrichment.json"), bundle)
        earnings_risk_symbols = [s for s, flagged in earnings_map.items() if flagged]
        context = {
            "summary": {
                "vix": vix,
                "macro_high_risk": macro_high_risk,
                "net_liquidation": get_net_liquidation(ib, account_id),
                "candidate_count": len(candidates),
                "features_ok": len(features),
                "ranked_count": len(ranked),
                "plan_count": len(plans),
                "earnings_risk_count": len(earnings_risk_symbols),
                "blacklist_active": len(blacklist) if args.exclude_blacklist else 0,
                "seed_enabled": bool(args.use_seed),
                "audit_recent_enabled": bool(args.use_audit_recent),
                "scanner_enabled": bool(args.use_scanner),
            }
        }
        write_md(str(out_dir / "report.md"), "Daily Trade Candidate Report", ranked, plans, context)

        summary_fields, artifact_fields = _cli_summary_payload(
            market=resolved_market,
            out_dir=out_dir,
            candidate_count=len(candidates),
            ranked_count=len(ranked),
            plan_count=len(plans),
            scanner_enabled=bool(args.use_scanner),
            watchlist_name=Path(watchlist_yaml).name if watchlist_yaml else "",
        )
        emit_cli_summary(
            command="ibkr-quant-trade-report",
            headline="trade report generated",
            summary=summary_fields,
            artifacts=artifact_fields,
        )
        log.info(f"Wrote report -> {out_dir / 'report.md'} (ranked={len(ranked)} plans={len(plans)})")
    finally:
        try:
            if ib.isConnected():
                ib.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
