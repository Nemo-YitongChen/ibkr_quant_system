from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

try:
    from ..common.logger import get_logger
    from ..common.markets import load_market_universe_config, load_symbols_from_symbol_master, market_config_path
    from ..common.runtime_paths import resolve_repo_path, resolve_scoped_runtime_path, scope_from_ibkr_config
    from ..common.signal_audit import SignalAuditWriter
    from ..common.storage import Storage
    from ..enrichment.providers import EnrichmentProviders
    from ..ibkr.account import AccountService
    from ..ibkr.connection import IBKRConnection
    from ..ibkr.fills import FillProcessor
    from ..ibkr.market_data import MarketDataService
    from ..ibkr.orders import OrderService
    from ..ibkr.universe import UniverseConfig, UniverseService, scanner_location_codes_from_config
    from ..offhours.candidates import load_watchlist_symbols
    from ..portfolio.allocator import AllocatorConfig, PortfolioAllocator
    from ..portfolio.entry_guard import EntryGuard, GuardConfig
    from ..risk.limits import DailyRiskGate, RiskContextConfig
    from ..risk.model import TradeRiskConfig
    from ..risk.short_safety import (
        ShortSafetyConfig,
        ShortSafetyGate,
        load_short_safety_rule_file,
        load_symbol_float_map,
    )
    from ..scheduler.runner import Runner, RunnerConfig
    from ..strategies import EngineStrategy, StrategyConfig
    from ..strategies.mid_regime import RegimeConfig
    from ..strategies.regime_adaptor import RegimeAdaptConfig, RegimeAdaptor
    from .engine import EngineConfig, TradingEngine
    from .signal_executor import SignalExecutor
except ImportError:
    from common.logger import get_logger
    from common.markets import load_market_universe_config, load_symbols_from_symbol_master, market_config_path
    from common.runtime_paths import resolve_repo_path, resolve_scoped_runtime_path, scope_from_ibkr_config
    from common.signal_audit import SignalAuditWriter
    from common.storage import Storage
    from enrichment.providers import EnrichmentProviders
    from ibkr.account import AccountService
    from ibkr.connection import IBKRConnection
    from ibkr.fills import FillProcessor
    from ibkr.market_data import MarketDataService
    from ibkr.orders import OrderService
    from ibkr.universe import UniverseConfig, UniverseService, scanner_location_codes_from_config
    from offhours.candidates import load_watchlist_symbols
    from portfolio.allocator import AllocatorConfig, PortfolioAllocator
    from portfolio.entry_guard import EntryGuard, GuardConfig
    from risk.limits import DailyRiskGate, RiskContextConfig
    from risk.model import TradeRiskConfig
    from risk.short_safety import (
        ShortSafetyConfig,
        ShortSafetyGate,
        load_short_safety_rule_file,
        load_symbol_float_map,
    )
    from scheduler.runner import Runner, RunnerConfig
    from strategies import EngineStrategy, StrategyConfig
    from strategies.mid_regime import RegimeConfig
    from strategies.regime_adaptor import RegimeAdaptConfig, RegimeAdaptor
    from app.engine import EngineConfig, TradingEngine
    from app.signal_executor import SignalExecutor

log = get_logger("main")


def load_yaml(base_dir: Path, path: str) -> Dict[str, Any]:
    cfg_path = resolve_repo_path(base_dir, path)
    with cfg_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def resolve_path(base_dir: Path, path_str: str) -> Path:
    return resolve_repo_path(base_dir, path_str)


def resolve_ibkr_config_path(base_dir: Path, market_code: str, ibkr_config_arg: str) -> str:
    explicit_cfg = str(ibkr_config_arg or "").strip()
    if explicit_cfg == "config/ibkr.yaml" and market_code:
        explicit_cfg = ""
    return str(market_config_path(base_dir, market_code, explicit_cfg or None))


def _load_runtime_borrow_fee_maps(
    base_dir: Path,
    storage: Storage,
    risk_context_raw: Dict[str, Any],
) -> Tuple[Dict[str, float], Dict[str, str], str]:
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
            fee_path = resolve_path(base_dir, short_borrow_fee_file)
            file_values, file_sources = load_symbol_float_map(
                fee_path,
                source_label=f"file:{Path(short_borrow_fee_file).name}",
                value_keys=("borrow_fee_bps", "short_borrow_fee_bps", "fee_bps", "value"),
            )
            short_borrow_fee_bps.update(file_values)
            for sym, source in file_sources.items():
                if sym in file_values or sym not in short_borrow_fee_bps:
                    short_borrow_fee_sources[sym] = source
        except Exception as exc:
            storage.insert_risk_event(
                "BORROW_FEE_FILE_LOAD_ERROR",
                0.0,
                f"path={short_borrow_fee_file} error={type(exc).__name__}",
            )
            log.warning(
                "borrow fee file load failed: path=%s error=%s %s",
                short_borrow_fee_file,
                type(exc).__name__,
                exc,
            )
    return short_borrow_fee_bps, short_borrow_fee_sources, short_borrow_fee_file


def _load_runtime_short_safety_rules(
    base_dir: Path,
    storage: Storage,
    short_safety_raw: Dict[str, Any],
) -> Tuple[Dict[str, Any], str]:
    short_safety_raw = dict(short_safety_raw or {})
    short_safety_file = str(short_safety_raw.get("short_safety_file", "") or "").strip()
    if short_safety_file:
        try:
            rules_path = resolve_path(base_dir, short_safety_file)
            rule_payload = load_short_safety_rule_file(
                rules_path,
                source_label=f"file:{Path(short_safety_file).name}",
            )
            for key in ("locate_status", "ssr_status", "spread_bps", "has_uptick_data", "sources"):
                merged = dict(short_safety_raw.get(key, {}) or {})
                merged.update(rule_payload.get(key, {}) or {})
                short_safety_raw[key] = merged
        except Exception as exc:
            storage.insert_risk_event(
                "SHORT_SAFETY_FILE_LOAD_ERROR",
                0.0,
                f"path={short_safety_file} error={type(exc).__name__}",
            )
            log.warning(
                "short safety file load failed: path=%s error=%s %s",
                short_safety_file,
                type(exc).__name__,
                exc,
            )
    return short_safety_raw, short_safety_file


def _build_strategy_config(
    strat_cfg_raw: Dict[str, Any],
    adapted_mid_cfg: RegimeConfig,
    trade_risk_cfg: TradeRiskConfig,
    *,
    runtime_mode: str = "",
    paper_allowed_execution_sources: List[str] | None = None,
    enforce_pretrade_risk_gate: bool = True,
) -> StrategyConfig:
    strategy_raw = dict(strat_cfg_raw.get("strategy", {}) or {})
    orders_raw = dict(strat_cfg_raw.get("orders", {}) or {})
    return StrategyConfig(
        trade_threshold=float(strategy_raw.get("trade_threshold", StrategyConfig.trade_threshold)),
        base_qty=float(strategy_raw.get("base_qty", StrategyConfig.base_qty)),
        take_profit_pct=float(orders_raw.get("default_take_profit_pct", StrategyConfig.take_profit_pct)),
        stop_loss_pct=float(orders_raw.get("default_stop_loss_pct", StrategyConfig.stop_loss_pct)),
        enable_pure_short=bool(strategy_raw.get("enable_pure_short", StrategyConfig.enable_pure_short)),
        short_threshold=float(strategy_raw.get("short_threshold", StrategyConfig.short_threshold)),
        mid_soft_floor=float(strategy_raw.get("mid_soft_floor", StrategyConfig.mid_soft_floor)),
        mid_qty_min=float(strategy_raw.get("mid_qty_min", StrategyConfig.mid_qty_min)),
        mid_qty_max=float(strategy_raw.get("mid_qty_max", StrategyConfig.mid_qty_max)),
        runtime_mode=str(runtime_mode or ""),
        paper_allowed_execution_sources=[
            str(value).upper()
            for value in list(paper_allowed_execution_sources or ["REALTIME"])
            if str(value).strip()
        ],
        enforce_pretrade_risk_gate=bool(enforce_pretrade_risk_gate),
        mid=adapted_mid_cfg,
        risk=trade_risk_cfg,
    )


def _run_startup_self_check(
    storage: Storage,
    *,
    market_code: str,
    runtime_mode: str,
    execution_mode: str,
    borrow_fee_file: str,
    borrow_fee_bps: Dict[str, float],
    borrow_fee_sources: Dict[str, str],
    short_data_sources: List[Dict[str, Any]],
    short_safety_file: str,
    short_safety_raw: Dict[str, Any],
    strategy_cfg: StrategyConfig,
    paper_min_qty_floor_enabled: bool,
    paper_min_order_qty: float,
    entry_timeout_sec: int,
) -> List[str]:
    notes: List[str] = []
    warnings: List[str] = []

    market_label = market_code or "DEFAULT"
    mode_label = str(runtime_mode or "").strip().lower() or "unknown"
    exec_label = str(execution_mode or "").strip().lower() or "intraday"
    notes.append(f"market={market_label}")
    notes.append(f"mode={mode_label}")
    notes.append(f"execution_mode={exec_label}")
    notes.append(f"base_qty={float(strategy_cfg.base_qty):.2f}")
    notes.append(f"trade_threshold={float(strategy_cfg.trade_threshold):.3f}")
    notes.append(f"short_threshold={float(strategy_cfg.short_threshold):.3f}")

    known_borrow = sum(
        1
        for source in borrow_fee_sources.values()
        if str(source or "").strip() and not str(source or "").lower().startswith("unknown:")
    )
    notes.append(f"borrow_fee_file={borrow_fee_file or 'none'}")
    notes.append(f"borrow_fee_symbols={len(borrow_fee_bps)}")
    notes.append(f"borrow_fee_known_sources={known_borrow}")

    locate_count = len(dict(short_safety_raw.get("locate_status", {}) or {}))
    ssr_count = len(dict(short_safety_raw.get("ssr_status", {}) or {}))
    spread_count = len(dict(short_safety_raw.get("spread_bps", {}) or {}))
    uptick_count = sum(1 for value in dict(short_safety_raw.get("has_uptick_data", {}) or {}).values() if bool(value))
    notes.append(f"short_safety_file={short_safety_file or 'none'}")
    notes.append(f"short_safety_locate={locate_count}")
    notes.append(f"short_safety_ssr={ssr_count}")
    notes.append(f"short_safety_spread={spread_count}")
    notes.append(f"short_safety_uptick={uptick_count}")
    notes.append(f"paper_min_qty_floor={'on' if paper_min_qty_floor_enabled else 'off'}")
    notes.append(f"paper_min_order_qty={float(paper_min_order_qty):.2f}")
    notes.append(f"entry_timeout_sec={int(entry_timeout_sec)}")
    notes.append(f"paper_allowed_execution_sources={','.join(strategy_cfg.paper_allowed_execution_sources) or 'none'}")

    enabled_short_data_sources = sum(1 for item in short_data_sources if bool(item.get("enabled", False)))
    notes.append(f"short_data_sources_enabled={enabled_short_data_sources}")

    intraday_enabled = exec_label == "intraday"

    if borrow_fee_file and not borrow_fee_bps and intraday_enabled:
        warnings.append("borrow_fee_file_loaded_but_no_values")
    if short_safety_file and (locate_count + ssr_count + spread_count) <= 0 and intraday_enabled:
        warnings.append("short_safety_file_loaded_but_no_rules")
    if intraday_enabled and mode_label == "paper" and not bool(short_safety_raw.get("shadow_mode", False)):
        warnings.append("paper_shadow_mode_disabled")
    if intraday_enabled and mode_label == "paper" and not paper_min_qty_floor_enabled:
        warnings.append("paper_min_qty_floor_disabled")
    if intraday_enabled and mode_label == "paper" and float(strategy_cfg.base_qty) < 1.0 and not paper_min_qty_floor_enabled:
        warnings.append("paper_base_qty_lt_one_without_floor")
    if intraday_enabled and mode_label == "paper" and "REALTIME" not in set(strategy_cfg.paper_allowed_execution_sources):
        warnings.append("paper_realtime_execution_disabled")
    if intraday_enabled and bool(short_safety_raw.get("require_borrow_data", False)) and enabled_short_data_sources <= 0 and not borrow_fee_bps:
        warnings.append("short_borrow_real_data_missing")

    kind = "STARTUP_SELF_CHECK_WARN" if warnings else "STARTUP_SELF_CHECK_OK"
    detail = " ".join(notes + ([f"warnings={','.join(warnings)}"] if warnings else ["warnings=none"]))
    storage.insert_risk_event(kind, float(len(warnings)), detail)
    if warnings:
        log.warning("startup self-check warnings: %s", ", ".join(warnings))
    else:
        log.info("startup self-check passed")
    return warnings


def run_intraday_engine(
    base_dir: Path,
    *,
    market_code: str,
    ibkr_config_arg: str,
    startup_check_only: bool = False,
) -> None:
    ibkr_cfg_path = resolve_ibkr_config_path(base_dir, market_code, ibkr_config_arg)
    ibkr_cfg = load_yaml(base_dir, ibkr_cfg_path)
    market_universe_cfg = load_market_universe_config(base_dir, market_code)
    risk_cfg = load_yaml(base_dir, str(ibkr_cfg.get("risk_config", "config/risk.yaml")))
    strat_cfg_raw = load_yaml(base_dir, str(ibkr_cfg.get("strategy_config", "config/strategy_defaults.yaml")))
    regime_adaptor_cfg_raw = load_yaml(base_dir, str(ibkr_cfg.get("regime_adaptor_config", "config/regime_adaptor.yaml")))
    runtime_mode = str(ibkr_cfg.get("mode", "") or "").strip().lower()
    execution_mode = str(ibkr_cfg.get("execution_mode", "intraday") or "intraday").strip().lower()
    paper_mode = runtime_mode == "paper"

    host = ibkr_cfg["host"]
    port = int(ibkr_cfg["port"])
    client_id = int(ibkr_cfg["client_id"])
    account_id = ibkr_cfg["account_id"]
    runtime_scope = scope_from_ibkr_config(ibkr_cfg)
    runtime_db_path = resolve_scoped_runtime_path(base_dir, "audit.db", runtime_scope)

    storage = Storage(str(runtime_db_path))
    log.info(
        "Runtime storage scope: mode=%s execution_mode=%s account_id=%s db=%s",
        runtime_scope.mode,
        runtime_scope.execution_mode,
        runtime_scope.account_id,
        runtime_db_path,
    )

    daily_loss_limit_short_pct = risk_cfg.get("daily_loss_limit_short_pct", None)
    legacy = risk_cfg.get("daily_loss_limit_short", None)
    if daily_loss_limit_short_pct is None:
        if isinstance(legacy, (int, float)) and abs(float(legacy)) <= 1:
            daily_loss_limit_short_pct = float(legacy)
        else:
            daily_loss_limit_short_pct = -0.01
    max_consecutive_losses = int(risk_cfg.get("max_consecutive_losses", 5))
    risk_context_raw = dict(risk_cfg.get("risk_context") or {})
    short_borrow_fee_bps, short_borrow_fee_sources, short_borrow_fee_file = _load_runtime_borrow_fee_maps(
        base_dir,
        storage,
        risk_context_raw,
    )
    risk_context_raw["short_borrow_fee_bps"] = short_borrow_fee_bps
    risk_context_raw["short_borrow_fee_sources"] = short_borrow_fee_sources
    short_safety_raw, short_safety_file = _load_runtime_short_safety_rules(
        base_dir,
        storage,
        risk_cfg.get("short_safety") or {},
    )

    trade_risk_cfg = TradeRiskConfig.from_dict(risk_cfg.get("trade_risk"))
    preview_mid_cfg = RegimeConfig(**(strat_cfg_raw.get("mid_regime", {}) or {}))
    paper_allowed_execution_sources = list(ibkr_cfg.get("paper_allowed_execution_sources", ["REALTIME"]) or ["REALTIME"])
    entry_timeout_sec = int(ibkr_cfg.get("entry_timeout_sec", 60 if paper_mode else 30))
    strategy_cfg = _build_strategy_config(
        strat_cfg_raw,
        preview_mid_cfg,
        trade_risk_cfg,
        runtime_mode=runtime_mode,
        paper_allowed_execution_sources=paper_allowed_execution_sources,
        enforce_pretrade_risk_gate=bool(ibkr_cfg.get("enforce_pretrade_risk_gate", True)),
    )
    paper_min_qty_floor_enabled = paper_mode and bool(risk_cfg.get("paper_enable_min_order_qty_floor", True))
    paper_min_order_qty = float(risk_cfg.get("paper_min_order_qty", 1.0))
    _run_startup_self_check(
        storage,
        market_code=market_code,
        runtime_mode=runtime_mode,
        execution_mode=execution_mode,
        borrow_fee_file=short_borrow_fee_file,
        borrow_fee_bps=short_borrow_fee_bps,
        borrow_fee_sources=short_borrow_fee_sources,
        short_data_sources=list(risk_cfg.get("short_data_sources", []) or []),
        short_safety_file=short_safety_file,
        short_safety_raw=short_safety_raw,
        strategy_cfg=strategy_cfg,
        paper_min_qty_floor_enabled=paper_min_qty_floor_enabled,
        paper_min_order_qty=paper_min_order_qty,
        entry_timeout_sec=entry_timeout_sec,
    )
    if startup_check_only:
        return
    if execution_mode != "intraday":
        storage.insert_risk_event(
            "INTRADAY_ENGINE_DISABLED",
            0.0,
            f"market={market_code or 'DEFAULT'} execution_mode={execution_mode}",
        )
        log.info(
            "Intraday engine disabled by config: market=%s execution_mode=%s",
            market_code or "DEFAULT",
            execution_mode,
        )
        return

    conn = IBKRConnection(host, port, client_id)
    ib = conn.connect()
    ib.RequestTimeout = 5

    account = AccountService(ib, account_id)
    account.start()

    gate = DailyRiskGate(
        storage=storage,
        account=account,
        daily_loss_limit_short_pct=float(daily_loss_limit_short_pct),
        max_consecutive_losses=max_consecutive_losses,
        context_cfg=RiskContextConfig.from_dict(risk_context_raw),
        providers=EnrichmentProviders(),
        market=market_code,
    )
    short_safety_gate = ShortSafetyGate(
        ShortSafetyConfig.from_dict(short_safety_raw, market=market_code),
        context=gate,
    )

    _fills = FillProcessor(ib, storage, gate)

    md = MarketDataService(ib)
    orders = OrderService(ib, account_id, storage)
    entry_guard = EntryGuard(ib, GuardConfig())
    allocator = PortfolioAllocator(
        ib,
        account,
        AllocatorConfig(
            risk_per_trade=float(risk_cfg.get("risk_per_trade", 0.002)),
            max_open_positions=int(risk_cfg.get("max_open_positions", 8)),
            max_gross_leverage=float(risk_cfg.get("max_gross_leverage", 1.2)),
            enable_min_order_qty_floor=paper_min_qty_floor_enabled,
            min_order_qty=float(paper_min_order_qty),
        ),
    )

    runner = Runner(
        ib,
        RunnerConfig(entry_timeout_sec=entry_timeout_sec),
        account=account,
    )

    symbol_master_db = str(market_universe_cfg.get("symbol_master_db", "symbol_master.db"))
    symbol_master_path = resolve_path(base_dir, symbol_master_db)
    symbol_master_symbols = load_symbols_from_symbol_master(symbol_master_path, market_code)
    if symbol_master_symbols:
        log.info(
            "Loaded live symbol master: market=%s path=%s symbols=%s",
            market_code or "DEFAULT",
            symbol_master_path,
            len(symbol_master_symbols),
        )
    seed_symbols = list(symbol_master_symbols or market_universe_cfg.get("seed_symbols", ibkr_cfg.get("seed_symbols", ["SPY"])))
    watchlist_yaml = market_universe_cfg.get("seed_watchlist_yaml", ibkr_cfg.get("seed_watchlist_yaml", ""))
    if market_code:
        log.info("Using market universe config: %s", base_dir / "config" / "markets" / market_code.lower() / "universe.yaml")
    if watchlist_yaml and not symbol_master_symbols:
        watchlist_path = resolve_path(base_dir, str(watchlist_yaml))
        watchlist_symbols = load_watchlist_symbols(str(watchlist_path))
        if watchlist_symbols:
            log.info(
                "Loaded seed watchlist: market=%s config=%s path=%s symbols=%s",
                market_code or "DEFAULT",
                ibkr_cfg_path,
                watchlist_path,
                len(watchlist_symbols),
            )
            seed_symbols.extend(watchlist_symbols)

    seed_symbols = list(dict.fromkeys(str(sym).upper() for sym in seed_symbols))
    live_candidate_target = max(int(ibkr_cfg.get("max_short_candidates", 15)), len(seed_symbols))

    universe = UniverseService(
        ib,
        UniverseConfig(
            scanner_enabled=bool(ibkr_cfg.get("scanner_enabled", True)),
            scanner_instrument=str(ibkr_cfg.get("scanner_instrument", "STK")),
            scanner_location_codes=scanner_location_codes_from_config(ibkr_cfg, default="STK.US.MAJOR"),
            scanner_location_code=str(ibkr_cfg.get("scanner_location_code", "STK.US.MAJOR")),
            seed_symbols=seed_symbols,
            seed_batch_enabled=bool(ibkr_cfg.get("seed_batch_enabled", True)),
            seed_batch_size=int(ibkr_cfg.get("seed_batch_size", 40)),
            seed_rotation_sec=int(ibkr_cfg.get("seed_rotation_sec", 300)),
            max_short_candidates=live_candidate_target,
            recent_trade_limit=int(ibkr_cfg.get("recent_trade_limit", 30)),
            scanner_refresh_sec=int(ibkr_cfg.get("scanner_refresh_sec", 120)),
            scanner_max_codes_per_run=int(ibkr_cfg.get("scanner_max_codes_per_run", 3)),
            scanner_limit=int(ibkr_cfg.get("scanner_limit", 20)),
            scanner_codes=list(ibkr_cfg.get("scanner_codes", ["HOT_BY_VOLUME", "TOP_PERC_GAIN", "TOP_PERC_LOSE"])),
            cooldown_enabled=bool(ibkr_cfg.get("cooldown_enabled", True)),
            cooldown_minutes=int(ibkr_cfg.get("cooldown_minutes", 30)),
            dup_per_bucket_threshold=float(ibkr_cfg.get("dup_per_bucket_threshold", 10.0)),
            max_gap_sec_threshold=int(ibkr_cfg.get("max_gap_sec_threshold", 1800)),
            min_buckets_for_eval=int(ibkr_cfg.get("min_buckets_for_eval", 1)),
            phase3_enabled=bool(ibkr_cfg.get("phase3_enabled", True)),
            phase3_lookback_bars=int(ibkr_cfg.get("phase3_lookback_bars", 48)),
            phase3_price_min=float(ibkr_cfg.get("phase3_price_min", 2.0)),
            phase3_avg_vol_min=float(ibkr_cfg.get("phase3_avg_vol_min", 50_000.0)),
            phase3_atr_pct_min=float(ibkr_cfg.get("phase3_atr_pct_min", 0.002)),
            phase3_volume_log_weight=float(ibkr_cfg.get("phase3_volume_log_weight", 1.0)),
            phase3_atr_pct_weight=float(ibkr_cfg.get("phase3_atr_pct_weight", 100.0)),
            phase3_price_bonus_weight=float(ibkr_cfg.get("phase3_price_bonus_weight", 0.0)),
            phase3_price_bonus_ref=float(ibkr_cfg.get("phase3_price_bonus_ref", 20.0)),
            phase3_repeat_halflife_min=float(ibkr_cfg.get("phase3_repeat_halflife_min", 60.0)),
            phase3_repeat_penalty=float(ibkr_cfg.get("phase3_repeat_penalty", 0.25)),
            phase3_bad_symbol_cooldown_min=int(ibkr_cfg.get("phase3_bad_symbol_cooldown_min", 30)),
        ),
        storage=storage,
        md=md,
    )

    mid_cfg = RegimeConfig(**(strat_cfg_raw.get("mid_regime", {}) or {}))
    regime_adaptor = RegimeAdaptor(
        market=market_code or "DEFAULT",
        base_cfg=mid_cfg,
        adapt_cfg=RegimeAdaptConfig.from_dict(regime_adaptor_cfg_raw.get("regime_adaptor")),
    )
    adapted_mid_cfg = regime_adaptor.refresh_if_due(md, storage=storage, force=True)
    strategy_cfg = _build_strategy_config(
        strat_cfg_raw,
        adapted_mid_cfg,
        trade_risk_cfg,
        runtime_mode=runtime_mode,
        paper_allowed_execution_sources=paper_allowed_execution_sources,
        enforce_pretrade_risk_gate=bool(ibkr_cfg.get("enforce_pretrade_risk_gate", True)),
    )
    strategy = EngineStrategy(
        orders=orders,
        gate=gate,
        cfg=strategy_cfg,
        audit_writer=SignalAuditWriter(storage),
    )
    executor = SignalExecutor(
        orders=orders,
        cfg=strategy_cfg,
        entry_guard=entry_guard,
        allocator=allocator,
        short_safety_gate=short_safety_gate,
    )

    engine = TradingEngine(
        ib=ib,
        universe_svc=universe,
        strategy=strategy,
        runner=runner,
        cfg=EngineConfig(),
        md=md,
        regime_adaptor=regime_adaptor,
        executor=executor,
        storage=storage,
    )

    log.info("Starting engine...")
    engine.run_forever()
