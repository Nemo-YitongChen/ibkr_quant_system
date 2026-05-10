from __future__ import annotations

from typing import Any, List, Mapping
import time

from ib_insync import Stock  # type: ignore

try:
    from ..ibkr.contracts import make_stock_contract
    from ..ibkr.connection import IBKRConnection
    from ..ibkr.market_data import MarketDataService
    from ..common.logger import get_logger
except ImportError:
    from ibkr.contracts import make_stock_contract
    from ibkr.connection import IBKRConnection
    from ibkr.market_data import MarketDataService
    from common.logger import get_logger

log = get_logger("offhours.ib_setup")


def get_net_liquidation(ib, account_id: str) -> float:
    """Best-effort NetLiquidation from IB accountSummary."""
    rows = []
    try:
        rows = ib.accountSummary(account_id)
    except Exception:
        try:
            rows = ib.accountSummary()
        except Exception:
            rows = []

    for r in rows:
        if getattr(r, "tag", "") != "NetLiquidation":
            continue
        if getattr(r, "account", account_id) not in ("", account_id):
            continue
        try:
            return float(getattr(r, "value", "0") or 0.0)
        except Exception:
            return 0.0
    return 0.0


def connect_ib(host: str, port: int, client_id: int, request_timeout: float = 5.0):
    conn = IBKRConnection(host, port, client_id)
    ib = conn.connect()
    # Keep requests from blocking too long in batch scripts
    ib.RequestTimeout = request_timeout
    return ib


def set_delayed_frozen(ib) -> None:
    """Prefer delayed-frozen market data for offhours scripts.

    This avoids failing when you do not have real-time market data subscriptions.
    """
    try:
        ib.reqMarketDataType(4)  # 4=Delayed-Frozen
    except Exception:
        pass


def _market_data_config_value(ibkr_cfg: Mapping[str, Any] | None, key: str, default: Any) -> Any:
    raw = ibkr_cfg if isinstance(ibkr_cfg, Mapping) else {}
    nested = raw.get("market_data")
    market_data_cfg = nested if isinstance(nested, Mapping) else {}
    if key in market_data_cfg:
        return market_data_cfg.get(key)
    return raw.get(key, default)


def _config_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _config_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _config_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def market_data_service_from_config(
    ib,
    ibkr_cfg: Mapping[str, Any] | None = None,
) -> MarketDataService:
    """Build MarketDataService with market-level cache and retry overrides.

    Prefer nested ``market_data`` keys but keep top-level compatibility so older
    YAML files can opt into the same knobs without a structural migration.
    """
    kwargs: dict[str, Any] = {
        "request_timeout_sec": _config_float(_market_data_config_value(ibkr_cfg, "request_timeout_sec", 12.0), 12.0),
        "use_rth": _config_bool(_market_data_config_value(ibkr_cfg, "use_rth", False), False),
        "what_to_show": str(_market_data_config_value(ibkr_cfg, "what_to_show", "TRADES") or "TRADES"),
        "hist_keep_up_to_date": _config_bool(_market_data_config_value(ibkr_cfg, "hist_keep_up_to_date", False), False),
        "hist_retry_attempts": _config_int(_market_data_config_value(ibkr_cfg, "hist_retry_attempts", 2), 2),
        "hist_retry_backoff_sec": _config_float(_market_data_config_value(ibkr_cfg, "hist_retry_backoff_sec", 1.5), 1.5),
        "hist_5m_cache_ttl_sec": _config_int(_market_data_config_value(ibkr_cfg, "hist_5m_cache_ttl_sec", 90), 90),
        "hist_5m_cache_stale_fallback_sec": _config_int(
            _market_data_config_value(ibkr_cfg, "hist_5m_cache_stale_fallback_sec", 900),
            900,
        ),
        "hist_daily_cache_ttl_sec": _config_int(
            _market_data_config_value(ibkr_cfg, "hist_daily_cache_ttl_sec", 21600),
            21600,
        ),
        "hist_daily_cache_stale_fallback_sec": _config_int(
            _market_data_config_value(ibkr_cfg, "hist_daily_cache_stale_fallback_sec", 604800),
            604800,
        ),
    }
    hist_cache_dir = _market_data_config_value(ibkr_cfg, "hist_cache_dir", "")
    hist_daily_cache_dir = _market_data_config_value(ibkr_cfg, "hist_daily_cache_dir", "")
    if str(hist_cache_dir or "").strip():
        kwargs["hist_cache_dir"] = str(hist_cache_dir)
    if str(hist_daily_cache_dir or "").strip():
        kwargs["hist_daily_cache_dir"] = str(hist_daily_cache_dir)
    return MarketDataService(ib, **kwargs)


def register_contracts(ib, md: MarketDataService, symbols: List[str]) -> None:
    """Qualify and register contracts with MarketDataService.

    MarketDataService.get_5m_bars() in this project expects contracts to be registered first;
    otherwise it can raise KeyError.
    """
    if not symbols:
        return
    symbol_pairs = [(str(sym), make_stock_contract(sym)) for sym in symbols]
    contracts = [contract for _raw, contract in symbol_pairs]
    try:
        ib.qualifyContracts(*contracts)
    except Exception:
        # Even if qualify fails, we still attempt to register by symbol.
        pass

    for raw_symbol, contract in symbol_pairs:
        try:
            # Register both the caller-facing symbol (e.g. 0700.HK) and the normalized IB symbol (e.g. 700),
            # but avoid duplicate aliases such as SPY -> SPY.
            aliases = {
                str(raw_symbol).upper(),
                str(getattr(contract, "symbol", raw_symbol)).upper(),
            }
            for alias in sorted(a for a in aliases if a):
                md.register(alias, contract)
        except Exception:
            # If md doesn't expose register, the script will fall back to direct reqHistoricalData
            pass
