from __future__ import annotations

from typing import List, Tuple
import time

from ib_insync import Stock  # type: ignore

from ..ibkr.contracts import make_stock_contract
from ..ibkr.connection import IBKRConnection
from ..ibkr.market_data import MarketDataService
from ..common.logger import get_logger

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
