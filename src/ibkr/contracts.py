from __future__ import annotations

"""Contract factory helpers."""

from dataclasses import dataclass

from ib_insync import Stock  # type: ignore


@dataclass(frozen=True)
class StockSpec:
    symbol: str
    exchange: str
    currency: str


def parse_stock_spec(raw: str, default_exchange: str = "SMART", default_currency: str = "USD") -> StockSpec:
    s = (raw or "").strip().upper()
    if not s:
        return StockSpec(symbol=s, exchange=default_exchange, currency=default_currency)

    if s.startswith("SSE:") or s.startswith("CN:"):
        code = s.split(":", 1)[1].strip()
        code = code.split(".", 1)[0].strip()
        if len(code) == 6 and code.isdigit():
            # A 股在 IBKR 里并不是 SMART/USD；这里优先按港交所互联互通映射。
            # 688/689 属于科创板，普通沪市股票走 SEHKNTL。
            exchange = "SEHKSTAR" if code.startswith(("688", "689")) else "SEHKNTL"
            return StockSpec(symbol=code, exchange=exchange, currency="CNH")

    if s.startswith("SZSE:"):
        code = s.split(":", 1)[1].strip()
        code = code.split(".", 1)[0].strip()
        if len(code) == 6 and code.isdigit():
            return StockSpec(symbol=code, exchange="SEHKSZSE", currency="CNH")

    if s.startswith("HK:"):
        code = s.split(":", 1)[1].strip()
        code = code.split(".", 1)[0].strip()
        code = code.lstrip("0") or "0"
        return StockSpec(symbol=code, exchange="SEHK", currency="HKD")

    if s.endswith(".HK"):
        code = s[:-3].strip()
        code = code.lstrip("0") or "0"
        return StockSpec(symbol=code, exchange="SEHK", currency="HKD")

    if s.startswith("ASX:") or s.startswith("AU:"):
        code = s.split(":", 1)[1].strip()
        code = code.split(".", 1)[0].strip()
        return StockSpec(symbol=code, exchange="ASX", currency="AUD")

    if s.endswith(".AX"):
        code = s[:-3].strip()
        return StockSpec(symbol=code, exchange="ASX", currency="AUD")

    if s.endswith(".SS"):
        code = s[:-3].strip()
        if len(code) == 6 and code.isdigit():
            exchange = "SEHKSTAR" if code.startswith(("688", "689")) else "SEHKNTL"
            return StockSpec(symbol=code, exchange=exchange, currency="CNH")

    if s.endswith(".SZ"):
        code = s[:-3].strip()
        if len(code) == 6 and code.isdigit():
            return StockSpec(symbol=code, exchange="SEHKSZSE", currency="CNH")

    if s.startswith("XETRA:") or s.startswith("DE:"):
        code = s.split(":", 1)[1].strip()
        code = code.split(".", 1)[0].strip()
        return StockSpec(symbol=code, exchange="IBIS", currency="EUR")

    if s.endswith(".DE") or s.endswith(".XETRA"):
        code = s.rsplit(".", 1)[0].strip()
        return StockSpec(symbol=code, exchange="IBIS", currency="EUR")

    if s.startswith("LSE:") or s.startswith("UK:"):
        code = s.split(":", 1)[1].strip()
        code = code.split(".", 1)[0].strip()
        return StockSpec(symbol=code, exchange="LSE", currency="GBP")

    if s.endswith(".L"):
        code = s[:-2].strip()
        return StockSpec(symbol=code, exchange="LSE", currency="GBP")

    # IBKR expects many US share-class symbols as "BRK B" rather than "BRK.B".
    if "." in s:
        base, suffix = s.rsplit(".", 1)
        if base and len(suffix) == 1 and suffix.isalpha():
            return StockSpec(symbol=f"{base} {suffix}", exchange=default_exchange, currency=default_currency)

    return StockSpec(symbol=s, exchange=default_exchange, currency=default_currency)


def make_stock_contract(raw: str, default_exchange: str = "SMART", default_currency: str = "USD") -> Stock:
    spec = parse_stock_spec(raw, default_exchange=default_exchange, default_currency=default_currency)
    return Stock(spec.symbol, spec.exchange, spec.currency)
