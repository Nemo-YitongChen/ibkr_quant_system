from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any, Dict, Optional

import yaml

MARKET_ALIASES: Dict[str, str] = {
    "US": "US",
    "USA": "US",
    "CN": "CN",
    "CHN": "CN",
    "SSE": "CN",
    "SZSE": "CN",
    "AU": "ASX",
    "AUS": "ASX",
    "ASX": "ASX",
    "HK": "HK",
    "HKG": "HK",
    "SEHK": "HK",
    "DE": "XETRA",
    "GER": "XETRA",
    "XETRA": "XETRA",
    "IBIS": "XETRA",
    "UK": "UK",
    "LSE": "UK",
}

MARKET_TIMEZONES: Dict[str, str] = {
    "US": "America/New_York",
    "CN": "Asia/Shanghai",
    "ASX": "Australia/Sydney",
    "HK": "Asia/Hong_Kong",
    "UK": "Europe/London",
    "XETRA": "Europe/Berlin",
}


def resolve_market_code(code: str | None) -> str:
    raw = str(code or "").strip().upper()
    if not raw:
        return ""
    return MARKET_ALIASES.get(raw, raw)


def market_timezone_name(code: str | None, fallback: str = "UTC") -> str:
    resolved = resolve_market_code(code)
    return MARKET_TIMEZONES.get(resolved, fallback)


def market_config_path(base_dir: Path, market: str | None, explicit_path: str | None = None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if path.is_absolute():
            return path
        for candidate in (base_dir / path, base_dir / "config" / path, Path.cwd() / path, Path.cwd() / "config" / path):
            if candidate.exists():
                return candidate.resolve()
        return (base_dir / path).resolve()

    code = resolve_market_code(market)
    if not code:
        return (base_dir / "config" / "ibkr.yaml").resolve()
    return (base_dir / "config" / f"ibkr_{code.lower()}.yaml").resolve()


def market_dir(base_dir: Path, market: str | None) -> Path:
    code = resolve_market_code(market)
    if not code:
        return (base_dir / "config" / "markets" / "default").resolve()
    return (base_dir / "config" / "markets" / code.lower()).resolve()


def market_universe_config_path(base_dir: Path, market: str | None) -> Path:
    return (market_dir(base_dir, market) / "universe.yaml").resolve()


def load_market_universe_config(base_dir: Path, market: str | None) -> Dict[str, Any]:
    path = market_universe_config_path(base_dir, market)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def infer_market_from_config_path(path: str | Path) -> str:
    stem = Path(path).stem.lower()
    if stem.startswith("ibkr_"):
        return resolve_market_code(stem.split("_", 1)[1])
    return ""


def symbol_matches_market(symbol: str, market: str | None) -> bool:
    code = resolve_market_code(market)
    sym = str(symbol or "").upper().strip()
    if not sym or not code:
        return True
    is_hk = sym.endswith(".HK") or sym.startswith("HK:")
    is_cn = (
        sym.endswith(".SS")
        or sym.endswith(".SZ")
        or sym.startswith("CN:")
        or sym.startswith("SSE:")
        or sym.startswith("SZSE:")
    )
    is_xetra = (
        sym.endswith(".DE")
        or sym.endswith(".XETRA")
        or sym.startswith("DE:")
        or sym.startswith("XETRA:")
    )
    is_uk = sym.endswith(".L") or sym.startswith("UK:") or sym.startswith("LSE:")
    is_asx = sym.endswith(".AX") or sym.startswith("ASX:") or sym.startswith("AU:")
    if code == "CN":
        return is_cn
    if code == "HK":
        return is_hk
    if code == "ASX":
        return is_asx
    if code == "XETRA":
        return is_xetra
    if code == "UK":
        return is_uk
    if code == "US":
        return not (is_cn or is_hk or is_xetra or is_uk or is_asx)
    return True


def load_symbols_from_symbol_master(db_path: str | Path, market: str | None) -> list[str]:
    code = resolve_market_code(market)
    if not code:
        return []
    try:
        c = sqlite3.connect(str(db_path))
        try:
            rows = c.execute(
                "select symbol from symbol_master where market=? order by symbol asc",
                (code,),
            ).fetchall()
            return [str(r[0]).upper() for r in rows if r and r[0]]
        finally:
            c.close()
    except Exception:
        return []


def add_market_args(parser) -> None:
    parser.add_argument("--market", default="", help="Market code such as US, CN, HK, ASX, XETRA, or UK.")
    parser.add_argument("-US", dest="market", action="store_const", const="US", help="Shortcut for --market US")
    parser.add_argument("-CN", dest="market", action="store_const", const="CN", help="Shortcut for --market CN")
    parser.add_argument("-AU", dest="market", action="store_const", const="ASX", help="Shortcut for --market ASX")
    parser.add_argument("-ASX", dest="market", action="store_const", const="ASX", help="Shortcut for --market ASX")
    parser.add_argument("-HK", dest="market", action="store_const", const="HK", help="Shortcut for --market HK")
    parser.add_argument("-DE", dest="market", action="store_const", const="XETRA", help="Shortcut for --market XETRA")
    parser.add_argument("-XETRA", dest="market", action="store_const", const="XETRA", help="Shortcut for --market XETRA")
    parser.add_argument("-UK", dest="market", action="store_const", const="UK", help="Shortcut for --market UK")
