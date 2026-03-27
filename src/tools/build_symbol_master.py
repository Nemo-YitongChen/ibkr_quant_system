from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from ..common.logger import get_logger
from ..common.markets import add_market_args, load_market_universe_config, market_dir, resolve_market_code
from ..offhours.candidates import load_watchlist_symbols

log = get_logger("tools.build_symbol_master")
BASE_DIR = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build local symbol master from market universe configs.")
    add_market_args(ap)
    ap.add_argument("--all", action="store_true", help="Build symbol master for all configured markets.")
    ap.add_argument("--db", default="symbol_master.db", help="SQLite file to write.")
    return ap.parse_args()


def _resolve_db(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (BASE_DIR / path).resolve()


def _configured_markets() -> List[str]:
    root = BASE_DIR / "config" / "markets"
    if not root.exists():
        return []
    out: List[str] = []
    for d in sorted(root.iterdir()):
        if not d.is_dir() or d.name.startswith("_"):
            continue
        out.append(resolve_market_code(d.name))
    return out


def _universe_rows(market: str) -> List[Tuple[str, str, str, str]]:
    cfg = load_market_universe_config(BASE_DIR, market)
    if not cfg:
        return []

    asset_class = str(cfg.get("asset_class", "unknown"))
    market_name = str(cfg.get("name", market.lower()))
    rows: List[Tuple[str, str, str, str]] = []

    def add_symbols(symbols: Iterable[str], source: str) -> None:
        for sym in symbols:
            s = str(sym).upper().strip()
            if s:
                rows.append((market, s, asset_class, source))

    add_symbols(cfg.get("seed_symbols", []) or [], f"{market_name}:seed_symbols")
    add_symbols(cfg.get("symbol_master_symbols", []) or [], f"{market_name}:symbol_master_symbols")

    for key in ("seed_watchlist_yaml", "report_watchlist_yaml"):
        watchlist = str(cfg.get(key, "") or "")
        if not watchlist:
            continue
        syms = load_watchlist_symbols(str((BASE_DIR / watchlist).resolve()))
        add_symbols(syms, f"{market_name}:{key}")

    for watchlist in list(cfg.get("symbol_master_watchlists", []) or []):
        watchlist_path = str(watchlist or "").strip()
        if not watchlist_path:
            continue
        syms = load_watchlist_symbols(str((BASE_DIR / watchlist_path).resolve()))
        add_symbols(syms, f"{market_name}:symbol_master_watchlists")

    deduped = {}
    for row in rows:
        deduped[(row[0], row[1])] = row
    return list(deduped.values())


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        create table if not exists symbol_master(
            market text not null,
            symbol text not null,
            asset_class text not null,
            source text not null,
            updated_ts integer not null,
            primary key(market, symbol)
        )
        """
    )
    conn.execute("create index if not exists idx_symbol_master_market on symbol_master(market)")


def _write_market(conn: sqlite3.Connection, market: str, rows: List[Tuple[str, str, str, str]]) -> None:
    now = int(time.time())
    conn.execute("delete from symbol_master where market=?", (market,))
    conn.executemany(
        """
        insert into symbol_master(market, symbol, asset_class, source, updated_ts)
        values(?,?,?,?,?)
        """,
        [(m, s, ac, src, now) for (m, s, ac, src) in rows],
    )
    conn.commit()


def main() -> None:
    args = parse_args()
    db_path = _resolve_db(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if args.all:
        markets = _configured_markets()
    else:
        market = resolve_market_code(getattr(args, "market", ""))
        if not market:
            raise SystemExit("Specify a market like -US or use --all")
        markets = [market]

    conn = sqlite3.connect(str(db_path))
    try:
        _init_db(conn)
        for market in markets:
            rows = _universe_rows(market)
            _write_market(conn, market, rows)
            log.info("Loaded symbol master market=%s rows=%s source=%s", market, len(rows), market_dir(BASE_DIR, market) / "universe.yaml")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
