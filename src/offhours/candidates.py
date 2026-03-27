from __future__ import annotations

import os
import sqlite3
from typing import List, Dict, Any

import yaml


def dedupe_keep_order(xs: List[str]) -> List[str]:
    """De-duplicate symbols but preserve the first-seen order."""
    seen = set()
    out: List[str] = []
    for x in xs:
        s = str(x).upper().strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_watchlist_symbols(path: str) -> List[str]:
    """Load a simple watchlist YAML: {symbols: [SPY, TSLA, ...]}"""
    if not path or not os.path.exists(path):
        return []
    cfg = load_yaml(path) or {}
    syms = cfg.get("symbols") or []
    if not isinstance(syms, list):
        return []
    return [str(s) for s in syms]


def read_recent_symbols_from_audit(db_path: str, limit: int = 500) -> List[str]:
    """Read recently-seen symbols from signals_audit to seed the next day's candidate pool.

    We keep this best-effort: if the table isn't present yet, return [].
    """
    if not os.path.exists(db_path):
        return []
    c = sqlite3.connect(db_path)
    try:
        rows = c.execute(
            "select symbol, max(ts) as mts from signals_audit group by symbol order by mts desc limit ?",
            (int(limit),),
        ).fetchall()
        return [r[0] for r in rows if r and r[0]]
    except Exception:
        return []
    finally:
        c.close()


def build_candidate_symbols(
    *,
    db_path: str,
    watchlist_yaml: str | None,
    default_symbols: List[str],
    audit_limit: int = 500,
) -> List[str]:
    """Return the final candidate pool (deduped, uppercase)."""
    base: List[str] = []
    if watchlist_yaml:
        base.extend(load_watchlist_symbols(watchlist_yaml))
    base.extend(read_recent_symbols_from_audit(db_path, limit=audit_limit))
    base.extend(default_symbols)
    return dedupe_keep_order(base)
