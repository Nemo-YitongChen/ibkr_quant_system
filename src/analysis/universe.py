from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Set


@dataclass
class UniverseResult:
    """Candidate pool plus per-symbol metadata for explainability."""

    symbols: List[str]
    meta: Dict[str, Dict[str, Any]]


def build_candidates(
    *,
    seed_symbols: List[str],
    recent_symbols: List[str],
    scanner_symbols: List[str],
    blacklist: Set[str],
    max_n: int = 60,
) -> UniverseResult:
    meta: Dict[str, Dict[str, Any]] = {}
    ordered: List[str] = []

    def add(sym: str, reason: str) -> None:
        s = str(sym).upper().strip()
        if not s or s in blacklist:
            return
        if s not in meta:
            meta[s] = {"reasons": []}
            ordered.append(s)
        meta[s]["reasons"].append(reason)

    for s in seed_symbols:
        add(s, "seed")
    for s in recent_symbols:
        add(s, "recent")
    for s in scanner_symbols:
        add(s, "scanner")

    return UniverseResult(symbols=ordered[:max_n], meta=meta)
