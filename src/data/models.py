from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict


@dataclass
class BarData:
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class QuoteSnapshot:
    symbol: str
    price: float
    source: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
