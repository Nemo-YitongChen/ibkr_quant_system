from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass
class MarketEvent:
    market: str
    symbol: str
    event_type: str
    event_ts: str
    exchange_ts: str = ""
    receive_ts: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SignalDecision:
    symbol: str
    market: str
    strategy: str
    long_score: float = 0.0
    short_score: float = 0.0
    total_score: float = 0.0
    regime_state: Dict[str, Any] = field(default_factory=dict)
    gates_passed: List[str] = field(default_factory=list)
    gates_blocked: List[str] = field(default_factory=list)
    action: str = ""
    reasons: List[str] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RiskDecision:
    symbol: str
    market: str
    allowed: bool
    sizing_result: Dict[str, Any] = field(default_factory=dict)
    block_reasons: List[str] = field(default_factory=list)
    reason_codes: List[str] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExecutionIntent:
    symbol: str
    market: str
    action: str
    current_qty: float = 0.0
    target_qty: float = 0.0
    delta_qty: float = 0.0
    target_weight: float = 0.0
    ref_price: float = 0.0
    order_value: float = 0.0
    status: str = "PLANNED"
    reasons: List[str] = field(default_factory=list)
    opportunity_status: str = ""
    opportunity_reason: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
