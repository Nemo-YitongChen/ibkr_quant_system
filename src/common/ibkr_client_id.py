from __future__ import annotations

from collections.abc import Mapping
from typing import Any

IBKR_CLIENT_ID_OFFSET_ENV = "IBKR_CLIENT_ID_OFFSET"
IBKR_CLIENT_ID_OVERRIDE_ENV = "IBKR_CLIENT_ID_OVERRIDE"
IBKR_CLIENT_ID_RETRY_SPAN_ENV = "IBKR_CLIENT_ID_RETRY_SPAN"
IBKR_CONNECT_MAX_ROUNDS_ENV = "IBKR_CONNECT_MAX_ROUNDS"

DEFAULT_CLIENT_ID_RETRY_SPAN = 1
DEFAULT_CONNECT_MAX_ROUNDS = 3

_TASK_CLIENT_ID_OFFSETS = {
    "generate_investment_report:": 1000,
    "generate_trade_report:": 1100,
    "sync_investment_broker_snapshot:": 1200,
    "run_investment_opportunity:": 1300,
    "run_investment_execution:": 1400,
    "run_investment_guard:": 1500,
    "short_safety_sync:": 1600,
    "label_investment_snapshots:": 1700,
}


def _int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _base_client_id(value: Any, *, default: int = 1) -> int:
    if isinstance(value, Mapping):
        raw = value.get("client_id", default)
    else:
        raw = value
    parsed = _int_value(raw)
    if parsed is None:
        return int(default)
    return parsed


def resolve_ibkr_client_id(
    value: Any,
    *,
    env: Mapping[str, str] | None = None,
    default: int = 1,
) -> int:
    """Resolve the effective IBKR clientId from config plus supervisor env.

    Normal scripts keep the configured client_id. Supervisor-launched scripts can
    supply a bounded offset so report/guard/execution subprocesses do not reuse
    the same market clientId while IB Gateway is still clearing stale sessions.
    """

    import os

    source_env = env if env is not None else os.environ
    override = _int_value(source_env.get(IBKR_CLIENT_ID_OVERRIDE_ENV))
    if override is not None and override > 0:
        return override

    base = _base_client_id(value, default=default)
    offset = _int_value(source_env.get(IBKR_CLIENT_ID_OFFSET_ENV))
    if offset is None:
        return base
    return base + offset


def resolve_ibkr_client_id_retry_span(
    *,
    env: Mapping[str, str] | None = None,
    default: int = DEFAULT_CLIENT_ID_RETRY_SPAN,
) -> int:
    import os

    source_env = env if env is not None else os.environ
    parsed = _int_value(source_env.get(IBKR_CLIENT_ID_RETRY_SPAN_ENV))
    if parsed is None:
        return max(1, int(default))
    return max(1, min(int(parsed), 10))


def resolve_ibkr_connect_max_rounds(
    *,
    env: Mapping[str, str] | None = None,
    default: int = DEFAULT_CONNECT_MAX_ROUNDS,
) -> int:
    import os

    source_env = env if env is not None else os.environ
    parsed = _int_value(source_env.get(IBKR_CONNECT_MAX_ROUNDS_ENV))
    if parsed is None:
        return max(1, int(default))
    return max(1, min(int(parsed), 20))


def ibkr_task_client_id_offset(task_name: str) -> int:
    normalized = str(task_name or "").strip().lower()
    for prefix, offset in _TASK_CLIENT_ID_OFFSETS.items():
        if normalized.startswith(prefix):
            return offset
    return 1900
