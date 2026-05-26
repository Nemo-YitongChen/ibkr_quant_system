from __future__ import annotations

import pytest

from src.common.ibkr_client_id import (
    IBKR_CLIENT_ID_OFFSET_ENV,
    IBKR_CLIENT_ID_OVERRIDE_ENV,
    IBKR_CLIENT_ID_RETRY_SPAN_ENV,
    IBKR_CONNECT_MAX_ROUNDS_ENV,
    ibkr_task_client_id_offset,
    resolve_ibkr_client_id,
    resolve_ibkr_client_id_retry_span,
    resolve_ibkr_connect_max_rounds,
)
from src.ibkr import connection as connection_module
from src.ibkr.connection import IBKRConnection


def test_resolve_ibkr_client_id_keeps_config_without_supervisor_env():
    assert resolve_ibkr_client_id({"client_id": "101"}, env={}) == 101


def test_resolve_ibkr_client_id_applies_supervisor_offset():
    assert resolve_ibkr_client_id(101, env={IBKR_CLIENT_ID_OFFSET_ENV: "1500"}) == 1601


def test_resolve_ibkr_client_id_override_wins_over_offset():
    env = {
        IBKR_CLIENT_ID_OFFSET_ENV: "1500",
        IBKR_CLIENT_ID_OVERRIDE_ENV: "999",
    }
    assert resolve_ibkr_client_id(101, env=env) == 999


def test_resolve_ibkr_client_id_retry_span_is_bounded():
    assert resolve_ibkr_client_id_retry_span(env={}) == 1
    assert resolve_ibkr_client_id_retry_span(env={IBKR_CLIENT_ID_RETRY_SPAN_ENV: "0"}) == 1
    assert resolve_ibkr_client_id_retry_span(env={IBKR_CLIENT_ID_RETRY_SPAN_ENV: "99"}) == 10


def test_resolve_ibkr_connect_max_rounds_is_bounded():
    assert resolve_ibkr_connect_max_rounds(env={}) == 3
    assert resolve_ibkr_connect_max_rounds(env={IBKR_CONNECT_MAX_ROUNDS_ENV: "0"}) == 1
    assert resolve_ibkr_connect_max_rounds(env={IBKR_CONNECT_MAX_ROUNDS_ENV: "99"}) == 20


def test_task_client_id_offset_is_stable_for_gateway_tasks():
    assert ibkr_task_client_id_offset("generate_investment_report:us:core") == 1000
    assert ibkr_task_client_id_offset("run_investment_guard:us:core") == 1500
    assert ibkr_task_client_id_offset("unknown_gateway_task:us") == 1900


def test_connection_retries_adjacent_client_id_after_first_failure(monkeypatch):
    class FakeIB:
        connect_client_ids: list[int] = []

        def __init__(self):
            self.connected = False

        def isConnected(self):
            return self.connected

        def connect(self, host, port, *, clientId, timeout):
            self.connect_client_ids.append(clientId)
            if len(self.connect_client_ids) == 1:
                raise RuntimeError("clientId already in use")
            self.connected = True

        def reqCurrentTime(self):
            return object()

        def disconnect(self):
            self.connected = False

    monkeypatch.setenv(IBKR_CLIENT_ID_OFFSET_ENV, "1000")
    monkeypatch.setenv(IBKR_CLIENT_ID_RETRY_SPAN_ENV, "2")
    monkeypatch.setattr(connection_module, "IB", FakeIB)

    conn = IBKRConnection("127.0.0.1", 4002, 101)
    ib = conn.connect(retry_seconds=0)

    assert ib.isConnected()
    assert FakeIB.connect_client_ids == [1101, 1102]
    assert conn.client_id == 1102


def test_connection_does_not_walk_client_ids_by_default(monkeypatch):
    class FakeIB:
        connect_client_ids: list[int] = []

        def __init__(self):
            self.connected = False

        def isConnected(self):
            return self.connected

        def connect(self, host, port, *, clientId, timeout):
            self.connect_client_ids.append(clientId)
            raise RuntimeError("clientId already in use")

        def disconnect(self):
            self.connected = False

    monkeypatch.setenv(IBKR_CLIENT_ID_OFFSET_ENV, "1000")
    monkeypatch.setenv(IBKR_CONNECT_MAX_ROUNDS_ENV, "1")
    monkeypatch.delenv(IBKR_CLIENT_ID_RETRY_SPAN_ENV, raising=False)
    monkeypatch.setattr(connection_module, "IB", FakeIB)

    conn = IBKRConnection("127.0.0.1", 4002, 101)
    with pytest.raises(RuntimeError, match="clientId already in use"):
        conn.connect(retry_seconds=0)

    assert FakeIB.connect_client_ids == [1101]
    assert conn.client_id == 1101
