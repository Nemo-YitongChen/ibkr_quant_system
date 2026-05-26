from __future__ import annotations

from datetime import datetime, timezone
from tempfile import NamedTemporaryFile
from unittest.mock import patch

from src.analysis.investment_portfolio import InvestmentPaperConfig
from src.app.investment_engine import InvestmentExecutionEngine
from src.common.storage import Storage
from src.portfolio.investment_allocator import InvestmentExecutionConfig


class _FrozenDateTime(datetime):
    frozen_now = datetime(2026, 5, 17, 13, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        value = cls.frozen_now
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)


class _DummyEvent:
    def __iadd__(self, _handler):
        return self


class _FakeIB:
    orderStatusEvent = _DummyEvent()
    errorEvent = _DummyEvent()
    execDetailsEvent = _DummyEvent()
    commissionReportEvent = _DummyEvent()


def _engine(*, include_overnight: bool = False) -> InvestmentExecutionEngine:
    with NamedTemporaryFile(suffix=".db") as tmp:
        storage = Storage(tmp.name)
        return InvestmentExecutionEngine(
            ib=_FakeIB(),
            account_id="DUQ152001",
            storage=storage,
            market="US",
            portfolio_id="US:test",
            paper_cfg=InvestmentPaperConfig(max_holdings=1, max_single_weight=0.25),
            execution_cfg=InvestmentExecutionConfig(include_overnight=include_overnight, outside_rth=include_overnight),
        )


def test_execution_session_blocks_weekend_even_when_overnight_enabled():
    _FrozenDateTime.frozen_now = datetime(2026, 5, 17, 13, 0, tzinfo=timezone.utc)
    engine = _engine(include_overnight=True)
    with patch("src.app.investment_engine.datetime", _FrozenDateTime):
        session = engine._current_execution_session_profile()
    assert session.session_bucket == "CLOSED"
    assert engine._market_open_for_submit(session) is False


def test_execution_session_allows_weekday_extended_hours_when_configured():
    _FrozenDateTime.frozen_now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    engine = _engine(include_overnight=True)
    with patch("src.app.investment_engine.datetime", _FrozenDateTime):
        session = engine._current_execution_session_profile()
    assert session.session_bucket == "OVERNIGHT"
    assert engine._market_open_for_submit(session) is True
