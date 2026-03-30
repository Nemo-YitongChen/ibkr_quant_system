from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

from src.app.engine import EngineConfig, TradingEngine
from src.risk.model import PreTradeRiskSnapshot
from src.strategies import engine_strategy as engine_strategy_module
from src.strategies.engine_strategy import EngineStrategy, StrategyConfig, TradeSignal


@dataclass
class _FakeBar:
    open: float
    high: float
    low: float
    close: float
    volume: float
    end_time: datetime


class _FakeStrategy:
    def __init__(self, signal: TradeSignal):
        self.signal = signal
        self.execute_calls = []
        self.orders = SimpleNamespace(storage=None)
        self.cfg = SimpleNamespace(mid=None)

    def evaluate_from_bar(self, symbol: str, bar: _FakeBar) -> TradeSignal:
        return self.signal

    def execute(self, symbol: str, sig: TradeSignal, runner: object) -> None:
        self.execute_calls.append((symbol, sig, runner))


class _FakeExecutor:
    def __init__(self):
        self.calls = []

    def execute(self, symbol: str, sig: TradeSignal, runner: object) -> None:
        self.calls.append((symbol, sig, runner))


class _FakeStorage:
    def __init__(self):
        self.rows = []

    def upsert_md_quality(self, **kwargs):
        self.rows.append(kwargs)


class _FakeAuditWriter:
    def __init__(self):
        self.storage = None
        self.contexts = []

    def write(self, context) -> None:
        self.contexts.append(context)


def _sample_signal() -> TradeSignal:
    return TradeSignal(
        should_trade=True,
        action="BUY",
        qty=1.0,
        entry_price=100.0,
        total_sig=0.7,
        short_sig=0.6,
        mid_scale=0.7,
        reason="test",
        channel="PURE_SHORT",
        audit_tag="ALWAYS_ON",
        audit_source="REALTIME",
    )


def test_trading_engine_prefers_explicit_executor() -> None:
    strategy = _FakeStrategy(_sample_signal())
    executor = _FakeExecutor()
    runner = object()
    engine = TradingEngine(
        ib=object(),
        universe_svc=SimpleNamespace(build=lambda: {}),
        strategy=strategy,
        runner=runner,
        cfg=EngineConfig(use_realtime_agg=False),
        executor=executor,
    )
    engine._latest_5m["AAPL"] = _FakeBar(
        open=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        volume=1000.0,
        end_time=datetime(2026, 3, 29, 0, 5, tzinfo=timezone.utc),
    )

    engine._maybe_calc_signal("AAPL", tag="ALWAYS_ON")

    assert len(executor.calls) == 1
    assert strategy.execute_calls == []


def test_engine_strategy_execute_delegates_to_executor() -> None:
    executor = _FakeExecutor()
    strategy = EngineStrategy(
        orders=SimpleNamespace(storage=None),
        gate=None,
        cfg=StrategyConfig(),
        executor=executor,
    )

    strategy.execute("AAPL", _sample_signal(), runner=object())

    assert len(executor.calls) == 1


def test_engine_strategy_uses_injected_audit_writer_without_orders(monkeypatch) -> None:
    audit_writer = _FakeAuditWriter()
    strategy = EngineStrategy(
        orders=None,
        gate=None,
        cfg=StrategyConfig(),
        audit_writer=audit_writer,
    )
    strategy.risk_model = SimpleNamespace(
        build_snapshot=lambda **kwargs: PreTradeRiskSnapshot(
            symbol="AAPL",
            action="BUY",
            entry_price=100.0,
            atr_stop=1.0,
            slippage_bps=5.0,
            gap_addon_pct=0.002,
            liquidity_haircut=0.0,
            slippage_addon_price=0.05,
            gap_addon_price=0.2,
            liquidity_addon_price=0.0,
            short_addon_price=0.0,
            stop_distance=1.25,
            take_profit_distance=2.25,
            stop_price=98.75,
            take_profit_price=102.25,
            event_risk="NONE",
            event_risk_reason="",
            short_borrow_fee_bps=0.0,
            short_borrow_source="",
            allowed=True,
            block_reasons=[],
            atr_pct=0.01,
            avg_bar_volume=10000.0,
            risk_per_share=1.25,
            expected_fill_price=100.05,
        )
    )

    monkeypatch.setattr(engine_strategy_module, "mr_signal", lambda close, cfg: 0.8)
    monkeypatch.setattr(engine_strategy_module, "bo_signal", lambda high, low, close, cfg: 0.2)
    monkeypatch.setattr(
        engine_strategy_module,
        "evaluate_regime",
        lambda close, cfg: SimpleNamespace(scale=0.7, risk_on=True, state="UPTREND", reason="ok"),
    )
    monkeypatch.setattr(
        engine_strategy_module,
        "to_regime_state_v2",
        lambda regime: SimpleNamespace(to_dict=lambda: {"state": regime.state, "scale": regime.scale}),
    )
    monkeypatch.setattr(engine_strategy_module, "fuse", lambda **kwargs: 0.9)

    signal = strategy.evaluate_from_bar(
        "AAPL",
        _FakeBar(
            open=100.0,
            high=101.0,
            low=99.5,
            close=100.5,
            volume=1000.0,
            end_time=datetime(2026, 3, 30, 0, 5, tzinfo=timezone.utc),
        ),
    )

    assert signal is not None
    assert signal.should_trade is True
    assert len(audit_writer.contexts) == 1
    assert audit_writer.contexts[0].symbol == "AAPL"


def test_trading_engine_md_quality_uses_explicit_storage() -> None:
    strategy = _FakeStrategy(_sample_signal())
    strategy.orders = None
    storage = _FakeStorage()
    engine = TradingEngine(
        ib=object(),
        universe_svc=SimpleNamespace(build=lambda: {}),
        strategy=strategy,
        runner=object(),
        cfg=EngineConfig(use_realtime_agg=False),
        storage=storage,
    )

    engine._update_quality("AAPL", int(datetime(2026, 3, 30, 0, 5, tzinfo=timezone.utc).timestamp()), is_duplicate=False)

    assert len(storage.rows) == 1
    assert storage.rows[0]["symbol"] == "AAPL"
