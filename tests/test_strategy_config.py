from __future__ import annotations

from src.strategies.engine_strategy import EngineStrategy, StrategyConfig


def test_strategy_config_from_dict_maps_nested_runtime_parameters():
    cfg = StrategyConfig.from_dict(
        {
            "strategy": {
                "trade_threshold": 0.71,
                "base_qty": 3.0,
                "enable_pure_short": False,
                "short_threshold": 0.52,
                "mid_qty_min": 0.4,
                "paper_allowed_execution_sources": ["realtime", "delayed"],
            },
            "orders": {
                "default_take_profit_pct": 0.01,
                "default_stop_loss_pct": 0.02,
            },
            "risk": {"atr_window": 9, "blocked_event_risks": ["high"]},
            "mr": {"lookback": 10, "entry_z": 1.8},
            "bo": {"lookback": 20, "confirm": 3},
            "mid": {"ma_slow": 30, "ma_fast": 11},
        }
    )

    assert cfg.trade_threshold == 0.71
    assert cfg.base_qty == 3.0
    assert cfg.enable_pure_short is False
    assert cfg.take_profit_pct == 0.01
    assert cfg.stop_loss_pct == 0.02
    assert cfg.paper_allowed_execution_sources == ["REALTIME", "DELAYED"]
    assert cfg.risk.atr_window == 9
    assert cfg.risk.blocked_event_risks == ["HIGH"]
    assert cfg.mr.lookback == 10
    assert cfg.bo.confirm == 3
    assert cfg.mid.ma_slow == 30


def test_engine_strategy_required_bars_is_pure_config_driven():
    cfg = StrategyConfig.from_dict(
        {
            "mr": {"lookback": 10},
            "bo": {"lookback": 20, "confirm": 3},
            "mid": {"ma_slow": 30},
        }
    )
    strategy = EngineStrategy(cfg=cfg)

    assert strategy.required_bars() == 32
