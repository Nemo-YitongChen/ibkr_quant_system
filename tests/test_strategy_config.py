from __future__ import annotations

from pathlib import Path

import yaml

from src.strategies.engine_strategy import EngineStrategy, StrategyConfig, combine_short_signals


def test_strategy_config_from_dict_maps_nested_runtime_parameters():
    cfg = StrategyConfig.from_dict(
        {
            "strategy": {
                "trade_threshold": 0.71,
                "base_qty": 3.0,
                "enable_pure_short": False,
                "short_threshold": 0.52,
                "paper_allowed_execution_sources": ["realtime", "delayed"],
            },
            "engine": {
                "mr_weight": 0.7,
                "bo_weight": 0.3,
                "mid_qty_min": 0.4,
                "mid_qty_max": 1.1,
                "mid_soft_floor": 0.2,
                "fusion_short_base_weight": 0.8,
                "fusion_short_mid_weight": 0.2,
            },
            "orders": {
                "default_take_profit_pct": 0.01,
                "default_stop_loss_pct": 0.02,
            },
            "risk": {"atr_window": 9, "blocked_event_risks": ["high"]},
            "mr": {"lookback": 10, "entry_z": 1.8},
            "bo": {"lookback": 20, "confirm": 3},
            "mid_regime": {"ma_slow": 30, "ma_fast": 11},
        }
    )

    assert cfg.trade_threshold == 0.71
    assert cfg.base_qty == 3.0
    assert cfg.enable_pure_short is False
    assert cfg.mr_weight == 0.7
    assert cfg.bo_weight == 0.3
    assert cfg.mid_qty_min == 0.4
    assert cfg.mid_qty_max == 1.1
    assert cfg.mid_soft_floor == 0.2
    assert cfg.fusion_short_base_weight == 0.8
    assert cfg.fusion_short_mid_weight == 0.2
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


def test_combine_short_signals_uses_configured_weights_without_amplifying():
    assert round(combine_short_signals(1.0, -1.0, mr_weight=0.6, bo_weight=0.4), 6) == 0.2
    assert combine_short_signals(0.25, 1.0, mr_weight=0.0, bo_weight=1.0) == 1.0
    assert combine_short_signals(1.0, 1.0, mr_weight=0.0, bo_weight=0.0) == 0.0


def test_project_strategy_defaults_expose_engine_parameters() -> None:
    base_dir = Path(__file__).resolve().parents[1]

    for cfg_path in sorted((base_dir / "config").glob("strategy_defaults*.yaml")):
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        cfg = StrategyConfig.from_dict(raw)

        assert cfg.mr_weight == 0.60
        assert cfg.bo_weight == 0.40
        assert cfg.mid_qty_min == 0.25
        assert cfg.mid_qty_max == 1.25
        assert cfg.mid.ma_slow == int(raw["mid_regime"]["ma_slow"])
