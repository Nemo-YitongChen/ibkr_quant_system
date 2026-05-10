# 2026-05-08 Strategy Config Pure Signal Regression

## 背景

Evidence lifecycle 已完成 action、effectiveness、blocked-vs-allowed 护栏和 dashboard v2 信息架构收敛。下一步进入策略技术债第一批：只做低风险配置化和纯函数 regression coverage，不自动调参、不改 live execution 行为。

## 已完成

- `StrategyConfig` 新增 engine-level 参数：
  - `mr_weight`
  - `bo_weight`
  - `mid_soft_floor`
  - `mid_qty_min`
  - `mid_qty_max`
  - `fusion_short_base_weight`
  - `fusion_short_mid_weight`
  - `fusion_long_weight`
  - `fusion_mid_bias_weight`
  - `fusion_momentum_block_mid_threshold`
  - `fusion_momentum_block_short_threshold`
- `StrategyConfig.from_dict()` 支持 `engine:` 配置段，并继续兼容旧 `strategy:` 字段。
- `StrategyConfig.from_dict()` 支持 `mid_regime:` alias，便于直接消费现有 strategy defaults YAML。
- `EngineStrategy` 的 MR/BO 短线信号融合从硬编码 `0.6 / 0.4` 改为配置驱动，默认行为不变。
- `fuse()` 支持配置化 fusion weights 和 risk-off chasing block 阈值，默认行为不变。
- `config/strategy_defaults*.yaml` 已补 `engine:` 默认段。
- `strategy_parameter_registry.yaml` 已纳入 `mr_weight / bo_weight / mid_*`，便于后续 weekly review 只建议一个 primary field。

## 验证

- `tests/test_strategy_config.py` 覆盖 engine 配置映射、`mid_regime` alias 和 MR/BO 权重融合。
- `tests/test_signal_fusion.py` 覆盖 configurable fusion weights、short disabled、risk-off chasing block。
- `tests/test_pure_strategy_signals.py` 覆盖 breakout 上/下突破，以及高波动/深回撤时 mid regime scale 下降。
- `tests/test_strategy_parameter_registry.py` 覆盖新增策略字段的 bounds / priority。

## 下一步

进入策略配置治理层：weekly review 可以基于 evidence 只建议 `SIGNAL_FUSION` 或 `MID_REGIME_SIZING` 的一个 primary field，但仍应保持 paper -> shadow -> limited live 的分阶段生效。
