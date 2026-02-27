# src/main.py
from __future__ import annotations

import yaml

from .common.logger import get_logger
from .common.storage import Storage

from .ibkr.connection import IBKRConnection
from .ibkr.market_data import MarketDataService
from .ibkr.orders import OrderService
from .ibkr.fills import FillProcessor
from .ibkr.account import AccountService
from .ibkr.universe import UniverseService, UniverseConfig

from .risk.limits import DailyRiskGate

from .scheduler.runner import Runner, RunnerConfig
from .app.engine import TradingEngine, EngineConfig

from .strategies import EngineStrategy, StrategyConfig  # ✅ 新增

log = get_logger("main")


def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    ibkr_cfg = load_yaml("config/ibkr.yaml")
    risk_cfg = load_yaml("config/risk.yaml")

    host = ibkr_cfg["host"]
    port = int(ibkr_cfg["port"])
    client_id = int(ibkr_cfg["client_id"])
    account_id = ibkr_cfg["account_id"]

    storage = Storage("audit.db")

    conn = IBKRConnection(host, port, client_id)
    ib = conn.connect()
    ib.RequestTimeout = 5

    account = AccountService(ib, account_id)
    account.start()

    # backward compatible risk config
    daily_loss_limit_short_pct = risk_cfg.get("daily_loss_limit_short_pct", None)
    legacy = risk_cfg.get("daily_loss_limit_short", None)
    if daily_loss_limit_short_pct is None:
        if isinstance(legacy, (int, float)) and abs(float(legacy)) <= 1:
            daily_loss_limit_short_pct = float(legacy)
        else:
            daily_loss_limit_short_pct = -0.01
    max_consecutive_losses = int(risk_cfg.get("max_consecutive_losses", 5))

    gate = DailyRiskGate(
        storage=storage,
        account=account,
        daily_loss_limit_short_pct=float(daily_loss_limit_short_pct),
        max_consecutive_losses=max_consecutive_losses,
    )

    _fills = FillProcessor(ib, storage, gate)

    md = MarketDataService(ib)
    orders = OrderService(ib, account_id, storage)

    runner = Runner(ib, RunnerConfig(), account=account)

    universe = UniverseService(ib, UniverseConfig(max_short_candidates=15))

    # ✅ 用 strategies/ 里的适配器，避免 app/ 再造 strategy.py
    strategy = EngineStrategy(orders=orders, gate=gate, cfg=StrategyConfig())

    engine = TradingEngine(
        ib=ib,
        universe_svc=universe,
        strategy=strategy,
        runner=runner,
        cfg=EngineConfig(),
        md=md,
    )

    log.info("Starting engine...")
    engine.run_forever()


if __name__ == "__main__":
    main()