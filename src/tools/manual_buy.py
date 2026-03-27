# src/tools/manual_buy.py
from __future__ import annotations

import argparse
import yaml
from ib_insync import Stock, MarketOrder  # type: ignore

from ..common.logger import get_logger
from ..common.storage import Storage
from ..ibkr.connection import IBKRConnection
from ..ibkr.market_data import MarketDataService
from ..ibkr.orders import OrderService

log = get_logger("tools.manual_buy")

def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    ap = argparse.ArgumentParser(description="Manual buy to validate paper order lifecycle.")
    ap.add_argument("--symbol", default="TSLA")
    ap.add_argument("--qty", type=float, default=2.0)
    ap.add_argument("--market", action="store_true", help="Use market order (default).")
    args = ap.parse_args()

    ibkr_cfg = load_yaml("config/ibkr.yaml")
    host = ibkr_cfg["host"]
    port = int(ibkr_cfg["port"])
    client_id = int(ibkr_cfg["client_id"])
    account_id = ibkr_cfg["account_id"]

    storage = Storage("audit.db")
    conn = IBKRConnection(host, port, client_id)
    ib = conn.connect()
    ib.RequestTimeout = 5

    md = MarketDataService(ib)
    orders = OrderService(ib, account_id, storage, md_svc=md)

    # Use delayed-frozen to avoid 420 if no RT permission
    try:
        ib.reqMarketDataType(4)
    except Exception:
        pass

    sym = args.symbol.upper()
    contract = Stock(sym, "SMART", "USD")
    orders.qualify(contract)

    log.info(f"Placing MANUAL BUY: symbol={sym} qty={args.qty} type={'MKT' if args.market else 'MKT'} account={account_id}")

    o = MarketOrder("BUY", args.qty, account=account_id)
    trade = ib.placeOrder(contract, o)

    # wait briefly for status updates
    # ib_insync runs event loop; we can wait until done or timeout
    for _ in range(60):
        ib.sleep(1)
        st = trade.orderStatus.status
        if st in ("Filled", "Cancelled", "Inactive"):
            break

    log.info(
        f"ManualBuyDone: symbol={sym} orderId={trade.order.orderId} status={trade.orderStatus.status} "
        f"filled={trade.orderStatus.filled} avgFill={getattr(trade.orderStatus,'avgFillPrice',0.0)}"
    )

if __name__ == "__main__":
    main()
