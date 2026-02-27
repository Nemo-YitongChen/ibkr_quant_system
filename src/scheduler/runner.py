# src/scheduler/runner.py
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Tuple, Optional, Any

from ..common.logger import get_logger

log = get_logger("runner")


@dataclass
class RunnerConfig:
    heartbeat_interval_sec: int = 5
    account_refresh_sec: int = 60

    # entry watch：用于管理 entry 订单超时（你原本在用）
    entry_timeout_sec: int = 30


class Runner:
    def __init__(self, ib, cfg: RunnerConfig, account=None):
        self.ib = ib
        self.cfg = cfg
        self.account = account

        self._last_hb = 0.0
        self._last_acct = 0.0

        # ✅ 必须初始化，否则 AttributeError
        # orderId -> (created_ts, any_metadata)
        self._entry_watch: Dict[int, Tuple[float, Any]] = {}

    # ---- public ----
    def tick(self):
        now = time.time()

        # heartbeat
        if now - self._last_hb >= self.cfg.heartbeat_interval_sec:
            self._last_hb = now
            self._heartbeat()

        # account refresh（如果你需要）
        if self.account and (now - self._last_acct >= self.cfg.account_refresh_sec):
            self._last_acct = now
            try:
                self.account.refresh()
            except Exception as e:
                log.warning(f"Account refresh failed: {type(e).__name__} {e}")

        # order management
        try:
            self._manage_orders()
        except Exception as e:
            # 不要让 runner 把整个 engine 打崩
            log.warning(f"Manage orders failed: {type(e).__name__} {e}")

    # ---- heartbeat ----
    def _heartbeat(self) -> bool:
    # 1) 纯本地探活：不阻塞，不受 RequestTimeout 影响
        if not self.ib.isConnected():
            log.warning("Heartbeat failed: disconnected")
            return False

        # 2) 每隔一段时间再做一次“轻量服务端探活”（可选）
        #    避免每 5 秒都打一次 reqCurrentTime（会受 RequestTimeout 和消息拥堵影响）
        try:
            cnt = getattr(self, "_hb_counter", 0) + 1
            self._hb_counter = cnt
            if cnt % 12 != 0:   # 例如：12*5s=60s 才做一次服务端探活
                log.info("Heartbeat OK")
                return True

            # 单独把这次探活允许更长 timeout（不改变全局）
            prev = getattr(self.ib, "RequestTimeout", 0)
            self.ib.RequestTimeout = 12
            _ = self.ib.reqCurrentTime()
            log.info("Heartbeat OK")
            return True

        except asyncio.TimeoutError:
            log.warning("Heartbeat timeout: TimeoutError")
            return True  # 关键：探活失败不等于交易失败，保持系统继续跑
        except Exception as e:
            log.warning(f"Heartbeat failed: {type(e).__name__} {e}")
            return True
        finally:
            if "prev" in locals():
                self.ib.RequestTimeout = prev

    # ---- entry watch helpers ----
    def watch_entry_order(self, order_id: int, meta: Optional[Any] = None):
        self._entry_watch[int(order_id)] = (time.time(), meta)

    def unwatch_entry_order(self, order_id: int):
        self._entry_watch.pop(int(order_id), None)

    # ---- order manager ----
    def _manage_orders(self):
        """
        这里保留你原本的“entry 超时撤单/处理”逻辑骨架：
        - 遍历 _entry_watch
        - 超时则取消订单
        """
        if not self._entry_watch:
            return

        now = time.time()
        timeout = self.cfg.entry_timeout_sec

        for oid, (ts0, meta) in list(self._entry_watch.items()):
            if now - ts0 < timeout:
                continue

            # 超时：尝试取消
            try:
                # ib_insync cancelOrder 接受 Order 对象；如果你这里只保存了 orderId，
                # 就需要从 openTrades 里找到对应的 Trade/Order
                trade = next((t for t in self.ib.openTrades() if getattr(t.order, "orderId", None) == oid), None)
                if trade:
                    self.ib.cancelOrder(trade.order)
                    log.info(f"Entry order timeout -> canceled: orderId={oid} meta={meta}")
                else:
                    log.info(f"Entry order timeout but not found in openTrades: orderId={oid} meta={meta}")
            except Exception as e:
                log.warning(f"Cancel timeout order failed: orderId={oid} err={type(e).__name__} {e}")
            finally:
                self._entry_watch.pop(oid, None)