import time
from ib_insync import IB

try:
    from ..common.ibkr_client_id import (
        resolve_ibkr_client_id,
        resolve_ibkr_client_id_retry_span,
        resolve_ibkr_connect_max_rounds,
    )
    from ..common.logger import get_logger
except ImportError:  # Support direct script execution from the src/ tree.
    from common.ibkr_client_id import (
        resolve_ibkr_client_id,
        resolve_ibkr_client_id_retry_span,
        resolve_ibkr_connect_max_rounds,
    )
    from common.logger import get_logger

log = get_logger("ibkr.connection")

class IBKRConnection:
    """Connection wrapper with bounded clientId attempts for IB Gateway sessions."""

    def __init__(self, host: str, port: int, client_id: int):
        self.host = host
        self.port = port
        self.config_client_id = int(client_id)
        self.client_id = resolve_ibkr_client_id(client_id)
        self.client_id_retry_span = resolve_ibkr_client_id_retry_span()
        self.connect_max_rounds = resolve_ibkr_connect_max_rounds()
        self.ib = IB()

    def _reset_ib(self) -> None:
        try:
            self.ib.disconnect()
        except Exception:
            pass
        self.ib = IB()

    def connect(self, retry_seconds: int = 5) -> IB:
        retry_span = max(1, int(self.client_id_retry_span or 1))
        max_rounds = max(1, int(self.connect_max_rounds or 1))
        last_error: Exception | None = None
        for connect_round in range(max_rounds):
            for retry_idx in range(retry_span):
                active_client_id = int(self.client_id) + retry_idx
                try:
                    if self.ib.isConnected():
                        return self.ib
                    log.info(f"Connecting to IBKR {self.host}:{self.port} clientId={active_client_id}")
                    self.ib.connect(self.host, self.port, clientId=active_client_id, timeout=10)
                    # Lightweight roundtrip to confirm API responsiveness after connect.
                    self.ib.reqCurrentTime()
                    self.client_id = active_client_id
                    log.info("Connected.")
                    return self.ib
                except Exception as e:
                    last_error = e
                    self._reset_ib()
                    if retry_idx < retry_span - 1:
                        next_client_id = active_client_id + 1
                        log.warning(
                            "Connect failed for clientId=%s: %s. Retrying with clientId=%s.",
                            active_client_id,
                            e,
                            next_client_id,
                        )
                        continue
                    if connect_round >= max_rounds - 1:
                        log.error(
                            "Connect failed for clientId=%s after %s round(s): %s",
                            active_client_id,
                            max_rounds,
                            e,
                        )
                        raise
                    log.error(
                        "Connect failed for clientId=%s: %s. Retry in %ss.",
                        active_client_id,
                        e,
                        retry_seconds,
                    )
                    time.sleep(max(0.1, float(retry_seconds)))
        if last_error is not None:
            raise last_error
        raise TimeoutError("IBKR connection failed without a captured exception")

    def disconnect(self):
        try:
            self.ib.disconnect()
            log.info("Disconnected.")
        except Exception:
            pass
