import time
from ib_insync import IB

try:
    from ..common.logger import get_logger
except ImportError:  # Support direct script execution from the src/ tree.
    from common.logger import get_logger

log = get_logger("ibkr.connection")

class IBKRConnection:
    """Connection wrapper with retry loop for IB Gateway sessions."""

    def __init__(self, host: str, port: int, client_id: int):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB()

    def connect(self, retry_seconds: int = 5) -> IB:
        while True:
            try:
                if self.ib.isConnected():
                    return self.ib
                log.info(f"Connecting to IBKR {self.host}:{self.port} clientId={self.client_id}")
                self.ib.connect(self.host, self.port, clientId=self.client_id, timeout=10)
                # Lightweight roundtrip to confirm API responsiveness after connect.
                self.ib.reqCurrentTime()
                log.info("Connected.")
                return self.ib
            except Exception as e:
                log.error(f"Connect failed: {e}. Retry in {retry_seconds}s.")
                time.sleep(retry_seconds)

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            log.info("Disconnected.")
