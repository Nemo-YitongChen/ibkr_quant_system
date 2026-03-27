from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from src.app.dashboard_control import DashboardControlService


class DashboardControlServiceTests(unittest.TestCase):
    def test_dashboard_control_service_start_and_stop(self):
        fake_server = MagicMock()
        fake_server.server_address = ("127.0.0.1", 8877)
        fake_thread = MagicMock()

        with patch("src.app.dashboard_control.ThreadingHTTPServer", return_value=fake_server) as server_cls, patch(
            "src.app.dashboard_control.threading.Thread", return_value=fake_thread
        ) as thread_cls:
            service = DashboardControlService(
                "127.0.0.1",
                0,
                get_state=lambda: {"ok": True},
                run_once=lambda payload: {"ok": True, "payload": payload},
                run_preflight=lambda payload: {"ok": True, "payload": payload},
                run_weekly_review=lambda payload: {"ok": True, "payload": payload},
                apply_weekly_feedback=lambda payload: {"ok": True, "payload": payload},
                refresh_dashboard=lambda payload: {"ok": True},
                toggle_flag=lambda payload: {"ok": True},
                set_execution_mode=lambda payload: {"ok": True, "payload": payload},
            )

            service.start()

            server_cls.assert_called_once()
            thread_cls.assert_called_once()
            fake_thread.start.assert_called_once()
            self.assertEqual(service.base_url, "http://127.0.0.1:8877")

            service.stop()

            fake_server.shutdown.assert_called_once()
            fake_server.server_close.assert_called_once()
            fake_thread.join.assert_called_once()


if __name__ == "__main__":
    unittest.main()
