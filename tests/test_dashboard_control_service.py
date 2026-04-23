from __future__ import annotations

import io
import unittest
from unittest.mock import MagicMock, patch

from src.app.dashboard_control import DashboardControlService


class DashboardControlServiceTests(unittest.TestCase):
    def _build_service(self, **overrides) -> DashboardControlService:
        def _default_handler(name: str):
            def _handler(payload):
                return {"ok": True, "handler": name, "payload": payload}

            return _handler

        handlers = {
            "run_once": overrides.get("run_once", _default_handler("run_once")),
            "run_preflight": overrides.get("run_preflight", _default_handler("run_preflight")),
            "run_weekly_review": overrides.get("run_weekly_review", _default_handler("run_weekly_review")),
            "apply_weekly_feedback": overrides.get("apply_weekly_feedback", _default_handler("apply_weekly_feedback")),
            "review_market_profile_patch": overrides.get(
                "review_market_profile_patch", _default_handler("review_market_profile_patch")
            ),
            "review_calibration_patch": overrides.get(
                "review_calibration_patch", _default_handler("review_calibration_patch")
            ),
            "refresh_dashboard": overrides.get("refresh_dashboard", _default_handler("refresh_dashboard")),
            "toggle_flag": overrides.get("toggle_flag", _default_handler("toggle_flag")),
            "set_execution_mode": overrides.get("set_execution_mode", _default_handler("set_execution_mode")),
        }
        return DashboardControlService(
            "127.0.0.1",
            0,
            get_state=lambda: {"ok": True},
            run_once=handlers["run_once"],
            run_preflight=handlers["run_preflight"],
            run_weekly_review=handlers["run_weekly_review"],
            apply_weekly_feedback=handlers["apply_weekly_feedback"],
            review_market_profile_patch=handlers["review_market_profile_patch"],
            review_calibration_patch=handlers["review_calibration_patch"],
            refresh_dashboard=handlers["refresh_dashboard"],
            toggle_flag=handlers["toggle_flag"],
            set_execution_mode=handlers["set_execution_mode"],
        )

    def _build_handler(self, service: DashboardControlService):
        handler_cls = service._make_handler()
        handler = handler_cls.__new__(handler_cls)
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()
        handler.wfile = MagicMock()
        return handler

    def test_dashboard_control_service_start_and_stop(self):
        fake_server = MagicMock()
        fake_server.server_address = ("127.0.0.1", 8877)
        fake_thread = MagicMock()

        with patch("src.app.dashboard_control.ThreadingHTTPServer", return_value=fake_server) as server_cls, patch(
            "src.app.dashboard_control.threading.Thread", return_value=fake_thread
        ) as thread_cls:
            service = self._build_service()

            service.start()

            server_cls.assert_called_once()
            thread_cls.assert_called_once()
            fake_thread.start.assert_called_once()
            self.assertEqual(service.base_url, "http://127.0.0.1:8877")

            service.stop()

            fake_server.shutdown.assert_called_once()
            fake_server.server_close.assert_called_once()
            fake_thread.join.assert_called_once()

    def test_send_json_ignores_common_client_disconnect_errors(self):
        service = self._build_service()
        handler_cls = service._make_handler()

        for error in (
            BrokenPipeError(32, "Broken pipe"),
            ConnectionResetError(54, "Connection reset by peer"),
            ConnectionAbortedError(53, "Software caused connection abort"),
        ):
            with self.subTest(error=type(error).__name__):
                handler = handler_cls.__new__(handler_cls)
                handler.send_response = MagicMock()
                handler.send_header = MagicMock()
                handler.end_headers = MagicMock()
                handler.wfile = MagicMock()
                handler.wfile.write.side_effect = error

                handler._send_json(200, {"ok": True})

                handler.send_response.assert_called_once_with(200)
                handler.end_headers.assert_called_once()
                handler.wfile.write.assert_called_once()

    def test_send_json_reraises_unexpected_os_error(self):
        service = self._build_service()
        handler_cls = service._make_handler()
        handler = handler_cls.__new__(handler_cls)
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()
        handler.wfile = MagicMock()
        handler.wfile.write.side_effect = OSError("unexpected socket failure")

        with self.assertRaises(OSError):
            handler._send_json(200, {"ok": True})

    def test_read_payload_returns_empty_dict_for_invalid_json(self):
        service = self._build_service()
        handler = self._build_handler(service)
        handler.headers = {"Content-Length": "12"}
        handler.rfile = io.BytesIO(b"not-json-123")

        self.assertEqual(handler._read_payload(), {})

    def test_do_post_dispatches_handlers_with_expected_status_codes(self):
        calls = {}

        def _capture(name: str, ok: bool):
            def _handler(payload):
                calls[name] = payload
                return {"ok": ok, "handler": name, "payload": payload}

            return _handler

        service = self._build_service(
            run_once=_capture("run_once", True),
            refresh_dashboard=_capture("refresh_dashboard", False),
            review_market_profile_patch=_capture("review_market_profile_patch", True),
        )

        scenarios = [
            ("/run_once", "run_once", 202),
            ("/refresh_dashboard", "refresh_dashboard", 500),
            ("/review_market_profile_patch", "review_market_profile_patch", 200),
        ]
        for path, handler_name, expected_status in scenarios:
            with self.subTest(path=path):
                handler = self._build_handler(service)
                handler.path = path
                handler._read_payload = MagicMock(return_value={"market": "US", "path": path})
                handler._send_json = MagicMock()

                handler.do_POST()

                self.assertEqual(calls[handler_name], {"market": "US", "path": path})
                status_code, payload = handler._send_json.call_args.args
                self.assertEqual(status_code, expected_status)
                self.assertEqual(payload["handler"], handler_name)

    def test_do_post_returns_not_found_for_unknown_route(self):
        service = self._build_service()
        handler = self._build_handler(service)
        handler.path = "/does_not_exist"
        handler._read_payload = MagicMock(return_value={"ignored": True})
        handler._send_json = MagicMock()

        handler.do_POST()

        status_code, payload = handler._send_json.call_args.args
        self.assertEqual(status_code, 404)
        self.assertEqual(payload["error"], "not_found")
        self.assertEqual(payload["path"], "/does_not_exist")


if __name__ == "__main__":
    unittest.main()
