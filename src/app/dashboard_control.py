from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict
from urllib.parse import urlparse


JsonHandler = Callable[[Dict[str, Any]], Dict[str, Any]]
StateHandler = Callable[[], Dict[str, Any]]


class DashboardControlService:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        get_state: StateHandler,
        run_once: JsonHandler,
        run_preflight: JsonHandler,
        run_weekly_review: JsonHandler,
        apply_weekly_feedback: JsonHandler,
        review_market_profile_patch: JsonHandler,
        refresh_dashboard: JsonHandler,
        toggle_flag: JsonHandler,
        set_execution_mode: JsonHandler,
    ) -> None:
        self.host = str(host or "127.0.0.1")
        self.port = int(port)
        self._get_state = get_state
        self._run_once = run_once
        self._run_preflight = run_preflight
        self._run_weekly_review = run_weekly_review
        self._apply_weekly_feedback = apply_weekly_feedback
        self._review_market_profile_patch = review_market_profile_patch
        self._refresh_dashboard = refresh_dashboard
        self._toggle_flag = toggle_flag
        self._set_execution_mode = set_execution_mode
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def base_url(self) -> str:
        if self._server is not None:
            host, port = self._server.server_address[:2]
            return f"http://{host}:{port}"
        return f"http://{self.host}:{self.port}"

    def _make_handler(self):
        service = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def _send_json(self, status_code: int, payload: Dict[str, Any]) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(int(status_code))
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()
                self.wfile.write(body)

            def _read_payload(self) -> Dict[str, Any]:
                try:
                    length = int(self.headers.get("Content-Length", "0") or "0")
                except Exception:
                    length = 0
                if length <= 0:
                    return {}
                raw = self.rfile.read(length)
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except Exception:
                    payload = {}
                return dict(payload or {})

            def do_OPTIONS(self) -> None:
                self._send_json(200, {"ok": True})

            def do_GET(self) -> None:
                path = urlparse(self.path).path
                if path in {"/health", "/state"}:
                    self._send_json(200, service._get_state())
                    return
                self._send_json(404, {"ok": False, "error": "not_found", "path": path})

            def do_POST(self) -> None:
                path = urlparse(self.path).path
                payload = self._read_payload()
                if path == "/run_once":
                    result = dict(service._run_once(payload) or {})
                    self._send_json(202 if bool(result.get("ok", False)) else 409, result)
                    return
                if path == "/run_preflight":
                    result = dict(service._run_preflight(payload) or {})
                    self._send_json(202 if bool(result.get("ok", False)) else 409, result)
                    return
                if path == "/run_weekly_review":
                    result = dict(service._run_weekly_review(payload) or {})
                    self._send_json(202 if bool(result.get("ok", False)) else 409, result)
                    return
                if path == "/apply_weekly_feedback":
                    result = dict(service._apply_weekly_feedback(payload) or {})
                    self._send_json(200 if bool(result.get("ok", False)) else 400, result)
                    return
                if path == "/review_market_profile_patch":
                    result = dict(service._review_market_profile_patch(payload) or {})
                    self._send_json(200 if bool(result.get("ok", False)) else 400, result)
                    return
                if path == "/refresh_dashboard":
                    result = dict(service._refresh_dashboard(payload) or {})
                    self._send_json(200 if bool(result.get("ok", False)) else 500, result)
                    return
                if path == "/toggle_flag":
                    result = dict(service._toggle_flag(payload) or {})
                    self._send_json(200 if bool(result.get("ok", False)) else 400, result)
                    return
                if path == "/set_execution_mode":
                    result = dict(service._set_execution_mode(payload) or {})
                    self._send_json(200 if bool(result.get("ok", False)) else 400, result)
                    return
                self._send_json(404, {"ok": False, "error": "not_found", "path": path})

        return Handler

    def start(self) -> None:
        if self._server is not None:
            return
        server = ThreadingHTTPServer((self.host, self.port), self._make_handler())
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, name="dashboard-control-service", daemon=True)
        thread.start()
        self._server = server
        self._thread = thread

    def stop(self) -> None:
        if self._server is None:
            return
        try:
            self._server.shutdown()
            self._server.server_close()
        finally:
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
