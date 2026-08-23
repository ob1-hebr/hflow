"""The dashboard's HTTP server: stdlib-only, loopback-only.

Serves the static frontend from the packaged ``_static`` directory and the
JSON API from a flat route table of ``(method, pattern, handler)`` tuples.
Two deliberate stances:

- Binds ``127.0.0.1`` and refuses requests whose ``Host`` header names
  anything else -- the DNS-rebinding guard that matters for a localhost
  server able to read the user's secrets file.
- Static assets are enumerated once at startup into a dict, so there is no
  filesystem lookup per request and no path-traversal surface at all.
"""

import json
import re
import sys
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from typing import Any, cast
from urllib.parse import parse_qs

from hflow.ui._handlers import (
    JsonResponse,
    airflow_credentials_handler,
    status_handler,
)
from hflow.ui._state import UiState

_ROUTES: list[tuple[str, re.Pattern[str], Any]] = [
    ("GET", re.compile(r"^/api/status$"), status_handler),
    ("GET", re.compile(r"^/api/airflow-credentials$"), airflow_credentials_handler),
]

_ALLOWED_HOSTNAMES = frozenset({"127.0.0.1", "localhost", "::1", "[::1]"})
_MAX_BODY_BYTES = 1024 * 1024

_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
}


def _host_allowed(host_header: str | None) -> bool:
    if not host_header:
        return False
    if host_header.startswith("["):  # bracketed IPv6, maybe with :port
        hostname = host_header.rsplit("]", 1)[0] + "]"
    else:
        hostname = host_header.rsplit(":", 1)[0] if ":" in host_header else host_header
    return hostname.lower() in _ALLOWED_HOSTNAMES


def _load_static_assets() -> dict[str, tuple[bytes, str]]:
    """Read the packaged frontend into memory as {relative path: (bytes, type)}."""
    assets: dict[str, tuple[bytes, str]] = {}
    static_root = resources.files("hflow.ui") / "_static"
    if not static_root.is_dir():
        return assets

    def walk(node: Any, key_prefix: str) -> None:
        for entry in node.iterdir():
            key = f"{key_prefix}{entry.name}"
            if entry.is_dir():
                walk(entry, f"{key}/")
            else:
                suffix = "." + entry.name.rsplit(".", 1)[-1] if "." in entry.name else ""
                content_type = _CONTENT_TYPES.get(suffix, "application/octet-stream")
                assets[key] = (entry.read_bytes(), content_type)

    walk(static_root, "")
    return assets


class UiServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], state: UiState) -> None:
        super().__init__(server_address, UiRequestHandler)
        self.state = state
        self.static_assets = _load_static_assets()


class UiRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def do_PUT(self) -> None:
        self._dispatch("PUT")

    def do_DELETE(self) -> None:
        self._dispatch("DELETE")

    def _dispatch(self, method: str) -> None:
        server = cast("UiServer", self.server)
        if not _host_allowed(self.headers.get("Host")):
            self._respond_json(403, {"error": "forbidden host header"})
            return
        path, _, raw_query = self.path.partition("?")
        if path.startswith("/api/"):
            self._dispatch_api(server, method, path, raw_query)
            return
        if method != "GET":
            self._respond_json(404, {"error": f"no such endpoint: {method} {path}"})
            return
        asset_key = "index.html" if path == "/" else path.lstrip("/")
        asset = server.static_assets.get(asset_key)
        if asset is None:
            self._respond_json(404, {"error": f"no such path: {path}"})
            return
        body, content_type = asset
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _dispatch_api(self, server: "UiServer", method: str, path: str, raw_query: str) -> None:
        body: dict[str, Any] | None = None
        if method in ("POST", "PUT"):
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length > _MAX_BODY_BYTES:
                self._respond_json(413, {"error": "request body too large"})
                return
            content_type = self.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                self._respond_json(415, {"error": "expected application/json"})
                return
            try:
                body = json.loads(self.rfile.read(content_length)) if content_length else {}
            except json.JSONDecodeError as error:
                self._respond_json(400, {"error": f"invalid JSON body: {error}"})
                return
            if not isinstance(body, dict):
                self._respond_json(400, {"error": "expected a JSON object body"})
                return
        for route_method, pattern, handler in _ROUTES:
            match = pattern.match(path)
            if route_method != method or match is None:
                continue
            query = parse_qs(raw_query)
            try:
                status, payload = cast("JsonResponse", handler(server.state, match, query, body))
            except ValueError as error:
                status, payload = 400, {"error": str(error)}
            except ModuleNotFoundError as error:
                # The optional-extra install hint (hflow[bucket]) travels whole.
                status, payload = 400, {"error": str(error)}
            except Exception as error:  # the dashboard must not die mid-poll
                status, payload = 500, {"error": f"{type(error).__name__}: {error}"}
            self._respond_json(status, payload)
            return
        self._respond_json(404, {"error": f"no such endpoint: {method} {path}"})

    def _respond_json(self, status: int, payload: dict[str, Any]) -> None:
        if status == 204:
            self.send_response(204)
            self.end_headers()
            return
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # stdlib signature
        pass


def create_ui_server(state: UiState, *, port: int = 0) -> UiServer:
    return UiServer(("127.0.0.1", port), state)


def serve_ui(state: UiState, *, port: int, open_browser: bool) -> int:
    server = create_ui_server(state, port=port)
    url = f"http://127.0.0.1:{server.server_port}"
    print(f"hflow dashboard: {url}")
    print("press Ctrl+C to stop", file=sys.stderr)
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
