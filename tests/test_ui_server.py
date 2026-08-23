"""The dashboard server over real HTTP: status, credentials, host guard.

Each test starts a UiServer on an ephemeral port and speaks urllib to it;
Airflow, when needed, is a stub ThreadingHTTPServer (the test_runtime_client
pattern). No Docker anywhere.
"""

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar

import pytest

from hflow.runtime import AirflowClient, RuntimeConfig, render_bundle
from hflow.ui import UiState, build_ui_state, create_ui_server

PIPELINE_SOURCE = "import hflow\n\napp = hflow.App('demo', data_root='/opt/airflow/data')\n"

# A reserved port nothing listens on: connection refused, immediately.
UNREACHABLE_AIRFLOW_URL = "http://127.0.0.1:1"


@pytest.fixture
def rendered_bundle_dir(tmp_path: Path) -> Path:
    pipeline_file = tmp_path / "demo.py"
    pipeline_file.write_text(PIPELINE_SOURCE)
    bundle_dir = tmp_path / "runtime"
    render_bundle(
        RuntimeConfig(pipeline_file=pipeline_file, data_root=tmp_path / "data"), bundle_dir
    )
    return bundle_dir


@contextmanager
def running_ui(state: UiState) -> Iterator[str]:
    server = create_ui_server(state, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


def request_json(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    host_header: str | None = None,
) -> tuple[int, dict[str, Any]]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(f"{base_url}{path}", data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    if host_header is not None:
        request.add_header("Host", host_header)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            text = response.read().decode()
            return response.status, json.loads(text) if text else {}
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode())


class _StubAirflowHandler(BaseHTTPRequestHandler):
    healthy: ClassVar[bool] = True

    def do_GET(self) -> None:
        if self.path == "/api/v2/monitor/health":
            status = "healthy" if type(self).healthy else "unhealthy"
            body = json.dumps(
                {
                    "metadatabase": {"status": status},
                    "scheduler": {"status": status},
                    "dag_processor": {"status": status},
                    "triggerer": {"status": None},
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        pass


@contextmanager
def stub_airflow() -> Iterator[str]:
    _StubAirflowHandler.healthy = True
    server = ThreadingHTTPServer(("127.0.0.1", 0), _StubAirflowHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


class TestStatus:
    def test_degrades_without_a_bundle(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=tmp_path / "absent", data_root=None)
        with running_ui(state) as base_url:
            status, payload = request_json(base_url, "/api/status")
        assert status == 200
        assert payload["bundle"] is None
        assert payload["airflow"] is None
        assert payload["secrets_wired"] is False
        assert isinstance(payload["hflow_version"], str)

    def test_reports_bundle_and_healthy_airflow(self, rendered_bundle_dir: Path) -> None:
        state = build_ui_state(bundle_dir=rendered_bundle_dir, data_root=None)
        with stub_airflow() as airflow_url:
            state.airflow = AirflowClient(airflow_url, "airflow", "pw")
            with running_ui(state) as base_url:
                status, payload = request_json(base_url, "/api/status")
        assert status == 200
        assert payload["bundle"]["dag_id"] == "demo_ingest"
        assert payload["airflow"] == {
            "reachable": True,
            "healthy": True,
            "components": {
                "metadatabase": "healthy",
                "scheduler": "healthy",
                "dag_processor": "healthy",
                "triggerer": None,
            },
        }
        # render_bundle wires the secrets env_file, so a fresh bundle reports wired.
        assert payload["secrets_wired"] is True

    def test_reports_unreachable_airflow(self, rendered_bundle_dir: Path) -> None:
        state = build_ui_state(bundle_dir=rendered_bundle_dir, data_root=None)
        state.airflow = AirflowClient(UNREACHABLE_AIRFLOW_URL, "airflow", "pw")
        with running_ui(state) as base_url:
            status, payload = request_json(base_url, "/api/status")
        assert status == 200
        assert payload["airflow"] == {"reachable": False, "healthy": False, "components": {}}


class TestAirflowCredentials:
    def test_serves_bundle_credentials(self, rendered_bundle_dir: Path) -> None:
        state = build_ui_state(bundle_dir=rendered_bundle_dir, data_root=None)
        assert state.bundle is not None
        with running_ui(state) as base_url:
            status, payload = request_json(base_url, "/api/airflow-credentials")
        assert status == 200
        assert payload == {
            "url": state.bundle.api_base_url,
            "username": state.bundle.admin_username,
            "password": state.bundle.admin_password,
        }

    def test_404_without_a_bundle(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=tmp_path / "absent", data_root=None)
        with running_ui(state) as base_url:
            status, payload = request_json(base_url, "/api/airflow-credentials")
        assert status == 404
        assert payload["hint"] == "hflow up"


class TestTransport:
    def test_foreign_host_header_is_refused(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=tmp_path / "absent", data_root=None)
        with running_ui(state) as base_url:
            status, payload = request_json(
                base_url, "/api/status", host_header="evil.example.com:4400"
            )
        assert status == 403
        assert "host" in payload["error"]

    def test_unknown_endpoint_is_404(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=tmp_path / "absent", data_root=None)
        with running_ui(state) as base_url:
            status, _ = request_json(base_url, "/api/does-not-exist")
        assert status == 404

    def test_mutation_requires_json_content_type(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=tmp_path / "absent", data_root=None)
        with running_ui(state) as base_url:
            request = urllib.request.Request(
                f"{base_url}/api/secrets/KEY", data=b"value=x", method="PUT"
            )
            request.add_header("Content-Type", "application/x-www-form-urlencoded")
            with pytest.raises(urllib.error.HTTPError) as excinfo:
                urllib.request.urlopen(request, timeout=10)
        assert excinfo.value.code == 415
