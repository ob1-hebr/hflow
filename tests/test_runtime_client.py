"""AirflowClient against a stub HTTP server (no Docker, no Airflow)."""

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar

import pytest

from hflow.runtime import AirflowClient, AirflowClientError


class _StubAirflowHandler(BaseHTTPRequestHandler):
    """Just enough of Airflow's surface: token, health, dagRuns."""

    issued_tokens: ClassVar[list[str]] = []
    requests_seen: ClassVar[list[tuple[str, str, dict[str, Any] | None, str | None]]] = []
    healthy: ClassVar[bool] = True
    expire_first_token: ClassVar[bool] = False
    dag_run_list: ClassVar[list[dict[str, Any]]] = []
    task_instances: ClassVar[list[dict[str, Any]]] = []
    dag_task_list: ClassVar[list[dict[str, Any]]] = []

    def _read_json(self) -> dict[str, Any] | None:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return None
        return json.loads(self.rfile.read(length))

    def _respond(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _record(self, payload: dict[str, Any] | None) -> str | None:
        authorization = self.headers.get("Authorization")
        type(self).requests_seen.append((self.command, self.path, payload, authorization))
        return authorization

    def do_POST(self) -> None:
        payload = self._read_json()
        authorization = self._record(payload)
        if self.path == "/auth/token":
            assert payload is not None
            if payload.get("password") != "right-password":
                self._respond(401, {"detail": "bad credentials"})
                return
            token = f"token-{len(type(self).issued_tokens)}"
            type(self).issued_tokens.append(token)
            self._respond(201, {"access_token": token})
            return
        if self.path.startswith("/api/v2/dags/") and self.path.endswith("/dagRuns"):
            if not self._bearer_ok(authorization):
                self._respond(401, {"detail": "expired"})
                return
            requested_run_id = (payload or {}).get("dag_run_id")
            if requested_run_id == "already-exists" or (payload or {}).get("conf", {}).get(
                "force_conflict"
            ):
                self._respond(409, {"detail": "dag run already exists"})
                return
            self._respond(200, {"dag_run_id": requested_run_id or "manual__1", "state": "queued"})
            return
        self._respond(404, {"detail": self.path})

    def do_GET(self) -> None:
        authorization = self._record(None)
        if "/xcomEntries/" in self.path:
            if not self._bearer_ok(authorization):
                self._respond(401, {"detail": "expired"})
                return
            self._respond(200, {"key": self.path.rsplit("/", 1)[-1], "value": "sub-run-7"})
            return
        if "/taskInstances" in self.path:
            if not self._bearer_ok(authorization):
                self._respond(401, {"detail": "expired"})
                return
            self._respond(200, {"task_instances": type(self).task_instances})
            return
        if self.path.endswith("/tasks"):
            if not self._bearer_ok(authorization):
                self._respond(401, {"detail": "expired"})
                return
            tasks = type(self).dag_task_list
            self._respond(200, {"tasks": tasks, "total_entries": len(tasks)})
            return
        if "/dagRuns?" in self.path:
            if not self._bearer_ok(authorization):
                self._respond(401, {"detail": "expired"})
                return
            self._respond(200, {"dag_runs": type(self).dag_run_list})
            return
        if "/dagRuns/" in self.path:
            if not self._bearer_ok(authorization):
                self._respond(401, {"detail": "expired"})
                return
            existing_run_id = self.path.rsplit("/", 1)[-1]
            self._respond(200, {"dag_run_id": existing_run_id, "state": "running"})
            return
        if self.path == "/api/v2/monitor/health":
            status = "healthy" if type(self).healthy else "unhealthy"
            self._respond(
                200,  # always 200: the body is the signal
                {
                    "metadatabase": {"status": status},
                    "scheduler": {"status": status, "latest_scheduler_heartbeat": "now"},
                    "triggerer": {"status": None},
                    "dag_processor": {"status": status},
                },
            )
            return
        self._respond(404, {"detail": self.path})

    def _bearer_ok(self, authorization: str | None) -> bool:
        if authorization is None or not authorization.startswith("Bearer "):
            return False
        token = authorization.removeprefix("Bearer ")
        if type(self).expire_first_token and token == type(self).issued_tokens[0]:
            return False  # simulate an expired first token
        return token in type(self).issued_tokens

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture()
def stub_server() -> Iterator[str]:
    _StubAirflowHandler.issued_tokens = []
    _StubAirflowHandler.requests_seen = []
    _StubAirflowHandler.healthy = True
    _StubAirflowHandler.expire_first_token = False
    _StubAirflowHandler.dag_run_list = []
    _StubAirflowHandler.task_instances = []
    _StubAirflowHandler.dag_task_list = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _StubAirflowHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()


def test_trigger_fetches_token_once_and_sends_bearer(stub_server: str) -> None:
    client = AirflowClient(stub_server, "airflow", "right-password")
    first = client.trigger_dag_run("pipeline_ingest", conf={"uris": ["a.mcap"]})
    second = client.ingest("pipeline_ingest", ["b.mcap"])
    assert first["state"] == "queued" and second["state"] == "queued"
    assert len(_StubAirflowHandler.issued_tokens) == 1  # token cached across calls

    trigger_requests = [
        entry for entry in _StubAirflowHandler.requests_seen if entry[1].endswith("/dagRuns")
    ]
    method, path, payload, authorization = trigger_requests[0]
    assert (method, path) == ("POST", "/api/v2/dags/pipeline_ingest/dagRuns")
    assert payload == {"logical_date": None, "conf": {"uris": ["a.mcap"]}}
    assert authorization == "Bearer token-0"


def test_expired_token_is_refreshed_once(stub_server: str) -> None:
    _StubAirflowHandler.expire_first_token = True
    client = AirflowClient(stub_server, "airflow", "right-password")
    client._token = None  # force initial fetch
    result = client.trigger_dag_run("pipeline_ingest")
    assert result["state"] == "queued"
    assert len(_StubAirflowHandler.issued_tokens) == 2  # refreshed exactly once


def test_caller_supplied_dag_run_id_makes_retries_idempotent(stub_server: str) -> None:
    client = AirflowClient(stub_server, "airflow", "right-password")
    created = client.ingest("pipeline_ingest", ["a.mcap"], dag_run_id="my-run-1")
    assert created == {"dag_run_id": "my-run-1", "state": "queued"}

    # A retry whose id already exists gets the EXISTING run back, not a 409.
    existing = client.trigger_dag_run("pipeline_ingest", dag_run_id="already-exists")
    assert existing == {"dag_run_id": "already-exists", "state": "running"}


def test_conflict_without_a_dag_run_id_still_raises(stub_server: str) -> None:
    # Without a caller id there is nothing to idempotently return; the 409
    # must surface.
    client = AirflowClient(stub_server, "airflow", "right-password")
    with pytest.raises(AirflowClientError) as error_info:
        client.trigger_dag_run("pipeline_ingest", conf={"force_conflict": True})
    assert error_info.value.status == 409


def test_bad_credentials_surface_clearly(stub_server: str) -> None:
    client = AirflowClient(stub_server, "airflow", "wrong-password")
    with pytest.raises(AirflowClientError) as error_info:
        client.trigger_dag_run("pipeline_ingest")
    assert error_info.value.status == 401


def test_health_parses_body_not_status(stub_server: str) -> None:
    client = AirflowClient(stub_server, "airflow", "right-password")
    assert client.health().healthy

    _StubAirflowHandler.healthy = False
    unhealthy = client.health()  # still HTTP 200: the body is the signal
    assert not unhealthy.healthy
    assert "scheduler=unhealthy" in unhealthy.summary()


def test_wait_until_healthy_times_out_with_last_status(stub_server: str) -> None:
    _StubAirflowHandler.healthy = False
    client = AirflowClient(stub_server, "airflow", "right-password")
    with pytest.raises(TimeoutError, match="scheduler=unhealthy"):
        client.wait_until_healthy(timeout_s=0.3, poll_interval_s=0.1)


RUN_ID_WITH_TIMESTAMP = "manual__2026-08-23T04:12:09+00:00"
QUOTED_RUN_ID = "manual__2026-08-23T04%3A12%3A09%2B00%3A00"


def test_dag_runs_list_passes_limit_and_order_by(stub_server: str) -> None:
    _StubAirflowHandler.dag_run_list = [{"dag_run_id": "manual__1", "state": "success"}]
    client = AirflowClient(stub_server, "airflow", "right-password")
    runs = client.dag_runs("pipeline_ingest", limit=7, order_by="-run_after")
    assert runs == [{"dag_run_id": "manual__1", "state": "success"}]
    method, path, _, _ = _StubAirflowHandler.requests_seen[-1]
    assert (method, path) == (
        "GET",
        "/api/v2/dags/pipeline_ingest/dagRuns?limit=7&order_by=-run_after",
    )


def test_task_instances_quotes_the_run_id(stub_server: str) -> None:
    _StubAirflowHandler.task_instances = [{"task_id": "plan", "state": "success"}]
    client = AirflowClient(stub_server, "airflow", "right-password")
    instances = client.task_instances("pipeline_ingest", RUN_ID_WITH_TIMESTAMP)
    assert instances == [{"task_id": "plan", "state": "success"}]
    _, path, _, _ = _StubAirflowHandler.requests_seen[-1]
    assert path == (f"/api/v2/dags/pipeline_ingest/dagRuns/{QUOTED_RUN_ID}/taskInstances?limit=100")


def test_dag_tasks_lists_task_definitions(stub_server: str) -> None:
    _StubAirflowHandler.dag_task_list = [
        {"task_id": "plan", "downstream_task_ids": ["process_batch"], "is_mapped": False}
    ]
    client = AirflowClient(stub_server, "airflow", "right-password")
    tasks = client.dag_tasks("pipeline_sync")
    assert tasks == _StubAirflowHandler.dag_task_list
    method, path, _, authorization = _StubAirflowHandler.requests_seen[-1]
    assert (method, path) == ("GET", "/api/v2/dags/pipeline_sync/tasks")
    assert authorization is not None and authorization.startswith("Bearer ")


def test_xcom_entry_fetches_one_key(stub_server: str) -> None:
    client = AirflowClient(stub_server, "airflow", "right-password")
    entry = client.xcom_entry(
        "pipeline_ingest", RUN_ID_WITH_TIMESTAMP, "trigger_sync", "trigger_run_id"
    )
    assert entry == {"key": "trigger_run_id", "value": "sub-run-7"}
    _, path, _, _ = _StubAirflowHandler.requests_seen[-1]
    assert path == (
        f"/api/v2/dags/pipeline_ingest/dagRuns/{QUOTED_RUN_ID}"
        "/taskInstances/trigger_sync/xcomEntries/trigger_run_id"
    )


def test_dag_run_detail_quotes_the_run_id(stub_server: str) -> None:
    client = AirflowClient(stub_server, "airflow", "right-password")
    client.dag_run("pipeline_ingest", RUN_ID_WITH_TIMESTAMP)
    _, path, _, _ = _StubAirflowHandler.requests_seen[-1]
    assert path == f"/api/v2/dags/pipeline_ingest/dagRuns/{QUOTED_RUN_ID}"
