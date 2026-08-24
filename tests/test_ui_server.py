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
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar

import pytest

import hflow
from hflow.catalog import Catalog, CheckRunRow
from hflow.runtime import AirflowClient, RuntimeConfig, render_bundle
from hflow.transform import EpisodeStamps
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
    dag_run_list: ClassVar[list[dict[str, Any]]] = []
    task_instances_by_run: ClassVar[dict[str, list[dict[str, Any]]]] = {}
    xcom_values: ClassVar[dict[tuple[str, str], str]] = {}  # (run_id, task_id) -> value
    dag_tasks_by_dag: ClassVar[dict[str, list[dict[str, Any]]]] = {}
    task_instance_requests: ClassVar[list[str]] = []
    xcom_requests: ClassVar[list[tuple[str, str]]] = []
    dag_tasks_requests: ClassVar[list[str]] = []
    reject_order_by: ClassVar[bool] = False

    def _respond_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path == "/auth/token":
            self._respond_json(201, {"access_token": "stub-token"})
            return
        self._respond_json(404, {"detail": self.path})

    def do_GET(self) -> None:
        from urllib.parse import unquote

        cls = type(self)
        path, _, query = self.path.partition("?")
        segments = [unquote(segment) for segment in path.split("/") if segment]
        if "xcomEntries" in segments:
            # /api/v2/dags/{dag}/dagRuns/{run}/taskInstances/{task}/xcomEntries/{key}
            run_id, task_id, key = segments[5], segments[7], segments[9]
            cls.xcom_requests.append((run_id, task_id))
            value = cls.xcom_values.get((run_id, task_id))
            if value is None:
                self._respond_json(404, {"detail": "no xcom"})
                return
            self._respond_json(200, {"key": key, "value": value})
            return
        if segments[-1] == "taskInstances":
            run_id = segments[5]
            cls.task_instance_requests.append(run_id)
            self._respond_json(200, {"task_instances": cls.task_instances_by_run.get(run_id, [])})
            return
        if segments[-1] == "tasks" and len(segments) == 5:
            # /api/v2/dags/{dag}/tasks
            dag_id = segments[3]
            cls.dag_tasks_requests.append(dag_id)
            tasks = cls.dag_tasks_by_dag.get(dag_id, [])
            self._respond_json(200, {"tasks": tasks, "total_entries": len(tasks)})
            return
        if segments[-1] == "dagRuns":
            if cls.reject_order_by and "order_by" in query:
                self._respond_json(400, {"detail": "unknown order_by field"})
                return
            self._respond_json(200, {"dag_runs": cls.dag_run_list})
            return
        if len(segments) >= 6 and segments[4] == "dagRuns":
            run_id = segments[5]
            for run in cls.dag_run_list:
                if run["dag_run_id"] == run_id:
                    self._respond_json(200, run)
                    return
            self._respond_json(404, {"detail": "no such run"})
            return
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
    _StubAirflowHandler.dag_run_list = []
    _StubAirflowHandler.task_instances_by_run = {}
    _StubAirflowHandler.xcom_values = {}
    _StubAirflowHandler.dag_tasks_by_dag = {}
    _StubAirflowHandler.task_instance_requests = []
    _StubAirflowHandler.xcom_requests = []
    _StubAirflowHandler.dag_tasks_requests = []
    _StubAirflowHandler.reject_order_by = False
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


SUCCESS_RUN_ID = "manual__2026-08-23T01:00:00+00:00"
RUNNING_RUN_ID = "manual__2026-08-23T02:00:00+00:00"

FULL_SUCCESS_INSTANCES = [
    {"task_id": f"{prefix}_{stage}", "state": "success"}
    for stage in ("sync", "meta", "labels", "media")
    for prefix in ("enabled", "trigger")
]
MID_META_INSTANCES = [
    {"task_id": "enabled_sync", "state": "success"},
    {"task_id": "trigger_sync", "state": "success"},
    {"task_id": "enabled_meta", "state": "success"},
    {"task_id": "trigger_meta", "state": "deferred"},
]


def seed_two_runs() -> None:
    _StubAirflowHandler.dag_run_list = [
        {
            "dag_run_id": RUNNING_RUN_ID,
            "state": "running",
            "run_after": "2026-08-23T02:00:00+00:00",
            "start_date": "2026-08-23T02:00:01+00:00",
            "end_date": None,
            "conf": {"uris": ["a.mcap", "b.mcap"], "profile": "full", "mode": "batch"},
        },
        {
            "dag_run_id": SUCCESS_RUN_ID,
            "state": "success",
            "run_after": "2026-08-23T01:00:00+00:00",
            "start_date": "2026-08-23T01:00:01+00:00",
            "end_date": "2026-08-23T01:07:01.500000+00:00",
            "conf": {"uris": ["a.mcap", "b.mcap", "c.mcap"], "profile": "full", "mode": "batch"},
        },
    ]
    _StubAirflowHandler.task_instances_by_run = {
        SUCCESS_RUN_ID: list(FULL_SUCCESS_INSTANCES),
        RUNNING_RUN_ID: list(MID_META_INSTANCES),
    }


ENCODED_SUCCESS_RUN_ID = "manual__2026-08-23T01%3A00%3A00%2B00%3A00"
STAGES = ("sync", "meta", "labels", "media")


def graph_task(
    task_id: str,
    downstream: list[str],
    *,
    display: str | None = None,
    operator: str = "@task",
    mapped: bool = False,
) -> dict[str, Any]:
    """One entry as Airflow's ``/dags/{id}/tasks`` returns it."""
    return {
        "task_id": task_id,
        "task_display_name": display or task_id,
        "operator_name": operator,
        "downstream_task_ids": downstream,
        "is_mapped": mapped,
        "doc_md": f"{task_id} doc line",
        "trigger_rule": "all_success",
    }


# The master's real shape: the profile fans out to every gate, and each
# trigger gates the next stage. 9 tasks, 11 edges.
MASTER_TASKS = [
    graph_task("resolve_profile", [f"enabled_{stage}" for stage in STAGES]),
    *[
        task
        for index, stage in enumerate(STAGES)
        for task in (
            graph_task(f"enabled_{stage}", [f"trigger_{stage}"]),
            graph_task(
                f"trigger_{stage}",
                [f"enabled_{STAGES[index + 1]}"] if index + 1 < len(STAGES) else [],
                display=f"trigger {stage} · waits (deferred)",
                operator="TriggerDagRunOperator",
            ),
        )
    ],
]
SUB_TASKS = [
    graph_task("plan", ["process_batch"], operator="@task.external_python"),
    graph_task(
        "process_batch", ["error_budget_gate"], operator="@task.external_python", mapped=True
    ),
    graph_task("error_budget_gate", [], operator="@task.external_python"),
]


def graph_instance(
    task_id: str,
    state: str | None,
    *,
    map_index: int = -1,
    start: str | None = None,
    end: str | None = None,
    duration: float | None = None,
    tries: int = 1,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "state": state,
        "map_index": map_index,
        "start_date": start,
        "end_date": end,
        "duration": duration,
        "try_number": tries,
        "rendered_map_index": str(map_index) if map_index >= 0 else None,
    }


SUB_RUN_ID = "manual__sub-sync"


def seed_graph_run(instances: list[dict[str, Any]], *, state: str = "running") -> None:
    """One master run plus both DAG structures the graph endpoints read."""
    _StubAirflowHandler.dag_run_list = [
        {
            "dag_run_id": SUCCESS_RUN_ID,
            "state": state,
            "run_after": "2026-08-23T01:00:00+00:00",
            "start_date": "2026-08-23T01:00:01+00:00",
            "end_date": None,
            "conf": {"uris": ["a.mcap", "b.mcap"], "profile": "full", "mode": "batch"},
        }
    ]
    _StubAirflowHandler.task_instances_by_run = {SUCCESS_RUN_ID: instances}
    _StubAirflowHandler.dag_tasks_by_dag = {"demo_ingest": MASTER_TASKS, "demo_sync": SUB_TASKS}


def seed_sub_run(instances: list[dict[str, Any]], *, state: str = "running") -> None:
    """A resolved sync sub-run for the stage graph, linked by the trigger XCom."""
    _StubAirflowHandler.xcom_values = {(SUCCESS_RUN_ID, "trigger_sync"): f'"{SUB_RUN_ID}"'}
    _StubAirflowHandler.dag_run_list.append(
        {
            "dag_run_id": SUB_RUN_ID,
            "state": state,
            "run_after": "2026-08-23T01:00:02+00:00",
            "start_date": "2026-08-23T01:00:03+00:00",
            "end_date": None,
            "conf": {"uris": ["a.mcap", "b.mcap"], "mode": "batch"},
        }
    )
    _StubAirflowHandler.task_instances_by_run[SUB_RUN_ID] = instances


@pytest.fixture
def observing_state(rendered_bundle_dir: Path) -> Iterator[UiState]:
    """A UiState whose bundle is rendered and whose Airflow is the stub."""
    state = build_ui_state(bundle_dir=rendered_bundle_dir, data_root=None)
    with stub_airflow() as airflow_url:
        state.airflow = AirflowClient(airflow_url, "airflow", "pw")
        yield state


class TestPipelinesRuns:
    def test_503_without_a_bundle(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=tmp_path / "absent", data_root=None)
        with running_ui(state) as base_url:
            status, payload = request_json(base_url, "/api/pipelines/runs")
        assert status == 503
        assert payload["hint"] == "hflow up"

    def test_503_when_airflow_is_unreachable(self, rendered_bundle_dir: Path) -> None:
        state = build_ui_state(bundle_dir=rendered_bundle_dir, data_root=None)
        state.airflow = AirflowClient(UNREACHABLE_AIRFLOW_URL, "airflow", "pw")
        with running_ui(state) as base_url:
            status, payload = request_json(base_url, "/api/pipelines/runs")
        assert status == 503
        assert payload["hint"] == "hflow up"

    def test_runs_map_to_summaries_with_stages(self, observing_state: UiState) -> None:
        seed_two_runs()
        with running_ui(observing_state) as base_url:
            status, payload = request_json(base_url, "/api/pipelines/runs")
        assert status == 200
        assert payload["dag_id"] == "demo_ingest"
        running_run, success_run = payload["runs"]  # newest first
        assert running_run["run_id"] == RUNNING_RUN_ID
        assert running_run["episode_count"] == 2
        assert running_run["duration_s"] is None
        assert running_run["stages"] == {
            "sync": "success",
            "meta": "running",
            "labels": "pending",
            "media": "pending",
        }
        assert success_run["state"] == "success"
        assert success_run["episode_count"] == 3
        assert success_run["duration_s"] == 420.5
        assert success_run["profile"] == "full"
        assert success_run["stages"] == {
            "sync": "success",
            "meta": "success",
            "labels": "success",
            "media": "success",
        }
        # Deep links are backend-owned and percent-encode the run id.
        assert success_run["airflow_url"].endswith(
            "/dags/demo_ingest/runs/manual__2026-08-23T01%3A00%3A00%2B00%3A00"
        )

    def test_terminal_stage_states_are_cached(self, observing_state: UiState) -> None:
        seed_two_runs()
        with running_ui(observing_state) as base_url:
            request_json(base_url, "/api/pipelines/runs")
            first_pass = list(_StubAirflowHandler.task_instance_requests)
            # A terminal run's instances changing later must not matter: the
            # second poll asks Airflow only about the still-running run.
            _StubAirflowHandler.task_instances_by_run[SUCCESS_RUN_ID] = []
            _, payload = request_json(base_url, "/api/pipelines/runs")
        assert sorted(first_pass) == sorted([SUCCESS_RUN_ID, RUNNING_RUN_ID])
        assert _StubAirflowHandler.task_instance_requests.count(SUCCESS_RUN_ID) == 1
        assert _StubAirflowHandler.task_instance_requests.count(RUNNING_RUN_ID) == 2
        _, success_run = payload["runs"]
        assert success_run["stages"]["media"] == "success"

    def test_order_by_rejection_falls_back(self, observing_state: UiState) -> None:
        seed_two_runs()
        _StubAirflowHandler.reject_order_by = True
        with running_ui(observing_state) as base_url:
            status, payload = request_json(base_url, "/api/pipelines/runs")
        assert status == 200
        assert [run["run_id"] for run in payload["runs"]] == [RUNNING_RUN_ID, SUCCESS_RUN_ID]


class TestRunDetail:
    def test_stages_with_sub_run_links(self, observing_state: UiState) -> None:
        seed_two_runs()
        _StubAirflowHandler.xcom_values = {
            # JSON-encoded and plain forms both appear in the wild.
            (SUCCESS_RUN_ID, "trigger_sync"): '"manual__sub-sync"',
            (SUCCESS_RUN_ID, "trigger_meta"): "manual__sub-meta",
        }
        encoded_run_id = "manual__2026-08-23T01%3A00%3A00%2B00%3A00"
        with running_ui(observing_state) as base_url:
            status, payload = request_json(base_url, f"/api/pipelines/runs/{encoded_run_id}")
        assert status == 200
        assert payload["run"]["run_id"] == SUCCESS_RUN_ID
        stages = {stage["stage"]: stage for stage in payload["stages"]}
        assert stages["sync"]["sub_dag_id"] == "demo_sync"
        assert stages["sync"]["sub_run_id"] == "manual__sub-sync"
        assert stages["sync"]["sub_run_url"].endswith("/dags/demo_sync/runs/manual__sub-sync")
        assert stages["meta"]["sub_run_id"] == "manual__sub-meta"
        # No XCom pushed yet: the stage page link still works, the run link is null.
        assert stages["labels"]["sub_run_id"] is None
        assert stages["labels"]["sub_run_url"] is None
        assert stages["labels"]["sub_dag_url"].endswith("/dags/demo_labels")

    def test_skipped_stages_ask_no_xcom(self, observing_state: UiState) -> None:
        backfill_run_id = "manual__backfill"
        _StubAirflowHandler.dag_run_list = [
            {
                "dag_run_id": backfill_run_id,
                "state": "success",
                "run_after": "2026-08-23T03:00:00+00:00",
                "conf": {"uris": ["a.mcap"], "profile": "metadata_backfill", "mode": "batch"},
            }
        ]
        _StubAirflowHandler.task_instances_by_run = {
            backfill_run_id: [
                {"task_id": "enabled_sync", "state": "skipped"},
                {"task_id": "enabled_meta", "state": "success"},
                {"task_id": "trigger_meta", "state": "success"},
                {"task_id": "enabled_labels", "state": "skipped"},
                {"task_id": "enabled_media", "state": "skipped"},
            ]
        }
        with running_ui(observing_state) as base_url:
            status, payload = request_json(base_url, f"/api/pipelines/runs/{backfill_run_id}")
        assert status == 200
        stages = {stage["stage"]: stage["state"] for stage in payload["stages"]}
        assert stages == {
            "sync": "skipped",
            "meta": "success",
            "labels": "skipped",
            "media": "skipped",
        }
        # Only the one enabled stage was worth an XCom lookup (its 404 here
        # degrades to a null sub_run_id without surfacing an error).
        assert _StubAirflowHandler.xcom_requests == [(backfill_run_id, "trigger_meta")]

    def test_unknown_run_is_404(self, observing_state: UiState) -> None:
        seed_two_runs()
        with running_ui(observing_state) as base_url:
            status, _ = request_json(base_url, "/api/pipelines/runs/manual__absent")
        assert status == 404

    def test_cards_carry_stage_identity_without_a_catalog(self, observing_state: UiState) -> None:
        # No data root to read: the pipeline still reads as a pipeline, it just
        # cannot say how many episodes are through.
        seed_two_runs()
        with running_ui(observing_state) as base_url:
            _, payload = request_json(base_url, f"/api/pipelines/runs/{ENCODED_SUCCESS_RUN_ID}")
        cards = {card["stage"]: card for card in payload["stages"]}
        assert cards["meta"]["title"] == "Quality checks"
        assert "quarantine" in cards["meta"]["description"]
        assert cards["meta"]["layer"] == "automated"
        assert cards["labels"]["layer"] == "model"
        assert all(card["progress"] is None for card in cards.values())


# --- stage cards over a real catalog -----------------------------------------
#
# Progress is read from the Parquet the pipeline itself writes, so these tests
# write real catalog appends and place the run's stage windows around them.
# Appends are stamped "now", which is why the runs are seeded relative to now
# rather than at fixed dates.

CATALOG_STAMPS = EpisodeStamps(
    schema_version="1",
    pipeline_version="abc123def456",
    ffmpeg_version="ffmpeg version test",
    robot_software_version="sim-0.1.0",
)
PROGRESS_URIS = ["a.mcap", "b.mcap", "c.mcap", "d.mcap", "e.mcap"]


def append_episode_row(
    data_root: Path, index: int, *, quarantined: bool = False, errored: bool = False
) -> None:
    """One finished episode, recorded the way a stage's process_batch records it."""
    canonical = data_root / f"episode-{index}.canonical.mcap"
    canonical.write_bytes(f"canonical bytes {index}".encode())
    check_rows = []
    if quarantined or errored:
        check_rows.append(
            CheckRunRow(
                check_name="camera_health",
                check_version="v1",
                critical=True,
                status=hflow.CheckStatus.ERROR if errored else hflow.CheckStatus.FAILED,
                duration_s=0.5,
                error="boom" if errored else None,
            )
        )
    Catalog(data_root / "catalog").append_episode(
        canonical_path=canonical,
        stamps=CATALOG_STAMPS,
        episode_metadata={},
        check_rows=check_rows,
        quarantine_tags=["quarantined:camera_health"] if quarantined else [],
        source_uri=PROGRESS_URIS[index],
    )


PROGRESS_RUN_ID = "manual__progress"


def seed_progress_run(
    *, run_state: str, stage_windows: dict[str, tuple[float, float | None]]
) -> None:
    """A run whose stage windows are offsets in seconds before now.

    ``stage_windows`` maps a stage to (started_s_ago, ended_s_ago); an ended
    offset of None leaves the stage running, so its window stays open.
    """
    now = datetime.now(UTC)

    def stamp(seconds_ago: float) -> str:
        return (now - timedelta(seconds=seconds_ago)).isoformat()

    instances: list[dict[str, Any]] = []
    for stage in STAGES:
        window = stage_windows.get(stage)
        if window is None:
            instances.append({"task_id": f"enabled_{stage}", "state": "success"})
            continue
        started, ended = window
        instances.append({"task_id": f"enabled_{stage}", "state": "success"})
        instances.append(
            {
                "task_id": f"trigger_{stage}",
                "state": "success" if ended is not None else "deferred",
                "start_date": stamp(started),
                "end_date": None if ended is None else stamp(ended),
            }
        )
    _StubAirflowHandler.dag_run_list = [
        {
            "dag_run_id": PROGRESS_RUN_ID,
            "state": run_state,
            "run_after": stamp(600),
            "start_date": stamp(600),
            "end_date": None if run_state == "running" else stamp(0),
            "conf": {"uris": list(PROGRESS_URIS), "profile": "full", "mode": "batch"},
        }
    ]
    _StubAirflowHandler.task_instances_by_run = {PROGRESS_RUN_ID: instances}


@pytest.fixture
def catalog_state(rendered_bundle_dir: Path, tmp_path: Path) -> Iterator[UiState]:
    """An observing UiState whose data root is where the catalog gets written."""
    data_root = tmp_path / "data"
    data_root.mkdir(exist_ok=True)
    state = build_ui_state(bundle_dir=rendered_bundle_dir, data_root=data_root)
    assert state.data_root is not None
    with stub_airflow() as airflow_url:
        state.airflow = AirflowClient(airflow_url, "airflow", "pw")
        yield state


class TestStageCardProgress:
    def test_a_running_stage_counts_the_episodes_it_finished(self, catalog_state: UiState) -> None:
        assert catalog_state.data_root is not None
        for index in range(3):
            append_episode_row(catalog_state.data_root, index)
        append_episode_row(catalog_state.data_root, 3, quarantined=True)
        append_episode_row(catalog_state.data_root, 4, errored=True)
        # Sync finished before any of those appends; meta is running now.
        seed_progress_run(
            run_state="running", stage_windows={"sync": (600, 500), "meta": (120, None)}
        )
        with running_ui(catalog_state) as base_url:
            status, payload = request_json(base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}")

        assert status == 200
        cards = {card["stage"]: card for card in payload["stages"]}
        meta = cards["meta"]["progress"]
        assert meta["done"] == 5
        assert meta["quarantined"] == 1
        assert meta["errors"] == 1
        assert meta["throughput_eps_per_min"] > 0
        assert meta["last_completed_at"] is not None
        assert meta["stalled"] is False
        # The window is what attributes an append to a stage: sync's closed
        # before these episodes landed, so none of them are its work.
        assert cards["sync"]["progress"]["done"] == 0
        assert cards["sync"]["total"] == len(PROGRESS_URIS)

    def test_an_estimate_appears_once_something_finishes(self, catalog_state: UiState) -> None:
        assert catalog_state.data_root is not None
        append_episode_row(catalog_state.data_root, 0)
        seed_progress_run(run_state="running", stage_windows={"meta": (60, None)})
        with running_ui(catalog_state) as base_url:
            _, payload = request_json(base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}")
        meta = {card["stage"]: card for card in payload["stages"]}["meta"]
        assert meta["progress"]["done"] == 1
        assert meta["progress"]["eta_s"] > 0  # four of five left to do
        assert meta["state"] == "running"

    def test_sync_appends_no_checks_and_still_counts(self, catalog_state: UiState) -> None:
        # The sync stage records episode rows with no check rows at all; those
        # episodes are done all the same.
        assert catalog_state.data_root is not None
        for index in range(2):
            append_episode_row(catalog_state.data_root, index)
        seed_progress_run(run_state="running", stage_windows={"sync": (60, None)})
        with running_ui(catalog_state) as base_url:
            _, payload = request_json(base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}")
        cards = {card["stage"]: card for card in payload["stages"]}
        assert cards["sync"]["progress"]["done"] == 2
        assert cards["sync"]["progress"]["errors"] == 0
        # A stage that has not been triggered has no window and no progress,
        # and names the unfinished stage ahead of it.
        assert cards["labels"]["progress"] is None
        assert cards["labels"]["waiting_on"] == "meta"

    def test_pending_stages_name_the_enabled_stage_they_wait_on(
        self, catalog_state: UiState
    ) -> None:
        _StubAirflowHandler.dag_run_list = [
            {
                "dag_run_id": PROGRESS_RUN_ID,
                "state": "running",
                "run_after": "2026-08-24T01:00:00+00:00",
                "conf": {"uris": ["a.mcap"], "profile": "metadata_backfill", "mode": "batch"},
            }
        ]
        _StubAirflowHandler.task_instances_by_run = {
            PROGRESS_RUN_ID: [
                {"task_id": "enabled_sync", "state": "skipped"},
                {"task_id": "enabled_meta", "state": "success"},
            ]
        }
        with running_ui(catalog_state) as base_url:
            _, payload = request_json(base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}")
        cards = {card["stage"]: card for card in payload["stages"]}
        # Nothing enabled runs before meta here, so it waits on nothing.
        assert cards["meta"]["state"] == "pending"
        assert cards["meta"]["waiting_on"] is None
        assert cards["sync"]["state"] == "skipped"
        assert cards["sync"]["waiting_on"] is None

    def test_a_replayed_run_still_shows_progress_by_batch(self, catalog_state: UiState) -> None:
        # A run over episodes already recorded appends nothing new (the catalog
        # dedupes), so the finished batches are the only evidence of progress.
        seed_progress_run(run_state="running", stage_windows={"meta": (120, None)})
        sub_run_id = "manual__sub-meta"
        _StubAirflowHandler.xcom_values = {
            (PROGRESS_RUN_ID, "trigger_meta"): sub_run_id,
            (sub_run_id, "plan"): json.dumps(
                [{"items": PROGRESS_URIS[:3]}, {"items": PROGRESS_URIS[3:]}]
            ),
        }
        _StubAirflowHandler.task_instances_by_run[sub_run_id] = [
            {
                "task_id": "process_batch",
                "state": "success",
                "map_index": 0,
                "end_date": datetime.now(UTC).isoformat(),
            },
            {"task_id": "process_batch", "state": "running", "map_index": 1, "end_date": None},
        ]
        with running_ui(catalog_state) as base_url:
            _, payload = request_json(base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}")
        meta = {card["stage"]: card for card in payload["stages"]}["meta"]
        # The three episodes of the finished batch; the batch in flight counts
        # for nothing until it ends.
        assert meta["progress"]["done"] == 3
        assert meta["progress"]["eta_s"] > 0

    def test_stages_of_a_failed_run_are_not_waiting_on_anything(
        self, catalog_state: UiState
    ) -> None:
        # Airflow leaves the stages after a failure upstream_failed, which the
        # stage vocabulary reads as pending; they never ran and never will.
        seed_progress_run(run_state="failed", stage_windows={"sync": (120, 60)})
        _StubAirflowHandler.dag_run_list[0]["state"] = "failed"
        with running_ui(catalog_state) as base_url:
            _, payload = request_json(base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}")
        cards = {card["stage"]: card for card in payload["stages"]}
        assert cards["labels"]["state"] == "pending"
        assert cards["labels"]["waiting_on"] is None
        assert cards["labels"]["reached"] is False
        assert cards["sync"]["reached"] is True

    def test_a_finished_stage_prefers_its_own_tally_and_is_computed_once(
        self, catalog_state: UiState
    ) -> None:
        assert catalog_state.data_root is not None
        append_episode_row(catalog_state.data_root, 0)
        seed_progress_run(run_state="success", stage_windows={"meta": (120, 0)})
        sub_run_id = "manual__sub-meta"
        _StubAirflowHandler.xcom_values = {
            (PROGRESS_RUN_ID, "trigger_meta"): sub_run_id,
            (sub_run_id, "quarantine_budget_gate"): json.dumps(
                {"total": 5, "quarantined": 2, "errors": 0, "budget": 8}
            ),
        }
        with running_ui(catalog_state) as base_url:
            status, payload = request_json(base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}")
            requests_after_first = list(_StubAirflowHandler.xcom_requests)
            instances_after_first = list(_StubAirflowHandler.task_instance_requests)
            request_json(base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}")

        assert status == 200
        meta = {card["stage"]: card for card in payload["stages"]}["meta"]
        # The gate counted 5 episodes; only one append is visible in the
        # catalog (a replayed run appends nothing), and the gate wins.
        assert meta["total"] == 5
        assert meta["progress"]["done"] == 5
        assert meta["progress"]["quarantined"] == 2
        assert meta["progress"]["eta_s"] is None
        assert meta["duration_s"] == pytest.approx(120, abs=2)
        # A finished run's cards never change, so the second poll recomputes
        # nothing: no task instances, no XComs.
        assert _StubAirflowHandler.xcom_requests == requests_after_first
        assert _StubAirflowHandler.task_instance_requests == instances_after_first

    def test_a_malformed_conf_costs_the_counts_not_the_page(self, catalog_state: UiState) -> None:
        _StubAirflowHandler.dag_run_list = [
            {
                "dag_run_id": PROGRESS_RUN_ID,
                "state": "running",
                "run_after": "2026-08-24T01:00:00+00:00",
                "conf": {"uris": "a.mcap"},  # a string where a list belongs
            }
        ]
        _StubAirflowHandler.task_instances_by_run = {
            PROGRESS_RUN_ID: [
                {"task_id": "enabled_sync", "state": "success"},
                {
                    "task_id": "trigger_sync",
                    "state": "deferred",
                    "start_date": datetime.now(UTC).isoformat(),
                },
            ]
        }
        with running_ui(catalog_state) as base_url:
            status, payload = request_json(base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}")
        assert status == 200
        cards = {card["stage"]: card for card in payload["stages"]}
        assert cards["sync"]["state"] == "running"
        assert cards["sync"]["progress"] is None
        assert cards["sync"]["total"] is None


class TestStageChecks:
    """What one stage's verification found, check by check."""

    def test_breakdown_counts_outcomes_per_check(self, catalog_state: UiState) -> None:
        assert catalog_state.data_root is not None
        for index in range(2):
            append_episode_row(catalog_state.data_root, index)
        append_episode_row(catalog_state.data_root, 2, quarantined=True)
        append_episode_row(catalog_state.data_root, 3, errored=True)
        seed_progress_run(run_state="running", stage_windows={"meta": (120, None)})
        with running_ui(catalog_state) as base_url:
            status, payload = request_json(
                base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}/stages/meta/checks"
            )
        assert status == 200
        assert payload["stage"] == "meta"
        (camera_health,) = payload["checks"]
        assert camera_health["name"] == "camera_health"
        assert camera_health["critical"] is True
        assert camera_health["statuses"]["failed"] == 1
        assert camera_health["statuses"]["error"] == 1
        assert camera_health["statuses"]["passed"] == 0
        assert camera_health["episodes"] == 2
        assert camera_health["avg_duration_s"] == 0.5

    def test_appends_outside_the_stage_window_are_not_its_evidence(
        self, catalog_state: UiState
    ) -> None:
        assert catalog_state.data_root is not None
        append_episode_row(catalog_state.data_root, 0, errored=True)
        # The stage finished well before that append landed.
        seed_progress_run(run_state="running", stage_windows={"meta": (600, 500)})
        with running_ui(catalog_state) as base_url:
            _, payload = request_json(
                base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}/stages/meta/checks"
            )
        assert payload["checks"] == []

    def test_a_stage_that_records_no_checks_is_empty_not_an_error(
        self, catalog_state: UiState
    ) -> None:
        # sync writes episode rows only; the breakdown has nothing to show.
        assert catalog_state.data_root is not None
        append_episode_row(catalog_state.data_root, 0)
        seed_progress_run(run_state="running", stage_windows={"sync": (60, None)})
        with running_ui(catalog_state) as base_url:
            status, payload = request_json(
                base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}/stages/sync/checks"
            )
        assert (status, payload["checks"]) == (200, [])

    def test_without_a_catalog_the_breakdown_is_empty(self, observing_state: UiState) -> None:
        seed_two_runs()
        with running_ui(observing_state) as base_url:
            status, payload = request_json(
                base_url,
                f"/api/pipelines/runs/{ENCODED_SUCCESS_RUN_ID}/stages/meta/checks",
            )
        assert (status, payload["checks"]) == (200, [])

    def test_unknown_stage_is_404(self, catalog_state: UiState) -> None:
        seed_progress_run(run_state="running", stage_windows={"meta": (60, None)})
        with running_ui(catalog_state) as base_url:
            status, _ = request_json(
                base_url, f"/api/pipelines/runs/{PROGRESS_RUN_ID}/stages/annotation/checks"
            )
        assert status == 404

    def test_unknown_run_is_404(self, catalog_state: UiState) -> None:
        seed_progress_run(run_state="running", stage_windows={"meta": (60, None)})
        with running_ui(catalog_state) as base_url:
            status, _ = request_json(
                base_url, "/api/pipelines/runs/manual__absent/stages/meta/checks"
            )
        assert status == 404

    def test_a_finished_run_is_read_once(self, catalog_state: UiState) -> None:
        assert catalog_state.data_root is not None
        append_episode_row(catalog_state.data_root, 0, errored=True)
        seed_progress_run(run_state="success", stage_windows={"meta": (120, 0)})
        path = f"/api/pipelines/runs/{PROGRESS_RUN_ID}/stages/meta/checks"
        with running_ui(catalog_state) as base_url:
            _, first = request_json(base_url, path)
            instances_after_first = list(_StubAirflowHandler.task_instance_requests)
            _, second = request_json(base_url, path)
        assert first == second
        assert first["checks"][0]["statuses"]["error"] == 1
        assert _StubAirflowHandler.task_instance_requests == instances_after_first


class TestRunGraph:
    """The master run's DAG, drawn by the dashboard from Airflow's own shape."""

    def request_graph(self, base_url: str, path: str = "") -> tuple[int, dict[str, Any]]:
        return request_json(base_url, f"/api/pipelines/runs/{ENCODED_SUCCESS_RUN_ID}{path}/graph")

    def test_nodes_and_edges_carry_this_run_s_state(self, observing_state: UiState) -> None:
        seed_graph_run(
            [
                graph_instance("resolve_profile", "success", duration=0.08),
                graph_instance("enabled_sync", "success", duration=0.08),
                graph_instance(
                    "trigger_sync",
                    "success",
                    start="2026-08-23T01:00:02Z",
                    end="2026-08-23T01:00:16Z",
                    duration=13.61,
                ),
                graph_instance("enabled_meta", "success", duration=0.08),
                graph_instance("trigger_meta", "deferred", start="2026-08-23T01:00:17Z", tries=1),
            ]
        )
        with running_ui(observing_state) as base_url:
            status, payload = self.request_graph(base_url)
        assert status == 200
        assert payload["dag_id"] == "demo_ingest"
        assert payload["run"]["run_id"] == SUCCESS_RUN_ID
        assert len(payload["nodes"]) == 9
        assert len(payload["edges"]) == 11
        nodes = {node["id"]: node for node in payload["nodes"]}

        sync = nodes["trigger_sync"]
        assert sync["state"] == "success"
        assert sync["duration_s"] == 13.6
        assert sync["label"] == "trigger sync · waits (deferred)"
        assert sync["operator"] == "TriggerDagRunOperator"
        assert sync["doc"] == "trigger_sync doc line"
        assert sync["stage"] == "sync"  # the drill-in target
        assert sync["mapped"] is None
        assert sync["airflow_url"].endswith(
            f"/dags/demo_ingest/runs/{ENCODED_SUCCESS_RUN_ID}/tasks/trigger_sync"
        )
        # Waiting on a stage is "running" to a reader; the raw state stays available.
        assert (nodes["trigger_meta"]["state"], nodes["trigger_meta"]["airflow_state"]) == (
            "running",
            "deferred",
        )
        # A task with no instance yet has not run, rather than having no state.
        assert nodes["enabled_labels"]["state"] == "pending"
        assert nodes["enabled_labels"]["airflow_state"] is None
        assert nodes["resolve_profile"]["stage"] is None
        assert {"source": "resolve_profile", "target": "enabled_media"} in payload["edges"]

    def test_upstream_failed_reads_as_its_own_state(self, observing_state: UiState) -> None:
        seed_graph_run(
            [
                graph_instance("trigger_sync", "failed"),
                graph_instance("enabled_meta", "upstream_failed"),
            ],
            state="failed",
        )
        with running_ui(observing_state) as base_url:
            _, payload = self.request_graph(base_url)
        nodes = {node["id"]: node["state"] for node in payload["nodes"]}
        assert nodes["trigger_sync"] == "failed"
        assert nodes["enabled_meta"] == "upstream_failed"

    def test_disabled_stages_show_skipped_gates(self, observing_state: UiState) -> None:
        seed_graph_run(
            [
                graph_instance("enabled_sync", "skipped"),
                graph_instance("enabled_meta", "success"),
                graph_instance("trigger_meta", "success"),
            ],
            state="success",
        )
        with running_ui(observing_state) as base_url:
            _, payload = self.request_graph(base_url)
        nodes = {node["id"]: node["state"] for node in payload["nodes"]}
        assert nodes["enabled_sync"] == "skipped"
        assert nodes["trigger_sync"] == "pending"

    def test_structure_is_fetched_once_and_refetched_after_a_re_render(
        self, observing_state: UiState
    ) -> None:
        seed_graph_run([graph_instance("resolve_profile", "success")])
        with running_ui(observing_state) as base_url:
            self.request_graph(base_url)
            self.request_graph(base_url)
            assert _StubAirflowHandler.dag_tasks_requests.count("demo_ingest") == 1
            # A bundle re-rendered under a running dashboard shows up as an
            # instance the cached shape does not know about.
            _StubAirflowHandler.dag_tasks_by_dag["demo_ingest"] = [
                *MASTER_TASKS,
                graph_task("verify_manifest", []),
            ]
            _StubAirflowHandler.task_instances_by_run[SUCCESS_RUN_ID] = [
                graph_instance("verify_manifest", "running")
            ]
            _, payload = self.request_graph(base_url)
        assert _StubAirflowHandler.dag_tasks_requests.count("demo_ingest") == 2
        assert any(node["id"] == "verify_manifest" for node in payload["nodes"])

    def test_unknown_run_is_404(self, observing_state: UiState) -> None:
        seed_graph_run([])
        with running_ui(observing_state) as base_url:
            status, _ = request_json(base_url, "/api/pipelines/runs/manual__absent/graph")
        assert status == 404

    def test_503_without_a_bundle(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=tmp_path / "absent", data_root=None)
        with running_ui(state) as base_url:
            status, payload = self.request_graph(base_url)
        assert status == 503
        assert payload["hint"] == "hflow up"

    def test_503_when_airflow_is_unreachable(self, rendered_bundle_dir: Path) -> None:
        state = build_ui_state(bundle_dir=rendered_bundle_dir, data_root=None)
        state.airflow = AirflowClient(UNREACHABLE_AIRFLOW_URL, "airflow", "pw")
        with running_ui(state) as base_url:
            status, payload = self.request_graph(base_url)
        assert status == 503
        assert payload["hint"] == "hflow up"


class TestStageGraph:
    """A stage's sub-run graph, reached without ever leaving the dashboard."""

    def request_graph(self, base_url: str, stage: str = "sync") -> tuple[int, dict[str, Any]]:
        return request_json(
            base_url, f"/api/pipelines/runs/{ENCODED_SUCCESS_RUN_ID}/stages/{stage}/graph"
        )

    def test_resolves_the_sub_run_and_folds_its_mapped_batches(
        self, observing_state: UiState
    ) -> None:
        seed_graph_run([graph_instance("trigger_sync", "deferred")])
        seed_sub_run(
            [
                graph_instance("plan", "success", duration=0.41),
                graph_instance(
                    "process_batch",
                    "success",
                    map_index=0,
                    start="2026-08-23T01:00:10Z",
                    end="2026-08-23T01:00:16Z",
                    duration=5.71,
                ),
                graph_instance(
                    "process_batch", "running", map_index=1, start="2026-08-23T01:00:11Z"
                ),
            ]
        )
        with running_ui(observing_state) as base_url:
            status, payload = self.request_graph(base_url)
        assert status == 200
        assert payload["stage"] == "sync"
        assert payload["dag_id"] == "demo_sync"
        assert payload["master_state"] == "running"
        assert payload["run"]["run_id"] == SUB_RUN_ID
        assert payload["run"]["episode_count"] == 2
        assert len(payload["nodes"]) == 3
        assert payload["edges"] == [
            {"source": "plan", "target": "process_batch"},
            {"source": "process_batch", "target": "error_budget_gate"},
        ]
        nodes = {node["id"]: node for node in payload["nodes"]}
        batch = nodes["process_batch"]
        assert batch["is_mapped"] is True
        # One node for the fan-out: still running, spanning the first start.
        assert batch["state"] == "running"
        assert batch["airflow_state"] is None
        assert batch["start_date"] == "2026-08-23T01:00:10Z"
        assert batch["end_date"] is None
        assert batch["duration_s"] is None
        assert [entry["map_index"] for entry in batch["mapped"]] == [0, 1]
        assert batch["mapped"][0]["duration_s"] == 5.7
        assert batch["mapped"][1]["airflow_url"].endswith("/tasks/process_batch/mapped/1")
        assert nodes["error_budget_gate"]["state"] == "pending"

    def test_finished_batches_fold_to_one_outcome(self, observing_state: UiState) -> None:
        seed_graph_run([graph_instance("trigger_sync", "failed")], state="failed")
        seed_sub_run(
            [
                graph_instance(
                    "process_batch",
                    "success",
                    map_index=0,
                    start="2026-08-23T01:00:10Z",
                    end="2026-08-23T01:00:16Z",
                ),
                graph_instance(
                    "process_batch",
                    "failed",
                    map_index=1,
                    start="2026-08-23T01:00:11Z",
                    end="2026-08-23T01:00:19Z",
                ),
            ],
            state="failed",
        )
        with running_ui(observing_state) as base_url:
            _, payload = self.request_graph(base_url)
        batch = next(node for node in payload["nodes"] if node["id"] == "process_batch")
        assert batch["state"] == "failed"
        # Timing spans the whole fan-out: first start to last end.
        assert (batch["start_date"], batch["end_date"]) == (
            "2026-08-23T01:00:10Z",
            "2026-08-23T01:00:19Z",
        )
        assert batch["duration_s"] == 9.0

    def test_a_skipped_stage_never_expands(self, observing_state: UiState) -> None:
        # A media plan that finds no cameras exits 99, skipping the whole stage:
        # process_batch stays a single instance with no map index.
        seed_graph_run([graph_instance("trigger_sync", "success")], state="success")
        seed_sub_run(
            [
                graph_instance("plan", "skipped"),
                graph_instance("process_batch", "skipped"),
                graph_instance("error_budget_gate", "skipped"),
            ],
            state="success",
        )
        with running_ui(observing_state) as base_url:
            _, payload = self.request_graph(base_url)
        batch = next(node for node in payload["nodes"] if node["id"] == "process_batch")
        assert batch["state"] == "skipped"
        assert batch["mapped"] == []

    def test_an_untriggered_stage_still_shows_its_shape(self, observing_state: UiState) -> None:
        seed_graph_run([graph_instance("resolve_profile", "success")])
        with running_ui(observing_state) as base_url:
            status, payload = self.request_graph(base_url)
        assert status == 200
        assert payload["run"] is None
        assert payload["master_state"] == "running"
        assert {node["state"] for node in payload["nodes"]} == {"pending"}
        assert [node["airflow_url"] for node in payload["nodes"]] == [None, None, None]
        assert len(payload["edges"]) == 2

    def test_unknown_stage_is_404(self, observing_state: UiState) -> None:
        seed_graph_run([])
        with running_ui(observing_state) as base_url:
            status, _ = self.request_graph(base_url, stage="bogus")
        assert status == 404

    def test_unknown_master_run_is_404(self, observing_state: UiState) -> None:
        seed_graph_run([])
        with running_ui(observing_state) as base_url:
            status, _ = request_json(
                base_url, "/api/pipelines/runs/manual__absent/stages/sync/graph"
            )
        assert status == 404


class TestStaticAssets:
    """The packaged frontend serves from memory with explicit content types."""

    def request_raw(self, base_url: str, path: str) -> tuple[int, str, bytes]:
        request = urllib.request.Request(f"{base_url}{path}")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, response.headers.get("Content-Type", ""), response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.headers.get("Content-Type", ""), error.read()

    def test_index_js_and_css_serve_with_types(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=tmp_path / "absent", data_root=None)
        with running_ui(state) as base_url:
            status, content_type, body = self.request_raw(base_url, "/")
            assert status == 200
            assert content_type.startswith("text/html")
            assert b"hflow" in body
            status, content_type, _ = self.request_raw(base_url, "/style.css")
            assert (status, content_type.split(";")[0]) == (200, "text/css")
            # Nested module files must serve too (the tabs live in js/tabs/).
            for module in (
                "/js/graph.js",
                "/js/stage_cards.js",
                "/js/tabs/pipelines.js",
                "/js/tabs/pipelines_run.js",
            ):
                status, content_type, _ = self.request_raw(base_url, module)
                assert (status, content_type.split(";")[0]) == (200, "text/javascript")

    def test_unknown_path_is_404(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=tmp_path / "absent", data_root=None)
        with running_ui(state) as base_url:
            status, _, _ = self.request_raw(base_url, "/../pyproject.toml")
            assert status == 404
            status, _, _ = self.request_raw(base_url, "/nothing.js")
            assert status == 404
