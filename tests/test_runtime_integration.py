"""End-to-end Compose runtime test: render the five Figure 4 DAGs, `docker
compose up`, and drive the MASTER DAG through the REST API across all three
lanes -- a full-profile batch run, a relabel-only run, and an online-mode
single-episode run -- then read the catalog rows back on the host.

Gated behind ``HFLOW_DOCKER_TESTS=1`` because it needs Docker, network (image
pulls, pip installs, the pinned in-container ffmpeg download), and minutes of
wall time. No pytest-timeout dependency: every wait below is internally bounded.

On failure, ``docker compose logs`` plus the Airflow task logs land in the
test's tmp dir (path printed) for diagnosis. Containers and volumes are always
torn down, success or failure.
"""

import os
import socket
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from hflow.curation import open_catalog_connection
from hflow.runtime import (
    AirflowClient,
    AirflowClientError,
    RuntimeConfig,
    bundle_dag_ids,
    compose_down,
    compose_logs,
    compose_up_detached,
    render_bundle,
)
from hflow.testing import SyntheticEpisodeSpec, synthesize_episode

pytestmark = pytest.mark.skipif(
    os.environ.get("HFLOW_DOCKER_TESTS") != "1",
    reason="Docker integration test; set HFLOW_DOCKER_TESTS=1 to run",
)

# Unique per test session so a crashed previous run's stack can never be
# adopted (and `down -v` here can never wipe someone else's volumes).
COMPOSE_PROJECT_NAME = f"hflow-itest-{uuid.uuid4().hex[:8]}"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
HEALTHY_TIMEOUT_S = 420.0
DAG_REGISTERED_TIMEOUT_S = 240.0
# The master waits for each triggered sub-DAG (deferrable, 5s poke); a full
# profile chains four sub-DAG runs, each three external-python tasks.
FULL_RUN_TIMEOUT_S = 900.0
PARTIAL_RUN_TIMEOUT_S = 600.0

# Executes inside the container's user venv: absolute in-container data root,
# one cheap check plus one enrichment (relabel must have something to append).
# The episodes carry a camera so the media stage renders a real contact sheet
# (the pinned ffmpeg downloads once into the user-venv volume).
PIPELINE_SOURCE = """\
import os

import hflow

app = hflow.App("itest", data_root="/opt/airflow/data")


@app.check()
def timestamps(ep: hflow.Episode) -> hflow.CheckResult:
    return hflow.checks.timestamp_regularity(ep, tolerance_s=0.010)


@app.enrich()
def caption(ep: hflow.Episode) -> hflow.EnrichmentResult:
    # secret_seen proves the user-level secrets store reached this task's
    # container environment (via the bundle's compose env_file).
    return hflow.EnrichmentResult(
        labels={
            "caption": "a robot arm moves",
            "secret_seen": os.environ.get("HFLOW_ITEST_SECRET", "<unset>"),
        }
    )
"""

# Deliberately shell-flavored: compose env_file handling must pass the value
# through verbatim, not interpolate the $.
ITEST_SECRET_VALUE = "it-works-$literally"

# The relabel scenario: an UPDATED labeler. An exact repeat is a deliberate
# no-op, while changed source or a changed observable outcome appends a new
# run fingerprint.
PIPELINE_SOURCE_V2 = PIPELINE_SOURCE.replace(
    '"caption": "a robot arm moves"', '"caption": "a robot arm moves (v2)"'
)


def _free_port() -> int:
    with socket.socket() as probe_socket:
        probe_socket.bind(("127.0.0.1", 0))
        return probe_socket.getsockname()[1]


def _wait_until_dags_registered(
    client: AirflowClient, dag_ids: list[str], *, timeout_s: float
) -> None:
    """Wait for the master AND sub-DAGs: the master's first trigger task
    would fail against an unregistered sub-DAG."""
    deadline = time.monotonic() + timeout_s
    pending = list(dag_ids)
    last_error: str = "not yet polled"
    while time.monotonic() < deadline:
        still_pending = []
        for dag_id in pending:
            try:
                client.dag(dag_id)
            except AirflowClientError as error:
                last_error = f"{dag_id}: {error}"
                still_pending.append(dag_id)
        pending = still_pending
        if not pending:
            return
        time.sleep(3.0)
    raise TimeoutError(
        f"dags {pending!r} not registered after {timeout_s:.0f}s "
        f"(is the dag-processor parsing? last error: {last_error})"
    )


def _wait_for_terminal_dag_run_state(
    client: AirflowClient, dag_id: str, dag_run_id: str, *, timeout_s: float
) -> str:
    deadline = time.monotonic() + timeout_s
    state = "unknown"
    while time.monotonic() < deadline:
        state = str(client.dag_run(dag_id, dag_run_id).get("state"))
        if state in ("success", "failed"):
            return state
        time.sleep(5.0)
    raise TimeoutError(f"dag run {dag_run_id!r} still {state!r} after {timeout_s:.0f}s")


def _collect_diagnostics(compose_file: Path, bundle_dir: Path, destination_dir: Path) -> str:
    """Write compose logs + Airflow task logs to files; return a printable tail."""
    destination_dir.mkdir(parents=True, exist_ok=True)
    sections: list[str] = []
    try:
        compose_log_text = compose_logs(compose_file, project_name=COMPOSE_PROJECT_NAME)
    except Exception as error:  # diagnosis must never mask the real failure
        compose_log_text = f"<failed to collect compose logs: {error}>"
    (destination_dir / "compose-logs.txt").write_text(compose_log_text)
    sections.append("=== docker compose logs (tail) ===")
    sections.extend(compose_log_text.splitlines()[-120:])
    for task_log_file in sorted((bundle_dir / "logs").rglob("*.log")):
        sections.append(f"=== task log {task_log_file.relative_to(bundle_dir)} (tail) ===")
        sections.extend(task_log_file.read_text(errors="replace").splitlines()[-60:])
    diagnostics = "\n".join(sections)
    (destination_dir / "diagnostics.txt").write_text(diagnostics)
    return diagnostics


def _run_master_to_success(
    client: AirflowClient,
    master_dag_id: str,
    uris: list[str],
    *,
    profile: str,
    online: bool,
    timeout_s: float,
) -> str:
    triggered = client.ingest(master_dag_id, uris, profile=profile, online=online)
    final_state = _wait_for_terminal_dag_run_state(
        client, master_dag_id, triggered["dag_run_id"], timeout_s=timeout_s
    )
    assert final_state == "success", (
        f"master run {triggered['dag_run_id']} over {uris} "
        f"(profile={profile!r}, online={online}) ended {final_state!r}"
    )
    return str(triggered["dag_run_id"])


def _successful_run_count(client: AirflowClient, dag_id: str) -> int:
    runs = client.dag_runs(dag_id)
    assert all(run.get("state") != "failed" for run in runs), (dag_id, runs)
    return sum(1 for run in runs if run.get("state") == "success")


def _count(connection: Any, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


def _assert_dashboard_observes_run(
    bundle_dir: Path, data_root: Path, client: AirflowClient, full_run_id: str
) -> None:
    """The UI endpoints against REAL Airflow: the flagged unknowns in one place.

    Verifies (1) the Airflow 3 UI run-page route the dashboard deep-links to
    answers 200, (2) ``order_by=-run_after`` is accepted, and (3) the trigger
    operator's ``trigger_run_id`` XCom names a real sub-DAG run.
    """
    import json as json_module
    import threading
    import urllib.parse
    import urllib.request

    from hflow.ui import build_ui_state, create_ui_server

    # (2) accepted by the real API, not just our stub
    assert client.dag_runs("itest_pipeline_ingest", order_by="-run_after")

    state = build_ui_state(bundle_dir=bundle_dir, data_root=data_root)
    server = create_ui_server(state, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        with urllib.request.urlopen(f"{base_url}/api/pipelines/runs", timeout=30) as response:
            runs_payload = json_module.loads(response.read())
        (run_summary,) = [run for run in runs_payload["runs"] if run["run_id"] == full_run_id]
        assert run_summary["stages"] == {
            "sync": "success",
            "meta": "success",
            "labels": "success",
            "media": "success",
        }
        assert run_summary["episode_count"] == 2

        # (1) the deep link answers 200 (the Airflow UI serves its app there)
        with urllib.request.urlopen(run_summary["airflow_url"], timeout=30) as response:
            assert response.status == 200

        # (3) sub-run linkage via the trigger operator's XCom
        encoded_run_id = urllib.parse.quote(full_run_id, safe="")
        with urllib.request.urlopen(
            f"{base_url}/api/pipelines/runs/{encoded_run_id}", timeout=60
        ) as response:
            detail_payload = json_module.loads(response.read())
        stages_by_name = {stage["stage"]: stage for stage in detail_payload["stages"]}
        sync_run_id = stages_by_name["sync"]["sub_run_id"]
        assert sync_run_id, detail_payload
        assert client.dag_run("itest_pipeline_sync", sync_run_id).get("state") == "success"
    finally:
        server.shutdown()
        server.server_close()


def test_master_profiles_and_online_lane_end_to_end(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    inbox_dir = data_root / "episodes-in"
    inbox_dir.mkdir(parents=True)
    # Camera-bearing episodes so the media sub-DAG does real work (a contact
    # sheet per camera via the pinned in-container ffmpeg). Two distinct
    # episodes so the full batch run maps over two staggered batches.
    camera_spec = SyntheticEpisodeSpec(duration_s=2.0, cameras=("wrist_cam",))
    synthesize_episode(inbox_dir / "episode-a.mcap", camera_spec)
    synthesize_episode(
        inbox_dir / "episode-b.mcap",
        SyntheticEpisodeSpec(duration_s=2.0, cameras=("wrist_cam",), seed=1),
    )

    pipeline_file = tmp_path / "itest_pipeline.py"
    pipeline_file.write_text(PIPELINE_SOURCE)

    # Before render/up: the store is referenced by the compose file and read
    # at container start.
    from hflow._user_config import set_secret

    set_secret("HFLOW_ITEST_SECRET", ITEST_SECRET_VALUE)

    config = RuntimeConfig(
        pipeline_file=pipeline_file,
        data_root=data_root,
        hflow_source=REPOSITORY_ROOT,
        api_port=_free_port(),
    )
    paths = render_bundle(config, tmp_path / "bundle")
    client = AirflowClient(paths.api_base_url, paths.admin_username, paths.admin_password)
    master_dag_id, sync_dag_id, meta_dag_id, labels_dag_id, media_dag_id = bundle_dag_ids(
        paths.dag_id
    )
    both_uris = ["episodes-in/episode-a.mcap", "episodes-in/episode-b.mcap"]
    canonical_paths = [
        data_root / "episodes" / stem / f"{stem}.canonical.mcap"
        for stem in ("episode-a", "episode-b")
    ]

    try:
        compose_up_detached(paths.compose_file, project_name=COMPOSE_PROJECT_NAME)
        client.wait_until_healthy(timeout_s=HEALTHY_TIMEOUT_S)
        _wait_until_dags_registered(
            client, bundle_dag_ids(paths.dag_id), timeout_s=DAG_REGISTERED_TIMEOUT_S
        )

        # (a) Full profile, batch lane: master -> all four sub-DAGs succeed.
        full_run_id = _run_master_to_success(
            client,
            master_dag_id,
            both_uris,
            profile="full",
            online=False,
            timeout_s=FULL_RUN_TIMEOUT_S,
        )
        for sub_dag_id in (sync_dag_id, meta_dag_id, labels_dag_id, media_dag_id):
            assert _successful_run_count(client, sub_dag_id) == 1, sub_dag_id
        for canonical_path in canonical_paths:
            assert canonical_path.is_file()
        canonical_mtimes = [path.stat().st_mtime_ns for path in canonical_paths]

        connection = open_catalog_connection(data_root / "catalog")
        try:
            episode_rows = connection.execute(
                "SELECT episode_id, source_uri, quarantined FROM episodes_latest "
                "ORDER BY source_uri"
            ).fetchall()
            assert len(episode_rows) == 2, episode_rows
            assert episode_rows[0][1].endswith("episodes-in/episode-a.mcap")
            assert episode_rows[1][1].endswith("episodes-in/episode-b.mcap")
            assert all(row[2] is False for row in episode_rows)
            assert (
                _count(
                    connection,
                    "SELECT count(*) FROM measurements WHERE check_name = 'timestamps'",
                )
                > 0
            )
            # The media sub-DAG recorded a real contact-sheet artifact.
            assert (
                _count(
                    connection,
                    "SELECT count(*) FROM measurements "
                    "WHERE check_name = 'media/contact_sheet' AND key LIKE 'artifact/%'",
                )
                > 0
            )
            caption_rows_after_full = _count(
                connection,
                "SELECT count(*) FROM measurements WHERE check_name = 'caption'",
            )
            assert caption_rows_after_full > 0
        finally:
            connection.close()
        # The artifact file itself is on the host, under the shared data root.
        media_sheets = list((data_root / "episodes").rglob("media/*.jpg"))
        assert len(media_sheets) == 2, media_sheets

        # The user-level secret reached the task container VERBATIM ($ intact).
        connection = open_catalog_connection(data_root / "catalog")
        try:
            secret_values = connection.execute(
                "SELECT DISTINCT value_text FROM measurements WHERE key = 'secret_seen'"
            ).fetchall()
        finally:
            connection.close()
        assert secret_values == [(ITEST_SECRET_VALUE,)], secret_values

        _assert_dashboard_observes_run(tmp_path / "bundle", data_root, client, full_run_id)

        # (b) Relabel profile over the same uris with an UPDATED labeler: only
        # the labels sub-DAG runs, the canonical files are untouched, and the
        # new-version enrichment rows are appended. The re-render refreshes
        # the bundle's user/ copy (a live bind mount -- no restart needed).
        pipeline_file.write_text(PIPELINE_SOURCE_V2)
        render_bundle(config, tmp_path / "bundle")
        _run_master_to_success(
            client,
            master_dag_id,
            both_uris,
            profile="relabel",
            online=False,
            timeout_s=PARTIAL_RUN_TIMEOUT_S,
        )
        assert _successful_run_count(client, labels_dag_id) == 2
        for untouched_dag_id in (sync_dag_id, meta_dag_id, media_dag_id):
            assert _successful_run_count(client, untouched_dag_id) == 1, untouched_dag_id
        assert [path.stat().st_mtime_ns for path in canonical_paths] == canonical_mtimes
        connection = open_catalog_connection(data_root / "catalog")
        try:
            assert (
                _count(
                    connection,
                    "SELECT count(*) FROM measurements WHERE check_name = 'caption'",
                )
                > caption_rows_after_full
            )
            # The appended rows carry the updated labeler's output (one per
            # episode), a NEW measurement identity next to the v1 rows.
            assert (
                _count(
                    connection,
                    "SELECT count(*) FROM measurements WHERE check_name = 'caption' "
                    "AND value_text = 'a robot arm moves (v2)'",
                )
                == 2
            )
        finally:
            connection.close()

        # (c) Online lane, single uri: latency-first (one immediate batch).
        _run_master_to_success(
            client,
            master_dag_id,
            ["episodes-in/episode-a.mcap"],
            profile="metadata_backfill",
            online=True,
            timeout_s=PARTIAL_RUN_TIMEOUT_S,
        )
        assert _successful_run_count(client, meta_dag_id) == 2
        online_confs = [run.get("conf") for run in client.dag_runs(meta_dag_id)]
        assert any(
            isinstance(conf, dict) and conf.get("mode") == "online" for conf in online_confs
        ), online_confs
    except BaseException:
        diagnostics_tail = _collect_diagnostics(
            paths.compose_file, paths.bundle_dir, tmp_path / "diagnostics"
        )
        print(f"diagnostics written to {tmp_path / 'diagnostics'}")
        print(diagnostics_tail[-8000:])
        raise
    finally:
        compose_down(paths.compose_file, project_name=COMPOSE_PROJECT_NAME, remove_volumes=True)
