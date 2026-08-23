"""JSON endpoint functions for the dashboard server.

Every handler is a pure function ``(state, match, query, body) -> (status,
payload)`` so the whole API surface tests without sockets; the request
handler in :mod:`hflow.ui._server` owns transport only. Errors travel as
``{"error": message}`` plus an optional ``"hint"`` the frontend renders as a
copyable command.
"""

import json
import re
import urllib.parse
from typing import Any

from hflow import __version__
from hflow.runtime import AirflowClient, AirflowClientError, bundle_dag_ids
from hflow.steps import Stage
from hflow.ui._state import UiState

JsonResponse = tuple[int, dict[str, Any]]
Query = dict[str, list[str]]

_RUNTIME_DOWN_HINT = "hflow up"


def _error(status: int, message: str, *, hint: str | None = None) -> JsonResponse:
    payload: dict[str, Any] = {"error": message}
    if hint is not None:
        payload["hint"] = hint
    return status, payload


def _secrets_wired(state: UiState) -> bool:
    """Whether the loaded bundle's compose file references the secrets file.

    Bundles rendered before the secrets feature lack the ``env_file`` block
    until the next ``hflow up`` re-render; the frontend surfaces that.
    """
    from hflow._user_config import secrets_file_path

    if state.bundle is None:
        return False
    try:
        return str(secrets_file_path()) in state.bundle.compose_file.read_text()
    except OSError:
        return False


def status_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    bundle_payload: dict[str, Any] | None = None
    airflow_payload: dict[str, Any] | None = None
    if state.bundle is not None:
        bundle_payload = {
            "bundle_dir": str(state.bundle.bundle_dir),
            "dag_id": state.bundle.dag_id,
            "api_base_url": state.bundle.api_base_url,
        }
        try:
            health = state.airflow_call(lambda client: client.health())
            airflow_payload = {
                "reachable": True,
                "healthy": health.healthy,
                "components": health.components,
            }
        except AirflowClientError:
            airflow_payload = {"reachable": False, "healthy": False, "components": {}}
    return 200, {
        "hflow_version": __version__,
        "bundle": bundle_payload,
        "airflow": airflow_payload,
        "secrets_wired": _secrets_wired(state),
    }


def airflow_credentials_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    if state.bundle is None:
        return _error(404, "no runtime bundle found", hint=_RUNTIME_DOWN_HINT)
    return 200, {
        "url": state.bundle.api_base_url,
        "username": state.bundle.admin_username,
        "password": state.bundle.admin_password,
    }


# ---------------------------------------------------------------------------
# Pipelines


def _airflow_run_url(base_url: str, dag_id: str, dag_run_id: str) -> str:
    """The Airflow 3 UI page for one run; run ids carry ``+`` and ``:``."""
    return f"{base_url}/dags/{dag_id}/runs/{urllib.parse.quote(dag_run_id, safe='')}"


# The trigger task's state seen from the MASTER run encodes the stage outcome
# (wait_for_completion=True). ``upstream_failed`` means the stage never ran
# because an earlier one failed -- that is "pending", not a failure of its own.
_TRIGGER_STATE_TO_STAGE_STATE = {
    "success": "success",
    "failed": "failed",
    "running": "running",
    "deferred": "running",
    "queued": "running",
    "scheduled": "running",
    "restarting": "running",
    "up_for_retry": "running",
    "up_for_reschedule": "running",
}

_TERMINAL_RUN_STATES = frozenset({"success", "failed"})
_STAGE_CACHE_LIMIT = 500


def _derive_stage_states(task_instances: list[dict[str, Any]]) -> dict[str, str]:
    states_by_task_id = {
        str(instance.get("task_id")): str(instance.get("state"))
        for instance in task_instances
        if instance.get("state") is not None
    }
    stage_states: dict[str, str] = {}
    for stage in Stage:
        if states_by_task_id.get(f"enabled_{stage.value}") == "skipped":
            stage_states[stage.value] = "skipped"
            continue
        trigger_state = states_by_task_id.get(f"trigger_{stage.value}")
        stage_states[stage.value] = _TRIGGER_STATE_TO_STAGE_STATE.get(
            trigger_state or "", "pending"
        )
    return stage_states


def _stages_for_run(state: UiState, dag_id: str, run: dict[str, Any]) -> dict[str, str]:
    run_id = str(run.get("dag_run_id"))
    cached = state.stage_cache.get(run_id)
    if cached is not None:
        return cached
    try:
        instances = state.airflow_call(lambda client: client.task_instances(dag_id, run_id))
    except AirflowClientError:
        return {stage.value: "pending" for stage in Stage}
    stage_states = _derive_stage_states(instances)
    if str(run.get("state")) in _TERMINAL_RUN_STATES:
        # Finished runs never change again; keep the poll at one Airflow call
        # plus one per still-active run. The cap is a leak guard, not an LRU.
        if len(state.stage_cache) >= _STAGE_CACHE_LIMIT:
            state.stage_cache.clear()
        state.stage_cache[run_id] = stage_states
    return stage_states


def _run_duration_s(run: dict[str, Any]) -> float | None:
    from datetime import datetime

    start_date, end_date = run.get("start_date"), run.get("end_date")
    if not isinstance(start_date, str) or not isinstance(end_date, str):
        return None
    try:
        started = datetime.fromisoformat(start_date)
        ended = datetime.fromisoformat(end_date)
    except ValueError:
        return None
    return round((ended - started).total_seconds(), 1)


def _run_summary(state: UiState, dag_id: str, run: dict[str, Any]) -> dict[str, Any]:
    assert state.bundle is not None
    raw_conf = run.get("conf")
    conf: dict[str, Any] = raw_conf if isinstance(raw_conf, dict) else {}
    uris = conf.get("uris")
    run_id = str(run.get("dag_run_id"))
    return {
        "run_id": run_id,
        "state": run.get("state"),
        "run_after": run.get("run_after") or run.get("logical_date"),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "duration_s": _run_duration_s(run),
        "profile": conf.get("profile"),
        "mode": conf.get("mode"),
        "episode_count": len(uris) if isinstance(uris, list) else None,
        "airflow_url": _airflow_run_url(state.bundle.api_base_url, dag_id, run_id),
        "stages": _stages_for_run(state, dag_id, run),
    }


def _fetch_runs(client: AirflowClient, dag_id: str, limit: int) -> list[dict[str, Any]]:
    try:
        return client.dag_runs(dag_id, limit=limit, order_by="-run_after")
    except AirflowClientError as error:
        # Sort-field vocabulary is the API's; an older server that rejects
        # ``run_after`` still lists fine unsorted (we re-sort client-side).
        if error.status in (400, 422):
            return client.dag_runs(dag_id, limit=limit)
        raise


def runs_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    if state.bundle is None:
        return _error(503, "no runtime bundle found", hint=_RUNTIME_DOWN_HINT)
    dag_id = state.bundle.dag_id
    limit = max(1, min(100, int(query.get("limit", ["25"])[0])))
    try:
        runs = state.airflow_call(lambda client: _fetch_runs(client, dag_id, limit))
    except AirflowClientError as error:
        return _error(503, f"Airflow is not reachable: {error}", hint=_RUNTIME_DOWN_HINT)
    runs.sort(
        key=lambda run: str(run.get("run_after") or run.get("logical_date") or ""), reverse=True
    )
    return 200, {
        "dag_id": dag_id,
        "dag_url": f"{state.bundle.api_base_url}/dags/{dag_id}",
        "runs": [_run_summary(state, dag_id, run) for run in runs],
    }


def _sub_run_id_from_xcom(state: UiState, dag_id: str, run_id: str, stage: Stage) -> str | None:
    """The stage run the trigger operator started, best-effort via its XCom."""
    try:
        entry = state.airflow_call(
            lambda client: client.xcom_entry(
                dag_id, run_id, f"trigger_{stage.value}", "trigger_run_id"
            )
        )
    except AirflowClientError:
        return None
    value = entry.get("value")
    if isinstance(value, str) and value.startswith('"'):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    return value if isinstance(value, str) and value else None


def run_detail_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    if state.bundle is None:
        return _error(503, "no runtime bundle found", hint=_RUNTIME_DOWN_HINT)
    dag_id = state.bundle.dag_id
    run_id = urllib.parse.unquote(match.group("run_id"))
    try:
        run = state.airflow_call(lambda client: client.dag_run(dag_id, run_id))
    except AirflowClientError as error:
        if error.status == 404:
            return _error(404, f"no run {run_id!r} on {dag_id}")
        return _error(503, f"Airflow is not reachable: {error}", hint=_RUNTIME_DOWN_HINT)
    summary = _run_summary(state, dag_id, run)
    base_url = state.bundle.api_base_url
    stage_dag_ids = dict(zip(Stage, bundle_dag_ids(dag_id)[1:], strict=True))
    stages: list[dict[str, Any]] = []
    for stage in Stage:
        sub_dag_id = stage_dag_ids[stage]
        stage_state = summary["stages"][stage.value]
        sub_run_id = None
        if stage_state not in ("pending", "skipped"):
            sub_run_id = _sub_run_id_from_xcom(state, dag_id, run_id, stage)
        stages.append(
            {
                "stage": stage.value,
                "sub_dag_id": sub_dag_id,
                "state": stage_state,
                "sub_dag_url": f"{base_url}/dags/{sub_dag_id}",
                "sub_run_id": sub_run_id,
                "sub_run_url": (
                    _airflow_run_url(base_url, sub_dag_id, sub_run_id) if sub_run_id else None
                ),
            }
        )
    return 200, {"run": summary, "stages": stages}


# ---------------------------------------------------------------------------
# Storage

_BUCKET_EXTRA_HINT = 'uv add "hflow[bucket]"'


def _root_kind(normalized_root: str) -> str:
    return "bucket" if "://" in normalized_root else "local"


def _all_roots(state: UiState) -> list[dict[str, Any]]:
    """Registered roots plus the implicit local data root, deduplicated.

    A root the user registered explicitly stays deletable even when it is
    also the implicit one, so explicit entries win the dedupe.
    """
    from hflow._user_config import normalize_storage_root, read_storage_registry, storage_root_id

    roots: list[dict[str, Any]] = [
        {
            "root_id": entry.root_id,
            "root": entry.root,
            "kind": _root_kind(entry.root),
            "implicit": False,
            "added_at": entry.added_at,
        }
        for entry in read_storage_registry()
    ]
    known_ids = {root["root_id"] for root in roots}
    if state.data_root is not None:
        implicit_root = normalize_storage_root(str(state.data_root))
        implicit_id = storage_root_id(implicit_root)
        if implicit_id not in known_ids:
            roots.append(
                {
                    "root_id": implicit_id,
                    "root": implicit_root,
                    "kind": "local",
                    "implicit": True,
                    "added_at": None,
                }
            )
    return roots


def _resolve_root(state: UiState, root_id: str) -> dict[str, Any] | None:
    for root in _all_roots(state):
        if root["root_id"] == root_id:
            return root
    return None


def storage_roots_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    return 200, {"roots": _all_roots(state)}


def storage_root_create_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    from hflow._user_config import add_storage_root

    root_value = (body or {}).get("root")
    if not isinstance(root_value, str) or not root_value.strip():
        return _error(400, "expected a non-empty 'root' string")
    entry = add_storage_root(root_value.strip())  # ValueError -> 400 upstream
    return 201, {
        "root_id": entry.root_id,
        "root": entry.root,
        "kind": _root_kind(entry.root),
        "implicit": False,
        "added_at": entry.added_at,
    }


def storage_root_delete_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    from hflow._user_config import remove_storage_root

    root_id = match.group("root_id")
    resolved = _resolve_root(state, root_id)
    if resolved is not None and resolved["implicit"]:
        return _error(409, "this data root is implicit (the served --data-root); not removable")
    if not remove_storage_root(root_id):
        return _error(404, f"no registered data root with id {root_id!r}")
    return 204, {}


def storage_browse_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    from hflow.storage import parse_storage_root

    resolved = _resolve_root(state, match.group("root_id"))
    if resolved is None:
        return _error(404, "no such data root")
    prefix = query.get("prefix", [""])[0].strip("/")
    try:
        listing = parse_storage_root(resolved["root"]).list_entries(prefix)
    except ModuleNotFoundError as error:
        return _error(400, str(error), hint=_BUCKET_EXTRA_HINT)
    return 200, {
        "root_id": resolved["root_id"],
        "prefix": prefix,
        "directories": listing.directories,
        "files": [{"name": name, "size": size} for name, size in listing.files],
    }


def storage_catalog_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    from hflow.curation import open_catalog_connection
    from hflow.storage import parse_storage_root

    resolved = _resolve_root(state, match.group("root_id"))
    if resolved is None:
        return _error(404, "no such data root")
    catalog_root = parse_storage_root(resolved["root"]).child("catalog")
    try:
        connection = open_catalog_connection(catalog_root)
    except FileNotFoundError:
        return 200, {"present": False}
    except ModuleNotFoundError as error:
        return _error(400, str(error), hint=_BUCKET_EXTRA_HINT)
    try:
        episode_count, quarantined_count = connection.execute(
            "SELECT count(*), count(*) FILTER (quarantined) FROM episodes_latest"
        ).fetchone() or (0, 0)
        # Formatted in SQL: fetching a TIMESTAMPTZ into Python needs pytz,
        # which is not a dependency.
        (latest_recorded_at,) = connection.execute(
            "SELECT strftime(max(recorded_at) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') "
            "FROM episodes_raw"
        ).fetchone() or (None,)
        measurement_keys = [
            key
            for (key,) in connection.execute(
                "SELECT DISTINCT key FROM measurements_latest ORDER BY key"
            ).fetchall()
        ]
    finally:
        connection.close()
    return 200, {
        "present": True,
        "episode_count": episode_count,
        "ok_count": episode_count - quarantined_count,
        "quarantined_count": quarantined_count,
        "latest_recorded_at": latest_recorded_at,
        "measurement_keys": measurement_keys,
    }
