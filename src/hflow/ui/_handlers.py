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
from datetime import UTC, datetime
from typing import Any

from hflow import __version__
from hflow.runtime import AirflowClient, AirflowClientError, bundle_dag_ids
from hflow.steps import STAGE_INFO, Stage
from hflow.ui._progress import (
    StageCounts,
    StageWindow,
    iso_timestamp,
    query_check_breakdown,
    query_stage_counts,
    stage_progress,
    stage_windows_from_instances,
)
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


def _stages_for_run(
    state: UiState,
    dag_id: str,
    run: dict[str, Any],
    instances: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Per-stage states, from the cache, from ``instances``, or by fetching them.

    A caller that needs the task instances for itself (the run page, for its
    stage windows) passes them in so the poll stays at one fetch.
    """
    run_id = str(run.get("dag_run_id"))
    cached = state.stage_cache.get(run_id)
    if cached is not None:
        return cached
    if instances is None:
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


def _run_conf(run: dict[str, Any]) -> dict[str, Any]:
    raw_conf = run.get("conf")
    return raw_conf if isinstance(raw_conf, dict) else {}


def _run_header(base_url: str, dag_id: str, run: dict[str, Any]) -> dict[str, Any]:
    """The facts every run page shows; a stage sub-run has no profile or stages."""
    conf = _run_conf(run)
    uris = conf.get("uris")
    run_id = str(run.get("dag_run_id"))
    return {
        "run_id": run_id,
        "state": run.get("state"),
        "run_after": run.get("run_after") or run.get("logical_date"),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "duration_s": _run_duration_s(run),
        "mode": conf.get("mode"),
        "episode_count": len(uris) if isinstance(uris, list) else None,
        "airflow_url": _airflow_run_url(base_url, dag_id, run_id),
    }


def _run_summary(
    state: UiState,
    dag_id: str,
    run: dict[str, Any],
    instances: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    assert state.bundle is not None
    return {
        **_run_header(state.bundle.api_base_url, dag_id, run),
        "profile": _run_conf(run).get("profile"),
        "stages": _stages_for_run(state, dag_id, run, instances),
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


# Each stage sub-DAG ends in a budget gate whose return value is that stage's
# own tally. It is the authoritative end-of-stage count -- immune to the
# catalog's replay blindness -- so a finished stage prefers it and falls back
# to the catalog when the gate raised (budget exceeded) or was never reached.
_GATE_TASK_IDS = dict.fromkeys(Stage, "error_budget_gate") | {Stage.META: "quarantine_budget_gate"}


def _gate_counts(
    state: UiState, sub_dag_id: str, sub_run_id: str, stage: Stage
) -> StageCounts | None:
    """One finished stage's own tally, from its budget gate's XCom."""
    try:
        entry = state.airflow_call(
            lambda client: client.xcom_entry(
                sub_dag_id, sub_run_id, _GATE_TASK_IDS[stage], "return_value"
            )
        )
    except AirflowClientError:
        return None
    value = entry.get("value")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    if not isinstance(value, dict) or not isinstance(value.get("total"), int):
        return None
    return StageCounts(
        done=value["total"],
        quarantined=int(value.get("quarantined") or 0),
        errors=int(value.get("errors") or 0),
    )


def _waiting_on(stage: Stage, stage_states: dict[str, str]) -> str | None:
    """The unfinished stage a pending stage is queued behind, if any.

    None once everything ahead of it is done or disabled -- it is about to
    start, not waiting on anyone.
    """
    stages = list(Stage)
    for earlier in reversed(stages[: stages.index(stage)]):
        if stage_states.get(earlier.value) not in ("skipped", "success"):
            return earlier.value
    return None


def _stage_cards(
    state: UiState,
    dag_id: str,
    run: dict[str, Any],
    stage_states: dict[str, str],
    instances: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """One card per stage: what it is, how far its episodes got, what it costs.

    The card's core -- identity, state, and episode progress -- is deliberately
    free of Airflow vocabulary: a verification stage backed by an external
    service (human review, say) can fill the same shape from its own API, with
    the orchestration block left null.
    """
    assert state.bundle is not None
    base_url = state.bundle.api_base_url
    run_id = str(run.get("dag_run_id"))
    episode_count = _run_header(base_url, dag_id, run)["episode_count"]
    stage_dag_ids = dict(zip(Stage, bundle_dag_ids(dag_id)[1:], strict=True))
    windows = stage_windows_from_instances(instances)
    now = datetime.now(UTC)
    catalog_counts = _catalog_counts(state, run, windows, now)

    cards: list[dict[str, Any]] = []
    for stage in Stage:
        info = STAGE_INFO[stage]
        stage_state = stage_states[stage.value]
        sub_dag_id = stage_dag_ids[stage]
        facts = _stage_orchestration_facts(state, dag_id, run_id, stage, stage_state, sub_dag_id)
        window = windows.get(stage)
        gate: StageCounts | None = facts["gate_counts"]
        # A readable catalog that names no episodes for this stage is the
        # answer "none through yet", which is not the same as having no
        # catalog to ask (progress stays null only for the latter).
        counts = gate
        if counts is None and catalog_counts is not None:
            counts = catalog_counts.get(stage, StageCounts())
        # A finished stage counted its own episodes; before then, the run's
        # conf is the only statement of how many there are to do.
        total = gate.done if gate is not None else episode_count
        cards.append(
            {
                "stage": stage.value,
                "title": info.title,
                "description": info.description,
                "layer": info.layer.value,
                "state": stage_state,
                "waiting_on": (
                    _waiting_on(stage, stage_states) if stage_state == "pending" else None
                ),
                "total": total,
                "started_at": iso_timestamp(window.start) if window else None,
                "ended_at": iso_timestamp(window.end) if window and window.end else None,
                "duration_s": _window_duration_s(window),
                "progress": (
                    stage_progress(
                        counts,
                        total=total,
                        window=window,
                        running=stage_state == "running",
                        now=now,
                    )
                    if window is not None and counts is not None
                    else None
                ),
                # Orchestration detail: where this stage ran, for the drill-in.
                "sub_dag_id": sub_dag_id,
                "sub_dag_url": f"{base_url}/dags/{sub_dag_id}",
                "sub_run_id": facts["sub_run_id"],
                "sub_run_url": (
                    _airflow_run_url(base_url, sub_dag_id, facts["sub_run_id"])
                    if facts["sub_run_id"]
                    else None
                ),
            }
        )
    return cards


def stage_checks_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    """What one stage's verification found, check by check."""
    if state.bundle is None:
        return _error(503, "no runtime bundle found", hint=_RUNTIME_DOWN_HINT)
    stage_name = match.group("stage")
    if stage_name not in _STAGE_VALUES:
        return _error(404, f"no stage named {stage_name!r}")
    stage = Stage(stage_name)
    dag_id = state.bundle.dag_id
    run_id = urllib.parse.unquote(match.group("run_id"))
    cached = state.stage_checks_cache.get((run_id, stage.value))
    if cached is not None:
        return 200, cached
    try:
        run = state.airflow_call(lambda client: client.dag_run(dag_id, run_id))
        instances = state.airflow_call(lambda client: client.task_instances(dag_id, run_id))
    except AirflowClientError as error:
        if error.status == 404:
            return _error(404, f"no run {run_id!r} on {dag_id}")
        return _error(503, f"Airflow is not reachable: {error}", hint=_RUNTIME_DOWN_HINT)
    window = stage_windows_from_instances(instances).get(stage)
    uris = _run_conf(run).get("uris")
    checks: list[dict[str, Any]] = []
    if window is not None and state.data_root is not None and isinstance(uris, list):
        checks = query_check_breakdown(
            state.data_root / "catalog",
            [uri for uri in uris if isinstance(uri, str)],
            window,
            now=datetime.now(UTC),
        )
    payload = {"stage": stage.value, "checks": checks}
    if str(run.get("state")) in _TERMINAL_RUN_STATES:
        if len(state.stage_checks_cache) >= _STAGE_CACHE_LIMIT:
            state.stage_checks_cache.clear()
        state.stage_checks_cache[(run_id, stage.value)] = payload
    return 200, payload


def _window_duration_s(window: StageWindow | None) -> float | None:
    if window is None or window.end is None:
        return None
    return round((window.end - window.start).total_seconds(), 1)


def _catalog_counts(
    state: UiState,
    run: dict[str, Any],
    windows: dict[Stage, StageWindow],
    now: datetime,
) -> dict[Stage, StageCounts] | None:
    """Episode counts per stage from the catalog the run itself wrote."""
    if state.data_root is None:
        return None
    uris = _run_conf(run).get("uris")
    if not isinstance(uris, list):
        return None
    return query_stage_counts(
        state.data_root / "catalog",
        [uri for uri in uris if isinstance(uri, str)],
        windows,
        now=now,
    )


def _stage_orchestration_facts(
    state: UiState,
    dag_id: str,
    run_id: str,
    stage: Stage,
    stage_state: str,
    sub_dag_id: str,
) -> dict[str, Any]:
    """The stage's sub-run id and, once it is finished, its gate tally.

    Cached per finished stage: a stage that ended cannot change again, so a
    long run stops re-asking Airflow about the stages it already completed.
    """
    if stage_state in ("pending", "skipped"):
        return {"sub_run_id": None, "gate_counts": None}
    cache_key = (run_id, stage.value)
    cached = state.stage_facts_cache.get(cache_key)
    if cached is not None:
        return cached
    sub_run_id = _sub_run_id_from_xcom(state, dag_id, run_id, stage)
    gate_counts = (
        _gate_counts(state, sub_dag_id, sub_run_id, stage)
        if sub_run_id and stage_state in ("success", "failed")
        else None
    )
    facts = {"sub_run_id": sub_run_id, "gate_counts": gate_counts}
    if stage_state in ("success", "failed"):
        if len(state.stage_facts_cache) >= _STAGE_CACHE_LIMIT:
            state.stage_facts_cache.clear()
        state.stage_facts_cache[cache_key] = facts
    return facts


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
    cached_cards = state.run_cards_cache.get(run_id)
    if cached_cards is not None:
        return 200, {"run": _run_summary(state, dag_id, run), "stages": cached_cards}
    try:
        instances = state.airflow_call(lambda client: client.task_instances(dag_id, run_id))
    except AirflowClientError:
        instances = []
    summary = _run_summary(state, dag_id, run, instances)
    cards = _stage_cards(state, dag_id, run, summary["stages"], instances)
    if str(run.get("state")) in _TERMINAL_RUN_STATES:
        # A finished run's cards are final; the poll that watched it end is the
        # last one that has to compute them.
        if len(state.run_cards_cache) >= _STAGE_CACHE_LIMIT:
            state.run_cards_cache.clear()
        state.run_cards_cache[run_id] = cards
    return 200, {"run": summary, "stages": cards}


# ---------------------------------------------------------------------------
# Run graphs
#
# The dashboard draws each run's DAG itself, so these endpoints pair the DAG's
# shape (task definitions, which carry the edges) with the run's task
# instances. Structure comes from Airflow rather than the templates: the
# graph then follows whatever a bundle actually rendered.


def _airflow_task_url(
    base_url: str, dag_id: str, dag_run_id: str, task_id: str, map_index: int | None = None
) -> str:
    """The Airflow page for one task instance -- where its logs live."""
    run_url = _airflow_run_url(base_url, dag_id, dag_run_id)
    url = f"{run_url}/tasks/{urllib.parse.quote(task_id, safe='')}"
    return url if map_index is None else f"{url}/mapped/{map_index}"


# Task nodes extend the stage vocabulary with ``upstream_failed``: at task
# altitude that is a real outcome worth showing (Airflow colors it too), where
# a whole stage that never ran reads better as "pending".
_TASK_STATE_TO_NODE_STATE = {
    **_TRIGGER_STATE_TO_STAGE_STATE,
    "skipped": "skipped",
    "removed": "skipped",
    "upstream_failed": "upstream_failed",
}
_TERMINAL_NODE_STATES = frozenset({"success", "failed", "skipped", "upstream_failed"})
_STAGE_VALUES = frozenset(stage.value for stage in Stage)


def _instance_facts(instance: dict[str, Any]) -> dict[str, Any]:
    """One task instance's state and timing; an absent instance is pending."""
    duration = instance.get("duration")
    return {
        "state": _TASK_STATE_TO_NODE_STATE.get(str(instance.get("state") or ""), "pending"),
        "airflow_state": instance.get("state"),
        "start_date": instance.get("start_date"),
        "end_date": instance.get("end_date"),
        "duration_s": round(float(duration), 1) if isinstance(duration, int | float) else None,
        "try_number": instance.get("try_number"),
    }


def _folded_mapped_facts(mapped: list[dict[str, Any]]) -> dict[str, Any]:
    """One node's facts from a mapped task's per-index instances.

    Timing spans the whole fan-out (first start to last end), which is what
    the batch actually cost; per-index timing stays in ``mapped``.
    """
    states = {entry["state"] for entry in mapped}
    if "failed" in states:
        state = "failed"
    elif states == {"pending"}:
        state = "pending"
    elif not states <= _TERMINAL_NODE_STATES:
        state = "running"
    elif "upstream_failed" in states:
        state = "upstream_failed"
    elif "success" in states:
        state = "success"
    else:
        state = "skipped"
    starts = [entry["start_date"] for entry in mapped if entry["start_date"]]
    ends = [entry["end_date"] for entry in mapped if entry["end_date"]]
    start = min(starts) if starts else None
    end = max(ends) if len(ends) == len(mapped) else None
    return {
        "state": state,
        "airflow_state": None,
        "start_date": start,
        "end_date": end,
        "duration_s": (
            _run_duration_s({"start_date": start, "end_date": end})
            if start is not None and end is not None
            else None
        ),
        "try_number": None,
    }


def _stage_of_trigger(task_id: str) -> str | None:
    """The stage a master ``trigger_<stage>`` task drives, for drill-in."""
    stage_name = task_id.removeprefix("trigger_")
    return stage_name if stage_name != task_id and stage_name in _STAGE_VALUES else None


def _graph_nodes(
    tasks: list[dict[str, Any]],
    instances: list[dict[str, Any]],
    *,
    base_url: str,
    dag_id: str,
    run_id: str | None,
) -> list[dict[str, Any]]:
    """One node per task definition, carrying this run's state for it."""
    by_task_id: dict[str, list[dict[str, Any]]] = {}
    for instance in instances:
        by_task_id.setdefault(str(instance.get("task_id")), []).append(instance)

    nodes: list[dict[str, Any]] = []
    for task in tasks:
        task_id = str(task.get("task_id"))
        mine = by_task_id.get(task_id, [])
        # A mapped task fans out to one instance per index; anything before
        # expansion (or a skipped plan) leaves a single index-less instance.
        expanded = sorted(
            (entry for entry in mine if int(entry.get("map_index", -1)) >= 0),
            key=lambda entry: int(entry["map_index"]),
        )
        mapped: list[dict[str, Any]] | None
        if expanded:
            mapped = [
                {
                    **_instance_facts(entry),
                    "map_index": int(entry["map_index"]),
                    "rendered_map_index": entry.get("rendered_map_index"),
                    "airflow_url": (
                        _airflow_task_url(
                            base_url, dag_id, run_id, task_id, int(entry["map_index"])
                        )
                        if run_id is not None
                        else None
                    ),
                }
                for entry in expanded
            ]
            facts = _folded_mapped_facts(mapped)
        else:
            mapped = [] if task.get("is_mapped") else None
            facts = _instance_facts(mine[0] if mine else {})
        nodes.append(
            {
                "id": task_id,
                "label": task.get("task_display_name") or task_id,
                "operator": task.get("operator_name"),
                "doc": task.get("doc_md"),
                "is_mapped": bool(task.get("is_mapped")),
                **facts,
                "airflow_url": (
                    _airflow_task_url(base_url, dag_id, run_id, task_id)
                    if run_id is not None
                    else None
                ),
                "mapped": mapped,
                "stage": _stage_of_trigger(task_id),
            }
        )
    return nodes


def _graph_edges(tasks: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Edges as the DAG declares them, including implicit XCom dependencies."""
    known = {str(task.get("task_id")) for task in tasks}
    edges = {
        (str(task.get("task_id")), str(target))
        for task in tasks
        for target in task.get("downstream_task_ids") or []
        if str(target) in known
    }
    return [{"source": source, "target": target} for source, target in sorted(edges)]


def _dag_structure(
    state: UiState, dag_id: str, instances: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """The DAG's task definitions, fetched once per dag_id.

    A shape only changes when the bundle is re-rendered, which the running
    dashboard learns about from task instances naming a task the cached
    structure lacks -- then it refetches.
    """
    cached = state.dag_tasks_cache.get(dag_id)
    if cached is not None:
        known = {str(task.get("task_id")) for task in cached}
        if all(str(instance.get("task_id")) in known for instance in instances):
            return cached
    tasks = state.airflow_call(lambda client: client.dag_tasks(dag_id))
    state.dag_tasks_cache[dag_id] = tasks
    return tasks


def run_graph_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    if state.bundle is None:
        return _error(503, "no runtime bundle found", hint=_RUNTIME_DOWN_HINT)
    dag_id = state.bundle.dag_id
    run_id = urllib.parse.unquote(match.group("run_id"))
    try:
        run = state.airflow_call(lambda client: client.dag_run(dag_id, run_id))
        instances = state.airflow_call(lambda client: client.task_instances(dag_id, run_id))
        tasks = _dag_structure(state, dag_id, instances)
    except AirflowClientError as error:
        if error.status == 404:
            return _error(404, f"no run {run_id!r} on {dag_id}")
        return _error(503, f"Airflow is not reachable: {error}", hint=_RUNTIME_DOWN_HINT)
    return 200, {
        "dag_id": dag_id,
        "run": _run_summary(state, dag_id, run),
        "nodes": _graph_nodes(
            tasks,
            instances,
            base_url=state.bundle.api_base_url,
            dag_id=dag_id,
            run_id=run_id,
        ),
        "edges": _graph_edges(tasks),
    }


def _stage_run_and_instances(
    state: UiState, sub_dag_id: str, sub_run_id: str
) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    """The stage run and its task instances; ``None`` once it is gone."""
    try:
        run = state.airflow_call(lambda client: client.dag_run(sub_dag_id, sub_run_id))
    except AirflowClientError as error:
        if error.status == 404:
            return None
        raise
    instances = state.airflow_call(lambda client: client.task_instances(sub_dag_id, sub_run_id))
    return run, instances


def stage_graph_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    if state.bundle is None:
        return _error(503, "no runtime bundle found", hint=_RUNTIME_DOWN_HINT)
    stage_name = match.group("stage")
    if stage_name not in _STAGE_VALUES:
        return _error(404, f"no stage named {stage_name!r}")
    stage = Stage(stage_name)
    dag_id = state.bundle.dag_id
    base_url = state.bundle.api_base_url
    run_id = urllib.parse.unquote(match.group("run_id"))
    sub_dag_id = dict(zip(Stage, bundle_dag_ids(dag_id)[1:], strict=True))[stage]
    try:
        master = state.airflow_call(lambda client: client.dag_run(dag_id, run_id))
        sub_run_id = _sub_run_id_from_xcom(state, dag_id, run_id, stage)
        resolved = (
            _stage_run_and_instances(state, sub_dag_id, sub_run_id)
            if sub_run_id is not None
            else None
        )
        sub_run = resolved[0] if resolved is not None else None
        instances = resolved[1] if resolved is not None else []
        # The shape is worth showing before the stage starts, so an
        # unresolved sub-run still renders -- every node pending.
        tasks = _dag_structure(state, sub_dag_id, instances)
    except AirflowClientError as error:
        if error.status == 404:
            return _error(404, f"no run {run_id!r} on {dag_id}")
        return _error(503, f"Airflow is not reachable: {error}", hint=_RUNTIME_DOWN_HINT)
    return 200, {
        "stage": stage.value,
        "dag_id": sub_dag_id,
        "master_state": master.get("state"),
        "run": _run_header(base_url, sub_dag_id, sub_run) if sub_run is not None else None,
        "nodes": _graph_nodes(
            tasks,
            instances,
            base_url=base_url,
            dag_id=sub_dag_id,
            run_id=str(sub_run["dag_run_id"]) if sub_run is not None else None,
        ),
        "edges": _graph_edges(tasks),
    }


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


# ---------------------------------------------------------------------------
# Secrets

_SECRET_MASK = "•" * 8  # constant-width: the mask must not leak length


def secrets_list_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    from hflow._user_config import read_secrets, secrets_file_path

    return 200, {
        "secrets": [
            {"name": name, "masked_value": _SECRET_MASK} for name in sorted(read_secrets())
        ],
        "secrets_file": str(secrets_file_path()),
        "wired_into_bundle": _secrets_wired(state),
    }


def secret_put_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    from hflow._user_config import set_secret

    value = (body or {}).get("value")
    if not isinstance(value, str) or not value:
        return _error(400, "expected a non-empty 'value' string")
    set_secret(match.group("name"), value)  # ValueError -> 400 upstream
    return 204, {}


def secret_delete_handler(
    state: UiState, match: re.Match[str], query: Query, body: dict[str, Any] | None
) -> JsonResponse:
    from hflow._user_config import delete_secret

    if not delete_secret(match.group("name")):
        return _error(404, f"no secret named {match.group('name')!r}")
    return 204, {}
