"""Shared state for the dashboard server: one bundle, one Airflow client.

The dashboard observes at most one rendered runtime bundle. A missing bundle
is a supported state, not an error -- the Pipelines tab degrades to its
"runtime not running" callout while Storage and Secrets stay fully usable.
"""

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

from hflow.runtime import AirflowClient, BundlePaths, client_for_bundle, load_bundle

_CallResult = TypeVar("_CallResult")


@dataclass
class UiState:
    bundle: BundlePaths | None
    airflow: AirflowClient | None
    data_root: Path | None
    # AirflowClient caches its bearer token with an unguarded refresh, so the
    # threaded server serializes ALL Airflow calls through one lock -- trivially
    # correct, and a localhost dashboard polling every few seconds never
    # contends meaningfully.
    airflow_lock: threading.Lock = field(default_factory=threading.Lock)
    # Per-stage states of finished runs never change again; caching them keeps
    # the runs-list poll at one Airflow call plus one per still-active run.
    stage_cache: dict[str, dict[str, str]] = field(default_factory=dict)
    # A DAG's shape is fixed by its render, so the graph pages fetch it once
    # per dag_id; a re-render mid-session is caught by the membership guard in
    # :func:`hflow.ui._handlers._dag_structure`.
    dag_tasks_cache: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # The run page's stage cards, for finished runs (nothing left to recompute).
    run_cards_cache: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # Per finished stage of a still-running run, keyed (run_id, stage): its
    # sub-run id and gate tally, so a long run stops re-asking Airflow about
    # the stages it already completed.
    stage_facts_cache: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    # One finished run's check breakdown per stage, keyed (run_id, stage).
    stage_checks_cache: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    # A stage sub-run's batch plan, keyed by sub-run id. Fixed once planned, so
    # the progress poll fetches each run's plan exactly once.
    stage_plan_cache: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def airflow_call(self, call: "Callable[[AirflowClient], _CallResult]") -> _CallResult:
        if self.airflow is None:
            raise RuntimeError("no runtime bundle loaded")
        with self.airflow_lock:
            return call(self.airflow)


def build_ui_state(*, bundle_dir: Path | None, data_root: Path | None) -> UiState:
    bundle: BundlePaths | None = None
    if bundle_dir is not None:
        try:
            bundle = load_bundle(bundle_dir)
        except FileNotFoundError:
            bundle = None
    airflow = client_for_bundle(bundle) if bundle is not None else None
    resolved_data_root = data_root if data_root is not None and data_root.is_dir() else None
    return UiState(bundle=bundle, airflow=airflow, data_root=resolved_data_root)
