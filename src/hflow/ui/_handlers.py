"""JSON endpoint functions for the dashboard server.

Every handler is a pure function ``(state, match, query, body) -> (status,
payload)`` so the whole API surface tests without sockets; the request
handler in :mod:`hflow.ui._server` owns transport only. Errors travel as
``{"error": message}`` plus an optional ``"hint"`` the frontend renders as a
copyable command.
"""

import re
from typing import Any

from hflow import __version__
from hflow.runtime import AirflowClientError
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
