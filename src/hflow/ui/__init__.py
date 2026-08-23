"""The local dashboard: ``hflow ui``.

A loopback-only stdlib HTTP server pairing the packaged static frontend with
a small JSON API over the same surfaces users can query themselves -- the
Airflow REST API, the Parquet catalog, and the user-level config. Step-level
observability (task logs, retries, re-runs) stays Airflow's own UI, which
run rows deep-link into.
"""

from hflow.ui._server import UiServer, create_ui_server, serve_ui
from hflow.ui._state import UiState, build_ui_state

__all__ = [
    "UiServer",
    "UiState",
    "build_ui_state",
    "create_ui_server",
    "serve_ui",
]
