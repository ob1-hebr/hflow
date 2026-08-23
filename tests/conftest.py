"""Shared test setup.

Pins the whole suite to the system ffmpeg via the explicit override so tests
never trigger the pinned-build download (a per-machine, network-bound step).
Set at import time, before any test imports resolve the (cached) binary path.

Also points the user-level config dir (secrets store, storage registry) at a
per-session temp directory so no test can read or write the developer's real
``~/.config/hflow`` -- bundle rendering references the secrets file path.
"""

import os
import shutil

import pytest

_system_ffmpeg = shutil.which("ffmpeg")
if _system_ffmpeg is not None:
    os.environ.setdefault("HFLOW_FFMPEG", _system_ffmpeg)


@pytest.fixture(autouse=True)
def _isolated_user_config_dir(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A FRESH directory per test: registry/secrets state must not leak between
    # tests any more than into the developer's real ~/.config/hflow.
    monkeypatch.setenv("HFLOW_CONFIG_DIR", str(tmp_path_factory.mktemp("hflow-user-config")))
