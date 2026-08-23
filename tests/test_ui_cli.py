"""``hflow ui``: argument plumbing into the library (server never started)."""

from pathlib import Path
from typing import Any

import pytest

from hflow.cli import main
from hflow.ui import UiState


@pytest.fixture
def captured_serve(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def fake_serve_ui(state: UiState, *, port: int, open_browser: bool) -> int:
        captured.update(state=state, port=port, open_browser=open_browser)
        return 0

    monkeypatch.setattr("hflow.ui.serve_ui", fake_serve_ui)
    return captured


def test_defaults_probe_bundle_and_open_browser(
    captured_serve: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["ui"]) == 0
    assert captured_serve["port"] == 4400
    assert captured_serve["open_browser"] is True
    state = captured_serve["state"]
    assert state.bundle is None  # nothing rendered here; degrade, not fail
    assert state.data_root is None  # ./data does not exist in the tmp cwd


def test_flags_plumb_through(
    captured_serve: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from hflow.runtime import RuntimeConfig, render_bundle

    monkeypatch.chdir(tmp_path)
    pipeline_file = tmp_path / "demo.py"
    pipeline_file.write_text("import hflow\napp = hflow.App('demo', data_root='d')\n")
    bundle_dir = tmp_path / "elsewhere" / "runtime"
    render_bundle(
        RuntimeConfig(pipeline_file=pipeline_file, data_root=tmp_path / "data"), bundle_dir
    )
    data_root = tmp_path / "corpus"
    data_root.mkdir()

    exit_code = main(
        [
            "ui",
            "--port",
            "5000",
            "--bundle-dir",
            str(bundle_dir),
            "--data-root",
            str(data_root),
            "--no-browser",
        ]
    )
    assert exit_code == 0
    assert captured_serve["port"] == 5000
    assert captured_serve["open_browser"] is False
    state = captured_serve["state"]
    assert state.bundle is not None and state.bundle.dag_id == "demo_ingest"
    assert state.data_root == data_root
