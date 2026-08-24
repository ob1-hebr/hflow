"""Secrets tab endpoints over HTTP: values never leave the server."""

import json
from pathlib import Path

from test_ui_server import request_json, running_ui

from hflow._user_config import read_secrets
from hflow.ui import build_ui_state


class TestSecrets:
    def test_put_list_delete_round_trip(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=None, data_root=None)
        with running_ui(state) as base_url:
            status, _ = request_json(
                base_url, "/api/secrets/OPENAI_API_KEY", method="PUT", body={"value": "sk-123"}
            )
            assert status == 204
            status, _ = request_json(
                base_url, "/api/secrets/HF_TOKEN", method="PUT", body={"value": "hf_abc"}
            )
            assert status == 204

            status, payload = request_json(base_url, "/api/secrets")
            assert status == 200
            assert payload["secrets"] == [
                {"name": "HF_TOKEN", "masked_value": "•" * 8},
                {"name": "OPENAI_API_KEY", "masked_value": "•" * 8},
            ]
            # The values themselves must never appear anywhere in the response.
            assert "sk-123" not in json.dumps(payload)
            assert "hf_abc" not in json.dumps(payload)
            assert payload["wired_into_bundle"] is False

            status, _ = request_json(base_url, "/api/secrets/HF_TOKEN", method="DELETE")
            assert status == 204
            status, _ = request_json(base_url, "/api/secrets/HF_TOKEN", method="DELETE")
            assert status == 404
        assert read_secrets() == {"OPENAI_API_KEY": "sk-123"}

    def test_put_upserts(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=None, data_root=None)
        with running_ui(state) as base_url:
            request_json(base_url, "/api/secrets/KEY", method="PUT", body={"value": "old"})
            status, _ = request_json(
                base_url, "/api/secrets/KEY", method="PUT", body={"value": "new"}
            )
        assert status == 204
        assert read_secrets() == {"KEY": "new"}

    def test_invalid_names_and_values_are_400(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=None, data_root=None)
        with running_ui(state) as base_url:
            status, payload = request_json(
                base_url, "/api/secrets/1BAD", method="PUT", body={"value": "x"}
            )
            assert status == 400
            assert "environment variable name" in payload["error"]
            status, payload = request_json(
                base_url, "/api/secrets/GOOD", method="PUT", body={"value": "with\nnewline"}
            )
            assert status == 400
            status, payload = request_json(base_url, "/api/secrets/GOOD", method="PUT", body={})
            assert status == 400
        assert read_secrets() == {}

    def test_wired_into_bundle_reflects_the_rendered_compose(self, tmp_path: Path) -> None:
        from hflow.runtime import RuntimeConfig, render_bundle

        pipeline_file = tmp_path / "demo.py"
        pipeline_file.write_text("import hflow\napp = hflow.App('demo', data_root='d')\n")
        bundle_dir = tmp_path / "runtime"
        render_bundle(
            RuntimeConfig(pipeline_file=pipeline_file, data_root=tmp_path / "data"), bundle_dir
        )
        state = build_ui_state(bundle_dir=bundle_dir, data_root=None)
        with running_ui(state) as base_url:
            _, payload = request_json(base_url, "/api/secrets")
            assert payload["wired_into_bundle"] is True
            # A bundle rendered before the secrets feature has no env_file
            # reference; the flag tells the frontend to suggest `hflow up`.
            assert state.bundle is not None
            compose_text = state.bundle.compose_file.read_text()
            state.bundle.compose_file.write_text(
                "\n".join(
                    line
                    for line in compose_text.splitlines()
                    if "secrets.env" not in line and "env_file" not in line
                )
            )
            _, payload = request_json(base_url, "/api/secrets")
            assert payload["wired_into_bundle"] is False
