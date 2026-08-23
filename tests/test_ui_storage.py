"""Storage tab endpoints over HTTP: roots CRUD, browse, catalog summary."""

from pathlib import Path

from test_ui_server import request_json, running_ui

import hflow
from hflow.catalog import Catalog, CheckRunRow
from hflow.transform import EpisodeStamps
from hflow.ui import build_ui_state

STAMPS = EpisodeStamps(
    schema_version="1",
    pipeline_version="abc123def456",
    ffmpeg_version="ffmpeg version test",
    robot_software_version="sim-0.1.0",
)


def _append_episode(
    catalog: Catalog, tmp_path: Path, name: str, *, quarantined: bool = False
) -> None:
    canonical = tmp_path / f"{name}.canonical.mcap"
    canonical.write_bytes(f"bytes of {name}".encode())
    catalog.append_episode(
        canonical_path=canonical,
        stamps=STAMPS,
        episode_metadata={"task": "fold_napkin"},
        check_rows=[
            CheckRunRow(
                check_name="camera_health",
                check_version="v1",
                critical=True,
                status=hflow.CheckStatus.FAILED if quarantined else hflow.CheckStatus.MEASURED,
                duration_s=0.01,
                measurements={"black_frame_pct": 40.0 if quarantined else 0.5},
                tags=[],
                intervals=[],
            )
        ],
        quarantine_tags=["camera_health"] if quarantined else (),
    )


class TestRootsCrud:
    def test_register_list_and_remove(self, tmp_path: Path) -> None:
        implicit_dir = tmp_path / "served-data"
        implicit_dir.mkdir()
        registered_dir = tmp_path / "other-data"
        registered_dir.mkdir()
        state = build_ui_state(bundle_dir=None, data_root=implicit_dir)
        with running_ui(state) as base_url:
            status, created = request_json(
                base_url, "/api/storage/roots", method="POST", body={"root": str(registered_dir)}
            )
            assert status == 201
            assert created["root"] == str(registered_dir.resolve())
            assert created["kind"] == "local"
            assert created["implicit"] is False

            status, listing = request_json(base_url, "/api/storage/roots")
            assert status == 200
            by_root = {root["root"]: root for root in listing["roots"]}
            assert by_root[str(registered_dir.resolve())]["implicit"] is False
            assert by_root[str(implicit_dir.resolve())]["implicit"] is True
            assert by_root[str(implicit_dir.resolve())]["added_at"] is None

            status, _ = request_json(
                base_url, f"/api/storage/roots/{created['root_id']}", method="DELETE"
            )
            assert status == 204
            status, listing = request_json(base_url, "/api/storage/roots")
            assert [root["root"] for root in listing["roots"]] == [str(implicit_dir.resolve())]

    def test_implicit_root_is_not_removable(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=None, data_root=tmp_path)
        with running_ui(state) as base_url:
            _, listing = request_json(base_url, "/api/storage/roots")
            (implicit,) = listing["roots"]
            status, payload = request_json(
                base_url, f"/api/storage/roots/{implicit['root_id']}", method="DELETE"
            )
        assert status == 409
        assert "implicit" in payload["error"]

    def test_registering_the_implicit_root_makes_it_removable(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=None, data_root=tmp_path)
        with running_ui(state) as base_url:
            status, created = request_json(
                base_url, "/api/storage/roots", method="POST", body={"root": str(tmp_path)}
            )
            assert status == 201
            _, listing = request_json(base_url, "/api/storage/roots")
            (only,) = listing["roots"]
            assert only["implicit"] is False
            status, _ = request_json(
                base_url, f"/api/storage/roots/{created['root_id']}", method="DELETE"
            )
            assert status == 204

    def test_duplicate_and_invalid_roots_are_400(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=None, data_root=None)
        with running_ui(state) as base_url:
            request_json(
                base_url, "/api/storage/roots", method="POST", body={"root": str(tmp_path)}
            )
            status, payload = request_json(
                base_url, "/api/storage/roots", method="POST", body={"root": str(tmp_path)}
            )
            assert status == 400
            assert "already registered" in payload["error"]
            status, payload = request_json(
                base_url, "/api/storage/roots", method="POST", body={"root": "http://nope/x"}
            )
            assert status == 400
            assert "unsupported storage scheme" in payload["error"]
            status, _ = request_json(base_url, "/api/storage/roots", method="POST", body={})
            assert status == 400

    def test_unknown_root_deletion_is_404(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=None, data_root=None)
        with running_ui(state) as base_url:
            status, _ = request_json(base_url, "/api/storage/roots/deadbeef", method="DELETE")
        assert status == 404


class TestBrowse:
    def test_levels_and_sizes(self, tmp_path: Path) -> None:
        (tmp_path / "episodes-in").mkdir()
        (tmp_path / "episodes-in" / "run_0001.mcap").write_bytes(b"x" * 5)
        (tmp_path / "manifest.parquet").write_bytes(b"x" * 3)
        state = build_ui_state(bundle_dir=None, data_root=tmp_path)
        with running_ui(state) as base_url:
            _, listing = request_json(base_url, "/api/storage/roots")
            root_id = listing["roots"][0]["root_id"]
            status, top = request_json(base_url, f"/api/storage/roots/{root_id}/browse")
            assert status == 200
            assert top["directories"] == ["episodes-in"]
            assert top["files"] == [{"name": "manifest.parquet", "size": 3}]
            status, nested = request_json(
                base_url, f"/api/storage/roots/{root_id}/browse?prefix=episodes-in"
            )
            assert nested["files"] == [{"name": "run_0001.mcap", "size": 5}]

    def test_unknown_root_is_404(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=None, data_root=None)
        with running_ui(state) as base_url:
            status, _ = request_json(base_url, "/api/storage/roots/deadbeef/browse")
        assert status == 404


class TestCatalogSummary:
    def test_counts_latest_and_measurement_keys(self, tmp_path: Path) -> None:
        data_root = tmp_path / "data"
        catalog = Catalog(data_root / "catalog")
        _append_episode(catalog, tmp_path, "good")
        _append_episode(catalog, tmp_path, "bad", quarantined=True)
        state = build_ui_state(bundle_dir=None, data_root=data_root)
        with running_ui(state) as base_url:
            _, listing = request_json(base_url, "/api/storage/roots")
            root_id = listing["roots"][0]["root_id"]
            status, payload = request_json(base_url, f"/api/storage/roots/{root_id}/catalog")
        assert status == 200
        assert payload["present"] is True
        assert payload["episode_count"] == 2
        assert payload["quarantined_count"] == 1
        assert payload["ok_count"] == 1
        assert payload["latest_recorded_at"] is not None
        assert "black_frame_pct" in payload["measurement_keys"]

    def test_bare_directory_reports_absent(self, tmp_path: Path) -> None:
        state = build_ui_state(bundle_dir=None, data_root=tmp_path)
        with running_ui(state) as base_url:
            _, listing = request_json(base_url, "/api/storage/roots")
            root_id = listing["roots"][0]["root_id"]
            status, payload = request_json(base_url, f"/api/storage/roots/{root_id}/catalog")
        assert status == 200
        assert payload == {"present": False}
