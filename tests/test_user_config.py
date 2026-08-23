"""User-level config: the secrets store and the storage-root registry.

Every test isolates the config directory via ``HFLOW_CONFIG_DIR`` so nothing
touches the developer's real ``~/.config/hflow``.
"""

import json
import stat
from pathlib import Path

import pytest

from hflow._user_config import (
    add_storage_root,
    delete_secret,
    ensure_secrets_file,
    normalize_storage_root,
    read_secrets,
    read_storage_registry,
    remove_storage_root,
    secrets_file_path,
    set_secret,
    storage_registry_path,
    user_config_dir,
)


@pytest.fixture(autouse=True)
def isolated_config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    config_dir = tmp_path / "hflow-config"
    monkeypatch.setenv("HFLOW_CONFIG_DIR", str(config_dir))
    return config_dir


def file_mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


class TestConfigDir:
    def test_explicit_override_wins(self, isolated_config_dir: Path) -> None:
        assert user_config_dir() == isolated_config_dir

    def test_xdg_config_home_fallback(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("HFLOW_CONFIG_DIR")
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
        assert user_config_dir() == tmp_path / "xdg" / "hflow"

    def test_home_fallback(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("HFLOW_CONFIG_DIR")
        monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
        monkeypatch.setenv("HOME", str(tmp_path))
        assert user_config_dir() == tmp_path / ".config" / "hflow"


class TestSecrets:
    def test_round_trip_and_owner_only_mode(self) -> None:
        set_secret("OPENAI_API_KEY", "sk-test-123")
        set_secret("HF_TOKEN", "hf_abc")
        assert read_secrets() == {"OPENAI_API_KEY": "sk-test-123", "HF_TOKEN": "hf_abc"}
        assert file_mode(secrets_file_path()) == 0o600

    def test_overwrite_updates_value(self) -> None:
        set_secret("HF_TOKEN", "old")
        set_secret("HF_TOKEN", "new")
        assert read_secrets() == {"HF_TOKEN": "new"}

    def test_delete(self) -> None:
        set_secret("HF_TOKEN", "value")
        assert delete_secret("HF_TOKEN") is True
        assert delete_secret("HF_TOKEN") is False
        assert read_secrets() == {}

    def test_ensure_is_create_if_absent(self) -> None:
        first = ensure_secrets_file()
        set_secret("KEY_ONE", "kept")
        assert ensure_secrets_file() == first
        assert read_secrets() == {"KEY_ONE": "kept"}
        assert file_mode(first) == 0o600

    def test_rewrite_leaves_no_temp_litter(self, isolated_config_dir: Path) -> None:
        set_secret("KEY_ONE", "one")
        set_secret("KEY_TWO", "two")
        assert sorted(entry.name for entry in isolated_config_dir.iterdir()) == ["secrets.env"]

    @pytest.mark.parametrize("bad_name", ["1BAD", "WITH-DASH", "WITH SPACE", "", "A.B"])
    def test_invalid_names_refused(self, bad_name: str) -> None:
        with pytest.raises(ValueError, match="environment variable name"):
            set_secret(bad_name, "value")

    @pytest.mark.parametrize("bad_value", ["line\nbreak", "carriage\rreturn", "nul\0byte"])
    def test_unroundtrippable_values_refused(self, bad_value: str) -> None:
        with pytest.raises(ValueError, match="newlines or NUL"):
            set_secret("KEY", bad_value)

    def test_surrounding_whitespace_refused(self) -> None:
        with pytest.raises(ValueError, match="whitespace"):
            set_secret("KEY", " padded ")

    def test_value_may_contain_equals_and_dollar(self) -> None:
        set_secret("CONNECTION", "user=a$b==c")
        assert read_secrets() == {"CONNECTION": "user=a$b==c"}
        # Stored single-quoted: Compose's env_file parser would interpolate
        # the $ out of an unquoted (or double-quoted) value.
        assert "CONNECTION='user=a$b==c'" in secrets_file_path().read_text()

    def test_single_quotes_in_values_refused(self) -> None:
        with pytest.raises(ValueError, match="single quotes"):
            set_secret("KEY", "it's-bad")


class TestStorageRegistry:
    def test_round_trip(self, tmp_path: Path) -> None:
        local_dir = tmp_path / "data"
        local_dir.mkdir()
        entry = add_storage_root(str(local_dir))
        assert entry.root == str(local_dir.resolve())
        (loaded,) = read_storage_registry()
        assert loaded == entry
        assert loaded.root_id == entry.root_id

    def test_duplicate_refused_across_spellings(self, tmp_path: Path) -> None:
        local_dir = tmp_path / "data"
        local_dir.mkdir()
        add_storage_root(str(local_dir))
        with pytest.raises(ValueError, match="already registered"):
            add_storage_root(str(local_dir / "..") + "/data")

    def test_bucket_url_normalization(self) -> None:
        assert normalize_storage_root("gs://bucket/prefix/") == "gs://bucket/prefix"

    def test_invalid_root_refused(self) -> None:
        with pytest.raises(ValueError, match="unsupported storage scheme"):
            add_storage_root("http://not-a-bucket/prefix")
        assert read_storage_registry() == []

    def test_remove_by_id(self, tmp_path: Path) -> None:
        entry = add_storage_root(str(tmp_path))
        assert remove_storage_root(entry.root_id) is True
        assert remove_storage_root(entry.root_id) is False
        assert read_storage_registry() == []

    def test_corrupt_registry_is_a_loud_error(self) -> None:
        registry_file = storage_registry_path()
        registry_file.parent.mkdir(parents=True)
        registry_file.write_text(json.dumps({"unexpected": True}))
        with pytest.raises(ValueError, match="corrupt"):
            read_storage_registry()
