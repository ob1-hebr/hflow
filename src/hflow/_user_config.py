"""User-level configuration: the secrets store and the storage-root registry.

Both live in one per-user directory (``HFLOW_CONFIG_DIR``, then
``$XDG_CONFIG_HOME/hflow``, then ``~/.config/hflow`` -- the same precedence
shape as the mirror cache in :mod:`hflow.storage`):

- ``secrets.env`` -- ``KEY=VALUE`` lines, owner-only (0600). Rendered runtime
  bundles reference this file by absolute path in their compose ``env_file``,
  so the values become task-container environment variables on the next
  ``hflow up``. Values never enter the bundle itself.
- ``storage_roots.json`` -- data roots the user registered in the dashboard's
  Storage tab. Registration is bookkeeping only; removing an entry never
  touches the data.

This module sits at the package top level (not under ``hflow.ui``) because
bundle rendering in :mod:`hflow.runtime` needs the secrets path at render
time, and the subpackages keep their underscore-private modules to themselves.
"""

import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from hflow.storage import LocalStorageRoot, parse_storage_root

SECRET_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_SECRETS_FILE_HEADER = (
    "# hflow user secrets (managed by `hflow ui`, Secrets tab). KEY=VALUE lines\n"
    "# become task-container environment variables on the next `hflow up`.\n"
)


def user_config_dir() -> Path:
    override = os.environ.get("HFLOW_CONFIG_DIR")
    if override:
        return Path(override)
    config_home = os.environ.get("XDG_CONFIG_HOME")
    config_base = Path(config_home) if config_home else Path.home() / ".config"
    return config_base / "hflow"


def parse_env_text(text: str) -> dict[str, str]:
    """Parse ``KEY=VALUE`` lines (comments and blanks skipped, values verbatim)."""
    env_values: dict[str, str] = {}
    for line in text.splitlines():
        stripped_line = line.strip()
        if not stripped_line or stripped_line.startswith("#") or "=" not in stripped_line:
            continue
        key, _, value = stripped_line.partition("=")
        env_values[key.strip()] = value.strip()
    return env_values


def format_env_text(env_values: dict[str, str], *, header: str) -> str:
    return header + "".join(f"{key}={value}\n" for key, value in env_values.items())


def _write_owner_only_atomically(destination: Path, text: str) -> None:
    """Replace ``destination`` with ``text`` at mode 0600, never exposing a partial file."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
        delete=False,
    ) as temporary_stream:
        temporary_stream.write(text)
        temporary_path = Path(temporary_stream.name)
    try:
        temporary_path.chmod(0o600)
        temporary_path.replace(destination)
    finally:
        temporary_path.unlink(missing_ok=True)


def secrets_file_path() -> Path:
    return user_config_dir() / "secrets.env"


def ensure_secrets_file() -> Path:
    """Create the secrets file if absent (0600, atomic) and return its path.

    Existing files are only re-chmod'd, mirroring how bundle rendering heals
    a pre-existing ``.env``: user content is never rewritten here.
    """
    secrets_file = secrets_file_path()
    secrets_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(secrets_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        secrets_file.chmod(0o600)
    else:
        with os.fdopen(descriptor, "w") as secrets_stream:
            secrets_stream.write(_SECRETS_FILE_HEADER)
    return secrets_file


def read_secrets() -> dict[str, str]:
    secrets_file = secrets_file_path()
    if not secrets_file.is_file():
        return {}
    return parse_env_text(secrets_file.read_text())


def _validate_secret_name(name: str) -> None:
    if not SECRET_NAME_PATTERN.fullmatch(name):
        raise ValueError(
            f"secret name {name!r} is not an environment variable name "
            "(letters, digits, and underscore, not starting with a digit)"
        )


def set_secret(name: str, value: str) -> None:
    _validate_secret_name(name)
    # The .env grammar is line-oriented and strips surrounding whitespace, so
    # values that would not round-trip are refused instead of silently mangled.
    if any(character in value for character in ("\n", "\r", "\0")):
        raise ValueError(f"secret {name!r} value must not contain newlines or NUL bytes")
    if value != value.strip():
        raise ValueError(f"secret {name!r} value must not start or end with whitespace")
    ensure_secrets_file()
    secrets = read_secrets()
    secrets[name] = value
    _write_owner_only_atomically(
        secrets_file_path(), format_env_text(secrets, header=_SECRETS_FILE_HEADER)
    )


def delete_secret(name: str) -> bool:
    _validate_secret_name(name)
    secrets = read_secrets()
    if name not in secrets:
        return False
    del secrets[name]
    _write_owner_only_atomically(
        secrets_file_path(), format_env_text(secrets, header=_SECRETS_FILE_HEADER)
    )
    return True


@dataclass(frozen=True)
class RegisteredRoot:
    """One storage root the user registered in the dashboard."""

    root: str  # normalized: local paths absolute, bucket URLs without trailing slash
    added_at: str  # ISO 8601 UTC

    @property
    def root_id(self) -> str:
        return storage_root_id(self.root)


def storage_root_id(normalized_root: str) -> str:
    """A stable, URL-safe id for one root (the compose project-name idiom)."""
    return sha256(normalized_root.encode()).hexdigest()[:8]


def normalize_storage_root(root: str) -> str:
    """Validate ``root`` via the storage boundary and normalize it for identity.

    Local paths become absolute (two spellings of one directory must collide);
    bucket URLs keep their exact form minus any trailing slash. Invalid roots
    raise the same loud ``ValueError`` as :func:`hflow.storage.parse_storage_root`.
    """
    parsed = parse_storage_root(root)
    if isinstance(parsed, LocalStorageRoot):
        return str(parsed.path.expanduser().resolve())
    return parsed.url.rstrip("/")


def storage_registry_path() -> Path:
    return user_config_dir() / "storage_roots.json"


def read_storage_registry() -> list[RegisteredRoot]:
    registry_file = storage_registry_path()
    if not registry_file.is_file():
        return []
    try:
        document = json.loads(registry_file.read_text())
        entries = document["roots"]
        return [RegisteredRoot(root=entry["root"], added_at=entry["added_at"]) for entry in entries]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise ValueError(f"storage registry {registry_file} is corrupt: {error}") from error


def _write_storage_registry(roots: list[RegisteredRoot]) -> None:
    document = {
        "version": 1,
        "roots": [{"root": entry.root, "added_at": entry.added_at} for entry in roots],
    }
    _write_owner_only_atomically(storage_registry_path(), json.dumps(document, indent=2) + "\n")


def add_storage_root(root: str) -> RegisteredRoot:
    normalized_root = normalize_storage_root(root)
    registry = read_storage_registry()
    if any(entry.root == normalized_root for entry in registry):
        raise ValueError("this data root is already registered")
    entry = RegisteredRoot(root=normalized_root, added_at=datetime.now(tz=UTC).isoformat())
    _write_storage_registry([*registry, entry])
    return entry


def remove_storage_root(root_id: str) -> bool:
    registry = read_storage_registry()
    remaining = [entry for entry in registry if entry.root_id != root_id]
    if len(remaining) == len(registry):
        return False
    _write_storage_registry(remaining)
    return True
