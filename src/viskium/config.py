"""Read-only configuration loading for Viskium."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from viskium.limits import MAX_CONFIG_FILE_BYTES
from viskium.paths import CONFIG_FILE_ENV, normalized_path, platform_config_file, resolve_data_root

CONFIG_SCHEMA_VERSION = 1


class ConfigError(ValueError):
    """Raised when an explicitly supplied configuration cannot be used."""


@dataclass(frozen=True, slots=True)
class StorageConfig:
    root: Path
    root_source: str

    def to_dict(self) -> dict[str, str]:
        return {"root": str(self.root), "root_source": self.root_source}


@dataclass(frozen=True, slots=True)
class EffectiveConfig:
    schema_version: int
    storage: StorageConfig
    config_file: Path
    config_loaded: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "storage": self.storage.to_dict(),
            "config": {
                "path": str(self.config_file),
                "loaded": self.config_loaded,
            },
        }


def _read_toml(path: Path, *, required: bool) -> dict[str, Any]:
    try:
        if not path.exists():
            if required:
                raise ConfigError(f"configuration file does not exist: {path}")
            return {}
        if not path.is_file():
            raise ConfigError(f"configuration path is not a file: {path}")
    except ConfigError:
        raise
    except OSError as error:
        raise ConfigError(f"cannot inspect configuration {path}: {error}") from error

    try:
        with path.open("rb") as stream:
            raw_document = stream.read(MAX_CONFIG_FILE_BYTES + 1)
        if len(raw_document) > MAX_CONFIG_FILE_BYTES:
            raise ConfigError(f"configuration file exceeds {MAX_CONFIG_FILE_BYTES} bytes: {path}")
        document = tomllib.loads(raw_document.decode("utf-8"))
    except ConfigError:
        raise
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise ConfigError(f"cannot load configuration {path}: {error}") from error

    if not isinstance(document, dict):  # Defensive: tomllib currently always returns dict.
        raise ConfigError(f"configuration root must be a TOML table: {path}")
    return document


def _configured_storage_root(document: Mapping[str, Any]) -> str | None:
    unknown_sections = set(document) - {"storage"}
    if unknown_sections:
        names = ", ".join(sorted(str(name) for name in unknown_sections))
        raise ConfigError(f"unsupported configuration section(s): {names}")
    storage = document.get("storage", {})
    if storage is None:
        return None
    if not isinstance(storage, Mapping):
        raise ConfigError("[storage] must be a TOML table")
    unknown_storage_keys = set(storage) - {"root"}
    if unknown_storage_keys:
        names = ", ".join(sorted(str(name) for name in unknown_storage_keys))
        raise ConfigError(f"unsupported [storage] key(s): {names}")

    root = storage.get("root")
    if root is None:
        return None
    if not isinstance(root, str) or not root.strip():
        raise ConfigError("storage.root must be a non-empty string")
    return root


def load_effective_config(
    *,
    cli_data_root: str | os.PathLike[str] | None = None,
    config_file: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
    platform: str | None = None,
    home: str | os.PathLike[str] | None = None,
) -> EffectiveConfig:
    """Load config and resolve values without creating or modifying any path."""

    env = os.environ if environ is None else environ
    try:
        selected_config_file = (
            normalized_path(config_file, environ=env)
            if config_file is not None
            else platform_config_file(environ=env, platform=platform, home=home)
        )
    except (TypeError, ValueError) as error:
        raise ConfigError(f"invalid configuration path: {error}") from error
    document = _read_toml(
        selected_config_file,
        required=config_file is not None or bool(env.get(CONFIG_FILE_ENV)),
    )
    configured_root = _configured_storage_root(document)
    try:
        resolved_root = resolve_data_root(
            cli_root=cli_data_root,
            config_root=configured_root,
            config_base=selected_config_file.parent,
            environ=env,
            platform=platform,
            home=home,
        )
    except (TypeError, ValueError) as error:
        raise ConfigError(f"invalid data root: {error}") from error

    return EffectiveConfig(
        schema_version=CONFIG_SCHEMA_VERSION,
        storage=StorageConfig(root=resolved_root.path, root_source=resolved_root.source),
        config_file=selected_config_file,
        config_loaded=selected_config_file.is_file(),
    )


__all__ = [
    "CONFIG_SCHEMA_VERSION",
    "ConfigError",
    "EffectiveConfig",
    "StorageConfig",
    "load_effective_config",
]
