"""Side-effect-free path resolution for Viskium.

This module only computes and inspects paths.  Directory creation belongs to an
explicit bootstrap/storage operation, never to configuration loading or doctor.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

DATA_ROOT_ENV = "VISKIUM_DATA_ROOT"
CONFIG_FILE_ENV = "VISKIUM_CONFIG_FILE"

_ENVIRONMENT_VARIABLE = re.compile(
    r"\$(?P<plain>[A-Za-z_][A-Za-z0-9_]*)|\$\{(?P<braced>[^}]+)\}|%(?P<windows>[^%]+)%"
)


@dataclass(frozen=True, slots=True)
class ResolvedPath:
    """An absolute path together with the precedence branch that selected it."""

    path: Path
    source: str


def _environment(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def _home_path(home: str | os.PathLike[str] | None) -> Path:
    return Path.home() if home is None else Path(home).expanduser()


def normalized_path(
    value: str | os.PathLike[str],
    *,
    base: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Expand and absolutize *value* without requiring the target to exist."""

    raw = os.fspath(value).strip()
    if not raw:
        raise ValueError("path must not be empty")

    env = _environment(environ)

    def expand_variable(match: re.Match[str]) -> str:
        name = match.group("plain") or match.group("braced") or match.group("windows")
        return env.get(name, match.group(0))

    expanded = _ENVIRONMENT_VARIABLE.sub(expand_variable, raw)
    path = Path(expanded).expanduser()
    windows_like = str(path).replace("/", "\\")
    if windows_like.startswith("\\\\"):
        raise ValueError("remote and device namespace paths are not supported")
    if path.drive and not path.is_absolute():
        raise ValueError("drive-relative paths are not supported")
    if not path.is_absolute():
        anchor = Path.cwd() if base is None else Path(base).expanduser()
        path = anchor / path
    return Path(os.path.abspath(path))


def platform_data_root(
    *,
    environ: Mapping[str, str] | None = None,
    platform: str | None = None,
    home: str | os.PathLike[str] | None = None,
) -> Path:
    """Return the platform fallback data root without creating it."""

    env = _environment(environ)
    platform_name = sys.platform if platform is None else platform
    home_path = _home_path(home)

    if platform_name.startswith("win"):
        local_app_data = env.get("LOCALAPPDATA")
        base = (
            Path(local_app_data).expanduser() if local_app_data else home_path / "AppData" / "Local"
        )
        return normalized_path(base / "Viskium", environ=env)
    if platform_name == "darwin":
        return normalized_path(
            home_path / "Library" / "Application Support" / "Viskium", environ=env
        )

    xdg_data_home = env.get("XDG_DATA_HOME")
    base = Path(xdg_data_home).expanduser() if xdg_data_home else home_path / ".local" / "share"
    return normalized_path(base / "viskium", environ=env)


def platform_config_file(
    *,
    environ: Mapping[str, str] | None = None,
    platform: str | None = None,
    home: str | os.PathLike[str] | None = None,
) -> Path:
    """Return the default config file path without creating or reading it."""

    env = _environment(environ)
    explicit = env.get(CONFIG_FILE_ENV)
    if explicit:
        return normalized_path(explicit, environ=env)

    platform_name = sys.platform if platform is None else platform
    home_path = _home_path(home)
    if platform_name.startswith("win"):
        app_data = env.get("APPDATA")
        base = Path(app_data).expanduser() if app_data else home_path / "AppData" / "Roaming"
        return normalized_path(base / "Viskium" / "config.toml", environ=env)
    if platform_name == "darwin":
        return normalized_path(
            home_path / "Library" / "Application Support" / "Viskium" / "config.toml",
            environ=env,
        )

    xdg_config_home = env.get("XDG_CONFIG_HOME")
    base = Path(xdg_config_home).expanduser() if xdg_config_home else home_path / ".config"
    return normalized_path(base / "viskium" / "config.toml", environ=env)


def resolve_data_root(
    *,
    cli_root: str | os.PathLike[str] | None = None,
    config_root: str | os.PathLike[str] | None = None,
    config_base: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
    platform: str | None = None,
    home: str | os.PathLike[str] | None = None,
) -> ResolvedPath:
    """Apply CLI -> environment -> config -> platform-default precedence."""

    env = _environment(environ)
    if cli_root is not None:
        return ResolvedPath(normalized_path(cli_root, environ=env), "cli")

    environment_root = env.get(DATA_ROOT_ENV)
    if environment_root:
        return ResolvedPath(normalized_path(environment_root, environ=env), "environment")

    if config_root is not None:
        return ResolvedPath(
            normalized_path(config_root, base=config_base, environ=env),
            "config",
        )

    return ResolvedPath(
        platform_data_root(environ=env, platform=platform, home=home),
        "platform_default",
    )


__all__ = [
    "CONFIG_FILE_ENV",
    "DATA_ROOT_ENV",
    "ResolvedPath",
    "normalized_path",
    "platform_config_file",
    "platform_data_root",
    "resolve_data_root",
]
