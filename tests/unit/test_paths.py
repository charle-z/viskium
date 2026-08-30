from __future__ import annotations

import os
from pathlib import Path

import pytest

from viskium import config as config_module
from viskium.config import ConfigError, load_effective_config
from viskium.paths import (
    normalized_path,
    platform_config_file,
    platform_data_root,
    resolve_data_root,
)


def test_data_root_precedence_is_cli_environment_config_fallback(tmp_path: Path) -> None:
    config_root = tmp_path / "from-config"
    environment_root = tmp_path / "from-environment"
    cli_root = tmp_path / "from-cli"

    resolved = resolve_data_root(
        cli_root=cli_root,
        config_root=config_root,
        environ={"VISKIUM_DATA_ROOT": str(environment_root)},
        platform="linux",
        home=tmp_path / "home",
    )
    assert resolved.path == cli_root.absolute()
    assert resolved.source == "cli"

    resolved = resolve_data_root(
        config_root=config_root,
        environ={"VISKIUM_DATA_ROOT": str(environment_root)},
        platform="linux",
        home=tmp_path / "home",
    )
    assert resolved.path == environment_root.absolute()
    assert resolved.source == "environment"

    resolved = resolve_data_root(
        config_root=config_root,
        environ={},
        platform="linux",
        home=tmp_path / "home",
    )
    assert resolved.path == config_root.absolute()
    assert resolved.source == "config"

    resolved = resolve_data_root(environ={}, platform="linux", home=tmp_path / "home")
    assert resolved.path == (tmp_path / "home" / ".local" / "share" / "viskium").absolute()
    assert resolved.source == "platform_default"


def test_relative_config_root_is_anchored_to_config_file(tmp_path: Path) -> None:
    config_file = tmp_path / "settings" / "config.toml"
    config_file.parent.mkdir()
    config_file.write_text('[storage]\nroot = "../data"\n', encoding="utf-8")

    effective = load_effective_config(config_file=config_file, environ={})

    assert effective.storage.root == Path(os.path.abspath(config_file.parent / ".." / "data"))
    assert effective.storage.root_source == "config"
    assert not effective.storage.root.exists()


def test_loading_config_does_not_create_data_root(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    data_root = tmp_path / "not-created"
    config_file.write_text(f'[storage]\nroot = "{data_root.as_posix()}"\n', encoding="utf-8")

    load_effective_config(config_file=config_file, environ={})

    assert not data_root.exists()


def test_explicit_missing_config_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="does not exist"):
        load_effective_config(config_file=tmp_path / "missing.toml", environ={})


def test_empty_existing_config_is_reported_as_loaded(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text("", encoding="utf-8")

    effective = load_effective_config(
        config_file=config_file,
        environ={},
        platform="linux",
        home=tmp_path / "home",
    )

    assert effective.config_loaded is True
    assert effective.storage.root_source == "platform_default"


def test_platform_fallbacks_are_stable(tmp_path: Path) -> None:
    windows = platform_data_root(environ={}, platform="win32", home=tmp_path)
    linux = platform_data_root(environ={}, platform="linux", home=tmp_path)
    macos = platform_data_root(environ={}, platform="darwin", home=tmp_path)

    assert windows == (tmp_path / "AppData" / "Local" / "Viskium").absolute()
    assert linux == (tmp_path / ".local" / "share" / "viskium").absolute()
    assert macos == (tmp_path / "Library" / "Application Support" / "Viskium").absolute()


def test_platform_config_file_honors_explicit_and_platform_locations(tmp_path: Path) -> None:
    explicit = platform_config_file(
        environ={"VISKIUM_CONFIG_FILE": str(tmp_path / "explicit.toml")},
        platform="linux",
        home=tmp_path,
    )
    windows = platform_config_file(environ={}, platform="win32", home=tmp_path)
    linux = platform_config_file(environ={}, platform="linux", home=tmp_path)
    macos = platform_config_file(environ={}, platform="darwin", home=tmp_path)

    assert explicit == (tmp_path / "explicit.toml").absolute()
    assert windows == (tmp_path / "AppData" / "Roaming" / "Viskium" / "config.toml").absolute()
    assert linux == (tmp_path / ".config" / "viskium" / "config.toml").absolute()
    assert (
        macos
        == (tmp_path / "Library" / "Application Support" / "Viskium" / "config.toml").absolute()
    )


def test_xdg_and_windows_environment_bases_are_honored(tmp_path: Path) -> None:
    assert (
        platform_data_root(
            environ={"LOCALAPPDATA": str(tmp_path / "local")},
            platform="win32",
            home=tmp_path / "home",
        )
        == (tmp_path / "local" / "Viskium").absolute()
    )
    assert (
        platform_data_root(
            environ={"XDG_DATA_HOME": str(tmp_path / "xdg-data")},
            platform="linux",
            home=tmp_path / "home",
        )
        == (tmp_path / "xdg-data" / "viskium").absolute()
    )
    assert (
        platform_config_file(
            environ={"APPDATA": str(tmp_path / "roaming")},
            platform="win32",
            home=tmp_path / "home",
        )
        == (tmp_path / "roaming" / "Viskium" / "config.toml").absolute()
    )
    assert (
        platform_config_file(
            environ={"XDG_CONFIG_HOME": str(tmp_path / "xdg-config")},
            platform="linux",
            home=tmp_path / "home",
        )
        == (tmp_path / "xdg-config" / "viskium" / "config.toml").absolute()
    )


def test_empty_path_is_rejected() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        normalized_path("  ")


@pytest.mark.parametrize(
    "value",
    [
        r"\\server\share\viskium",
        r"\\?\C:\viskium",
        r"\\.\pipe\viskium",
        "//server/share/viskium",
    ],
)
def test_remote_and_device_paths_are_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="remote and device"):
        normalized_path(value)


def test_paths_collapse_dot_segments_without_resolving_the_target(tmp_path: Path) -> None:
    normalized = normalized_path(tmp_path / "one" / ".." / "two")

    assert normalized == tmp_path / "two"
    assert not normalized.exists()


def test_injected_environment_is_authoritative_for_expansion(tmp_path: Path) -> None:
    resolved = resolve_data_root(
        environ={
            "VISKIUM_DATA_ROOT": "$VISKIUM_TEST_BASE/data",
            "VISKIUM_TEST_BASE": str(tmp_path),
        },
        platform="linux",
        home=tmp_path / "home",
    )

    assert resolved.path == tmp_path / "data"


def test_missing_environment_selected_config_is_an_error(tmp_path: Path) -> None:
    missing = tmp_path / "missing.toml"

    with pytest.raises(ConfigError, match="does not exist"):
        load_effective_config(environ={"VISKIUM_CONFIG_FILE": str(missing)})


def test_config_size_and_encoding_are_bounded(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized.toml"
    oversized.write_bytes(b"#" * (1_048_576 + 1))
    with pytest.raises(ConfigError, match="exceeds"):
        load_effective_config(config_file=oversized, environ={})

    invalid_utf8 = tmp_path / "invalid-utf8.toml"
    invalid_utf8.write_bytes(b"\xff")
    with pytest.raises(ConfigError, match="cannot load"):
        load_effective_config(config_file=invalid_utf8, environ={})


@pytest.mark.parametrize(
    ("contents", "message"),
    [
        ("unexpected = true\n", "unsupported configuration section"),
        ("[storage]\nroot = 'data'\nextra = true\n", "unsupported.*storage"),
    ],
)
def test_unknown_config_keys_fail_closed(tmp_path: Path, contents: str, message: str) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(contents, encoding="utf-8")

    with pytest.raises(ConfigError, match=message):
        load_effective_config(config_file=config_file, environ={})


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"storage": 1}, "must be a TOML table"),
        ({"storage": {"root": ""}}, "must be a non-empty string"),
        ({"storage": {"root": 42}}, "must be a non-empty string"),
    ],
)
def test_invalid_storage_config_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    document: dict[str, object],
    message: str,
) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text("# replaced by test double", encoding="utf-8")
    monkeypatch.setattr(config_module, "_read_toml", lambda *_args, **_kwargs: document)

    with pytest.raises(ConfigError, match=message):
        load_effective_config(config_file=config_file, environ={})


@pytest.mark.parametrize("document", [{"storage": None}, {"storage": {}}, {}])
def test_absent_storage_root_uses_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    document: dict[str, object],
) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text("# replaced by test double", encoding="utf-8")
    monkeypatch.setattr(config_module, "_read_toml", lambda *_args, **_kwargs: document)

    effective = load_effective_config(
        config_file=config_file,
        environ={},
        platform="linux",
        home=tmp_path / "home",
    )

    assert effective.storage.root_source == "platform_default"


def test_directory_and_malformed_config_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="is not a file"):
        load_effective_config(config_file=tmp_path, environ={})

    malformed = tmp_path / "malformed.toml"
    malformed.write_text("[storage\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="cannot load configuration"):
        load_effective_config(config_file=malformed, environ={})


def test_defensive_non_mapping_toml_result_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text("ignored", encoding="utf-8")
    monkeypatch.setattr(config_module.tomllib, "loads", lambda _document: [])

    with pytest.raises(ConfigError, match="root must be a TOML table"):
        load_effective_config(config_file=config_file, environ={})
