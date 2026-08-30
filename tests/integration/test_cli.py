from __future__ import annotations

import json
import runpy
from pathlib import Path
from types import SimpleNamespace

import pytest

from viskium import __version__
from viskium import cli as cli_module
from viskium.cli import main
from viskium.config import EffectiveConfig, StorageConfig
from viskium.resources import doctor as doctor_module


def test_version_is_available_without_a_subcommand(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])

    assert exit_info.value.code == 0
    assert capsys.readouterr().out.strip() == f"viskium {__version__}"


def test_version_has_one_package_source_of_truth() -> None:
    from viskium._version import __version__ as source_version

    assert __version__ == source_version == "0.1.0"


def test_module_entrypoint_delegates_to_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_module, "main", lambda: 7)

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_module("viskium", run_name="__main__")

    assert exit_info.value.code == 7


def test_doctor_json_is_read_only_for_missing_data_root(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_root = tmp_path / "must-not-be-created"

    exit_code = main(["--data-root", str(data_root), "doctor", "--json"])
    report = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert report["read_only"] is True
    assert report["status"] == "warning"
    assert report["data_root"]["path"] == str(data_root.absolute())
    assert report["data_root"]["source"] == "cli"
    assert report["data_root"]["exists"] is False
    assert report["data_root"]["disk"]["free_bytes"] > 0
    assert not data_root.exists()


def test_doctor_reports_existing_file_as_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    not_a_directory = tmp_path / "plain-file"
    not_a_directory.write_text("not a data root", encoding="utf-8")

    exit_code = main(["doctor", "--data-root", str(not_a_directory), "--json"])
    report = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert report["status"] == "error"
    assert report["data_root"]["exists"] is True
    assert report["data_root"]["is_directory"] is False
    assert report["issues"][0]["code"] == "data_root_not_directory"


def test_doctor_existing_directory_is_ok(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["doctor", "--data-root", str(tmp_path), "--json"]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["status"] == "ok"
    assert report["issues"] == []
    assert report["data_root"]["exists"] is True
    assert report["data_root"]["is_directory"] is True


def test_doctor_reports_uninspectable_root_and_unresolved_volume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = EffectiveConfig(
        schema_version=1,
        storage=StorageConfig(tmp_path / "data", "cli"),
        config_file=tmp_path / "config.toml",
        config_loaded=False,
    )
    monkeypatch.setattr(Path, "stat", lambda _self: (_ for _ in ()).throw(PermissionError()))
    monkeypatch.setattr(
        doctor_module,
        "_disk_report",
        lambda _path: {
            "available": False,
            "probe_path": None,
            "total_bytes": None,
            "used_bytes": None,
            "free_bytes": None,
            "reason": "no_existing_ancestor",
        },
    )

    report = doctor_module.build_doctor_report(config)

    assert report["status"] == "error"
    assert [issue["code"] for issue in report["issues"]] == [
        "data_root_inspection_failed",
        "volume_space_unavailable",
    ]


def test_disk_probe_failures_are_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(doctor_module, "_nearest_existing_directory", lambda _path: None)
    assert doctor_module._disk_report(tmp_path)["reason"] == "no_existing_ancestor"

    monkeypatch.setattr(doctor_module, "_nearest_existing_directory", lambda _path: tmp_path)
    monkeypatch.setattr(
        doctor_module.shutil,
        "disk_usage",
        lambda _path: (_ for _ in ()).throw(OSError()),
    )
    report = doctor_module._disk_report(tmp_path)
    assert report["available"] is False
    assert report["reason"] == "disk_usage_failed:OSError"


def test_config_show_effective_json_uses_declared_root(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text('[storage]\nroot = "data"\n', encoding="utf-8")

    exit_code = main(["config", "show", "--config", str(config_file), "--effective", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["storage"] == {
        "root": str((tmp_path / "data").absolute()),
        "root_source": "config",
    }
    assert payload["config"] == {"path": str(config_file.absolute()), "loaded": True}


def test_config_text_output_and_explicit_config_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["config", "show", "--effective"]) == 0
    assert "schema_version: 1" in capsys.readouterr().out

    with pytest.raises(SystemExit) as exit_info:
        main(["config", "show", "--config", str(tmp_path / "missing.toml"), "--effective"])
    assert exit_info.value.code == 2
    assert "configuration file does not exist" in capsys.readouterr().err


def test_scalar_text_output(capsys: pytest.CaptureFixture[str]) -> None:
    cli_module._print_payload("plain", as_json=False)
    assert capsys.readouterr().out == "plain\n"


@pytest.mark.parametrize("frames", ["nope", "-1"])
def test_replay_rejects_invalid_frame_count(frames: str) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["replay", "--mode", "exhaustive", "--frames", frames, "--json"])
    assert exit_info.value.code == 2


@pytest.mark.parametrize("option", ["--data-root", "--config"])
@pytest.mark.parametrize("value", ["", "   "])
def test_cli_rejects_empty_paths(option: str, value: str) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["doctor", option, value, "--json"])
    assert exit_info.value.code == 2


def test_replay_cli_rejects_frame_count_above_safety_ceiling() -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["replay", "--mode", "exhaustive", "--frames", "10001", "--json"])
    assert exit_info.value.code == 2


def test_cli_reports_remote_data_root_as_an_argument_error() -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["doctor", "--data-root", r"\\server\share\viskium", "--json"])
    assert exit_info.value.code == 2


@pytest.mark.parametrize("mode", ["exhaustive", "faithful"])
def test_replay_cli_delegates_to_runtime(
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from viskium.runtime import replay

    calls: list[tuple[str, int]] = []

    def fake_run(selected_mode: str, frame_count: int) -> SimpleNamespace:
        calls.append((selected_mode, frame_count))
        return SimpleNamespace(to_dict=lambda: {"mode": selected_mode, "frames": frame_count})

    monkeypatch.setattr(replay, "run_synthetic_replay", fake_run)

    assert main(["replay", "--mode", mode, "--frames", "3", "--json"]) == 0
    assert calls == [(mode, 3)]
    assert json.loads(capsys.readouterr().out) == {"mode": mode, "frames": 3}
