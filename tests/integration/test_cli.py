from __future__ import annotations

import json
import runpy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from viskium import __version__
from viskium import cli as cli_module
from viskium.adapters import DeterministicProcessor, SyntheticSource
from viskium.cli import main
from viskium.config import EffectiveConfig, StorageConfig
from viskium.resources import doctor as doctor_module
from viskium.storage import SQLiteStore, initialize_data_root, verify_data_root


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
    assert report["data_root"]["read_only"] is True
    assert report["data_root"]["layout"] == {
        "inspection_mode": "read_only",
        "initialized": False,
        "valid": None,
        "reason": "data_root_missing",
    }
    assert report["sqlite_runtime_version"]
    assert report["storage_policy"] == {
        "desired_journal_mode": "DELETE",
        "wal_enabled": False,
        "automatic_vacuum": False,
    }
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


def test_doctor_existing_unmarked_directory_is_a_read_only_warning(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["doctor", "--data-root", str(tmp_path), "--json"]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["status"] == "warning"
    assert report["data_root"]["exists"] is True
    assert report["data_root"]["is_directory"] is True
    assert report["issues"][0]["code"] == "data_root_not_initialized"


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


def test_storage_status_is_read_only_and_init_is_explicit_idempotent(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "data"

    assert main(["storage", "status", "--data-root", str(root), "--json"]) == 1
    missing = json.loads(capsys.readouterr().out)
    assert missing["reason"] == "data_root_missing"
    assert not root.exists()

    assert main(["storage", "init", "--data-root", str(root), "--json"]) == 0
    created = json.loads(capsys.readouterr().out)
    assert created["status"] == "ok"
    assert created["data_root"]["created"] is True
    assert created["database"]["created"] is True
    database = Path(created["database"]["path"])
    before_entries = sorted(path.name for path in root.rglob("*"))
    before_mtime = database.stat().st_mtime_ns

    assert main(["storage", "status", "--data-root", str(root), "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["read_only"] is True
    assert status["database"]["inspection_mode"] == "read_only"
    assert status["database"]["journal_mode"] == "DELETE"
    assert status["database"]["row_count"] == 0
    assert database.stat().st_mtime_ns == before_mtime
    assert sorted(path.name for path in root.rglob("*")) == before_entries

    assert main(["storage", "init", "--data-root", str(root), "--json"]) == 0
    repeated = json.loads(capsys.readouterr().out)
    assert repeated["data_root"]["created"] is False
    assert repeated["database"]["created"] is False


def test_storage_purge_requires_existing_store_and_is_bounded(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    missing = tmp_path / "missing"
    assert main(["storage", "purge-expired", "--data-root", str(missing), "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["reason"] == "storage_purge_failed"
    assert not missing.exists()

    root = tmp_path / "data"
    assert main(["storage", "init", "--data-root", str(root), "--json"]) == 0
    capsys.readouterr()
    layout = verify_data_root(root)
    database = layout.observations / "observations.sqlite3"
    observation = DeterministicProcessor().process(next(SyntheticSource(1)), session_id="cli")
    expired = replace(observation, ttl_ns=1)
    with SQLiteStore(database, volume_reserve_bytes=0, now_unix_ns=lambda: 1) as store:
        assert store.put(expired).accepted

    assert (
        main(
            [
                "storage",
                "purge-expired",
                "--data-root",
                str(root),
                "--limit",
                "1",
                "--json",
            ]
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report["rows_deleted"] == 1
    assert report["limit"] == 1
    assert report["database"]["row_count"] == 0


@pytest.mark.parametrize("limit", ["0", "513", "not-an-integer"])
def test_storage_purge_limit_is_bounded(limit: str) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["storage", "purge-expired", "--limit", limit, "--json"])
    assert exit_info.value.code == 2


def test_consent_lifecycle_is_explicit_bounded_and_status_is_read_only(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "data"

    assert main(["consent", "status", "--data-root", str(root), "--json"]) == 1
    missing = json.loads(capsys.readouterr().out)
    assert missing["reason"] == "consent_unavailable"
    assert not root.exists()

    layout = initialize_data_root(root)
    assert (
        main(
            [
                "consent",
                "grant",
                "--data-root",
                str(root),
                "--scope",
                "observation.read",
                "--scope",
                "snapshot.read",
                "--duration-seconds",
                "60",
                "--snapshot-quota",
                "2",
                "--sensitivity-ceiling",
                "operational",
                "--json",
            ]
        )
        == 0
    )
    granted = json.loads(capsys.readouterr().out)
    assert granted["status"] == "ok"
    assert granted["consent"]["scopes"] == ["observation.read", "snapshot.read"]
    assert granted["consent"]["snapshot_quota"] == 2
    assert granted["consent"]["snapshot_attempts"] == 0
    consent_path = layout.category("state") / "agent-consent.json"
    before = consent_path.read_bytes()
    before_mtime = consent_path.stat().st_mtime_ns
    assert b"token" not in before.lower()
    assert b"secret" not in before.lower()

    assert main(["consent", "status", "--data-root", str(root), "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["read_only"] is True
    assert status["active"] is True
    assert status["consent"] == granted["consent"]
    assert consent_path.read_bytes() == before
    assert consent_path.stat().st_mtime_ns == before_mtime

    assert main(["consent", "revoke", "--data-root", str(root), "--json"]) == 0
    revoked = json.loads(capsys.readouterr().out)
    assert revoked["revoked"] is True
    assert not consent_path.exists()
    assert main(["consent", "revoke", "--data-root", str(root), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["revoked"] is False


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--duration-seconds", "0"),
        ("--duration-seconds", "604801"),
        ("--duration-seconds", "invalid"),
        ("--snapshot-quota", "-1"),
        ("--snapshot-quota", "1025"),
        ("--snapshot-quota", "invalid"),
    ],
)
def test_consent_cli_bounds_duration_and_snapshot_quota(option: str, value: str) -> None:
    arguments = [
        "consent",
        "grant",
        "--scope",
        "observation.read",
        "--duration-seconds",
        "60",
        option,
        value,
        "--json",
    ]
    if option == "--duration-seconds":
        del arguments[5:7]

    with pytest.raises(SystemExit) as exit_info:
        main(arguments)

    assert exit_info.value.code == 2


def test_consent_status_fails_closed_for_corrupt_state(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    layout = initialize_data_root(tmp_path / "data")
    (layout.category("state") / "agent-consent.json").write_text("not-json", encoding="utf-8")

    assert main(["consent", "status", "--data-root", str(layout.root), "--json"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["reason"] == "consent_unavailable"
    assert "cannot be decoded" in report["message"]


def test_agent_serve_requires_initialized_root_without_touching_stdout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "missing"

    assert main(["agent", "serve", "--data-root", str(root)]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "cannot inspect layout path" in captured.err
    assert not root.exists()


def test_agent_serve_builds_tuned_service_and_does_not_open_camera(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from viskium.agent import mcp_server

    root = initialize_data_root(tmp_path / "data").root
    services: list[object] = []
    monkeypatch.setattr(mcp_server, "run_mcp_server", services.append)

    exit_code = main(
        [
            "agent",
            "serve",
            "--data-root",
            str(root),
            "--device-index",
            "2",
            "--width",
            "1280",
            "--height",
            "720",
            "--fps",
            "24",
            "--max-snapshot-bytes",
            str(8 * 1_024 * 1_024),
            "--max-snapshot-edge-px",
            "1920",
            "--max-wait-ms",
            "15000",
            "--max-wire-bytes",
            str(1 * 1_024 * 1_024),
            "--max-inflight-requests",
            "32",
        ]
    )

    assert exit_code == 0
    assert capsys.readouterr().out == ""
    assert len(services) == 1
    service = services[0]
    assert service.limits.max_snapshot_bytes == 8 * 1_024 * 1_024
    assert service.limits.max_snapshot_edge_px == 1_920
    assert service.limits.max_wait_ms == 15_000
    assert service.limits.max_wire_bytes == 1 * 1_024 * 1_024
    assert service.limits.max_inflight_requests == 32
    assert service.status().outcome == "ok"
    assert service.metrics.snapshot_requests == 0


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--device-index", "-1"),
        ("--width", "0"),
        ("--height", "8193"),
        ("--fps", "0"),
        ("--fps", "nan"),
        ("--max-snapshot-bytes", "65535"),
        ("--max-snapshot-edge-px", "1921"),
        ("--max-wait-ms", "15001"),
        ("--max-wire-bytes", "65535"),
        ("--max-inflight-requests", "33"),
    ],
)
def test_agent_serve_rejects_unusable_or_unbounded_options(
    option: str,
    value: str,
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["agent", "serve", option, value])

    assert exit_info.value.code == 2
