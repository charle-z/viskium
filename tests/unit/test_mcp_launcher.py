from __future__ import annotations

import ast
import importlib.util
import tomllib
from pathlib import Path
from typing import NoReturn

import pytest

_REPOSITORY_ROOT = Path(__file__).parents[2]
_LAUNCHER_PATH = _REPOSITORY_ROOT / "scripts" / "viskium_mcp_launcher.py"
_SPEC = importlib.util.spec_from_file_location("viskium_project_mcp_launcher", _LAUNCHER_PATH)
assert _SPEC is not None
assert _SPEC.loader is not None
launcher = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(launcher)


def test_launcher_syntax_remains_compatible_with_global_python() -> None:
    source = _LAUNCHER_PATH.read_text(encoding="utf-8")

    for feature_version in ((3, 8), (3, 9)):
        ast.parse(source, filename=str(_LAUNCHER_PATH), feature_version=feature_version)


def test_project_mcp_configuration_is_safe_and_points_to_launcher() -> None:
    config_path = _REPOSITORY_ROOT / ".codex" / "config.toml"
    document = tomllib.loads(config_path.read_text(encoding="utf-8"))
    server = document["mcp_servers"]["viskium"]

    assert server == {
        "command": "python",
        "args": ["scripts/viskium_mcp_launcher.py"],
        "enabled": False,
        "startup_timeout_sec": 20,
        "tool_timeout_sec": 20,
    }
    assert (_REPOSITORY_ROOT / server["args"][0]).resolve() == _LAUNCHER_PATH.resolve()


@pytest.mark.parametrize(
    ("platform", "relative_interpreter"),
    [
        ("nt", Path(".venv/Scripts/python.exe")),
        ("posix", Path(".venv/bin/python")),
    ],
)
def test_launcher_selects_platform_interpreter(
    tmp_path: Path,
    platform: str,
    relative_interpreter: Path,
) -> None:
    interpreter = tmp_path / relative_interpreter
    interpreter.parent.mkdir(parents=True)
    interpreter.touch()

    assert launcher._venv_interpreter(tmp_path, platform=platform) == interpreter


def test_launcher_selects_python3_when_posix_python_alias_is_absent(tmp_path: Path) -> None:
    interpreter = tmp_path / ".venv" / "bin" / "python3"
    interpreter.parent.mkdir(parents=True)
    interpreter.touch()

    assert launcher._venv_interpreter(tmp_path, platform="posix") == interpreter


def test_launcher_execs_exact_project_environment_and_data_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    relative = (
        Path(".venv/Scripts/python.exe") if launcher.os.name == "nt" else Path(".venv/bin/python")
    )
    interpreter = tmp_path / relative
    interpreter.parent.mkdir(parents=True)
    interpreter.touch()
    monkeypatch.chdir(tmp_path.parent)
    invocation: list[object] = []

    class Replaced(Exception):
        pass

    def replace(path: str, argv: list[str]) -> NoReturn:
        invocation.extend((path, argv))
        raise Replaced

    with pytest.raises(Replaced):
        launcher.main(repository_root=tmp_path, execv=replace, platform="posix")

    assert invocation == [
        str(interpreter),
        [
            str(interpreter),
            "-m",
            "viskium",
            "agent",
            "serve",
            "--data-root",
            str(tmp_path / ".viskium"),
        ],
    ]
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_launcher_missing_environment_fails_without_creating_or_using_stdout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    before = tuple(tmp_path.iterdir())

    assert launcher.main(repository_root=tmp_path) == 1

    assert tuple(tmp_path.iterdir()) == before
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "project environment missing" in captured.err
    assert str(tmp_path) not in captured.err


def test_windows_launcher_runs_child_with_inherited_protocol_process(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    interpreter = tmp_path / ".venv" / "Scripts" / "python.exe"
    interpreter.parent.mkdir(parents=True)
    interpreter.touch()
    invocation: list[list[str]] = []

    def run(argv: list[str]) -> int:
        invocation.append(argv)
        return 17

    assert launcher.main(repository_root=tmp_path, run_process=run, platform="nt") == 17

    assert invocation == [
        [
            str(interpreter),
            "-m",
            "viskium",
            "agent",
            "serve",
            "--data-root",
            str(tmp_path / ".viskium"),
        ]
    ]
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_launcher_sanitizes_exec_failure(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    relative = (
        Path(".venv/Scripts/python.exe") if launcher.os.name == "nt" else Path(".venv/bin/python")
    )
    interpreter = tmp_path / relative
    interpreter.parent.mkdir(parents=True)
    interpreter.touch()

    def fail(_path: str, _argv: list[str]) -> NoReturn:
        raise OSError("private host detail")

    assert launcher.main(repository_root=tmp_path, execv=fail, platform="posix") == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "could not be started" in captured.err
    assert "private host detail" not in captured.err
