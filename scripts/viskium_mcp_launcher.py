"""Start the project MCP server from its already-synchronized virtual environment."""

from __future__ import annotations

# The launcher is called by a host-global Python, so keep the typing imports
# compatible with Python 3.8 even though the project itself targets 3.13.
# ruff: noqa: UP006, UP035, UP045
import os
import subprocess
import sys
from pathlib import Path
from typing import Callable, List, NoReturn, Optional

ExecV = Callable[[str, List[str]], NoReturn]
RunProcess = Callable[[List[str]], int]


def _repository_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _venv_interpreter(repository_root: Path, *, platform: str = os.name) -> Optional[Path]:
    windows = repository_root / ".venv" / "Scripts" / "python.exe"
    posix = repository_root / ".venv" / "bin" / "python"
    posix3 = repository_root / ".venv" / "bin" / "python3"
    candidates = (windows, posix, posix3) if platform == "nt" else (posix, posix3, windows)
    return next((candidate for candidate in candidates if candidate.is_file()), None)


def _run_process(argv: List[str]) -> int:
    return subprocess.run(argv, check=False).returncode


def main(
    *,
    repository_root: Optional[Path] = None,
    execv: ExecV = os.execv,
    run_process: RunProcess = _run_process,
    platform: str = os.name,
) -> int:
    """Replace this process with Viskium while preserving MCP stdio."""

    root = _repository_root() if repository_root is None else repository_root.resolve()
    interpreter = _venv_interpreter(root, platform=platform)
    if interpreter is None:
        print(
            "viskium MCP launcher: project environment missing; run uv sync with agent and camera extras",
            file=sys.stderr,
        )
        return 1

    argv = [
        str(interpreter),
        "-m",
        "viskium",
        "agent",
        "serve",
        "--data-root",
        str(root / ".viskium"),
    ]
    try:
        if platform == "nt":
            return run_process(argv)
        execv(str(interpreter), argv)
    except OSError:
        print(
            "viskium MCP launcher: project environment could not be started",
            file=sys.stderr,
        )
        return 1
    print("viskium MCP launcher: process replacement returned unexpectedly", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
