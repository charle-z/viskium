from __future__ import annotations

import builtins
import os
import runpy
import sys
import time
from typing import Any

import pytest

import viskium._camera_worker_subprocess as worker
import viskium.adapters.opencv_process_camera as camera_module
from viskium._worker_transport import launch_socket_subprocess


class _Connection:
    def __init__(self, reply: object = ("close",)) -> None:
        self.reply = reply
        self.sent: list[tuple[object, ...]] = []
        self.closed = False

    def send(self, message: tuple[object, ...]) -> None:
        self.sent.append(message)

    def recv(self) -> object:
        if isinstance(self.reply, BaseException):
            raise self.reply
        return self.reply

    def close(self) -> None:
        self.closed = True


def _main(
    monkeypatch: pytest.MonkeyPatch,
    connection: _Connection,
    *,
    handle: str = "12",
) -> int:
    monkeypatch.setattr(worker, "Connection", lambda value: connection)
    return worker.main(["--pipe-handle", handle])


@pytest.mark.parametrize(
    "arguments", [[], ["--pipe-handle"], ["--other", "1"], ["--pipe-handle", "x"]]
)
def test_camera_worker_entrypoint_rejects_invalid_handles(
    arguments: list[str],
) -> None:
    assert worker.main(arguments) == 2


def test_camera_worker_module_guard_rejects_invalid_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, "argv", ["viskium._camera_worker_subprocess", "--bad"])
    monkeypatch.delitem(sys.modules, "viskium._camera_worker_subprocess")

    with pytest.raises(SystemExit) as raised:
        runpy.run_module("viskium._camera_worker_subprocess", run_name="__main__")

    assert raised.value.code == 2


def test_camera_worker_real_subprocess_rejects_before_backend_import() -> None:
    started_at = time.monotonic()
    deadline = started_at + 10.0
    connection, process = launch_socket_subprocess(
        "viskium._camera_worker_subprocess",
        windows=os.name == "nt",
        deadline=deadline,
        monotonic=time.monotonic,
    )

    try:
        remaining = max(0.0, deadline - time.monotonic())
        assert connection.poll(remaining)
        assert connection.recv() == ("worker_started",)

        # The real entrypoint validates this command before importing cv2 or
        # dispatching to _run_worker, so this path cannot touch a camera.
        connection.send(("request",))
        remaining = max(0.0, deadline - time.monotonic())
        assert connection.poll(remaining)
        assert connection.recv() == ("open_error", "invalid_worker_command")

        assert process.wait(timeout=max(0.0, deadline - time.monotonic())) == 0
        assert process.poll() == 0
    finally:
        connection.close()
        if process.is_alive():
            process.terminate()
            process.wait(timeout=1.0)
            if process.is_alive():
                process.kill()
                process.wait(timeout=1.0)


def test_camera_worker_entrypoint_rejects_connection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_connection(handle: int) -> Any:
        raise OSError("private handle detail")

    monkeypatch.setattr(worker, "Connection", fail_connection)
    assert worker.main(["--pipe-handle", "12"]) == 2


def test_camera_worker_entrypoint_acknowledges_then_exits_on_eof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(EOFError())

    assert _main(monkeypatch, connection) == 0
    assert connection.sent == [("worker_started",)]
    assert connection.closed


def test_camera_worker_entrypoint_rejects_malformed_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(("request",))

    assert _main(monkeypatch, connection) == 0
    assert connection.sent == [("worker_started",), ("open_error", "invalid_worker_command")]
    assert connection.closed


def test_camera_worker_entrypoint_sanitizes_cv2_import_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(("request", object()))
    real_import = builtins.__import__

    def fail_cv2(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "cv2":
            raise ImportError("private import detail")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fail_cv2)
    assert _main(monkeypatch, connection) == 0
    assert connection.sent == [("worker_started",), ("open_error", "opencv_unavailable")]
    assert connection.closed


def test_camera_worker_entrypoint_sanitizes_unexpected_import_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(("request", object()))
    real_import = builtins.__import__

    def fail_cv2(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "cv2":
            raise RuntimeError("private import detail")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fail_cv2)
    assert _main(monkeypatch, connection) == 0
    assert connection.sent == [("worker_started",), ("open_error", "opencv_worker_error")]
    assert connection.closed


def test_camera_worker_entrypoint_sanitizes_unexpected_handle_conversion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenInt:
        def __new__(cls, value: object, base: int = 10) -> int:
            del value, base
            raise ValueError("private conversion detail")

    monkeypatch.setattr(worker, "int", BrokenInt, raising=False)
    assert worker.main(["--pipe-handle", "12"]) == 2


def test_camera_worker_entrypoint_sanitizes_dispatch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(("request", object()))

    def fail_worker(connection: object, request: object, cv2: object) -> None:
        raise RuntimeError("private dispatch detail")

    monkeypatch.setitem(sys.modules, "cv2", object())
    monkeypatch.setattr(camera_module, "_run_worker", fail_worker)
    assert _main(monkeypatch, connection) == 0
    assert connection.sent == [("worker_started",), ("open_error", "opencv_worker_error")]
    assert connection.closed


def test_camera_worker_entrypoint_swallows_send_and_close_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenConnection(_Connection):
        def send(self, message: tuple[object, ...]) -> None:
            raise BrokenPipeError("private send detail")

        def close(self) -> None:
            raise OSError("private close detail")

    connection = BrokenConnection(EOFError())
    assert _main(monkeypatch, connection) == 0
