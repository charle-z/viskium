from __future__ import annotations

import subprocess
import sys
from typing import Any

import pytest

import viskium._worker_transport as transport
from viskium._worker_transport import SocketSubprocessLaunchError


class _FakeSocket:
    def __init__(self, handle: int) -> None:
        self.handle = handle
        self.closed = False

    def detach(self) -> int:
        return self.handle

    def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, handle: int) -> None:
        self.handle = handle
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakePopen:
    def __init__(self) -> None:
        self.returncode: int | None = None
        self.terminate_calls = 0
        self.kill_calls = 0

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return 0 if self.returncode is None else self.returncode

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.returncode = -15

    def kill(self) -> None:
        self.kill_calls += 1
        self.returncode = -9


class _StubbornPopen(_FakePopen):
    def terminate(self) -> None:
        self.terminate_calls += 1

    def kill(self) -> None:
        self.kill_calls += 1
        self.returncode = -9


class _UnkillablePopen(_StubbornPopen):
    def kill(self) -> None:
        self.kill_calls += 1


def _fake_socketpair() -> tuple[_FakeSocket, _FakeSocket]:
    return _FakeSocket(11), _FakeSocket(22)


def _patch_fake_transport(
    monkeypatch: pytest.MonkeyPatch,
    *,
    popen: Any,
    inheritable: list[tuple[int, bool]],
) -> tuple[_FakeConnection, _FakeSocket, _FakeSocket, list[tuple[list[str], dict[str, Any]]]]:
    parent_socket, child_socket = _fake_socketpair()
    connection = _FakeConnection(11)
    calls: list[tuple[list[str], dict[str, Any]]] = []

    monkeypatch.setattr(transport.socket, "socketpair", _fake_socketpair)
    monkeypatch.setattr(transport, "Connection", lambda handle: connection)
    monkeypatch.setattr(transport, "close_socket_handle", lambda handle: None)

    class _StartupInfo:
        lpAttributeList: dict[str, object] | None = None

    monkeypatch.setattr(transport.subprocess, "STARTUPINFO", _StartupInfo, raising=False)
    monkeypatch.setattr(
        transport.os,
        "set_handle_inheritable",
        lambda handle, enabled: inheritable.append((handle, enabled)),
        raising=False,
    )

    def fake_popen(command: list[str], **kwargs: Any) -> Any:
        calls.append((command, kwargs))
        return popen()

    monkeypatch.setattr(transport.subprocess, "Popen", fake_popen)
    return connection, parent_socket, child_socket, calls


def test_windows_socket_transport_uses_only_explicit_handle_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inheritable: list[tuple[int, bool]] = []
    process = _FakePopen()
    connection, parent_socket, child_socket, calls = _patch_fake_transport(
        monkeypatch,
        popen=lambda: process,
        inheritable=inheritable,
    )

    result_connection, result_process = transport.launch_socket_subprocess(
        "viskium._camera_worker_subprocess",
        windows=True,
    )

    assert result_connection is connection
    assert result_process.is_alive()
    command, kwargs = calls[0]
    assert command[:3] == [sys.executable, "-m", "viskium._camera_worker_subprocess"]
    assert command[3:] == ["--pipe-handle", "22"]
    assert kwargs["stdin"] is subprocess.DEVNULL
    assert kwargs["stdout"] is subprocess.DEVNULL
    assert kwargs["stderr"] is subprocess.DEVNULL
    assert kwargs["shell"] is False
    assert kwargs["close_fds"] is True
    startupinfo = kwargs["startupinfo"]
    assert startupinfo.lpAttributeList == {"handle_list": [22]}
    assert inheritable == [(22, True), (22, False)]
    assert not parent_socket.closed
    assert not child_socket.closed
    assert kwargs.get("creationflags") == getattr(subprocess, "CREATE_NO_WINDOW", 0)


def test_subprocess_process_wrapper_exposes_bounded_process_lifecycle() -> None:
    process = _FakePopen()
    wrapper = transport.SubprocessProcess(process)  # type: ignore[arg-type]

    assert wrapper.exitcode is None
    assert wrapper.poll() is None
    assert wrapper.is_alive()
    with pytest.raises(RuntimeError, match="already started"):
        wrapper.start()
    wrapper.terminate()
    wrapper.join(timeout=1)
    assert wrapper.exitcode == -15
    assert not wrapper.is_alive()
    assert wrapper.wait(timeout=1) == -15
    wrapper.kill()
    assert process.kill_calls == 1


def test_subprocess_process_wrapper_handles_wait_timeout_and_transport_close_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TimeoutPopen(_FakePopen):
        def wait(self, timeout: float | None = None) -> int:
            raise subprocess.TimeoutExpired("worker", timeout)

    process = TimeoutPopen()
    wrapper = transport.SubprocessProcess(process)  # type: ignore[arg-type]
    wrapper.join(timeout=0.0)
    assert wrapper.wait(timeout=0.0) is None

    class BrokenSocket:
        def __init__(self, *, fileno: int) -> None:
            del fileno

        def close(self) -> None:
            raise OSError("private close detail")

    monkeypatch.setattr(transport.socket, "socket", BrokenSocket)
    transport.close_socket_handle(None)
    transport.close_socket_handle(22)


def test_cleanup_returns_success_for_already_exited_process() -> None:
    process = _FakePopen()
    process.returncode = 0
    wrapper = transport.SubprocessProcess(process)  # type: ignore[arg-type]

    assert transport._cleanup_subprocess(wrapper)
    assert process.terminate_calls == 0


def test_connection_construction_failure_closes_detached_parent_and_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_socket, child_socket = _fake_socketpair()
    closed_handles: list[int] = []

    monkeypatch.setattr(transport.socket, "socketpair", lambda: (parent_socket, child_socket))
    monkeypatch.setattr(transport, "Connection", lambda handle: _raise_oserror())
    monkeypatch.setattr(
        transport,
        "close_socket_handle",
        lambda handle: closed_handles.append(handle) if handle is not None else None,
    )

    with pytest.raises(SocketSubprocessLaunchError):
        transport.launch_socket_subprocess("viskium._camera_worker_subprocess", windows=True)

    assert closed_handles == [11]
    assert child_socket.closed


def test_windows_deadline_is_checked_before_popen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inheritable: list[tuple[int, bool]] = []
    connection, _, _, calls = _patch_fake_transport(
        monkeypatch,
        popen=lambda: _FakePopen(),
        inheritable=inheritable,
    )

    with pytest.raises(SocketSubprocessLaunchError) as caught:
        transport.launch_socket_subprocess(
            "viskium._camera_worker_subprocess",
            windows=True,
            deadline=10,
            monotonic=lambda: 10,
        )

    assert caught.value.reason == "camera_worker_start_timeout"
    assert calls == []
    assert inheritable == [(22, True), (22, False)]
    assert connection.closed


def test_windows_launch_without_creation_flag_is_still_narrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inheritable: list[tuple[int, bool]] = []
    process = _FakePopen()
    _, _, _, calls = _patch_fake_transport(
        monkeypatch,
        popen=lambda: process,
        inheritable=inheritable,
    )
    monkeypatch.setattr(transport.subprocess, "CREATE_NO_WINDOW", 0, raising=False)

    _, _ = transport.launch_socket_subprocess(
        "viskium._camera_worker_subprocess",
        windows=True,
    )

    assert "creationflags" not in calls[0][1]


def test_posix_deadline_is_checked_before_and_after_popen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pre_connection, _, _, pre_calls = _patch_fake_transport(
        monkeypatch,
        popen=lambda: _FakePopen(),
        inheritable=[],
    )
    with pytest.raises(SocketSubprocessLaunchError) as pre_error:
        transport.launch_socket_subprocess(
            "viskium._camera_worker_subprocess",
            windows=False,
            deadline=10,
            monotonic=lambda: 10,
        )
    assert pre_error.value.reason == "camera_worker_start_timeout"
    assert pre_calls == []
    assert pre_connection.closed

    post_connection, _, _, _ = _patch_fake_transport(
        monkeypatch,
        popen=lambda: _FakePopen(),
        inheritable=[],
    )
    clock = iter([0, 10])
    with pytest.raises(SocketSubprocessLaunchError) as post_error:
        transport.launch_socket_subprocess(
            "viskium._camera_worker_subprocess",
            windows=False,
            deadline=10,
            monotonic=lambda: next(clock),
        )
    assert post_error.value.reason == "camera_worker_start_timeout"
    assert post_connection.closed


def test_windows_popen_failure_revokes_inheritance_and_retains_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inheritable: list[tuple[int, bool]] = []

    def fail_popen() -> Any:
        raise OSError("private launch detail")

    connection, _, _, _ = _patch_fake_transport(
        monkeypatch,
        popen=fail_popen,
        inheritable=inheritable,
    )

    with pytest.raises(SocketSubprocessLaunchError) as caught:
        transport.launch_socket_subprocess("viskium._camera_worker_subprocess", windows=True)

    assert caught.value.connection is None
    assert caught.value.process is None
    assert inheritable == [(22, True), (22, False)]
    assert connection.closed


def test_windows_revoke_failure_retains_child_for_bounded_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakePopen()
    calls: list[tuple[int, bool]] = []
    connection, _, _, _ = _patch_fake_transport(
        monkeypatch,
        popen=lambda: process,
        inheritable=calls,
    )

    def fail_revoke(handle: int, inheritable: bool) -> None:
        calls.append((handle, inheritable))
        if not inheritable:
            raise OSError("private revoke detail")

    monkeypatch.setattr(transport.os, "set_handle_inheritable", fail_revoke, raising=False)

    with pytest.raises(SocketSubprocessLaunchError) as caught:
        transport.launch_socket_subprocess("viskium._camera_worker_subprocess", windows=True)

    assert caught.value.connection is None
    assert caught.value.process is None
    assert process.terminate_calls == 1
    assert process.poll() is not None
    assert connection.closed


def test_enable_failure_still_attempts_inheritance_revocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, bool]] = []
    connection, _, _, _ = _patch_fake_transport(
        monkeypatch,
        popen=lambda: _FakePopen(),
        inheritable=calls,
    )

    def fail_enable(handle: int, enabled: bool) -> None:
        calls.append((handle, enabled))
        if enabled:
            raise OSError("private inherit detail")

    monkeypatch.setattr(transport.os, "set_handle_inheritable", fail_enable, raising=False)

    with pytest.raises(SocketSubprocessLaunchError) as caught:
        transport.launch_socket_subprocess("viskium._camera_worker_subprocess", windows=True)

    assert caught.value.reason == "camera_worker_start_failed"
    assert caught.value.connection is None
    assert caught.value.process is None
    assert calls == [(22, True), (22, False)]
    assert connection.closed


def test_windows_popen_return_after_deadline_is_cleaned_and_not_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0]
    process = _FakePopen()
    inheritable: list[tuple[int, bool]] = []
    connection, _, _, _ = _patch_fake_transport(
        monkeypatch,
        popen=lambda: process,
        inheritable=inheritable,
    )

    def fake_popen() -> _FakePopen:
        clock[0] = 11
        return process

    monkeypatch.setattr(transport.subprocess, "Popen", lambda command, **kwargs: fake_popen())

    with pytest.raises(SocketSubprocessLaunchError) as caught:
        transport.launch_socket_subprocess(
            "viskium._camera_worker_subprocess",
            windows=True,
            deadline=10,
            monotonic=lambda: clock[0],
        )

    assert caught.value.reason == "camera_worker_start_timeout"
    assert caught.value.connection is None
    assert caught.value.process is None
    assert process.terminate_calls == 1
    assert process.poll() == -15
    assert inheritable == [(22, True), (22, False)]
    assert connection.closed


def test_cleanup_escalates_from_terminate_to_kill_and_reports_stuck() -> None:
    process = _UnkillablePopen()
    wrapper = transport.SubprocessProcess(process)  # type: ignore[arg-type]

    assert transport._cleanup_subprocess(wrapper) is False
    assert process.terminate_calls == 1
    assert process.kill_calls == 1
    assert process.poll() is None


def test_posix_socket_transport_requests_narrow_pass_fds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakePopen()
    connection, _, _, calls = _patch_fake_transport(
        monkeypatch,
        popen=lambda: process,
        inheritable=[],
    )

    result_connection, result_process = transport.launch_socket_subprocess(
        "viskium._camera_worker_subprocess",
        windows=False,
    )

    assert result_connection is connection
    assert result_process.is_alive()
    command, kwargs = calls[0]
    assert command[-2:] == ["--pipe-handle", "22"]
    assert kwargs["close_fds"] is True
    assert kwargs["pass_fds"] == (22,)
    assert "startupinfo" not in kwargs


def test_parent_socket_cleanup_is_fail_closed_when_detach_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenSocket(_FakeSocket):
        def detach(self) -> int:
            raise OSError("private detach detail")

    parent_socket = BrokenSocket(11)
    child_socket = _FakeSocket(22)
    monkeypatch.setattr(transport.socket, "socketpair", lambda: (parent_socket, child_socket))

    with pytest.raises(SocketSubprocessLaunchError):
        transport.launch_socket_subprocess("viskium._camera_worker_subprocess", windows=False)

    assert parent_socket.closed
    assert child_socket.closed


def test_launch_failure_after_popen_closes_connection_and_reports_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _FakePopen()
    connection, _, _, _ = _patch_fake_transport(
        monkeypatch,
        popen=lambda: process,
        inheritable=[],
    )
    calls = [0]

    def fail_first_close(handle: int | None) -> None:
        calls[0] += 1
        if calls[0] == 1:
            raise RuntimeError("private close detail")

    monkeypatch.setattr(transport, "close_socket_handle", fail_first_close)
    with pytest.raises(SocketSubprocessLaunchError) as caught:
        transport.launch_socket_subprocess("viskium._camera_worker_subprocess", windows=False)

    assert caught.value.reason == "camera_worker_start_failed"
    assert connection.closed


def _raise_oserror() -> Any:
    raise OSError("private connection detail")
