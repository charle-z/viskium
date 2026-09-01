"""Small, bounded subprocess transport for the isolated camera worker.

The Windows path uses a socket pair and an explicit ``handle_list`` so the
worker receives exactly one communication handle.  POSIX callers can opt into
the same transport with ``pass_fds``.  This module imports only the standard
library and never touches a camera or optional dependency.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from collections.abc import Callable, Sequence
from contextlib import suppress
from multiprocessing.connection import Connection
from threading import Lock
from typing import Any

_WINDOWS_LAUNCH_LOCK = Lock()


class SubprocessProcess:
    """Process-shaped facade for a :class:`subprocess.Popen` child."""

    daemon = True

    def __init__(self, process: subprocess.Popen[bytes]) -> None:
        self._process = process

    @property
    def exitcode(self) -> int | None:
        return self._process.poll()

    def poll(self) -> int | None:
        """Expose the bounded exit code used by worker cleanup."""

        return self._process.poll()

    def start(self) -> None:
        raise RuntimeError("subprocess worker is already started")

    def is_alive(self) -> bool:
        return self._process.poll() is None

    def join(self, timeout: float | None = None) -> None:
        with suppress(subprocess.TimeoutExpired, OSError, ValueError):
            self._process.wait(timeout=timeout)

    def wait(self, timeout: float | None = None) -> int | None:
        with suppress(subprocess.TimeoutExpired, OSError, ValueError):
            return self._process.wait(timeout=timeout)
        return self._process.poll()

    def terminate(self) -> None:
        self._process.terminate()

    def kill(self) -> None:
        self._process.kill()


class SocketSubprocessLaunchError(RuntimeError):
    """Internal launch error retaining only cleanup handles."""

    def __init__(
        self,
        *,
        connection: Connection | None,
        process: SubprocessProcess | None,
        reason: str = "camera_worker_start_failed",
    ) -> None:
        super().__init__(reason)
        self.connection = connection
        self.process = process
        self.reason = reason


def close_socket_handle(handle: int | None) -> None:
    """Close one detached socket handle without exposing its value."""

    if handle is None:
        return
    with suppress(OSError, ValueError):
        socket.socket(fileno=handle).close()


def _deadline_expired(
    deadline: float | int | None,
    monotonic: Callable[[], float | int] | None,
) -> bool:
    if deadline is None or monotonic is None:
        return False
    return monotonic() >= deadline


def _cleanup_subprocess(process: SubprocessProcess) -> bool:
    """Terminate and reap a child without waiting beyond the current deadline."""

    if not process.is_alive():
        return True
    with suppress(OSError, ValueError):
        process.terminate()
    process.wait(timeout=0.0)
    if not process.is_alive():
        return True
    with suppress(OSError, ValueError):
        process.kill()
    process.wait(timeout=0.0)
    return not process.is_alive()


def launch_socket_subprocess(
    module: str,
    *,
    extra_args: Sequence[str] = (),
    windows: bool | None = None,
    deadline: float | int | None = None,
    monotonic: Callable[[], float | int] | None = None,
) -> tuple[Connection, SubprocessProcess]:
    """Launch ``python -m module --pipe-handle HANDLE`` over a socket pair.

    On Windows, the child handle is temporarily inheritable and is passed via
    ``STARTUPINFO.lpAttributeList['handle_list']`` while ``close_fds`` stays
    enabled.  The inheritable bit is revoked under the same lock even when
    ``Popen`` fails.  On POSIX, ``pass_fds`` provides the equivalent narrow
    inheritance boundary.
    """

    selected_windows = os.name == "nt" if windows is None else windows
    parent_socket: socket.socket | None = None
    child_socket: socket.socket | None = None
    parent_connection: Connection | None = None
    child_handle: int | None = None
    process: SubprocessProcess | None = None
    inheritance_attempted = False
    launch_error: BaseException | None = None
    revoke_error: BaseException | None = None

    try:
        parent_socket, child_socket = socket.socketpair()
        parent_handle = parent_socket.detach()
        parent_socket = None
        try:
            parent_connection = Connection(parent_handle)
        except Exception:
            close_socket_handle(parent_handle)
            raise
        child_handle = child_socket.detach()
        child_socket = None

        command = [
            sys.executable,
            "-m",
            module,
            "--pipe-handle",
            str(child_handle),
            *extra_args,
        ]
        kwargs: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
            "shell": False,
            "close_fds": True,
        }
        if selected_windows:
            with _WINDOWS_LAUNCH_LOCK:
                inheritance_attempted = True
                try:
                    os.set_handle_inheritable(child_handle, True)
                    if _deadline_expired(deadline, monotonic):
                        launch_error = RuntimeError("worker launch deadline expired")
                    else:
                        startupinfo = subprocess.STARTUPINFO()
                        startupinfo.lpAttributeList = {"handle_list": [child_handle]}
                        kwargs["startupinfo"] = startupinfo
                        creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
                        if creationflags:
                            kwargs["creationflags"] = creationflags
                        process = SubprocessProcess(subprocess.Popen(command, **kwargs))
                        if _deadline_expired(deadline, monotonic):
                            launch_error = RuntimeError("worker launch deadline expired")
                except Exception as error:
                    launch_error = error
                finally:
                    if inheritance_attempted:
                        try:
                            os.set_handle_inheritable(child_handle, False)
                        except Exception as error:
                            revoke_error = error
            if revoke_error is not None:
                if process is not None:
                    _cleanup_subprocess(process)
                if parent_connection is not None:
                    with suppress(OSError, ValueError):
                        parent_connection.close()
                raise SocketSubprocessLaunchError(
                    connection=None,
                    process=process if process is not None and process.is_alive() else None,
                    reason="camera_worker_start_failed",
                ) from revoke_error
            if launch_error is not None:
                if process is not None:
                    _cleanup_subprocess(process)
                if parent_connection is not None:
                    with suppress(OSError, ValueError):
                        parent_connection.close()
                reason = (
                    "camera_worker_start_timeout"
                    if _deadline_expired(deadline, monotonic)
                    else "camera_worker_start_failed"
                )
                raise SocketSubprocessLaunchError(
                    connection=None,
                    process=process if process is not None and process.is_alive() else None,
                    reason=reason,
                ) from launch_error
        else:
            if _deadline_expired(deadline, monotonic):
                if parent_connection is not None:
                    with suppress(OSError, ValueError):
                        parent_connection.close()
                raise SocketSubprocessLaunchError(
                    connection=None,
                    process=None,
                    reason="camera_worker_start_timeout",
                )
            kwargs["pass_fds"] = (child_handle,)
            process = SubprocessProcess(subprocess.Popen(command, **kwargs))
            if _deadline_expired(deadline, monotonic):
                _cleanup_subprocess(process)
                if parent_connection is not None:
                    with suppress(OSError, ValueError):
                        parent_connection.close()
                raise SocketSubprocessLaunchError(
                    connection=None,
                    process=process if process.is_alive() else None,
                    reason="camera_worker_start_timeout",
                )

        close_socket_handle(child_handle)
        child_handle = None
        if process is None or parent_connection is None:
            raise RuntimeError("worker subprocess launch returned no transport")
        return parent_connection, process
    except SocketSubprocessLaunchError:
        raise
    except Exception as error:
        if parent_connection is not None:
            with suppress(OSError, ValueError):
                parent_connection.close()
        raise SocketSubprocessLaunchError(
            connection=None,
            process=process if process is not None and process.is_alive() else None,
            reason="camera_worker_start_failed",
        ) from error
    finally:
        if child_socket is not None:
            with suppress(OSError):
                child_socket.close()
        if child_handle is not None:
            close_socket_handle(child_handle)
        if parent_socket is not None:
            with suppress(OSError):
                parent_socket.close()


__all__ = [
    "SocketSubprocessLaunchError",
    "SubprocessProcess",
    "close_socket_handle",
    "launch_socket_subprocess",
]
