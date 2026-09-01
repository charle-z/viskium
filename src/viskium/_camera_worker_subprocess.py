"""Windows camera worker entrypoint over one inherited socket handle.

Only standard-library modules are imported before the initial lifecycle ACK.
The parent sends the request after that ACK, which keeps process bootstrap,
backend selection, device open, and configuration as distinct bounded phases.
"""

from __future__ import annotations

import sys
from contextlib import suppress
from multiprocessing.connection import Connection


def _send(connection: Connection, message: tuple[object, ...]) -> None:
    with suppress(BrokenPipeError, EOFError, OSError, ValueError):
        connection.send(message)


def _close(connection: Connection) -> None:
    with suppress(OSError, ValueError):
        connection.close()


def _pipe_handle(arguments: list[str]) -> int | None:
    if len(arguments) != 2 or arguments[0] != "--pipe-handle":
        return None
    value = arguments[1]
    if not value or not value.isdecimal():
        return None
    try:
        handle = int(value, 10)
    except ValueError:
        return None
    return handle if handle >= 0 else None


def main(arguments: list[str] | None = None) -> int:
    """Run one request-driven worker and return a content-free exit status."""

    handle = _pipe_handle(sys.argv[1:] if arguments is None else arguments)
    if handle is None:
        return 2
    try:
        connection = Connection(handle)
    except (OSError, TypeError, ValueError):
        return 2

    try:
        # This is the first application-level action.  In particular, cv2 and
        # the adapter package are not imported until the parent sends a request.
        _send(connection, ("worker_started",))
        try:
            command = connection.recv()
        except (EOFError, OSError, ValueError):
            return 0
        if type(command) is not tuple or len(command) != 2 or command[0] != "request":
            _send(connection, ("open_error", "invalid_worker_command"))
            return 0
        try:
            import cv2

            from viskium.adapters.opencv_process_camera import _run_worker
        except ImportError:
            _send(connection, ("open_error", "opencv_unavailable"))
            return 0
        except Exception:
            _send(connection, ("open_error", "opencv_worker_error"))
            return 0
        try:
            _run_worker(connection, command[1], cv2)
        except Exception:
            _send(connection, ("open_error", "opencv_worker_error"))
        return 0
    finally:
        _close(connection)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
