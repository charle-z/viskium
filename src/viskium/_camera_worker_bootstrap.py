"""Minimal spawn bootstrap for the isolated camera worker.

This module intentionally imports only the standard library.  On platforms
that use ``multiprocessing`` spawn, the child imports this module before it
can execute its target.  Sending the lifecycle ACK here keeps that bootstrap
boundary independent from OpenCV and the rest of the adapter package.
"""

from __future__ import annotations

from contextlib import suppress
from typing import Any


def _close(connection: Any) -> None:
    with suppress(Exception):
        connection.close()


def _send(connection: Any, message: tuple[object, ...]) -> None:
    with suppress(Exception):
        connection.send(message)


def _camera_worker_bootstrap(connection: Any, request: Any) -> None:
    """ACK immediately, then load and run the hardware-bound worker."""

    _send(connection, ("worker_started",))
    try:
        # Keep these imports after the ACK.  Importing the adapter package can
        # load OpenCV and other optional dependencies on the child side.
        import cv2

        from viskium.adapters.opencv_process_camera import _run_worker
    except (ImportError, OSError):
        _send(connection, ("open_error", "opencv_unavailable"))
        _close(connection)
        return
    except Exception:
        _send(connection, ("open_error", "opencv_worker_error"))
        _close(connection)
        return

    try:
        _run_worker(connection, request, cv2)
    except Exception:
        # The heavy worker already sanitizes its own failures.  This catches
        # only an unexpected import/dispatch boundary failure and never sends
        # exception text or process identifiers over the protocol.
        _send(connection, ("open_error", "opencv_worker_error"))
        _close(connection)


__all__ = ["_camera_worker_bootstrap"]
