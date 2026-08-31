"""Deadline-enforced OpenCV camera backend isolated in a child process."""

from __future__ import annotations

import multiprocessing
import time
from collections.abc import Callable
from contextlib import suppress
from enum import StrEnum
from multiprocessing.connection import Connection
from threading import get_ident
from typing import Any, Protocol, runtime_checkable

from viskium.capture import (
    BackendFrame,
    CaptureCapabilities,
    CaptureDeadlineExceeded,
    CaptureOpenError,
    CaptureOwnershipError,
    CaptureRead,
    CaptureRequest,
    CaptureStateError,
    DeadlineCapability,
    NegotiatedStream,
    ReadStatus,
)

_MAX_REASON_CHARS = 128
_DEFAULT_CLEANUP_SECONDS = 0.25
_MAX_INT64 = 2**63 - 1


class OpenCVWorkerState(StrEnum):
    """Content-free lifecycle state for the isolated camera worker."""

    ABSENT = "absent"
    RUNNING = "running"
    EXITED = "exited"
    UNKNOWN = "unknown"
    STUCK = "stuck"


@runtime_checkable
class _ProcessPort(Protocol):
    daemon: bool

    def start(self) -> None: ...

    def is_alive(self) -> bool: ...

    def join(self, timeout: float | None = None) -> None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


@runtime_checkable
class _ConnectionPort(Protocol):
    def poll(self, timeout: float = 0.0) -> bool: ...

    def recv(self) -> object: ...

    def send(self, obj: object) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class _ProcessContextPort(Protocol):
    def Pipe(self, duplex: bool = True) -> tuple[_ConnectionPort, _ConnectionPort]: ...

    def Process(self, **kwargs: object) -> _ProcessPort: ...


def _reason(value: object, fallback: str) -> str:
    if not isinstance(value, str) or not value or not value.strip():
        return fallback
    return value[:_MAX_REASON_CHARS]


def _remaining_seconds(deadline_ns: int, monotonic_ns: Callable[[], int]) -> float:
    checked_deadline = _bounded_ns(deadline_ns, "deadline_monotonic_ns")
    now_ns = _bounded_ns(monotonic_ns(), "monotonic_ns result")
    remaining_ns = checked_deadline - now_ns
    if remaining_ns <= 0:
        return 0.0
    return remaining_ns / 1_000_000_000


def _bounded_ns(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not 0 <= value <= _MAX_INT64:
        raise ValueError(f"{field_name} must be between zero and signed int64 max")
    return value


def _send_worker_message(connection: _ConnectionPort, message: tuple[object, ...]) -> None:
    with suppress(BrokenPipeError, EOFError, OSError):
        connection.send(message)


def _opencv_worker(connection: Connection, request: CaptureRequest) -> None:  # pragma: no cover
    """Import OpenCV only inside the isolated worker process."""

    try:
        import cv2
    except (ImportError, OSError):
        _send_worker_message(connection, ("open_error", "opencv_unavailable"))
        connection.close()
        return
    _run_worker(connection, request, cv2)


def _run_worker(connection: _ConnectionPort, request: CaptureRequest, cv2: Any) -> None:
    """Run the small request/response worker; injectable for contract tests."""

    capture: Any = None
    try:
        capture = cv2.VideoCapture(request.device_index)
        if capture is None or not bool(capture.isOpened()):
            _send_worker_message(connection, ("open_error", "device_open_failed"))
            return
        capture.set(cv2.CAP_PROP_FRAME_WIDTH, float(request.requested_width))
        capture.set(cv2.CAP_PROP_FRAME_HEIGHT, float(request.requested_height))
        capture.set(cv2.CAP_PROP_FPS, request.requested_fps)

        width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH)) or request.requested_width
        height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT)) or request.requested_height
        fps_value = float(capture.get(cv2.CAP_PROP_FPS))
        fps = fps_value if 0.0 < fps_value <= 240.0 else None
        if width <= 0 or height <= 0 or width * height * 3 > request.max_frame_bytes:
            _send_worker_message(connection, ("open_error", "negotiated_mode_exceeds_limit"))
            return
        _send_worker_message(connection, ("opened", width, height, fps, width * 3))

        while True:
            try:
                command = connection.recv()
            except (EOFError, OSError):
                return
            if command == ("close",):
                return
            if command != ("read",):
                _send_worker_message(connection, ("fatal", "invalid_worker_command"))
                return
            try:
                ok, frame = capture.read()
            except Exception:
                _send_worker_message(connection, ("recoverable", "capture_read_error"))
                continue
            if not ok or frame is None:
                _send_worker_message(connection, ("disconnected", "capture_read_failed"))
                continue
            try:
                shape = tuple(frame.shape)
                if len(shape) != 3 or int(shape[2]) != 3 or str(frame.dtype) != "uint8":
                    raise ValueError("unexpected frame type")
                height = int(shape[0])
                width = int(shape[1])
                payload = bytes(frame.tobytes(order="C"))
                stride = width * 3
                if (
                    width <= 0
                    or height <= 0
                    or len(payload) != stride * height
                    or len(payload) > request.max_frame_bytes
                ):
                    raise ValueError("frame exceeds negotiated bounds")
            except (AttributeError, MemoryError, TypeError, ValueError):
                _send_worker_message(connection, ("fatal", "invalid_backend_frame"))
                return
            _send_worker_message(
                connection,
                ("frame", payload, width, height, stride, "bgr24", time.monotonic_ns()),
            )
    except Exception:
        _send_worker_message(connection, ("open_error", "opencv_worker_error"))
    finally:
        if capture is not None:
            with suppress(Exception):
                capture.release()
        with suppress(OSError):
            connection.close()


class OpenCVProcessCameraBackend:
    """Use one child process so open/read deadlines are actually enforceable.

    Exactly one tiny command and one bounded response may be in flight.  Raw
    frames are transferred in memory and never written.  On a deadline the
    entire worker is terminated before this backend can be reopened.
    """

    def __init__(
        self,
        *,
        context: _ProcessContextPort | None = None,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        cleanup_timeout_seconds: float = _DEFAULT_CLEANUP_SECONDS,
    ) -> None:
        selected_context: _ProcessContextPort = (
            multiprocessing.get_context("spawn") if context is None else context
        )
        if not isinstance(selected_context, _ProcessContextPort):
            raise TypeError("context must provide Pipe and Process")
        if not callable(monotonic_ns):
            raise TypeError("monotonic_ns must be callable")
        if (
            isinstance(cleanup_timeout_seconds, bool)
            or not isinstance(cleanup_timeout_seconds, (int, float))
            or not 0.01 <= float(cleanup_timeout_seconds) <= 2.0
        ):
            raise ValueError("cleanup_timeout_seconds must be between 0.01 and 2")
        self._context = selected_context
        self._monotonic_ns = monotonic_ns
        self._cleanup_timeout_seconds = float(cleanup_timeout_seconds)
        self._owner_thread_id: int | None = None
        self._process: _ProcessPort | None = None
        self._connection: _ConnectionPort | None = None
        self._request: CaptureRequest | None = None
        self._stream: NegotiatedStream | None = None
        self._termination_failed = False

    @property
    def capabilities(self) -> CaptureCapabilities:
        return CaptureCapabilities(
            open_deadline=DeadlineCapability.ENFORCED,
            read_deadline=DeadlineCapability.ENFORCED,
            cooperative_close=True,
        )

    @property
    def is_open(self) -> bool:
        return self._process is not None and self._connection is not None

    def _claim_or_check_owner(self) -> None:
        current = get_ident()
        if self._owner_thread_id is None:
            self._owner_thread_id = current
        elif self._owner_thread_id != current:
            raise CaptureOwnershipError("camera backend accessed outside its owner thread")

    @property
    def worker_state(self) -> OpenCVWorkerState:
        """Return bounded process state without identifiers, paths, or frame data."""

        process = self._process
        if process is None:
            return OpenCVWorkerState.ABSENT
        if self._termination_failed:
            return OpenCVWorkerState.STUCK
        alive = self._process_alive_state(process)
        if alive is True:
            return OpenCVWorkerState.RUNNING
        if alive is False:
            return OpenCVWorkerState.EXITED
        return OpenCVWorkerState.UNKNOWN

    def open(
        self,
        request: CaptureRequest,
        *,
        deadline_monotonic_ns: int,
    ) -> NegotiatedStream:
        self._claim_or_check_owner()
        if not isinstance(request, CaptureRequest):
            raise TypeError("request must be a CaptureRequest")
        if self._process is not None or self._connection is not None:
            raise CaptureStateError("camera worker is already open or cleanup is incomplete")
        if _remaining_seconds(deadline_monotonic_ns, self._monotonic_ns) <= 0.0:
            raise CaptureDeadlineExceeded("camera open deadline expired")

        try:
            parent, child = self._context.Pipe(duplex=True)
            process = self._context.Process(
                target=_opencv_worker,
                args=(child, request),
                name="viskium-opencv-camera",
                daemon=True,
            )
            self._process = process
            self._connection = parent
            self._request = request
            self._termination_failed = False
            try:
                process.start()
            finally:
                with suppress(OSError):
                    child.close()
            remaining = _remaining_seconds(deadline_monotonic_ns, self._monotonic_ns)
            if remaining <= 0.0 or not parent.poll(remaining):
                self._terminate_worker()
                raise CaptureOpenError("camera_open_timeout")
            message = parent.recv()
            if not isinstance(message, tuple) or not message:
                self._terminate_worker()
                raise CaptureOpenError("invalid_open_response")
            if message[0] != "opened" or len(message) != 5:
                reason = _reason(message[1] if len(message) > 1 else None, "camera_open_failed")
                self._terminate_worker()
                raise CaptureOpenError(reason)
            stream = NegotiatedStream(
                backend_id="opencv-process",
                width=_worker_integer(message[1], "width"),
                height=_worker_integer(message[2], "height"),
                fps=_worker_optional_float(message[3], "fps"),
                pixel_format="bgr24",
                stride=_worker_integer(message[4], "stride"),
            )
            if stream.stride * stream.height > request.max_frame_bytes:
                raise ValueError("negotiated stream exceeds request budget")
            self._stream = stream
            return stream
        except (CaptureOpenError, CaptureStateError):
            raise
        except (BrokenPipeError, EOFError, OSError, TypeError, ValueError) as error:
            self._terminate_worker()
            raise CaptureOpenError("camera_worker_start_failed") from error

    def read(self, *, deadline_monotonic_ns: int) -> CaptureRead:
        self._claim_or_check_owner()
        process = self._process
        connection = self._connection
        request = self._request
        stream = self._stream
        if process is None or connection is None or request is None or stream is None:
            raise CaptureStateError("camera worker is not open")
        if _remaining_seconds(deadline_monotonic_ns, self._monotonic_ns) <= 0.0:
            self._terminate_worker()
            return CaptureRead(ReadStatus.TIMEOUT, reason_code="camera_read_deadline_expired")
        if not process.is_alive():
            self._terminate_worker()
            return CaptureRead(ReadStatus.DISCONNECTED, reason_code="camera_worker_exited")
        try:
            connection.send(("read",))
            remaining = _remaining_seconds(deadline_monotonic_ns, self._monotonic_ns)
            if remaining <= 0.0 or not connection.poll(remaining):
                self._terminate_worker()
                return CaptureRead(ReadStatus.TIMEOUT, reason_code="camera_read_timeout")
            message = connection.recv()
        except (BrokenPipeError, EOFError, OSError):
            self._terminate_worker()
            return CaptureRead(ReadStatus.DISCONNECTED, reason_code="camera_worker_disconnected")
        if not isinstance(message, tuple) or not message:
            self._terminate_worker()
            return CaptureRead(ReadStatus.FATAL_ERROR, reason_code="invalid_worker_response")
        status = message[0]
        if status == "frame" and len(message) == 7:
            try:
                payload = message[1]
                if not isinstance(payload, bytes):
                    raise TypeError("frame payload must be bytes")
                if len(payload) > request.max_frame_bytes:
                    raise ValueError("frame exceeds request")
                frame = BackendFrame(
                    payload=payload,
                    width=_worker_integer(message[2], "width"),
                    height=_worker_integer(message[3], "height"),
                    stride=_worker_integer(message[4], "stride"),
                    pixel_format=_worker_string(message[5], "pixel_format"),
                    received_monotonic_ns=_worker_integer(
                        message[6],
                        "received_monotonic_ns",
                    ),
                )
                if (
                    frame.width != stream.width
                    or frame.height != stream.height
                    or frame.stride != stream.stride
                    or frame.pixel_format != stream.pixel_format
                ):
                    raise ValueError("frame differs from negotiated stream")
            except (TypeError, ValueError):
                self._terminate_worker()
                return CaptureRead(ReadStatus.FATAL_ERROR, reason_code="invalid_backend_frame")
            return CaptureRead(ReadStatus.FRAME, frame=frame)
        reason = _reason(message[1] if len(message) > 1 else None, "camera_worker_error")
        if status == "disconnected":
            return CaptureRead(ReadStatus.DISCONNECTED, reason_code=reason)
        if status == "recoverable":
            return CaptureRead(ReadStatus.RECOVERABLE_ERROR, reason_code=reason)
        self._terminate_worker()
        return CaptureRead(ReadStatus.FATAL_ERROR, reason_code=reason)

    def close(self) -> None:
        self._claim_or_check_owner()
        if self._process is None and self._connection is None:
            return
        connection = self._connection
        process = self._process
        if connection is not None:
            with suppress(BrokenPipeError, EOFError, OSError):
                connection.send(("close",))
        if process is not None:
            with suppress(AssertionError, OSError, ValueError):
                process.join(self._cleanup_timeout_seconds)
        self._terminate_worker()

    def _terminate_worker(self) -> None:
        process = self._process
        connection = self._connection
        self._connection = None
        self._request = None
        self._stream = None
        if connection is not None:
            with suppress(OSError):
                connection.close()
        if process is not None and self._process_alive_state(process) is not False:
            with suppress(AssertionError, OSError, ValueError):
                process.terminate()
                process.join(self._cleanup_timeout_seconds)
        if process is not None and self._process_alive_state(process) is not False:
            with suppress(AssertionError, OSError, ValueError):
                process.kill()
                process.join(self._cleanup_timeout_seconds)
        if process is not None and self._process_alive_state(process) is not False:
            self._process = process
            self._termination_failed = True
            raise CaptureStateError("camera worker could not be terminated")
        self._process = None
        self._termination_failed = False

    @staticmethod
    def _process_alive_state(process: _ProcessPort) -> bool | None:
        try:
            return process.is_alive()
        except (AssertionError, OSError, ValueError):
            return None


def _worker_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"worker {field_name} must be an integer")
    return value


def _worker_optional_float(value: object, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, float):
        raise TypeError(f"worker {field_name} must be a float or null")
    return value


def _worker_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"worker {field_name} must be a string")
    return value


__all__ = ["OpenCVProcessCameraBackend", "OpenCVWorkerState"]
