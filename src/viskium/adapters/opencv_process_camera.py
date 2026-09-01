"""Deadline-enforced OpenCV camera backend isolated in a child process."""

from __future__ import annotations

import math
import multiprocessing
import os
import time
from collections.abc import Callable
from contextlib import suppress
from enum import StrEnum
from multiprocessing.connection import Connection
from threading import get_ident
from typing import Any, Protocol, runtime_checkable

from viskium._camera_worker_bootstrap import _camera_worker_bootstrap
from viskium._worker_transport import (
    SocketSubprocessLaunchError,
)
from viskium._worker_transport import (
    launch_socket_subprocess as _launch_socket_subprocess,
)
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
    VideoIOPreference,
)
from viskium.capture.contracts import MAX_CAPTURE_DIMENSION, MAX_CAPTURE_FPS

_DEFAULT_CLEANUP_SECONDS = 0.25
_MAX_INT64 = 2**63 - 1

_OPEN_REASON_CODES = frozenset(
    {
        "opencv_unavailable",
        "device_open_failed",
        "opencv_worker_error",
        "directshow_unavailable",
        "mediafoundation_unavailable",
        "videoio_backend_unavailable",
        "invalid_backend_preference",
        "busy",
    }
)
_CONFIGURE_REASON_CODES = frozenset(
    {
        "camera_configure_failed",
        "negotiated_mode_exceeds_limit",
    }
)
_READ_REASON_CODES = frozenset(
    {
        "capture_read_error",
        "capture_read_failed",
        "camera_worker_error",
        "camera_worker_disconnected",
        "invalid_worker_command",
        "invalid_backend_frame",
    }
)


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


def _reason(value: object, fallback: str, allowed: frozenset[str]) -> str:
    if type(value) is not str or value not in allowed:
        return fallback
    return value


def _select_videoio_api(
    cv2: Any,
    preference: VideoIOPreference,
    *,
    platform_name: str | None = None,
) -> int:
    """Select one available OpenCV API without opening a device or falling back."""

    if type(preference) is not VideoIOPreference:
        raise TypeError("videoio preference must be a VideoIOPreference")
    selected_platform = os.name if platform_name is None else platform_name
    if selected_platform != "nt":
        if preference is not VideoIOPreference.AUTO:
            reason = (
                "mediafoundation_unavailable"
                if preference is VideoIOPreference.MEDIA_FOUNDATION
                else "directshow_unavailable"
            )
            raise RuntimeError(reason)
        api = getattr(cv2, "CAP_ANY", 0)
        if isinstance(api, bool) or not isinstance(api, int):
            raise RuntimeError("videoio_backend_unavailable")
        return api

    def available_api(attribute: str, reason: str) -> int:
        api = getattr(cv2, attribute, None)
        if isinstance(api, bool) or not isinstance(api, int):
            raise RuntimeError(reason)
        registry = getattr(cv2, "videoio_registry", None)
        has_backend = getattr(registry, "hasBackend", None)
        if not callable(has_backend):
            raise RuntimeError(reason)
        try:
            available = has_backend(api)
        except Exception as error:
            raise RuntimeError(reason) from error
        if type(available) is not bool or not available:
            raise RuntimeError(reason)
        return api

    if preference is VideoIOPreference.MEDIA_FOUNDATION:
        return available_api("CAP_MSMF", "mediafoundation_unavailable")
    if preference is VideoIOPreference.DIRECTSHOW:
        return available_api("CAP_DSHOW", "directshow_unavailable")

    # AUTO has one deterministic Windows order.  The first available backend
    # is selected before the worker calls VideoCapture; a failed open is not a
    # reason to open the device a second time through another API.
    try:
        return available_api("CAP_MSMF", "mediafoundation_unavailable")
    except RuntimeError:
        return available_api("CAP_DSHOW", "directshow_unavailable")


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


def _process_has_exited(process: _ProcessPort | None) -> bool:
    """Return only a bounded exit fact; never expose process details."""

    if process is None:
        return False
    try:
        alive = process.is_alive()
    except (AssertionError, OSError, ValueError):
        alive = None
    if alive is False:
        return True
    if alive is True:
        return False
    try:
        exitcode = getattr(process, "exitcode", None)
    except (AssertionError, OSError, ValueError):
        return False
    return exitcode is not None


def _receive_worker_message(
    connection: _ConnectionPort,
    *,
    process: _ProcessPort | None = None,
    deadline_monotonic_ns: int,
    monotonic_ns: Callable[[], int],
    timeout_reason: str,
    invalid_reason: str,
) -> object:
    """Receive exactly one bounded worker message under one absolute deadline."""

    remaining = _remaining_seconds(deadline_monotonic_ns, monotonic_ns)
    if remaining <= 0.0:
        if _process_has_exited(process):
            raise CaptureOpenError("camera_worker_exited")
        raise CaptureOpenError(timeout_reason)
    try:
        if not connection.poll(remaining):
            if _process_has_exited(process):
                raise CaptureOpenError("camera_worker_exited")
            raise CaptureOpenError(timeout_reason)
        message = connection.recv()
        if _remaining_seconds(deadline_monotonic_ns, monotonic_ns) <= 0.0:
            raise CaptureOpenError(timeout_reason)
        return message
    except CaptureOpenError:
        raise
    except (BrokenPipeError, EOFError, OSError, TypeError, ValueError) as error:
        if _process_has_exited(process):
            raise CaptureOpenError("camera_worker_exited") from error
        raise CaptureOpenError(invalid_reason) from error


def _opencv_worker(connection: Connection, request: CaptureRequest) -> None:  # pragma: no cover
    """Compatibility wrapper for the private lightweight bootstrap target."""

    _camera_worker_bootstrap(connection, request)


def _run_worker(connection: _ConnectionPort, request: CaptureRequest, cv2: Any) -> None:
    """Run the small request/response worker; injectable for contract tests."""

    capture: Any = None
    try:
        try:
            api = _select_videoio_api(cv2, request.videoio_preference)
        except RuntimeError as error:
            reason = _reason(
                error.args[0] if error.args else None,
                "videoio_backend_unavailable",
                _OPEN_REASON_CODES,
            )
            _send_worker_message(connection, ("open_error", reason))
            return
        except (TypeError, ValueError):
            _send_worker_message(connection, ("open_error", "invalid_backend_preference"))
            return
        # The backend has been selected without opening the device.  Keep the
        # selected API in the bounded protocol so the parent can validate the
        # exact phase and type without exposing driver details.
        _send_worker_message(connection, ("backend_ready", api))
        capture = cv2.VideoCapture(request.device_index, api)
        if capture is None or not bool(capture.isOpened()):
            _send_worker_message(connection, ("open_error", "device_open_failed"))
            return
        # This ACK is deliberately before all set/get calls.  The parent can
        # therefore distinguish a device-open deadline from configuration.
        _send_worker_message(connection, ("opened",))
        try:
            # OpenCV backends commonly report False for an accepted best-effort
            # request.  The subsequent get() values are the negotiated truth.
            capture.set(cv2.CAP_PROP_FRAME_WIDTH, float(request.requested_width))
            capture.set(cv2.CAP_PROP_FRAME_HEIGHT, float(request.requested_height))
            capture.set(cv2.CAP_PROP_FPS, request.requested_fps)

            width = _worker_dimension(capture.get(cv2.CAP_PROP_FRAME_WIDTH), "width")
            height = _worker_dimension(capture.get(cv2.CAP_PROP_FRAME_HEIGHT), "height")
            fps = _worker_negotiated_fps(capture.get(cv2.CAP_PROP_FPS))
            if width <= 0 or height <= 0 or width * height * 3 > request.max_frame_bytes:
                _send_worker_message(
                    connection,
                    ("configure_error", "negotiated_mode_exceeds_limit"),
                )
                return
        except Exception:
            _send_worker_message(connection, ("configure_error", "camera_configure_failed"))
            return
        _send_worker_message(connection, ("configured", width, height, fps, width * 3))

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
        # A caller-supplied context is intentionally treated as a test/fake or
        # an explicit POSIX transport.  The production Windows default uses a
        # narrow socketpair + handle-list subprocess boundary.
        self._use_windows_subprocess = context is None and os.name == "nt"
        self._monotonic_ns = monotonic_ns
        self._cleanup_timeout_seconds = float(cleanup_timeout_seconds)
        self._owner_thread_id: int | None = None
        self._process: _ProcessPort | None = None
        self._connection: _ConnectionPort | None = None
        self._request: CaptureRequest | None = None
        self._stream: NegotiatedStream | None = None
        self._process_started = False
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
            child: _ConnectionPort | None = None
            parent: _ConnectionPort
            process: _ProcessPort
            if self._use_windows_subprocess:
                try:
                    parent, process = _launch_socket_subprocess(
                        "viskium._camera_worker_subprocess",
                        deadline=deadline_monotonic_ns,
                        monotonic=self._monotonic_ns,
                    )
                except SocketSubprocessLaunchError as error:
                    self._connection = error.connection
                    self._process = error.process
                    self._request = request if error.process is not None else None
                    self._process_started = error.process is not None
                    self._termination_failed = False
                    self._terminate_worker()
                    raise CaptureOpenError(error.reason) from error
                self._process = process
                self._connection = parent
                self._request = request
                self._process_started = True
                self._termination_failed = False
            else:
                parent, child = self._context.Pipe(duplex=True)
                try:
                    process = self._context.Process(
                        target=_camera_worker_bootstrap,
                        args=(child, request),
                        name="viskium-opencv-camera",
                        daemon=True,
                    )
                except Exception as error:
                    with suppress(OSError):
                        parent.close()
                    with suppress(OSError):
                        child.close()
                    raise CaptureOpenError("camera_worker_start_failed") from error
                self._process = process
                self._connection = parent
                self._request = request
                self._termination_failed = False
                try:
                    process.start()
                except Exception as error:
                    with suppress(OSError):
                        child.close()
                    self._terminate_worker()
                    raise CaptureOpenError("camera_worker_start_failed") from error
                else:
                    self._process_started = True
                    with suppress(OSError):
                        child.close()
            message = _receive_worker_message(
                parent,
                process=process,
                deadline_monotonic_ns=deadline_monotonic_ns,
                monotonic_ns=self._monotonic_ns,
                timeout_reason="camera_worker_start_timeout",
                invalid_reason="invalid_open_response",
            )
            if type(message) is not tuple or message != ("worker_started",):
                self._terminate_worker()
                raise CaptureOpenError("invalid_open_response")

            if self._use_windows_subprocess:
                if _remaining_seconds(deadline_monotonic_ns, self._monotonic_ns) <= 0.0:
                    self._terminate_worker()
                    raise CaptureOpenError("camera_worker_start_timeout")
                try:
                    parent.send(("request", request))
                except (BrokenPipeError, EOFError, OSError, TypeError, ValueError) as error:
                    self._terminate_worker()
                    raise CaptureOpenError("camera_worker_start_failed") from error
                if _remaining_seconds(deadline_monotonic_ns, self._monotonic_ns) <= 0.0:
                    self._terminate_worker()
                    raise CaptureOpenError("camera_worker_start_timeout")

            message = _receive_worker_message(
                parent,
                process=process,
                deadline_monotonic_ns=deadline_monotonic_ns,
                monotonic_ns=self._monotonic_ns,
                timeout_reason="camera_backend_init_timeout",
                invalid_reason="invalid_open_response",
            )
            if type(message) is not tuple or not message:
                self._terminate_worker()
                raise CaptureOpenError("invalid_open_response")
            status = message[0]
            if type(status) is not str:
                self._terminate_worker()
                raise CaptureOpenError("invalid_open_response")
            if status == "open_error":
                if len(message) != 2:
                    self._terminate_worker()
                    raise CaptureOpenError("invalid_open_response")
                reason = _reason(message[1], "camera_open_failed", _OPEN_REASON_CODES)
                self._terminate_worker()
                raise CaptureOpenError(reason)
            if (
                status != "backend_ready"
                or len(message) != 2
                or isinstance(message[1], bool)
                or type(message[1]) is not int
            ):
                self._terminate_worker()
                raise CaptureOpenError("invalid_open_response")

            message = _receive_worker_message(
                parent,
                process=process,
                deadline_monotonic_ns=deadline_monotonic_ns,
                monotonic_ns=self._monotonic_ns,
                timeout_reason="camera_device_open_timeout",
                invalid_reason="invalid_open_response",
            )
            if type(message) is not tuple or not message:
                self._terminate_worker()
                raise CaptureOpenError("invalid_open_response")
            status = message[0]
            if type(status) is not str:
                self._terminate_worker()
                raise CaptureOpenError("invalid_open_response")
            if status == "open_error":
                if len(message) != 2:
                    self._terminate_worker()
                    raise CaptureOpenError("invalid_open_response")
                reason = _reason(message[1], "camera_open_failed", _OPEN_REASON_CODES)
                self._terminate_worker()
                raise CaptureOpenError(reason)
            if status != "opened" or len(message) != 1:
                self._terminate_worker()
                raise CaptureOpenError("invalid_open_response")

            message = _receive_worker_message(
                parent,
                process=process,
                deadline_monotonic_ns=deadline_monotonic_ns,
                monotonic_ns=self._monotonic_ns,
                timeout_reason="camera_configure_timeout",
                invalid_reason="invalid_configure_response",
            )
            if type(message) is not tuple or not message:
                self._terminate_worker()
                raise CaptureOpenError("invalid_configure_response")
            status = message[0]
            if type(status) is not str:
                self._terminate_worker()
                raise CaptureOpenError("invalid_configure_response")
            if status == "configure_error":
                if len(message) != 2:
                    self._terminate_worker()
                    raise CaptureOpenError("invalid_configure_response")
                reason = _reason(
                    message[1],
                    "invalid_configure_response",
                    _CONFIGURE_REASON_CODES,
                )
                self._terminate_worker()
                raise CaptureOpenError(reason)
            if status != "configured" or len(message) != 5:
                self._terminate_worker()
                raise CaptureOpenError("invalid_configure_response")
            try:
                width = _worker_integer(message[1], "width")
                height = _worker_integer(message[2], "height")
                fps = _worker_optional_float(message[3], "fps")
                stride = _worker_integer(message[4], "stride")
            except (TypeError, ValueError) as error:
                self._terminate_worker()
                raise CaptureOpenError("camera_configure_failed") from error
            if (
                width <= 0
                or height <= 0
                or stride <= 0
                or stride != width * 3
                or stride * height > request.max_frame_bytes
                or (fps is not None and (not math.isfinite(fps) or fps <= 0.0 or fps > 240.0))
            ):
                self._terminate_worker()
                raise CaptureOpenError("camera_configure_failed")
            if _remaining_seconds(deadline_monotonic_ns, self._monotonic_ns) <= 0.0:
                self._terminate_worker()
                raise CaptureOpenError("camera_configure_timeout")
            try:
                stream = NegotiatedStream(
                    backend_id="opencv-process",
                    width=width,
                    height=height,
                    fps=fps,
                    pixel_format="bgr24",
                    stride=stride,
                )
            except (TypeError, ValueError) as error:
                self._terminate_worker()
                raise CaptureOpenError("camera_configure_failed") from error
            if _remaining_seconds(deadline_monotonic_ns, self._monotonic_ns) <= 0.0:
                self._terminate_worker()
                raise CaptureOpenError("camera_configure_timeout")
            self._stream = stream
            return stream
        except CaptureOpenError:
            self._terminate_worker()
            raise
        except CaptureStateError:
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
        if type(status) is not str:
            self._terminate_worker()
            return CaptureRead(ReadStatus.FATAL_ERROR, reason_code="invalid_worker_response")
        if status == "frame":
            if len(message) != 7:
                self._terminate_worker()
                return CaptureRead(ReadStatus.FATAL_ERROR, reason_code="invalid_worker_response")
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
        if status not in {"disconnected", "recoverable", "fatal"} or len(message) != 2:
            self._terminate_worker()
            return CaptureRead(ReadStatus.FATAL_ERROR, reason_code="invalid_worker_response")
        reason = _reason(
            message[1],
            "camera_worker_error",
            _READ_REASON_CODES,
        )
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
        process_started = self._process_started
        if connection is not None:
            with suppress(OSError):
                connection.close()
        if (
            process is not None
            and process_started
            and self._process_alive_state(process) is not False
        ):
            with suppress(AssertionError, OSError, ValueError):
                process.terminate()
                process.join(self._cleanup_timeout_seconds)
        if (
            process is not None
            and process_started
            and self._process_alive_state(process) is not False
        ):
            with suppress(AssertionError, OSError, ValueError):
                process.kill()
                process.join(self._cleanup_timeout_seconds)
        if (
            process is not None
            and process_started
            and self._process_alive_state(process) is not False
        ):
            self._process = process
            self._termination_failed = True
            raise CaptureStateError("camera worker could not be terminated")
        self._process = None
        self._process_started = False
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


def _worker_positive_finite(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"worker {field_name} must be a number")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0.0:
        raise ValueError(f"worker {field_name} must be finite and positive")
    return numeric


def _worker_dimension(value: object, field_name: str) -> int:
    numeric = _worker_positive_finite(value, field_name)
    integer = int(numeric)
    if numeric != integer or integer > MAX_CAPTURE_DIMENSION:
        raise ValueError(f"worker {field_name} must be a bounded integer")
    return integer


def _worker_negotiated_fps(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("worker fps must be a number or null")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0.0 or numeric > MAX_CAPTURE_FPS:
        raise ValueError("worker fps must be finite and within bounds")
    return None if numeric == 0.0 else numeric


def _worker_optional_float(value: object, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, float):
        raise TypeError(f"worker {field_name} must be a float or null")
    return None if value == 0.0 else value


def _worker_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"worker {field_name} must be a string")
    return value


__all__ = ["OpenCVProcessCameraBackend", "OpenCVWorkerState"]
