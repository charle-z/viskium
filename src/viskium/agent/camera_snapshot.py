"""One-shot camera snapshots for the local agent-read boundary.

Each admitted call creates one backend, opens it, performs a bounded number of
reads, encodes one still image in memory, and closes the backend in the calling
thread. No frame or encoded result is cached.  On a close failure, exactly one
backend is retained as an opaque terminal lifecycle handle so it cannot be
silently replaced; the default process adapter has already purged its frame,
stream, request, and connection references before reporting that failure.
"""

from __future__ import annotations

import math
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Literal, cast

from viskium.capture import (
    BackendFactory,
    BackendFrame,
    CameraLease,
    CaptureBackend,
    CaptureCapabilities,
    CaptureDeadlineExceeded,
    CaptureOpenError,
    CaptureRead,
    CaptureRequest,
    FileCameraLease,
    NegotiatedStream,
    ReadStatus,
)
from viskium.capture.contracts import MAX_CAPTURE_COOLDOWN_NS
from viskium.core import FrameEnvelope
from viskium.core.contracts import SensitivityClass
from viskium.resources import ResourceAdmissionGate
from viskium.resources.budget import BudgetDecision
from viskium.snapshots import (
    MAX_SNAPSHOT_BYTES,
    MAX_SNAPSHOT_EDGE_PX,
    MAX_SNAPSHOT_WAIT_SECONDS,
    SnapshotEnvelope,
    encode_png_snapshot,
)

from .service import (
    SnapshotCaptureResult,
    SnapshotReasonCode,
    normalize_snapshot_reason,
    normalize_snapshot_reason_for_outcome,
)

DEFAULT_MINIMUM_OPEN_INTERVAL_SECONDS = 0.5
DEFAULT_WARMUP_FRAMES = 3
DEFAULT_ESTIMATED_BACKEND_BYTES = 96 * 1_024 * 1_024
MAX_ESTIMATED_BACKEND_BYTES = 4 * 1_024 * 1_024 * 1_024
MAX_ONE_SHOT_ATTEMPTS = 2
MAX_ONE_SHOT_WARMUP_FRAMES = 16
_MAX_IDENTIFIER_CHARS = 256
_MAX_INT64 = 2**63 - 1

type SnapshotWorkerState = Literal[
    "absent",
    "not_reported",
    "running",
    "exited",
    "unknown",
    "stuck",
]
_WORKER_STATES: frozenset[str] = frozenset(
    {"absent", "not_reported", "running", "exited", "unknown", "stuck"}
)


def _default_backend_factory() -> CaptureBackend:
    # Importing the adapter does not import cv2; OpenCV is loaded only inside
    # its deadline-enforced worker process after admission succeeds.
    from viskium.adapters.opencv_process_camera import OpenCVProcessCameraBackend

    return OpenCVProcessCameraBackend()


def _bounded_integer(
    value: object,
    field_name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
    return value


def _bounded_seconds(
    value: object,
    field_name: str,
    *,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number")
    seconds = float(value)
    if not math.isfinite(seconds) or not 0.0 <= seconds <= maximum:
        raise ValueError(f"{field_name} must be finite and between zero and {maximum:g}")
    return seconds


def _bounded_clock(clock: Callable[[], int]) -> int:
    return _bounded_integer(
        clock(),
        "monotonic clock value",
        minimum=0,
        maximum=_MAX_INT64,
    )


def _require_source_id(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("source_id must be a string")
    if not value or not value.strip():
        raise ValueError("source_id must not be empty")
    if len(value) > _MAX_IDENTIFIER_CHARS:
        raise ValueError(f"source_id must not exceed {_MAX_IDENTIFIER_CHARS} characters")
    return value


def _safe_capture_open_reason(error: CaptureOpenError) -> SnapshotReasonCode:
    """Read one exact, allowlisted exception argument without formatting it."""

    if type(error) is not CaptureOpenError:
        return "generic"
    arguments = error.args
    if type(arguments) is not tuple or len(arguments) != 1 or type(arguments[0]) is not str:
        return "generic"
    return normalize_snapshot_reason(arguments[0])


@dataclass(frozen=True, slots=True)
class CameraSnapshotMetrics:
    """Content-free counters for one-shot hardware access."""

    requests: int
    busy: int
    throttled: int
    resource_denied: int
    backend_instances: int
    opens: int
    reads: int
    delivered: int
    timeouts: int
    failures: int
    close_failures: int
    backend_retained: bool
    close_stuck: bool
    worker_state: SnapshotWorkerState
    active: bool

    def __post_init__(self) -> None:
        for field_name in (
            "requests",
            "busy",
            "throttled",
            "resource_denied",
            "backend_instances",
            "opens",
            "reads",
            "delivered",
            "timeouts",
            "failures",
            "close_failures",
        ):
            _bounded_integer(
                getattr(self, field_name),
                field_name,
                minimum=0,
                maximum=_MAX_INT64,
            )
        for field_name in ("backend_retained", "close_stuck", "active"):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a boolean")
        if self.worker_state not in _WORKER_STATES:
            raise ValueError("unsupported snapshot worker state")
        if self.close_stuck != self.backend_retained:
            raise ValueError("a close-stuck provider must retain exactly one backend")
        if self.close_stuck != (self.close_failures > 0):
            raise ValueError("close_failures and close_stuck must agree")
        if self.close_stuck and self.worker_state in {"absent", "not_reported"}:
            raise ValueError("a close-stuck provider requires an unresolved worker state")
        if not self.close_stuck and self.worker_state not in {"absent", "not_reported"}:
            raise ValueError("a healthy provider cannot report an unresolved worker")


class CameraSnapshotProvider:
    """Synchronous, resource-gated implementation of SnapshotProvider.

    The operation lock is deliberately non-blocking. A concurrent caller or
    one inside the configured camera-open interval receives busy instead of
    queuing another future hardware open. The process-wide device lease is held
    from before factory creation until a close is confirmed. max_attempts=2
    permits exactly one additional read only after RECOVERABLE_ERROR; it never
    reopens the backend and never extends the original deadline.
    """

    __slots__ = (
        "_attempt_sequence",
        "_backend_factory",
        "_capture_request",
        "_close_stuck",
        "_estimated_backend_bytes",
        "_last_open_started_ns",
        "_last_worker_state",
        "_lease",
        "_lease_held",
        "_max_attempts",
        "_metrics_counts",
        "_metrics_lock",
        "_minimum_open_interval_ns",
        "_monotonic_ns",
        "_operation_lock",
        "_resource_gate",
        "_retained_backend",
        "_sensitivity_class",
        "_source_id",
        "_warmup_frames",
    )

    _REQUESTS = 0
    _BUSY = 1
    _THROTTLED = 2
    _RESOURCE_DENIED = 3
    _BACKEND_INSTANCES = 4
    _OPENS = 5
    _READS = 6
    _DELIVERED = 7
    _TIMEOUTS = 8
    _FAILURES = 9
    _CLOSE_FAILURES = 10

    def __init__(
        self,
        *,
        backend_factory: BackendFactory | None = None,
        capture_request: CaptureRequest | None = None,
        resource_gate: ResourceAdmissionGate | None = None,
        source_id: str = "camera-0",
        sensitivity_class: SensitivityClass = "identifiable",
        warmup_frames: int = DEFAULT_WARMUP_FRAMES,
        max_attempts: int = 1,
        minimum_open_interval_seconds: float = DEFAULT_MINIMUM_OPEN_INTERVAL_SECONDS,
        estimated_backend_bytes: int = DEFAULT_ESTIMATED_BACKEND_BYTES,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        lease: CameraLease | None = None,
    ) -> None:
        selected_factory = _default_backend_factory if backend_factory is None else backend_factory
        if not callable(selected_factory):
            raise TypeError("backend_factory must be callable")
        selected_request = (
            CaptureRequest(
                device_index=0,
                requested_width=640,
                requested_height=480,
                requested_fps=15.0,
                max_frame_bytes=640 * 480 * 3,
            )
            if capture_request is None
            else capture_request
        )
        if not isinstance(selected_request, CaptureRequest):
            raise TypeError("capture_request must be a CaptureRequest")
        if resource_gate is not None and not callable(getattr(resource_gate, "evaluate", None)):
            raise TypeError("resource_gate must provide evaluate()")
        if sensitivity_class not in {"public", "operational", "sensitive", "identifiable"}:
            raise ValueError("snapshot sensitivity must be allowed and non-prohibited")
        if not callable(monotonic_ns):
            raise TypeError("monotonic_ns must be callable")
        if lease is not None:
            if not callable(getattr(lease, "acquire", None)):
                raise TypeError("lease must provide acquire()")
            if not callable(getattr(lease, "release", None)):
                raise TypeError("lease must provide release()")

        interval_seconds = _bounded_seconds(
            minimum_open_interval_seconds,
            "minimum_open_interval_seconds",
            maximum=MAX_CAPTURE_COOLDOWN_NS / 1_000_000_000,
        )
        self._backend_factory = selected_factory
        self._capture_request = selected_request
        self._resource_gate = resource_gate
        self._source_id = _require_source_id(source_id)
        self._sensitivity_class = sensitivity_class
        self._warmup_frames = _bounded_integer(
            warmup_frames,
            "warmup_frames",
            minimum=0,
            maximum=MAX_ONE_SHOT_WARMUP_FRAMES,
        )
        self._max_attempts = _bounded_integer(
            max_attempts,
            "max_attempts",
            minimum=1,
            maximum=MAX_ONE_SHOT_ATTEMPTS,
        )
        self._minimum_open_interval_ns = int(interval_seconds * 1_000_000_000)
        self._estimated_backend_bytes = _bounded_integer(
            estimated_backend_bytes,
            "estimated_backend_bytes",
            minimum=0,
            maximum=MAX_ESTIMATED_BACKEND_BYTES,
        )
        self._monotonic_ns = monotonic_ns
        self._lease = FileCameraLease(selected_request.device_index) if lease is None else lease
        self._operation_lock = Lock()
        self._metrics_lock = Lock()
        self._metrics_counts = [0] * 11
        self._last_open_started_ns: int | None = None
        self._attempt_sequence = 0
        self._retained_backend: CaptureBackend | None = None
        self._lease_held = False
        self._close_stuck = False
        self._last_worker_state: SnapshotWorkerState = "absent"

    @property
    def sensitivity_class(self) -> SensitivityClass:
        """Return the immutable upper sensitivity declared before hardware access."""

        return self._sensitivity_class

    @property
    def metrics(self) -> CameraSnapshotMetrics:
        with self._metrics_lock:
            counts = tuple(self._metrics_counts)
            backend_retained = self._retained_backend is not None
            close_stuck = self._close_stuck
            worker_state = self._last_worker_state
        return CameraSnapshotMetrics(
            requests=counts[0],
            busy=counts[1],
            throttled=counts[2],
            resource_denied=counts[3],
            backend_instances=counts[4],
            opens=counts[5],
            reads=counts[6],
            delivered=counts[7],
            timeouts=counts[8],
            failures=counts[9],
            close_failures=counts[10],
            backend_retained=backend_retained,
            close_stuck=close_stuck,
            worker_state=worker_state,
            active=self._operation_lock.locked(),
        )

    def capture(
        self,
        *,
        max_edge_px: int,
        max_bytes: int,
        timeout_seconds: float,
    ) -> SnapshotCaptureResult:
        """Capture at most one encoded still within one total deadline."""

        edge_limit = _bounded_integer(
            max_edge_px,
            "max_edge_px",
            minimum=1,
            maximum=MAX_SNAPSHOT_EDGE_PX,
        )
        byte_limit = _bounded_integer(
            max_bytes,
            "max_bytes",
            minimum=1,
            maximum=MAX_SNAPSHOT_BYTES,
        )
        timeout = _bounded_seconds(
            timeout_seconds,
            "timeout_seconds",
            maximum=MAX_SNAPSHOT_WAIT_SECONDS,
        )
        self._count(self._REQUESTS)
        if timeout == 0.0:
            return self._timeout()
        if not self._operation_lock.acquire(blocking=False):
            self._count(self._BUSY)
            return SnapshotCaptureResult("busy")

        try:
            # Recheck only after acquiring the operation lock.  A previous
            # capture can latch a close failure between an optimistic check
            # and this acquisition; no later call may create another backend.
            if self._is_close_stuck():
                return self._failed("close_stuck")
            if self._lease_held:
                return self._failed("lease_release_failed")
            try:
                started_ns = _bounded_clock(self._monotonic_ns)
            except Exception:
                return self._failed("generic")
            duration_ns = int(timeout * 1_000_000_000)
            if duration_ns <= 0:
                return self._timeout()
            deadline_ns = min(_MAX_INT64, started_ns + duration_ns)
            if self._open_is_throttled(started_ns):
                self._count(self._BUSY)
                self._count(self._THROTTLED)
                return SnapshotCaptureResult("busy", reason_code="throttled")
            resource_reason = self._resources_admit(byte_limit)
            if resource_reason is not None:
                if resource_reason == "resource_denied":
                    self._count(self._RESOURCE_DENIED)
                else:
                    self._count(self._FAILURES)
                return SnapshotCaptureResult("unavailable", reason_code=resource_reason)

            try:
                open_started_ns = _bounded_clock(self._monotonic_ns)
            except Exception:
                return self._failed("generic")
            if open_started_ns < started_ns:
                return self._failed("generic")
            if open_started_ns >= deadline_ns:
                return self._timeout()
            if self._open_is_throttled(open_started_ns):
                self._count(self._BUSY)
                self._count(self._THROTTLED)
                return SnapshotCaptureResult("busy", reason_code="throttled")

            try:
                lease_held = self._lease.acquire()
            except Exception:
                return self._failed("generic")
            if not isinstance(lease_held, bool) or not lease_held:
                self._count(self._BUSY)
                return SnapshotCaptureResult("busy", reason_code="lease_busy")
            self._lease_held = True
            self._last_open_started_ns = open_started_ns
            self._attempt_sequence = min(_MAX_INT64, self._attempt_sequence + 1)
            attempt_sequence = self._attempt_sequence
            return self._create_use_and_close_backend(
                attempt_sequence=attempt_sequence,
                deadline_ns=deadline_ns,
                edge_limit=edge_limit,
                byte_limit=byte_limit,
            )
        finally:
            pending_exception = sys.exc_info()[0] is not None
            try:
                if self._lease_held and not self._is_close_stuck():
                    try:
                        self._lease.release()
                    except Exception:
                        # Ownership is intentionally retained when release is not
                        # confirmed; subsequent calls remain fail-closed.
                        if pending_exception:
                            self._count(self._FAILURES)
                        else:
                            return self._failed("lease_release_failed")
                    else:
                        self._lease_held = False
            finally:
                # A non-application interrupt from a lease implementation must
                # never strand the provider's in-process operation lock.
                self._operation_lock.release()

    def _resources_admit(self, byte_limit: int) -> SnapshotReasonCode | None:
        gate = self._resource_gate
        if gate is None:
            return None
        estimated_bytes = (
            self._estimated_backend_bytes + self._capture_request.max_frame_bytes + (2 * byte_limit)
        )
        try:
            decision = gate.evaluate(stage="processing", estimated_bytes=estimated_bytes)
        except Exception:
            return "resource_gate_failed"
        if not isinstance(decision, BudgetDecision):
            return "resource_gate_failed"
        if not decision.allow_capture or not decision.allow_processing:
            return "resource_denied"
        return None

    def _open_is_throttled(self, now_ns: int) -> bool:
        previous_ns = self._last_open_started_ns
        if previous_ns is None or self._minimum_open_interval_ns == 0:
            return False
        if now_ns < previous_ns:
            return True
        return now_ns - previous_ns < self._minimum_open_interval_ns

    def _is_close_stuck(self) -> bool:
        with self._metrics_lock:
            return self._close_stuck

    @staticmethod
    def _backend_worker_state(backend: CaptureBackend) -> SnapshotWorkerState:
        try:
            state = getattr(backend, "worker_state", None)
        except Exception:
            return "unknown"
        if state is None:
            return "not_reported"
        if isinstance(state, str) and state in _WORKER_STATES:
            return cast(SnapshotWorkerState, state)
        return "unknown"

    def _record_closed_backend(self, backend: CaptureBackend) -> bool:
        state = self._backend_worker_state(backend)
        if state not in {"absent", "not_reported"}:
            self._latch_close_failure(backend, state=state)
            return False
        with self._metrics_lock:
            self._last_worker_state = state
        return True

    def _latch_close_failure(
        self,
        backend: CaptureBackend,
        *,
        state: SnapshotWorkerState | None = None,
    ) -> None:
        worker_state = self._backend_worker_state(backend) if state is None else state
        if worker_state in {"absent", "not_reported"}:
            worker_state = "unknown"
        with self._metrics_lock:
            if self._metrics_counts[self._CLOSE_FAILURES] < _MAX_INT64:
                self._metrics_counts[self._CLOSE_FAILURES] += 1
            self._retained_backend = backend
            self._close_stuck = True
            self._last_worker_state = worker_state

    def _create_use_and_close_backend(
        self,
        *,
        attempt_sequence: int,
        deadline_ns: int,
        edge_limit: int,
        byte_limit: int,
    ) -> SnapshotCaptureResult:
        try:
            candidate = self._backend_factory()
        except Exception:
            return self._failed("generic")
        if not isinstance(candidate, CaptureBackend):
            return self._failed("generic")

        result = SnapshotCaptureResult("failed")
        close_failed = False
        try:
            self._count(self._BACKEND_INSTANCES)
            try:
                result = self._capture_with_backend(
                    candidate,
                    attempt_sequence=attempt_sequence,
                    deadline_ns=deadline_ns,
                    edge_limit=edge_limit,
                    byte_limit=byte_limit,
                )
            except CaptureDeadlineExceeded:
                result = self._timeout()
            except CaptureOpenError as error:
                reason = _safe_capture_open_reason(error)
                result = (
                    self._timeout(reason)
                    if reason
                    in {
                        "camera_worker_start_timeout",
                        "camera_backend_init_timeout",
                        "camera_device_open_timeout",
                        "camera_open_timeout",
                        "camera_configure_timeout",
                    }
                    else SnapshotCaptureResult("unavailable", reason_code=reason)
                )
            except Exception:
                result = self._failed("generic")
        finally:
            try:
                candidate.close()
            except Exception:
                self._latch_close_failure(candidate)
                close_failed = True
            except BaseException:
                # Preserve cancellation/interrupt semantics without ever
                # dropping the only lifecycle handle at the unsafe boundary.
                self._latch_close_failure(candidate, state="unknown")
                raise
            else:
                try:
                    close_failed = not self._record_closed_backend(candidate)
                except BaseException:
                    self._latch_close_failure(candidate, state="unknown")
                    raise
        if close_failed:
            return self._failed("close_failed")
        if result.outcome == "ok":
            self._count(self._DELIVERED)
        return result

    def _capture_with_backend(
        self,
        backend: CaptureBackend,
        *,
        attempt_sequence: int,
        deadline_ns: int,
        edge_limit: int,
        byte_limit: int,
    ) -> SnapshotCaptureResult:
        capabilities = backend.capabilities
        if not isinstance(capabilities, CaptureCapabilities) or not capabilities.safe_in_process:
            return self._failed("generic")
        if self._deadline_expired(deadline_ns):
            return self._timeout()

        stream = backend.open(self._capture_request, deadline_monotonic_ns=deadline_ns)
        if not isinstance(stream, NegotiatedStream):
            return self._failed("generic")
        self._count(self._OPENS)

        for _ in range(self._warmup_frames):
            warmup = self._read_once(backend, deadline_ns=deadline_ns)
            if warmup.status is not ReadStatus.FRAME:
                return self._map_read_failure(warmup)
            if not self._frame_matches_stream(warmup.frame, stream):
                return self._failed("generic")

        target: CaptureRead | None = None
        target_sequence = self._warmup_frames
        for attempt in range(self._max_attempts):
            target = self._read_once(backend, deadline_ns=deadline_ns)
            if target.status is ReadStatus.FRAME:
                target_sequence += attempt
                break
            if target.status is not ReadStatus.RECOVERABLE_ERROR:
                return self._map_read_failure(target)
            if attempt + 1 >= self._max_attempts:
                return self._failed(
                    normalize_snapshot_reason_for_outcome(
                        target.reason_code,
                        outcome="failed",
                    )
                )
        if target is None or not self._frame_matches_stream(target.frame, stream):
            return self._failed("generic")
        backend_frame = target.frame
        if backend_frame is None:
            return self._failed("generic")

        frame = FrameEnvelope(
            source_id=self._source_id,
            stream_epoch=attempt_sequence,
            sequence=target_sequence,
            received_monotonic_ns=backend_frame.received_monotonic_ns,
            payload=backend_frame.payload,
            width=backend_frame.width,
            height=backend_frame.height,
            pixel_format=backend_frame.pixel_format,
            stride=backend_frame.stride,
        )
        snapshot = encode_png_snapshot(
            frame,
            sensitivity_class=self._sensitivity_class,
            max_edge_px=edge_limit,
            max_bytes=byte_limit,
        )
        if not isinstance(snapshot, SnapshotEnvelope):
            return self._failed("generic")
        if self._deadline_expired(deadline_ns):
            return self._timeout()
        return SnapshotCaptureResult("ok", snapshot)

    def _read_once(self, backend: CaptureBackend, *, deadline_ns: int) -> CaptureRead:
        if self._deadline_expired(deadline_ns):
            return CaptureRead(ReadStatus.TIMEOUT, reason_code="snapshot_deadline_expired")
        result = backend.read(deadline_monotonic_ns=deadline_ns)
        self._count(self._READS)
        if not isinstance(result, CaptureRead):
            raise TypeError("backend returned an invalid capture result")
        return result

    def _deadline_expired(self, deadline_ns: int) -> bool:
        return _bounded_clock(self._monotonic_ns) >= deadline_ns

    def _frame_matches_stream(
        self,
        frame: BackendFrame | None,
        stream: NegotiatedStream,
    ) -> bool:
        return (
            isinstance(frame, BackendFrame)
            and frame.width == stream.width
            and frame.height == stream.height
            and frame.stride == stream.stride
            and frame.pixel_format == stream.pixel_format
            and len(frame.payload) <= self._capture_request.max_frame_bytes
        )

    def _map_read_failure(self, result: CaptureRead) -> SnapshotCaptureResult:
        reason = normalize_snapshot_reason(result.reason_code)
        if result.status is ReadStatus.TIMEOUT:
            return SnapshotCaptureResult("timeout", reason_code=reason)
        if result.status is ReadStatus.DISCONNECTED:
            return SnapshotCaptureResult("unavailable", reason_code=reason)
        return SnapshotCaptureResult("failed", reason_code=reason)

    def _timeout(self, reason_code: object = "timeout") -> SnapshotCaptureResult:
        self._count(self._TIMEOUTS)
        return SnapshotCaptureResult(
            "timeout",
            reason_code=normalize_snapshot_reason(reason_code),
        )

    def _failed(self, reason_code: object | None = None) -> SnapshotCaptureResult:
        self._count(self._FAILURES)
        return SnapshotCaptureResult(
            "failed",
            reason_code=(None if reason_code is None else normalize_snapshot_reason(reason_code)),
        )

    def _count(self, index: int) -> None:
        with self._metrics_lock:
            current = self._metrics_counts[index]
            if current < _MAX_INT64:
                self._metrics_counts[index] = current + 1


__all__ = [
    "DEFAULT_ESTIMATED_BACKEND_BYTES",
    "DEFAULT_MINIMUM_OPEN_INTERVAL_SECONDS",
    "DEFAULT_WARMUP_FRAMES",
    "MAX_ESTIMATED_BACKEND_BYTES",
    "MAX_ONE_SHOT_ATTEMPTS",
    "MAX_ONE_SHOT_WARMUP_FRAMES",
    "CameraLease",
    "CameraSnapshotMetrics",
    "CameraSnapshotProvider",
    "FileCameraLease",
]
