"""Single-owner camera lifecycle with bounded retries and latest-frame delivery."""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from threading import Condition, Event, Thread, get_ident

from viskium.core import FrameEnvelope
from viskium.resources.budget import BudgetDecision

from .contracts import (
    MAX_REASON_CODE_CHARS,
    BackendFrame,
    CameraPolicy,
    CameraState,
    CaptureCapabilities,
    CaptureDeadlineExceeded,
    CaptureError,
    CaptureOpenError,
    CaptureRead,
    CaptureRequest,
    CaptureStateError,
    NegotiatedStream,
    ReadStatus,
)
from .latest import LatestFrameSlot, OfferStatus
from .lease import CameraLease, FileCameraLease
from .ports import CaptureBackend, ResourceAdmission

_MAX_INT64 = 2**63 - 1
_MAX_WAIT_SECONDS = 60.0
# Keep continuous capture's default aligned with the one-shot provider's
# conservative backend estimate without importing the agent layer.
DEFAULT_ESTIMATED_BACKEND_BYTES = 96 * 1_024 * 1_024
MAX_ESTIMATED_BACKEND_BYTES = 4 * 1_024 * 1_024 * 1_024

type BackendFactory = Callable[[], CaptureBackend]


class _FactoryResult(StrEnum):
    READY = "ready"
    RETRY = "retry"
    TERMINAL = "terminal"


class _StreamResult(StrEnum):
    STOPPED = "stopped"
    RETRY = "retry"
    FATAL = "fatal"
    STUCK = "stuck"
    SLOT_CLOSED = "slot_closed"


def _validate_reason_code(reason_code: object) -> str:
    if not isinstance(reason_code, str):
        raise TypeError("reason_code must be a string")
    if not reason_code or not reason_code.strip():
        raise ValueError("reason_code must not be empty")
    if len(reason_code) > MAX_REASON_CODE_CHARS:
        raise ValueError(f"reason_code exceeds {MAX_REASON_CODE_CHARS} characters")
    return reason_code


def _validate_non_negative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not 0 <= value <= _MAX_INT64:
        raise ValueError(f"{field_name} must be between zero and signed int64 max")
    return value


def _validate_wait_seconds(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number")
    normalized = float(value)
    if not math.isfinite(normalized) or not 0.0 <= normalized <= _MAX_WAIT_SECONDS:
        raise ValueError(f"{field_name} must be finite and between zero and {_MAX_WAIT_SECONDS:g}")
    return normalized


@dataclass(frozen=True, slots=True)
class CameraControllerMetrics:
    """Immutable counters that never retain captured payloads or exceptions."""

    backend_instances: int
    open_attempts: int
    open_successes: int
    open_failures: int
    unsafe_backends: int
    read_attempts: int
    frames_read: int
    warmup_frames_discarded: int
    frames_offered: int
    frames_replaced: int
    frames_rejected_closed: int
    read_timeouts: int
    disconnects: int
    recoverable_errors: int
    fatal_errors: int
    backend_failures: int
    contract_violations: int
    retries_scheduled: int
    stop_timeouts: int
    last_reason_code: str | None

    def __post_init__(self) -> None:
        for field_name in _COUNTER_NAMES:
            _validate_non_negative_int(getattr(self, field_name), field_name)
        if self.last_reason_code is not None:
            _validate_reason_code(self.last_reason_code)


_COUNTER_NAMES = tuple(
    field_name
    for field_name in CameraControllerMetrics.__dataclass_fields__
    if field_name != "last_reason_code"
)


class CameraController:
    """Own a backend on one daemon thread and publish only its newest frame.

    A backend is created, opened, read, and closed by the same worker.  The
    controller never calls a second factory until the prior backend's close has
    returned.  A device lease is acquired before the first factory call and is
    held across retries until cleanup is confirmed. Raw frames live only in the
    worker stack or the capacity-one slot.
    """

    def __init__(
        self,
        *,
        backend_factory: BackendFactory,
        request: CaptureRequest,
        policy: CameraPolicy,
        frames: LatestFrameSlot,
        source_id: str,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        lease: CameraLease | None = None,
        resource_gate: ResourceAdmission | None = None,
        estimated_backend_bytes: int = DEFAULT_ESTIMATED_BACKEND_BYTES,
    ) -> None:
        if not callable(backend_factory):
            raise TypeError("backend_factory must be callable")
        if not isinstance(request, CaptureRequest):
            raise TypeError("request must be a CaptureRequest")
        if not isinstance(policy, CameraPolicy):
            raise TypeError("policy must be a CameraPolicy")
        if not isinstance(frames, LatestFrameSlot):
            raise TypeError("frames must be a LatestFrameSlot")
        _validate_reason_code(source_id)
        if not callable(monotonic_ns):
            raise TypeError("monotonic_ns must be callable")
        if lease is not None:
            if not callable(getattr(lease, "acquire", None)):
                raise TypeError("lease must provide acquire()")
            if not callable(getattr(lease, "release", None)):
                raise TypeError("lease must provide release()")
        if resource_gate is not None and not isinstance(resource_gate, ResourceAdmission):
            raise TypeError("resource_gate must implement ResourceAdmission")
        if isinstance(estimated_backend_bytes, bool) or not isinstance(
            estimated_backend_bytes, int
        ):
            raise TypeError("estimated_backend_bytes must be an integer")
        if not 0 <= estimated_backend_bytes <= MAX_ESTIMATED_BACKEND_BYTES:
            raise ValueError(
                f"estimated_backend_bytes must be between zero and {MAX_ESTIMATED_BACKEND_BYTES}"
            )
        if request.max_frame_bytes > policy.max_frame_bytes:
            raise ValueError("request max_frame_bytes cannot exceed the camera policy")

        self._backend_factory = backend_factory
        self._request = request
        self._policy = policy
        self._frames = frames
        self._source_id = source_id
        self._monotonic_ns = monotonic_ns
        self._lease = FileCameraLease(request.device_index) if lease is None else lease
        self._resource_gate = resource_gate
        self._estimated_backend_bytes = estimated_backend_bytes
        self._condition = Condition()
        self._stop_requested = Event()
        self._state = CameraState.CLOSED
        self._last_reason_code: str | None = None
        self._started = False
        self._worker: Thread | None = None
        self._worker_thread_id: int | None = None
        self._stream_epoch: int | None = None
        self._negotiated_stream: NegotiatedStream | None = None
        self._retained_backend: CaptureBackend | None = None
        self._lease_held = False
        self._counters: dict[str, int] = dict.fromkeys(_COUNTER_NAMES, 0)

    @property
    def state(self) -> CameraState:
        with self._condition:
            return self._state

    @property
    def metrics(self) -> CameraControllerMetrics:
        with self._condition:
            return CameraControllerMetrics(
                **self._counters,
                last_reason_code=self._last_reason_code,
            )

    @property
    def worker_thread_id(self) -> int | None:
        with self._condition:
            return self._worker_thread_id

    @property
    def stream_epoch(self) -> int | None:
        with self._condition:
            return self._stream_epoch

    @property
    def negotiated_stream(self) -> NegotiatedStream | None:
        with self._condition:
            return self._negotiated_stream

    @property
    def has_retained_backend(self) -> bool:
        """Report a terminal close failure without exposing the backend handle."""

        with self._condition:
            return self._retained_backend is not None

    @property
    def is_alive(self) -> bool:
        with self._condition:
            worker = self._worker
        return worker is not None and worker.is_alive()

    def start(self) -> None:
        """Start this one-shot controller exactly once."""

        with self._condition:
            if self._started:
                raise CaptureStateError("a camera controller can only be started once")
            if self._frames.closed:
                raise CaptureStateError("cannot start with a closed latest-frame slot")
            self._started = True
            self._state = CameraState.OPENING
            worker = Thread(
                target=self._run,
                name="viskium-camera-controller",
                daemon=True,
            )
            self._worker = worker
            try:
                worker.start()
            except Exception:
                self._state = CameraState.FAILED
                self._set_reason_locked("worker_start_failed")
                self._condition.notify_all()
                self._frames.close()
                raise
            self._condition.notify_all()

    def stop(self, *, deadline_monotonic_ns: int | None = None) -> bool:
        """Request shutdown and report whether the owner exited by the deadline."""

        now_ns = time.monotonic_ns()
        if deadline_monotonic_ns is None:
            requested_deadline = min(now_ns + self._policy.shutdown_timeout_ns, _MAX_INT64)
        else:
            requested_deadline = _validate_non_negative_int(
                deadline_monotonic_ns,
                "deadline_monotonic_ns",
            )
        hard_deadline = min(
            requested_deadline,
            now_ns + self._policy.shutdown_timeout_ns,
            _MAX_INT64,
        )

        self._stop_requested.set()
        self._frames.close()
        with self._condition:
            worker = self._worker
            if not self._started:
                self._condition.notify_all()
                return True
            if worker is None:
                return True
            if self._state is CameraState.STUCK and worker.is_alive():
                return False
            if worker.ident == get_ident():
                if self._state not in {CameraState.FAILED, CameraState.STUCK}:
                    self._state = CameraState.STOPPING
                    self._set_reason_locked("stop_requested")
                    self._condition.notify_all()
                return False
            if worker.is_alive() and self._state not in {CameraState.FAILED, CameraState.STUCK}:
                self._state = CameraState.STOPPING
                self._set_reason_locked("stop_requested")
                self._condition.notify_all()

        remaining_ns = min(
            self._policy.shutdown_timeout_ns,
            max(0, hard_deadline - time.monotonic_ns()),
        )
        worker.join(remaining_ns / 1_000_000_000)
        if worker.is_alive():
            with self._condition:
                if self._state is not CameraState.STUCK:
                    self._increment_locked("stop_timeouts")
                self._state = CameraState.STUCK
                self._set_reason_locked("shutdown_deadline_exceeded")
                self._condition.notify_all()
            return False
        return True

    def wait_for_state(self, expected: CameraState, *, timeout_seconds: float) -> bool:
        """Wait a bounded amount of real time for one observable state."""

        if not isinstance(expected, CameraState):
            raise TypeError("expected must be a CameraState")
        timeout = _validate_wait_seconds(timeout_seconds, "timeout_seconds")
        with self._condition:
            return self._condition.wait_for(lambda: self._state is expected, timeout)

    def _clock_ns(self) -> int:
        return _validate_non_negative_int(self._monotonic_ns(), "monotonic_ns result")

    @staticmethod
    def _deadline_ns(started_ns: int, timeout_ns: int) -> int:
        return min(started_ns + timeout_ns, _MAX_INT64)

    def _set_reason_locked(self, reason_code: str) -> None:
        self._last_reason_code = _validate_reason_code(reason_code)

    def _set_reason(self, reason_code: str) -> None:
        with self._condition:
            self._set_reason_locked(reason_code)
            self._condition.notify_all()

    def _set_state(self, state: CameraState, reason_code: str | None = None) -> None:
        with self._condition:
            if self._state is CameraState.STUCK and state is not CameraState.STUCK:
                return
            self._state = state
            if reason_code is not None:
                self._set_reason_locked(reason_code)
            self._condition.notify_all()

    def _increment_locked(self, name: str) -> None:
        current = self._counters[name]
        self._counters[name] = min(current + 1, _MAX_INT64)

    def _increment(self, name: str) -> None:
        with self._condition:
            self._increment_locked(name)

    def _set_active_stream(self, stream: NegotiatedStream | None) -> None:
        with self._condition:
            self._negotiated_stream = stream
            self._condition.notify_all()

    def _release_lease_if_safe(self) -> bool:
        """Release only after all backend handles have been confirmed closed."""

        with self._condition:
            if not self._lease_held or self._retained_backend is not None:
                return False
        try:
            self._lease.release()
        except Exception:
            self._set_state(CameraState.STUCK, "camera_lease_release_failed")
            return False
        with self._condition:
            self._lease_held = False
            self._condition.notify_all()
        return True

    def _create_backend(self) -> tuple[CaptureBackend | None, _FactoryResult]:
        try:
            candidate = self._backend_factory()
        except Exception:
            self._increment("backend_failures")
            self._set_reason("backend_factory_failed")
            return None, _FactoryResult.RETRY
        if not isinstance(candidate, CaptureBackend):
            self._increment("contract_violations")
            self._set_state(CameraState.FAILED, "backend_protocol_invalid")
            return None, _FactoryResult.TERMINAL
        self._increment("backend_instances")
        try:
            capabilities = candidate.capabilities
        except Exception:
            self._increment("backend_failures")
            self._set_state(CameraState.FAILED, "backend_capabilities_failed")
            return candidate, _FactoryResult.TERMINAL
        if not isinstance(capabilities, CaptureCapabilities):
            self._increment("contract_violations")
            self._set_state(CameraState.FAILED, "backend_capabilities_invalid")
            return candidate, _FactoryResult.TERMINAL
        if not capabilities.safe_in_process:
            self._increment("unsafe_backends")
            self._set_state(CameraState.FAILED, "unsafe_backend_capabilities")
            return candidate, _FactoryResult.TERMINAL
        return candidate, _FactoryResult.READY

    def _close_backend(self, backend: CaptureBackend) -> bool:
        close_started_ns = time.monotonic_ns()
        try:
            backend.close()
        except Exception:
            self._increment("backend_failures")
            self._set_state(CameraState.FAILED, "backend_close_failed")
            return False
        close_elapsed_ns = time.monotonic_ns() - close_started_ns
        if close_elapsed_ns > self._policy.shutdown_timeout_ns:
            self._increment("contract_violations")
            self._set_state(CameraState.STUCK, "backend_close_deadline_violated")
            return False
        return True

    def _close_or_retain_backend(self, backend: CaptureBackend) -> bool:
        """Close on the owner thread or retain the exact unsafe handle terminally."""

        close_ok = self._close_backend(backend)
        if close_ok:
            return True
        with self._condition:
            if self._retained_backend is None:
                self._retained_backend = backend
            elif self._retained_backend is not backend:
                self._increment_locked("contract_violations")
                self._state = CameraState.STUCK
                self._set_reason_locked("retained_backend_invariant_failed")
            self._condition.notify_all()
        return False

    def _schedule_retry(self, failed_attempts: int, last_open_attempt_ns: int | None) -> bool:
        if failed_attempts > self._policy.max_reopen_attempts:
            # There is no backend left to reopen.  Release before publishing
            # the terminal state so observers never see FAILED while the
            # owner thread is still completing lease cleanup.
            self._release_lease_if_safe()
            self._set_state(CameraState.FAILED)
            return False
        delay_ns = self._policy.cooldown_ns(failed_attempts)
        if last_open_attempt_ns is not None:
            now_ns = self._clock_ns()
            elapsed_ns = max(0, now_ns - last_open_attempt_ns)
            interval_remaining_ns = max(
                0,
                self._policy.minimum_reopen_interval_ns - elapsed_ns,
            )
            delay_ns = max(delay_ns, interval_remaining_ns)
        self._increment("retries_scheduled")
        self._set_state(CameraState.COOLDOWN)
        return not self._stop_requested.wait(delay_ns / 1_000_000_000)

    def _validate_stream(self, stream: object) -> NegotiatedStream | None:
        if not isinstance(stream, NegotiatedStream):
            self._increment("contract_violations")
            self._set_state(CameraState.FAILED, "negotiated_stream_invalid")
            return None
        frame_bytes = stream.stride * stream.height
        if (
            frame_bytes > self._request.max_frame_bytes
            or frame_bytes > self._policy.max_frame_bytes
        ):
            self._increment("contract_violations")
            self._set_state(CameraState.FAILED, "negotiated_stream_exceeds_limit")
            return None
        return stream

    def _frame_matches_stream(self, frame: BackendFrame, stream: NegotiatedStream) -> bool:
        return (
            frame.width == stream.width
            and frame.height == stream.height
            and frame.stride == stream.stride
            and frame.pixel_format == stream.pixel_format
            and len(frame.payload) <= self._request.max_frame_bytes
            and len(frame.payload) <= self._policy.max_frame_bytes
            and frame.received_monotonic_ns <= _MAX_INT64
        )

    def _offer_frame(
        self,
        frame: BackendFrame,
        stream: NegotiatedStream,
        *,
        epoch: int,
        sequence: int,
    ) -> _StreamResult | None:
        if sequence > _MAX_INT64:
            self._set_reason("frame_sequence_exhausted")
            return _StreamResult.FATAL
        envelope = FrameEnvelope(
            source_id=self._source_id,
            stream_epoch=epoch,
            sequence=sequence,
            received_monotonic_ns=frame.received_monotonic_ns,
            payload=frame.payload,
            timestamp_quality="received",
            width=frame.width,
            height=frame.height,
            pixel_format=frame.pixel_format,
            stride=frame.stride,
            buffer_id=f"{stream.backend_id}:{epoch}:{sequence}",
            generation=sequence,
        )
        offer = self._frames.offer(envelope)
        if offer.status is OfferStatus.CLOSED:
            self._increment("frames_rejected_closed")
            self._set_reason("frame_slot_closed")
            self._stop_requested.set()
            return _StreamResult.SLOT_CLOSED
        self._increment("frames_offered")
        if offer.status is OfferStatus.REPLACED:
            self._increment("frames_replaced")
        return None

    def _read_stream(
        self,
        backend: CaptureBackend,
        stream: NegotiatedStream,
        *,
        epoch: int,
        opened_ns: int,
    ) -> tuple[_StreamResult, bool]:
        warmup_remaining = self._policy.warmup_frames
        sequence = 0
        last_frame_progress_ns = opened_ns
        self._set_state(CameraState.WARMING if warmup_remaining else CameraState.STREAMING)

        while not self._stop_requested.is_set():
            read_started_ns = self._clock_ns()
            read_deadline_ns = self._deadline_ns(read_started_ns, self._policy.read_timeout_ns)
            self._increment("read_attempts")
            deadline_reported = False
            try:
                result = backend.read(deadline_monotonic_ns=read_deadline_ns)
            except CaptureDeadlineExceeded:
                deadline_reported = True
                result = CaptureRead(ReadStatus.TIMEOUT, reason_code="read_deadline_exceeded")
            except CaptureError:
                self._increment("backend_failures")
                self._set_reason("backend_read_failed")
                finished_ns = self._clock_ns()
                stable = finished_ns - opened_ns >= self._policy.stable_reset_after_ns
                return _StreamResult.RETRY, stable
            except Exception:
                self._increment("backend_failures")
                self._set_reason("backend_read_unexpected_error")
                finished_ns = self._clock_ns()
                stable = finished_ns - opened_ns >= self._policy.stable_reset_after_ns
                return _StreamResult.RETRY, stable

            read_finished_ns = self._clock_ns()
            if read_finished_ns < read_started_ns:
                self._set_reason("monotonic_clock_moved_backwards")
                return _StreamResult.FATAL, False
            if read_finished_ns > read_deadline_ns and not deadline_reported:
                self._increment("contract_violations")
                self._set_state(CameraState.STUCK, "backend_read_deadline_violated")
                return _StreamResult.STUCK, False
            stable = read_finished_ns - opened_ns >= self._policy.stable_reset_after_ns
            if self._stop_requested.is_set():
                return _StreamResult.STOPPED, stable
            if not isinstance(result, CaptureRead):
                self._increment("contract_violations")
                self._set_reason("backend_read_result_invalid")
                return _StreamResult.FATAL, stable

            if result.status is ReadStatus.FRAME:
                frame = result.frame
                if frame is None or not self._frame_matches_stream(frame, stream):
                    self._increment("contract_violations")
                    self._set_reason("backend_frame_contract_invalid")
                    return _StreamResult.FATAL, stable
                self._increment("frames_read")
                last_frame_progress_ns = read_finished_ns
                if warmup_remaining:
                    warmup_remaining -= 1
                    self._increment("warmup_frames_discarded")
                    if not warmup_remaining:
                        self._set_state(CameraState.STREAMING)
                    continue
                offer_result = self._offer_frame(
                    frame,
                    stream,
                    epoch=epoch,
                    sequence=sequence,
                )
                if offer_result is not None:
                    return offer_result, stable
                sequence += 1
                self._set_state(CameraState.STREAMING)
                continue

            if result.status is ReadStatus.TIMEOUT:
                self._increment("read_timeouts")
                self._set_state(CameraState.DEGRADED, result.reason_code)
                remaining_ns = max(0, read_deadline_ns - read_finished_ns)
                if remaining_ns and self._stop_requested.wait(remaining_ns / 1_000_000_000):
                    return _StreamResult.STOPPED, stable
                after_wait_ns = self._clock_ns()
                stable = after_wait_ns - opened_ns >= self._policy.stable_reset_after_ns
                if after_wait_ns - last_frame_progress_ns >= self._policy.stale_after_ns:
                    self._set_reason("capture_stale")
                    return _StreamResult.RETRY, stable
                self._set_state(CameraState.WARMING if warmup_remaining else CameraState.STREAMING)
                continue

            if result.status is ReadStatus.DISCONNECTED:
                self._increment("disconnects")
                self._set_state(CameraState.DEGRADED, result.reason_code)
                return _StreamResult.RETRY, stable
            if result.status is ReadStatus.RECOVERABLE_ERROR:
                self._increment("recoverable_errors")
                self._set_state(CameraState.DEGRADED, result.reason_code)
                return _StreamResult.RETRY, stable

            self._increment("fatal_errors")
            self._set_reason(result.reason_code or "fatal_capture_error")
            return _StreamResult.FATAL, stable

        finished_ns = self._clock_ns()
        return (
            _StreamResult.STOPPED,
            finished_ns - opened_ns >= self._policy.stable_reset_after_ns,
        )

    def _resources_admit(self) -> bool:
        gate = self._resource_gate
        if gate is None:
            return True
        try:
            if self._stop_requested.is_set():
                return True
            decision = gate.evaluate(
                stage="processing",
                estimated_bytes=(
                    self._estimated_backend_bytes + (2 * self._request.max_frame_bytes)
                ),
            )
        except Exception:
            self._set_state(CameraState.FAILED, "resource_admission_failed")
            return False
        if not isinstance(decision, BudgetDecision):
            self._set_state(CameraState.FAILED, "resource_admission_invalid")
            return False
        if not decision.allow_capture or not decision.allow_processing:
            self._set_state(CameraState.FAILED, "resource_admission_denied")
            return False
        return True

    def _run(self) -> None:
        with self._condition:
            self._worker_thread_id = get_ident()
            self._condition.notify_all()

        failed_attempts = 0
        next_epoch = 0
        last_open_attempt_ns: int | None = None
        active_backend: CaptureBackend | None = None
        lease_held = False
        try:
            if self._stop_requested.is_set():
                return
            if not self._resources_admit():
                return
            if self._stop_requested.is_set():
                return
            try:
                lease_held = self._lease.acquire()
            except Exception:
                self._set_state(CameraState.FAILED, "camera_lease_failed")
                return
            if not isinstance(lease_held, bool) or not lease_held:
                self._set_state(CameraState.FAILED, "camera_lease_busy")
                return
            with self._condition:
                self._lease_held = True
                self._condition.notify_all()
            while not self._stop_requested.is_set():
                if next_epoch > _MAX_INT64:
                    self._set_state(CameraState.FAILED, "stream_epoch_exhausted")
                    break
                self._set_state(CameraState.OPENING)
                backend, factory_result = self._create_backend()
                if backend is not None:
                    active_backend = backend
                if self._stop_requested.is_set():
                    if active_backend is not None:
                        self._close_or_retain_backend(active_backend)
                        active_backend = None
                    break
                if factory_result is _FactoryResult.TERMINAL:
                    if active_backend is not None:
                        self._close_or_retain_backend(active_backend)
                        active_backend = None
                    break
                if factory_result is _FactoryResult.RETRY:
                    failed_attempts += 1
                    if not self._schedule_retry(failed_attempts, last_open_attempt_ns):
                        break
                    continue
                if active_backend is None:
                    self._set_state(CameraState.FAILED, "backend_factory_invariant_failed")
                    break

                open_started_ns = self._clock_ns()
                last_open_attempt_ns = open_started_ns
                open_deadline_ns = self._deadline_ns(
                    open_started_ns,
                    self._policy.open_timeout_ns,
                )
                self._increment("open_attempts")
                try:
                    candidate_stream = active_backend.open(
                        self._request,
                        deadline_monotonic_ns=open_deadline_ns,
                    )
                    open_error: Exception | None = None
                except Exception as error:
                    candidate_stream = None
                    open_error = error
                open_finished_ns = self._clock_ns()
                if open_finished_ns < open_started_ns:
                    self._set_state(CameraState.FAILED, "monotonic_clock_moved_backwards")
                    self._close_or_retain_backend(active_backend)
                    active_backend = None
                    break
                if open_finished_ns > open_deadline_ns and not isinstance(
                    open_error,
                    CaptureDeadlineExceeded,
                ):
                    self._increment("contract_violations")
                    self._set_state(CameraState.STUCK, "backend_open_deadline_violated")
                    self._close_or_retain_backend(active_backend)
                    active_backend = None
                    break
                if open_error is not None:
                    self._increment("open_failures")
                    if isinstance(open_error, CaptureDeadlineExceeded):
                        self._set_reason("open_deadline_exceeded")
                    elif isinstance(open_error, CaptureOpenError):
                        self._set_reason("open_failed")
                    elif isinstance(open_error, CaptureError):
                        self._set_reason("backend_open_failed")
                    else:
                        self._increment("backend_failures")
                        self._set_reason("backend_open_unexpected_error")
                    if not self._close_or_retain_backend(active_backend):
                        active_backend = None
                        break
                    active_backend = None
                    failed_attempts += 1
                    if not self._schedule_retry(failed_attempts, last_open_attempt_ns):
                        break
                    continue

                stream = self._validate_stream(candidate_stream)
                if stream is None:
                    self._close_or_retain_backend(active_backend)
                    active_backend = None
                    break
                self._increment("open_successes")
                epoch = next_epoch
                next_epoch += 1
                with self._condition:
                    self._stream_epoch = epoch
                    self._negotiated_stream = stream
                    self._condition.notify_all()

                stream_result, stream_was_stable = self._read_stream(
                    active_backend,
                    stream,
                    epoch=epoch,
                    opened_ns=open_finished_ns,
                )
                close_ok = self._close_or_retain_backend(active_backend)
                active_backend = None
                self._set_active_stream(None)
                if not close_ok:
                    break
                if stream_result in {_StreamResult.STOPPED, _StreamResult.SLOT_CLOSED}:
                    break
                if stream_result is _StreamResult.STUCK:
                    self._set_state(CameraState.STUCK)
                    break
                if stream_result is _StreamResult.FATAL:
                    self._set_state(CameraState.FAILED)
                    break
                if stream_was_stable:
                    failed_attempts = 0
                failed_attempts += 1
                if not self._schedule_retry(failed_attempts, last_open_attempt_ns):
                    break
        except Exception:
            self._increment("backend_failures")
            self._set_state(CameraState.FAILED, "controller_internal_error")
        finally:
            if active_backend is not None:
                self._close_or_retain_backend(active_backend)
            self._set_active_stream(None)
            self._frames.close()
            with self._condition:
                retained_backend = self._retained_backend is not None
            if lease_held and not retained_backend:
                self._release_lease_if_safe()
            with self._condition:
                if self._state not in {CameraState.FAILED, CameraState.STUCK}:
                    self._state = CameraState.CLOSED
                self._condition.notify_all()


__all__ = [
    "DEFAULT_ESTIMATED_BACKEND_BYTES",
    "MAX_ESTIMATED_BACKEND_BYTES",
    "BackendFactory",
    "CameraController",
    "CameraControllerMetrics",
    "CameraLease",
    "FileCameraLease",
    "ResourceAdmission",
]
