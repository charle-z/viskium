"""Single-owner observation writer with bounded, non-blocking admission."""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from viskium.core import ObservationEnvelope, ObservationStore, PersistenceReceipt
from viskium.core.serialization import observation_size

_MAX_CONTROL_TIMEOUT_SECONDS = 30.0
_DEFAULT_READY_TIMEOUT_SECONDS = 5.0
_DEFAULT_STOP_TIMEOUT_SECONDS = 5.0
_DEFAULT_PENDING_COUNT = 256
_DEFAULT_PENDING_BYTES = 1_048_576
_DEFAULT_RECEIPT_HISTORY = 32
_MAX_REASON_CHARS = 128


class WriterState(StrEnum):
    NEW = "new"
    RUNNING = "running"
    DRAINING = "draining"
    STOPPED = "stopped"
    FAILED = "failed"
    STUCK = "stuck"


class SubmissionStatus(StrEnum):
    QUEUED = "queued"
    REJECTED = "rejected"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class SubmissionResult:
    status: SubmissionStatus
    reason: str | None = None
    canonical_bytes: int = 0

    def __post_init__(self) -> None:
        _validate_optional_reason(self.reason)

    @property
    def queued(self) -> bool:
        return self.status is SubmissionStatus.QUEUED


@dataclass(frozen=True, slots=True)
class WriterStartReport:
    state: WriterState
    ready: bool
    reason: str | None = None

    def __post_init__(self) -> None:
        _validate_optional_reason(self.reason)


@dataclass(frozen=True, slots=True)
class WriterStopReport:
    state: WriterState
    clean: bool
    discarded: int
    reason: str | None = None

    def __post_init__(self) -> None:
        _validate_optional_reason(self.reason)


@dataclass(frozen=True, slots=True)
class WriterReceiptMetadata:
    status: str
    reason: str | None
    store_sequence: int | None
    bytes_accepted: int
    submitted_bytes: int

    def __post_init__(self) -> None:
        _validate_optional_reason(self.reason)


@dataclass(frozen=True, slots=True)
class WriterMetrics:
    state: WriterState
    queued: int
    rejected: int
    persisted_accepted: int
    persisted_coalesced: int
    persisted_rejected: int
    persisted_failed: int
    discarded: int
    pending_count: int
    pending_bytes: int
    in_flight: bool
    recent_receipts: int
    failure_reason: str | None

    def __post_init__(self) -> None:
        _validate_optional_reason(self.failure_reason, "failure_reason")


@dataclass(frozen=True, slots=True)
class _PendingObservation:
    observation: ObservationEnvelope
    canonical_bytes: int


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _bounded_timeout(value: object, name: str, *, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a finite number")
    timeout = float(value)
    minimum_valid = timeout >= 0.0 if allow_zero else timeout > 0.0
    if not math.isfinite(timeout) or not minimum_valid:
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{name} must be finite and {qualifier}")
    if timeout > _MAX_CONTROL_TIMEOUT_SECONDS:
        raise ValueError(f"{name} exceeds {_MAX_CONTROL_TIMEOUT_SECONDS} seconds")
    return timeout


def _validate_optional_reason(value: object, field_name: str = "reason") -> None:
    if value is None:
        return
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string when provided")
    if not value or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    if len(value) > _MAX_REASON_CHARS:
        raise ValueError(f"{field_name} exceeds {_MAX_REASON_CHARS} characters")


class ObservationWriter:
    """Own an ``ObservationStore`` and its calls inside exactly one daemon thread."""

    def __init__(
        self,
        store_factory: Callable[[], ObservationStore],
        *,
        max_pending_count: int = _DEFAULT_PENDING_COUNT,
        max_pending_bytes: int = _DEFAULT_PENDING_BYTES,
        receipt_history_limit: int = _DEFAULT_RECEIPT_HISTORY,
        thread_name: str = "viskium-observation-writer",
    ) -> None:
        if not callable(store_factory):
            raise TypeError("store_factory must be callable")
        if not isinstance(thread_name, str) or not thread_name.strip():
            raise ValueError("thread_name must be a non-empty string")
        self._store_factory = store_factory
        self._max_pending_count = _positive_integer(max_pending_count, "max_pending_count")
        self._max_pending_bytes = _positive_integer(max_pending_bytes, "max_pending_bytes")
        self._receipt_history_limit = _positive_integer(
            receipt_history_limit, "receipt_history_limit"
        )
        self._thread_name = thread_name
        self._condition = threading.Condition()
        self._stop_lock = threading.Lock()
        self._ready = threading.Event()
        self._queue: deque[_PendingObservation] = deque()
        self._recent: deque[WriterReceiptMetadata] = deque(maxlen=self._receipt_history_limit)
        self._thread: threading.Thread | None = None
        self._active_store: ObservationStore | None = None
        self._retained_store: ObservationStore | None = None
        self._state = WriterState.NEW
        self._failure_reason: str | None = None
        self._stop_requested = False
        self._drain_requested = False
        self._in_flight = False
        self._pending_bytes = 0
        self._queued = 0
        self._rejected = 0
        self._persisted_accepted = 0
        self._persisted_coalesced = 0
        self._persisted_rejected = 0
        self._persisted_failed = 0
        self._discarded = 0

    @property
    def state(self) -> WriterState:
        with self._condition:
            return self._state

    @property
    def has_retained_store(self) -> bool:
        """Whether shutdown retained a store whose close did not meet its deadline."""

        with self._condition:
            return self._retained_store is not None

    def start(
        self,
        *,
        ready_timeout: float = _DEFAULT_READY_TIMEOUT_SECONDS,
    ) -> WriterStartReport:
        timeout = _bounded_timeout(ready_timeout, "ready_timeout")
        with self._condition:
            if self._thread is not None or self._state is not WriterState.NEW:
                raise RuntimeError("ObservationWriter can only be started once")
            thread = threading.Thread(target=self._run, name=self._thread_name, daemon=True)
            self._thread = thread
            thread.start()
        if not self._ready.wait(timeout):
            with self._condition:
                if self._ready.is_set():
                    return WriterStartReport(
                        state=self._state,
                        ready=self._state.value == WriterState.RUNNING.value,
                        reason=self._failure_reason,
                    )
                self._state = WriterState.STUCK
                self._failure_reason = "factory_ready_timeout"
                self._stop_requested = True
                self._condition.notify_all()
                return WriterStartReport(
                    state=self._state,
                    ready=False,
                    reason=self._failure_reason,
                )
        with self._condition:
            return WriterStartReport(
                state=self._state,
                ready=self._state.value == WriterState.RUNNING.value,
                reason=self._failure_reason,
            )

    def submit(self, observation: ObservationEnvelope) -> SubmissionResult:
        if not isinstance(observation, ObservationEnvelope):
            with self._condition:
                self._rejected += 1
            return SubmissionResult(status=SubmissionStatus.REJECTED, reason="observation_required")
        try:
            canonical_bytes = observation_size(
                observation,
                stop_after=self._max_pending_bytes,
            )
        except (RecursionError, TypeError, ValueError):
            canonical_bytes = None
        if canonical_bytes is None:
            with self._condition:
                self._rejected += 1
            return SubmissionResult(
                status=SubmissionStatus.REJECTED,
                reason="observation_exceeds_pending_byte_limit",
            )
        with self._condition:
            if self._state is not WriterState.RUNNING:
                self._rejected += 1
                reason = {
                    WriterState.NEW: "writer_not_started",
                    WriterState.DRAINING: "writer_draining",
                    WriterState.STOPPED: "writer_stopped",
                    WriterState.FAILED: "writer_failed",
                    WriterState.STUCK: "writer_stuck",
                }.get(self._state, "writer_closed")
                return SubmissionResult(status=SubmissionStatus.CLOSED, reason=reason)
            if len(self._queue) >= self._max_pending_count:
                self._rejected += 1
                return SubmissionResult(
                    status=SubmissionStatus.REJECTED,
                    reason="pending_count_limit_reached",
                )
            if self._pending_bytes + canonical_bytes > self._max_pending_bytes:
                self._rejected += 1
                return SubmissionResult(
                    status=SubmissionStatus.REJECTED,
                    reason="pending_byte_limit_reached",
                )
            self._queue.append(
                _PendingObservation(observation=observation, canonical_bytes=canonical_bytes)
            )
            self._pending_bytes += canonical_bytes
            self._queued += 1
            self._condition.notify()
            return SubmissionResult(
                status=SubmissionStatus.QUEUED,
                canonical_bytes=canonical_bytes,
            )

    def stop(
        self,
        *,
        drain: bool,
        timeout: float = _DEFAULT_STOP_TIMEOUT_SECONDS,
    ) -> WriterStopReport:
        selected_timeout = _bounded_timeout(timeout, "timeout", allow_zero=True)
        started = time.monotonic()
        if not self._stop_lock.acquire(timeout=selected_timeout):
            with self._condition:
                return WriterStopReport(
                    state=self._state,
                    clean=False,
                    discarded=0,
                    reason="stop_in_progress",
                )
        try:
            remaining = max(0.0, selected_timeout - (time.monotonic() - started))
            return self._stop_serialized(drain=drain, timeout=remaining)
        finally:
            self._stop_lock.release()

    def _stop_serialized(self, *, drain: bool, timeout: float) -> WriterStopReport:
        with self._condition:
            if self._state is WriterState.STOPPED:
                return WriterStopReport(state=self._state, clean=True, discarded=0)
            thread = self._thread
            if thread is None:
                if self._state is WriterState.NEW:
                    self._state = WriterState.STOPPED
                    return WriterStopReport(state=self._state, clean=True, discarded=0)
                return WriterStopReport(
                    state=self._state,
                    clean=False,
                    discarded=0,
                    reason=self._failure_reason,
                )
            discarded_now = 0
            if self._state in {WriterState.RUNNING, WriterState.DRAINING}:
                if self._state is not WriterState.DRAINING:
                    self._state = WriterState.DRAINING
                    self._stop_requested = True
                    self._drain_requested = drain
                    if not drain:
                        discarded_now = len(self._queue)
                        self._discarded += discarded_now
                        self._queue.clear()
                        self._pending_bytes = 0
                self._condition.notify_all()
            else:
                # FAILED/STUCK are terminal observations, but their worker may
                # still be inside store.close().  Always join it before
                # reporting shutdown, and retain the exact store on timeout.
                self._stop_requested = True
                self._drain_requested = drain
                self._condition.notify_all()
        thread.join(timeout)
        with self._condition:
            if thread.is_alive():
                self._state = WriterState.STUCK
                self._failure_reason = "stop_timeout"
                if self._active_store is not None:
                    self._retained_store = self._active_store
                return WriterStopReport(
                    state=self._state,
                    clean=False,
                    discarded=discarded_now,
                    reason=self._failure_reason,
                )
            return WriterStopReport(
                state=self._state,
                clean=self._state is WriterState.STOPPED,
                discarded=discarded_now,
                reason=self._failure_reason,
            )

    def metrics(self) -> WriterMetrics:
        with self._condition:
            return WriterMetrics(
                state=self._state,
                queued=self._queued,
                rejected=self._rejected,
                persisted_accepted=self._persisted_accepted,
                persisted_coalesced=self._persisted_coalesced,
                persisted_rejected=self._persisted_rejected,
                persisted_failed=self._persisted_failed,
                discarded=self._discarded,
                pending_count=len(self._queue),
                pending_bytes=self._pending_bytes,
                in_flight=self._in_flight,
                recent_receipts=len(self._recent),
                failure_reason=self._failure_reason,
            )

    def recent_receipts(self) -> tuple[WriterReceiptMetadata, ...]:
        with self._condition:
            return tuple(self._recent)

    def _record_receipt(self, receipt: PersistenceReceipt, submitted_bytes: int) -> None:
        if receipt.status == "accepted":
            self._persisted_accepted += 1
        elif receipt.status == "coalesced":
            self._persisted_coalesced += 1
        elif receipt.status == "rejected":
            self._persisted_rejected += 1
        else:
            self._persisted_failed += 1
        self._recent.append(
            WriterReceiptMetadata(
                status=receipt.status,
                reason=receipt.reason,
                store_sequence=receipt.store_sequence,
                bytes_accepted=receipt.bytes_accepted,
                submitted_bytes=submitted_bytes,
            )
        )

    def _discard_pending_locked(self) -> None:
        self._discarded += len(self._queue)
        self._queue.clear()
        self._pending_bytes = 0

    def _run(self) -> None:
        store: ObservationStore | None = None
        try:
            try:
                store = self._store_factory()
                if not isinstance(store, ObservationStore):
                    raise TypeError("factory result does not implement ObservationStore")
            except Exception as error:
                with self._condition:
                    self._state = WriterState.FAILED
                    self._failure_reason = f"store_factory_failed:{type(error).__name__}"
                    self._discard_pending_locked()
                    self._ready.set()
                    self._condition.notify_all()
                return

            with self._condition:
                self._active_store = store
                if self._state is WriterState.STUCK:
                    self._ready.set()
                elif self._stop_requested:
                    self._state = WriterState.DRAINING
                    self._ready.set()
                else:
                    self._state = WriterState.RUNNING
                    self._ready.set()
                self._condition.notify_all()

            while True:
                with self._condition:
                    self._condition.wait_for(lambda: bool(self._queue) or self._stop_requested)
                    if self._queue:
                        pending = self._queue.popleft()
                        self._pending_bytes -= pending.canonical_bytes
                        self._in_flight = True
                    elif self._stop_requested:
                        break
                    else:  # Defensive: the wait predicate makes this branch unreachable.
                        continue
                try:
                    receipt = store.put(pending.observation)
                    if not isinstance(receipt, PersistenceReceipt):
                        raise TypeError("store.put returned an invalid receipt")
                except Exception as error:
                    with self._condition:
                        self._in_flight = False
                        self._persisted_failed += 1
                        self._state = WriterState.FAILED
                        self._failure_reason = f"store_put_failed:{type(error).__name__}"
                        self._discard_pending_locked()
                        self._condition.notify_all()
                    break
                with self._condition:
                    self._in_flight = False
                    self._record_receipt(receipt, pending.canonical_bytes)
                    if self._stop_requested and not self._drain_requested:
                        break
                    self._condition.notify_all()
        finally:
            close_failure: str | None = None
            if store is not None:
                try:
                    store.close()
                except Exception as error:
                    close_failure = f"store_close_failed:{type(error).__name__}"
            with self._condition:
                self._in_flight = False
                if self._state is WriterState.STUCK:
                    if store is not None:
                        self._retained_store = store
                elif close_failure is not None:
                    self._state = WriterState.FAILED
                    self._failure_reason = close_failure
                elif self._state not in {WriterState.FAILED, WriterState.STUCK}:
                    self._state = WriterState.STOPPED
                self._active_store = None
                self._ready.set()
                self._condition.notify_all()


__all__ = [
    "ObservationWriter",
    "SubmissionResult",
    "SubmissionStatus",
    "WriterMetrics",
    "WriterReceiptMetadata",
    "WriterStartReport",
    "WriterState",
    "WriterStopReport",
]
