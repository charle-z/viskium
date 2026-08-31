"""Bounded single-worker coordination for live frames."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from threading import Event, Lock, Thread
from typing import Protocol, runtime_checkable

from viskium.capture.latest import LatestFrameSlot
from viskium.core import FrameEnvelope, ObservationEnvelope, ObservationStore, Processor
from viskium.core.serialization import observation_size
from viskium.observations import LatestObservationSlot
from viskium.resources.budget import BudgetDecision
from viskium.storage.writer import ObservationWriter, SubmissionStatus, WriterState

_MAX_INT64 = 2**63 - 1
_MAX_WRITER_CONTROL_TIMEOUT_SECONDS = 30.0


def _positive_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not 1 <= value <= _MAX_INT64:
        raise ValueError(f"{field_name} must be between one and signed int64 max")
    return value


def _positive_float(value: object, field_name: str, *, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number")
    normalized = float(value)
    if not 0.0 < normalized <= maximum:
        raise ValueError(f"{field_name} must be greater than zero and at most {maximum:g}")
    return normalized


class LiveSchedulerState(StrEnum):
    """Observable lifecycle of one scheduler instance."""

    NEW = "new"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    STUCK = "stuck"


@dataclass(frozen=True, slots=True)
class LiveSchedulerPolicy:
    """Hard freshness, size, polling, and shutdown ceilings."""

    max_frame_age_ns: int = 500_000_000
    max_result_age_ns: int = 2_000_000_000
    max_observation_bytes: int = 1_048_576
    idle_wait_seconds: float = 0.05
    shutdown_timeout_seconds: float = 2.0
    writer_ready_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        _positive_integer(self.max_frame_age_ns, "max_frame_age_ns")
        _positive_integer(self.max_result_age_ns, "max_result_age_ns")
        _positive_integer(self.max_observation_bytes, "max_observation_bytes")
        _positive_float(self.idle_wait_seconds, "idle_wait_seconds", maximum=0.25)
        _positive_float(
            self.shutdown_timeout_seconds,
            "shutdown_timeout_seconds",
            maximum=60.0,
        )
        _positive_float(
            self.writer_ready_timeout_seconds,
            "writer_ready_timeout_seconds",
            maximum=_MAX_WRITER_CONTROL_TIMEOUT_SECONDS,
        )
        if self.max_result_age_ns < self.max_frame_age_ns:
            raise ValueError("max_result_age_ns cannot be shorter than max_frame_age_ns")


@runtime_checkable
class AdmissionGate(Protocol):
    """Small policy boundary used before processing and persistence."""

    def evaluate(
        self,
        *,
        stage: str,
        estimated_bytes: int,
        queue_bytes: int = 0,
        queue_count: int = 0,
    ) -> BudgetDecision: ...


@dataclass(frozen=True, slots=True)
class AlwaysAllowAdmission:
    """Default gate for callers that enforce budgets outside the scheduler."""

    def evaluate(
        self,
        *,
        stage: str,
        estimated_bytes: int,
        queue_bytes: int = 0,
        queue_count: int = 0,
    ) -> BudgetDecision:
        if stage not in {"processing", "persistence"}:
            raise ValueError("unsupported admission stage")
        if isinstance(estimated_bytes, bool) or not isinstance(estimated_bytes, int):
            raise TypeError("estimated_bytes must be an integer")
        if estimated_bytes < 0:
            raise ValueError("estimated_bytes must be non-negative")
        for value, field_name in (
            (queue_bytes, "queue_bytes"),
            (queue_count, "queue_count"),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{field_name} must be an integer")
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")
        return BudgetDecision(
            allow_capture=True,
            allow_processing=True,
            allow_persistence=True,
            severity="normal",
        )


@dataclass(frozen=True, slots=True)
class LiveSchedulerMetrics:
    """Immutable counters; no frames or observations are retained here."""

    frames_taken: int
    frames_expired: int
    frames_future: int
    frames_epoch_rejected: int
    processing_budget_rejected: int
    admission_failures: int
    processor_failures: int
    results_identity_rejected: int
    results_epoch_rejected: int
    results_late: int
    results_prohibited: int
    results_discarded: int
    results_oversized: int
    observations_published: int
    observations_replaced: int
    observations_closed: int
    persistence_accepted: int
    persistence_coalesced: int
    persistence_rejected: int
    persistence_skipped: int
    persistence_failures: int
    persistence_queued: int
    persistence_queue_rejected: int
    persistence_queue_closed: int
    persistence_pending_count: int
    persistence_pending_bytes: int
    persistence_discarded: int
    writer_start_failures: int
    writer_stop_failures: int
    stop_timeouts: int


_COUNTER_NAMES = tuple(LiveSchedulerMetrics.__dataclass_fields__)


class LiveScheduler:
    """Consume a capacity-one frame slot with exactly one processor worker.

    The scheduler owns its processor worker and the lifecycle of a writer supplied
    in ``NEW`` state. A direct store or an already-running writer remains owned by
    the composition root.
    Processor calls are cooperative: a stop timeout marks the scheduler stuck,
    but Python cannot safely terminate a native call running in a thread.
    """

    def __init__(
        self,
        *,
        frames: LatestFrameSlot,
        processor: Processor,
        observations: LatestObservationSlot,
        session_id: str,
        store: ObservationStore | None = None,
        writer: ObservationWriter | None = None,
        policy: LiveSchedulerPolicy | None = None,
        admission: AdmissionGate | None = None,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        current_epoch: Callable[[], int | None] | None = None,
    ) -> None:
        if not isinstance(frames, LatestFrameSlot):
            raise TypeError("frames must be a LatestFrameSlot")
        if not isinstance(processor, Processor):
            raise TypeError("processor must implement Processor")
        if not isinstance(observations, LatestObservationSlot):
            raise TypeError("observations must be a LatestObservationSlot")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id must be a non-empty string")
        if store is not None and not isinstance(store, ObservationStore):
            raise TypeError("store must implement ObservationStore")
        if writer is not None and not isinstance(writer, ObservationWriter):
            raise TypeError("writer must be an ObservationWriter")
        if store is not None and writer is not None:
            raise ValueError("store and writer are mutually exclusive persistence sinks")
        selected_policy = LiveSchedulerPolicy() if policy is None else policy
        selected_admission = AlwaysAllowAdmission() if admission is None else admission
        if not isinstance(selected_policy, LiveSchedulerPolicy):
            raise TypeError("policy must be a LiveSchedulerPolicy")
        if not isinstance(selected_admission, AdmissionGate):
            raise TypeError("admission must implement AdmissionGate")
        if not callable(monotonic_ns):
            raise TypeError("monotonic_ns must be callable")
        if current_epoch is not None and not callable(current_epoch):
            raise TypeError("current_epoch must be callable")

        self._frames = frames
        self._processor = processor
        self._observations = observations
        self._session_id = session_id
        self._store = store
        self._writer = writer
        self._owns_writer = False
        self._policy = selected_policy
        self._admission = selected_admission
        self._monotonic_ns = monotonic_ns
        self._current_epoch = current_epoch
        self._lifecycle_lock = Lock()
        self._state_lock = Lock()
        self._result_lock = Lock()
        self._state = LiveSchedulerState.NEW
        self._stop_requested = Event()
        self._worker: Thread | None = None
        self._counters: dict[str, int] = dict.fromkeys(_COUNTER_NAMES, 0)

    @property
    def state(self) -> LiveSchedulerState:
        with self._state_lock:
            return self._state

    @property
    def metrics(self) -> LiveSchedulerMetrics:
        with self._state_lock:
            counters = dict(self._counters)
        if self._writer is not None:
            writer_metrics = self._writer.metrics()
            counters["persistence_accepted"] += writer_metrics.persisted_accepted
            counters["persistence_coalesced"] += writer_metrics.persisted_coalesced
            counters["persistence_rejected"] += writer_metrics.persisted_rejected
            counters["persistence_failures"] += writer_metrics.persisted_failed
            counters["persistence_pending_count"] = writer_metrics.pending_count
            counters["persistence_pending_bytes"] = writer_metrics.pending_bytes
            counters["persistence_discarded"] = writer_metrics.discarded
        return LiveSchedulerMetrics(**counters)

    def start(self) -> None:
        """Start this one-shot scheduler exactly once."""

        with self._lifecycle_lock:
            self._start_serialized()

    def _start_serialized(self) -> None:
        """Run the complete start transition under the lifecycle lock."""

        with self._state_lock:
            if self._state is not LiveSchedulerState.NEW:
                raise RuntimeError("a live scheduler can only be started once")
            self._state = LiveSchedulerState.RUNNING
        if self._writer is not None:
            try:
                if self._writer.state is WriterState.NEW:
                    writer_start = self._writer.start(
                        ready_timeout=self._policy.writer_ready_timeout_seconds
                    )
                    self._owns_writer = True
                    if not writer_start.ready:
                        raise RuntimeError(writer_start.reason or "writer_not_ready")
                elif self._writer.state is not WriterState.RUNNING:
                    raise RuntimeError(f"writer_not_running:{self._writer.state.value}")
            except Exception as error:
                with self._state_lock:
                    self._counters["writer_start_failures"] += 1
                    self._state = LiveSchedulerState.FAILED
                raise RuntimeError(
                    f"observation writer failed to start: {type(error).__name__}"
                ) from None
        with self._state_lock:
            worker = Thread(
                target=self._run,
                name="viskium-live-processor",
                daemon=True,
            )
            self._worker = worker
        worker.start()

    def stop(self, *, timeout_seconds: float | None = None) -> bool:
        """Request shutdown and report whether the worker stopped in time."""

        timeout = (
            self._policy.shutdown_timeout_seconds
            if timeout_seconds is None
            else _positive_float(timeout_seconds, "timeout_seconds", maximum=60.0)
        )
        started = time.monotonic()
        if not self._lifecycle_lock.acquire(timeout=timeout):
            return False
        try:
            remaining = max(0.0, timeout - (time.monotonic() - started))
            return self._stop_serialized(remaining)
        finally:
            self._lifecycle_lock.release()

    def _stop_serialized(self, timeout: float) -> bool:
        """Run the complete stop transition under the lifecycle lock."""

        deadline = time.monotonic() + timeout
        with self._state_lock:
            if self._state is LiveSchedulerState.NEW:
                self._state = LiveSchedulerState.STOPPED
                return True
            if self._state is LiveSchedulerState.STOPPED and not self._owns_writer:
                return True
            if self._state is LiveSchedulerState.FAILED and not self._owns_writer:
                return True
            if self._state not in {LiveSchedulerState.FAILED, LiveSchedulerState.STUCK}:
                self._state = LiveSchedulerState.STOPPING
            worker = self._worker
        # Event.set() is the stop linearization point.  Do not wait for the
        # result boundary here: a direct store is allowed to be slow or stuck,
        # and stop() must still honor its worker join deadline.
        self._stop_requested.set()

        if worker is not None:
            worker.join(timeout)
        worker_alive = worker is not None and worker.is_alive()
        if worker_alive:
            with self._state_lock:
                self._counters["stop_timeouts"] += 1
                self._state = LiveSchedulerState.STUCK
        writer_clean = True
        if self._writer is not None and self._owns_writer:
            remaining = max(0.0, deadline - time.monotonic())
            writer_clean = self._stop_owned_writer(
                drain=not worker_alive,
                timeout=remaining,
            )
        if worker_alive or not writer_clean:
            return False
        with self._state_lock:
            if self._state is not LiveSchedulerState.FAILED:
                self._state = LiveSchedulerState.STOPPED
        return True

    def _increment(self, name: str) -> None:
        with self._state_lock:
            self._counters[name] += 1

    def _stop_owned_writer(self, *, drain: bool, timeout: float) -> bool:
        """Stop an owned writer and project its terminal state onto the scheduler."""

        if self._writer is None or not self._owns_writer:
            return True
        try:
            writer_stop = self._writer.stop(drain=drain, timeout=timeout)
        except Exception:
            self._increment("writer_stop_failures")
            with self._state_lock:
                if self._state is not LiveSchedulerState.STUCK:
                    self._state = LiveSchedulerState.FAILED
            return False
        if writer_stop.clean:
            return True
        if writer_stop.reason == "stop_in_progress":
            # Another bounded stop owns the writer-control lock.  Leave the
            # scheduler state untouched so a later retry can observe success.
            return False
        with self._state_lock:
            self._counters["writer_stop_failures"] += 1
            if writer_stop.state is WriterState.STUCK:
                self._state = LiveSchedulerState.STUCK
            elif self._state is not LiveSchedulerState.STUCK:
                self._state = LiveSchedulerState.FAILED
        return False

    def _read_epoch(self) -> tuple[bool, int | None]:
        if self._current_epoch is None:
            return True, None
        try:
            value = self._current_epoch()
        except Exception:
            self._increment("admission_failures")
            return False, None
        if value is None:
            return True, None
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _MAX_INT64:
            self._increment("admission_failures")
            return False, None
        return True, value

    def _writer_queue_context(self) -> dict[str, int]:
        if self._writer is None:
            return {}
        writer_metrics = self._writer.metrics()
        return {
            "queue_bytes": writer_metrics.pending_bytes,
            "queue_count": writer_metrics.pending_count,
        }

    def _run(self) -> None:
        try:
            while not self._stop_requested.is_set():
                frame = self._frames.take(timeout_seconds=self._policy.idle_wait_seconds)
                if frame is None:
                    if self._frames.closed:
                        break
                    continue
                self._increment("frames_taken")
                self._process(frame)
        except Exception:
            with self._state_lock:
                self._state = LiveSchedulerState.FAILED
        finally:
            # A closed frame slot is a natural worker exit, not an explicit
            # scheduler stop.  The scheduler still owns a writer started from
            # NEW, so it must close that writer before exposing STOPPED.
            if not self._stop_requested.is_set():
                self._stop_owned_writer(
                    drain=True,
                    timeout=self._policy.shutdown_timeout_seconds,
                )
            with self._state_lock:
                if self._state not in {LiveSchedulerState.FAILED, LiveSchedulerState.STUCK}:
                    self._state = LiveSchedulerState.STOPPED

    def _process(self, frame: FrameEnvelope) -> None:
        now_ns = self._monotonic_ns()
        if frame.received_monotonic_ns > now_ns:
            self._increment("frames_future")
            return
        if now_ns - frame.received_monotonic_ns > self._policy.max_frame_age_ns:
            self._increment("frames_expired")
            return

        epoch_valid, epoch = self._read_epoch()
        if not epoch_valid or (epoch is not None and epoch != frame.stream_epoch):
            self._increment("frames_epoch_rejected")
            return

        try:
            decision = self._admission.evaluate(
                stage="processing",
                estimated_bytes=len(frame.payload),
                **self._writer_queue_context(),
            )
        except Exception:
            self._increment("admission_failures")
            return
        if not decision.allow_processing:
            self._increment("processing_budget_rejected")
            return

        try:
            observation = self._processor.process(frame, session_id=self._session_id)
        except Exception:
            self._increment("processor_failures")
            return
        if self._stop_requested.is_set():
            self._increment("results_discarded")
            return
        if not isinstance(observation, ObservationEnvelope) or not self._identity_matches(
            frame, observation
        ):
            self._increment("results_identity_rejected")
            return
        if observation.sensitivity_class == "prohibited":
            self._increment("results_prohibited")
            return

        epoch_valid, epoch = self._read_epoch()
        if not epoch_valid or (epoch is not None and epoch != frame.stream_epoch):
            self._increment("results_epoch_rejected")
            return
        finished_ns = self._monotonic_ns()
        if (
            finished_ns < frame.received_monotonic_ns
            or finished_ns - frame.received_monotonic_ns > self._policy.max_result_age_ns
        ):
            self._increment("results_late")
            return
        encoded_size = observation_size(
            observation,
            stop_after=self._policy.max_observation_bytes,
        )
        if encoded_size is None:
            self._increment("results_oversized")
            self._increment("persistence_rejected")
            return

        # Stop is checked and linearized with publication.  Persistence is
        # deliberately outside this lock so a blocking direct store cannot
        # prevent stop() from setting the event and joining by its deadline.
        with self._result_lock:
            if self._stop_requested.is_set():
                self._increment("results_discarded")
                return
            offer = self._observations.offer(observation)
            if offer == "accepted":
                self._increment("observations_published")
            elif offer == "replaced":
                self._increment("observations_published")
                self._increment("observations_replaced")
            else:
                self._increment("observations_closed")
                return

        if not self._stop_requested.is_set():
            self._persist(observation, encoded_size=encoded_size)

    def _identity_matches(
        self,
        frame: FrameEnvelope,
        observation: ObservationEnvelope,
    ) -> bool:
        return (
            observation.session_id == self._session_id
            and observation.source_id == frame.source_id
            and observation.stream_epoch == frame.stream_epoch
            and observation.source_sequence == frame.sequence
            and observation.producer_id == self._processor.producer_id
            and observation.producer_version == self._processor.producer_version
        )

    def _persist(
        self,
        observation: ObservationEnvelope,
        *,
        encoded_size: int | None = None,
    ) -> None:
        if self._store is None and self._writer is None:
            self._increment("persistence_skipped")
            return
        if encoded_size is None:
            encoded_size = observation_size(
                observation,
                stop_after=self._policy.max_observation_bytes,
            )
        if encoded_size is None:
            self._increment("persistence_rejected")
            return
        try:
            decision = self._admission.evaluate(
                stage="persistence",
                estimated_bytes=encoded_size,
                **self._writer_queue_context(),
            )
        except Exception:
            self._increment("admission_failures")
            self._increment("persistence_skipped")
            return
        if not decision.allow_persistence:
            self._increment("persistence_skipped")
            return
        if self._writer is not None:
            submission = self._writer.submit(observation)
            if submission.status is SubmissionStatus.QUEUED:
                self._increment("persistence_queued")
            elif submission.status is SubmissionStatus.REJECTED:
                self._increment("persistence_queue_rejected")
                self._increment("persistence_rejected")
            else:
                self._increment("persistence_queue_closed")
                self._increment("persistence_failures")
            return
        try:
            if self._store is None:  # Narrowed defensively after the sink checks above.
                raise RuntimeError("persistence sink disappeared")
            receipt = self._store.put(observation)
        except Exception:
            self._increment("persistence_failures")
            return
        if receipt.status == "accepted":
            self._increment("persistence_accepted")
        elif receipt.status == "coalesced":
            self._increment("persistence_coalesced")
        elif receipt.status == "failed":
            self._increment("persistence_failures")
        else:
            self._increment("persistence_rejected")


__all__ = [
    "AdmissionGate",
    "AlwaysAllowAdmission",
    "LiveScheduler",
    "LiveSchedulerMetrics",
    "LiveSchedulerPolicy",
    "LiveSchedulerState",
]
