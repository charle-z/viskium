from __future__ import annotations

import time
from dataclasses import replace
from pathlib import Path
from threading import Barrier, Event, Thread

import pytest

from viskium.adapters import DeterministicProcessor, MemoryStore
from viskium.capture import LatestFrameSlot
from viskium.core import FrameEnvelope, ObservationEnvelope, PersistenceReceipt
from viskium.observations import LatestObservationSlot
from viskium.resources.budget import BudgetDecision
from viskium.runtime.clocks import VirtualClock
from viskium.runtime.live import (
    AlwaysAllowAdmission,
    LiveScheduler,
    LiveSchedulerPolicy,
    LiveSchedulerState,
)
from viskium.storage import ObservationWriter, SQLiteStore, WriterState


def _frame(sequence: int = 0, *, epoch: int = 1, received_ns: int = 100) -> FrameEnvelope:
    return FrameEnvelope(
        source_id="camera-0",
        stream_epoch=epoch,
        sequence=sequence,
        received_monotonic_ns=received_ns,
        payload=bytes([sequence % 256]),
    )


def _policy(**overrides: object) -> LiveSchedulerPolicy:
    values: dict[str, object] = {
        "max_frame_age_ns": 100,
        "max_result_age_ns": 200,
        "max_observation_bytes": 8_192,
        "idle_wait_seconds": 0.005,
        "shutdown_timeout_seconds": 0.2,
    }
    return LiveSchedulerPolicy(**(values | overrides))  # type: ignore[arg-type]


def _wait_for(predicate: object, *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if callable(predicate) and predicate():
            return
        time.sleep(0.001)
    raise AssertionError("condition did not become true")


def _scheduler(
    *,
    frames: LatestFrameSlot,
    clock: VirtualClock,
    processor: object = None,
    store: object = None,
    writer: object = None,
    observations: LatestObservationSlot | None = None,
    admission: object = None,
    epoch: object = None,
    policy: LiveSchedulerPolicy | None = None,
) -> LiveScheduler:
    return LiveScheduler(
        frames=frames,
        processor=processor or DeterministicProcessor(),  # type: ignore[arg-type]
        observations=LatestObservationSlot() if observations is None else observations,
        session_id="session-1",
        store=store,  # type: ignore[arg-type]
        writer=writer,  # type: ignore[arg-type]
        policy=policy or _policy(),
        admission=admission or AlwaysAllowAdmission(),  # type: ignore[arg-type]
        monotonic_ns=clock.monotonic_ns,
        current_epoch=epoch,  # type: ignore[arg-type]
    )


def test_scheduler_processes_latest_frame_and_persists_once() -> None:
    clock = VirtualClock(100)
    frames = LatestFrameSlot()
    observations = LatestObservationSlot()
    store = MemoryStore(max_observations=4, max_bytes=16_384)
    frames.offer(_frame(0))
    frames.offer(_frame(1))
    scheduler = _scheduler(
        frames=frames,
        clock=clock,
        observations=observations,
        store=store,
        epoch=lambda: 1,
    )

    scheduler.start()
    _wait_for(lambda: scheduler.metrics.observations_published == 1)
    assert scheduler.stop()

    latest = observations.read(now_monotonic_ns=100, max_age_ns=100)
    assert latest.outcome == "ok"
    assert latest.observation is not None
    assert latest.observation.source_sequence == 1
    assert len(store) == 1
    assert scheduler.metrics.frames_taken == 1
    assert scheduler.metrics.persistence_accepted == 1
    assert frames.replaced_count == 1


@pytest.mark.parametrize(
    ("clock_ns", "received_ns", "metric"),
    [(201, 100, "frames_expired"), (99, 100, "frames_future")],
)
def test_scheduler_rejects_frames_outside_time_boundary(
    clock_ns: int,
    received_ns: int,
    metric: str,
) -> None:
    clock = VirtualClock(clock_ns)
    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=clock)

    scheduler.start()
    frames.offer(_frame(received_ns=received_ns))
    _wait_for(lambda: getattr(scheduler.metrics, metric) == 1)
    assert scheduler.stop()

    assert getattr(scheduler.metrics, metric) == 1
    assert scheduler.metrics.observations_published == 0


def test_scheduler_rejects_wrong_epoch_before_processing() -> None:
    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=VirtualClock(100), epoch=lambda: 2)

    scheduler.start()
    frames.offer(_frame(epoch=1))
    _wait_for(lambda: scheduler.metrics.frames_epoch_rejected == 1)
    assert scheduler.stop()

    assert scheduler.metrics.frames_epoch_rejected == 1


def test_scheduler_rejects_epoch_change_and_late_results() -> None:
    class EpochChangingProcessor(DeterministicProcessor):
        def process(self, frame: FrameEnvelope, *, session_id: str) -> ObservationEnvelope:
            epoch[0] = 2
            return super().process(frame, session_id=session_id)

    epoch = [1]
    frames = LatestFrameSlot()
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        processor=EpochChangingProcessor(),
        epoch=lambda: epoch[0],
    )
    scheduler.start()
    frames.offer(_frame())
    _wait_for(lambda: scheduler.metrics.results_epoch_rejected == 1)
    assert scheduler.stop()
    assert scheduler.metrics.results_epoch_rejected == 1

    class SlowProcessor(DeterministicProcessor):
        def process(self, frame: FrameEnvelope, *, session_id: str) -> ObservationEnvelope:
            clock.advance_ns(201)
            return super().process(frame, session_id=session_id)

    clock = VirtualClock(100)
    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=clock, processor=SlowProcessor())
    scheduler.start()
    frames.offer(_frame())
    _wait_for(lambda: scheduler.metrics.results_late == 1)
    assert scheduler.stop()
    assert scheduler.metrics.results_late == 1


def test_processor_failure_is_contained_and_next_frame_can_run() -> None:
    class FailOnceProcessor(DeterministicProcessor):
        def __init__(self) -> None:
            self.fail = True

        def process(self, frame: FrameEnvelope, *, session_id: str) -> ObservationEnvelope:
            if self.fail:
                self.fail = False
                raise RuntimeError("injected")
            return super().process(frame, session_id=session_id)

    frames = LatestFrameSlot()
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        processor=FailOnceProcessor(),
    )
    scheduler.start()
    frames.offer(_frame(0))
    _wait_for(lambda: scheduler.metrics.processor_failures == 1)
    frames.offer(_frame(1))
    _wait_for(lambda: scheduler.metrics.observations_published == 1)
    assert scheduler.stop()


def test_scheduler_rejects_identity_mismatch_and_prohibited_result() -> None:
    class RewritingProcessor(DeterministicProcessor):
        def __init__(self, field: str, value: object) -> None:
            self.field = field
            self.value = value

        def process(self, frame: FrameEnvelope, *, session_id: str) -> ObservationEnvelope:
            result = super().process(frame, session_id=session_id)
            return replace(result, **{self.field: self.value})

    for processor, metric in (
        (RewritingProcessor("source_id", "other"), "results_identity_rejected"),
        (RewritingProcessor("sensitivity_class", "prohibited"), "results_prohibited"),
    ):
        frames = LatestFrameSlot()
        scheduler = _scheduler(frames=frames, clock=VirtualClock(100), processor=processor)
        scheduler.start()
        frames.offer(_frame())
        _wait_for(lambda current=scheduler, counter=metric: getattr(current.metrics, counter) == 1)
        assert scheduler.stop()
        assert getattr(scheduler.metrics, metric) == 1


def test_admission_can_deny_processing_or_only_persistence() -> None:
    class Gate:
        def __init__(self, denied_stage: str) -> None:
            self.denied_stage = denied_stage

        def evaluate(self, *, stage: str, estimated_bytes: int) -> BudgetDecision:
            return BudgetDecision(
                allow_capture=True,
                allow_processing=stage != self.denied_stage,
                allow_persistence=stage != self.denied_stage,
                severity="critical" if stage == self.denied_stage else "normal",
                reasons=("test_pressure",) if stage == self.denied_stage else (),
            )

    processing_frames = LatestFrameSlot()
    denied = _scheduler(
        frames=processing_frames,
        clock=VirtualClock(100),
        admission=Gate("processing"),
    )
    denied.start()
    processing_frames.offer(_frame())
    _wait_for(lambda: denied.metrics.processing_budget_rejected == 1)
    assert denied.stop()
    assert denied.metrics.processing_budget_rejected == 1

    persistence_frames = LatestFrameSlot()
    store = MemoryStore(max_observations=2, max_bytes=8_192)
    persistence = _scheduler(
        frames=persistence_frames,
        clock=VirtualClock(100),
        admission=Gate("persistence"),
        store=store,
    )
    persistence.start()
    persistence_frames.offer(_frame())
    _wait_for(lambda: persistence.metrics.persistence_skipped == 1)
    assert persistence.stop()
    assert persistence.metrics.observations_published == 1
    assert persistence.metrics.persistence_skipped == 1
    assert len(store) == 0


def test_admission_and_store_failures_are_contained() -> None:
    class BrokenGate:
        def evaluate(self, *, stage: str, estimated_bytes: int) -> BudgetDecision:
            raise RuntimeError(stage)

    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=VirtualClock(100), admission=BrokenGate())
    scheduler.start()
    frames.offer(_frame())
    _wait_for(lambda: scheduler.metrics.admission_failures == 1)
    assert scheduler.stop()
    assert scheduler.metrics.admission_failures == 1

    class BrokenStore:
        def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
            raise RuntimeError(observation.idempotency_key)

        def close(self) -> None:
            pass

    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=VirtualClock(100), store=BrokenStore())
    scheduler.start()
    frames.offer(_frame())
    _wait_for(lambda: scheduler.metrics.persistence_failures == 1)
    assert scheduler.stop()
    assert scheduler.metrics.persistence_failures == 1


def test_oversized_observation_is_rejected_before_publication_or_persistence() -> None:
    class LargeProcessor(DeterministicProcessor):
        def process(self, frame: FrameEnvelope, *, session_id: str) -> ObservationEnvelope:
            result = super().process(frame, session_id=session_id)
            return replace(result, payload={"text": "x" * 512})

    frames = LatestFrameSlot()
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        processor=LargeProcessor(),
        store=MemoryStore(max_observations=2, max_bytes=8_192),
        policy=_policy(max_observation_bytes=128),
    )
    scheduler.start()
    frames.offer(_frame())
    _wait_for(lambda: scheduler.metrics.persistence_rejected == 1)
    assert scheduler.stop()
    assert scheduler.metrics.observations_published == 0
    assert scheduler.metrics.results_oversized == 1
    assert scheduler.metrics.persistence_rejected == 1


def test_closed_observation_slot_prevents_persistence() -> None:
    observations = LatestObservationSlot()
    observations.close()
    frames = LatestFrameSlot()
    store = MemoryStore(max_observations=2, max_bytes=8_192)
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        observations=observations,
        store=store,
    )
    scheduler.start()
    frames.offer(_frame())
    _wait_for(lambda: scheduler.metrics.observations_closed == 1)
    assert scheduler.stop()
    assert scheduler.metrics.observations_closed == 1
    assert len(store) == 0


def test_stop_marks_non_cooperative_processor_stuck_then_recovers() -> None:
    entered = Event()
    release = Event()

    class BlockingProcessor(DeterministicProcessor):
        def process(self, frame: FrameEnvelope, *, session_id: str) -> ObservationEnvelope:
            entered.set()
            release.wait(1.0)
            return super().process(frame, session_id=session_id)

    frames = LatestFrameSlot()
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        processor=BlockingProcessor(),
    )
    scheduler.start()
    frames.offer(_frame())
    assert entered.wait(1.0)

    assert not scheduler.stop(timeout_seconds=0.01)
    assert scheduler.state == LiveSchedulerState.STUCK
    assert scheduler.metrics.stop_timeouts == 1
    release.set()
    assert scheduler.stop(timeout_seconds=0.2)
    assert scheduler.state == LiveSchedulerState.STOPPED


def test_result_returning_after_stop_is_discarded_without_publish_or_persist() -> None:
    entered = Event()
    release = Event()

    class BlockingProcessor(DeterministicProcessor):
        def process(self, frame: FrameEnvelope, *, session_id: str) -> ObservationEnvelope:
            entered.set()
            release.wait(1.0)
            return super().process(frame, session_id=session_id)

    frames = LatestFrameSlot()
    observations = LatestObservationSlot()
    store = MemoryStore(max_observations=2, max_bytes=8_192)
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        processor=BlockingProcessor(),
        observations=observations,
        store=store,
    )
    scheduler.start()
    frames.offer(_frame())
    assert entered.wait(1.0)

    assert not scheduler.stop(timeout_seconds=0.01)
    release.set()
    assert scheduler.stop(timeout_seconds=0.2)

    assert scheduler.metrics.results_discarded == 1
    assert scheduler.metrics.observations_published == 0
    assert len(store) == 0
    assert observations.read(now_monotonic_ns=100, max_age_ns=100).outcome == "empty"


def test_stop_deadline_is_not_held_by_blocking_direct_persistence() -> None:
    entered = Event()
    release = Event()

    class BlockingStore:
        def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
            entered.set()
            release.wait()
            return PersistenceReceipt(status="accepted", store_sequence=1)

        def close(self) -> None:
            return None

    frames = LatestFrameSlot()
    observations = LatestObservationSlot()
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        observations=observations,
        store=BlockingStore(),
    )
    scheduler.start()
    frames.offer(_frame(0))
    assert entered.wait(1.0)

    started = time.monotonic()
    assert not scheduler.stop(timeout_seconds=0.01)
    assert time.monotonic() - started < 0.2
    assert scheduler.state is LiveSchedulerState.STUCK

    frames.offer(_frame(1))
    release.set()
    assert scheduler.stop(timeout_seconds=0.2) is True
    assert scheduler.metrics.observations_published == 1
    assert observations.read(now_monotonic_ns=100, max_age_ns=100).observation is not None
    assert observations.read(now_monotonic_ns=100, max_age_ns=100).observation.source_sequence == 0


def test_natural_worker_exit_reports_owned_writer_close_failure() -> None:
    class FailingCloseStore(MemoryStore):
        def close(self) -> None:
            raise OSError("injected close failure")

    writer = ObservationWriter(FailingCloseStore)
    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=VirtualClock(100), writer=writer)
    scheduler.start()
    frames.close()

    _wait_for(lambda: scheduler.state is LiveSchedulerState.FAILED)
    assert writer.state is WriterState.FAILED
    assert not scheduler.stop(timeout_seconds=1.0)


def test_natural_worker_exit_reports_owned_writer_close_timeout_and_retention() -> None:
    entered = Event()
    release = Event()

    class BlockingCloseStore(MemoryStore):
        def close(self) -> None:
            entered.set()
            release.wait()

    writer = ObservationWriter(BlockingCloseStore)
    frames = LatestFrameSlot()
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        writer=writer,
        policy=_policy(shutdown_timeout_seconds=0.05),
    )
    scheduler.start()
    frames.close()

    assert entered.wait(1.0)
    _wait_for(lambda: scheduler.state is LiveSchedulerState.STUCK)
    assert writer.state is WriterState.STUCK
    assert writer.has_retained_store
    release.set()
    assert writer._thread is not None
    writer._thread.join(timeout=1.0)


def test_lifecycle_and_policy_validation() -> None:
    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=VirtualClock(100))
    assert scheduler.stop()
    assert scheduler.stop()
    with pytest.raises(RuntimeError, match="only be started once"):
        scheduler.start()

    with pytest.raises(ValueError, match="shorter"):
        LiveSchedulerPolicy(max_frame_age_ns=2, max_result_age_ns=1)
    with pytest.raises(ValueError, match="at most"):
        LiveSchedulerPolicy(idle_wait_seconds=1.0)
    with pytest.raises(ValueError, match="unsupported"):
        AlwaysAllowAdmission().evaluate(stage="capture", estimated_bytes=1)


def test_closed_frame_slot_stops_idle_scheduler() -> None:
    frames = LatestFrameSlot()
    frames.close()
    scheduler = _scheduler(frames=frames, clock=VirtualClock(100))
    scheduler.start()
    _wait_for(lambda: scheduler.state == LiveSchedulerState.STOPPED)
    assert scheduler.metrics.frames_taken == 0


def test_writer_and_direct_store_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        _scheduler(
            frames=LatestFrameSlot(),
            clock=VirtualClock(100),
            store=MemoryStore(),
            writer=ObservationWriter(MemoryStore),
        )


def test_scheduler_owns_new_writer_and_sqlite_factory_runs_in_writer_thread(
    tmp_path: Path,
) -> None:
    database = tmp_path / "live.sqlite3"
    writer = ObservationWriter(
        lambda: SQLiteStore(database, volume_reserve_bytes=0, now_unix_ns=lambda: 10)
    )
    frames = LatestFrameSlot()
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        writer=writer,
        policy=_policy(shutdown_timeout_seconds=1.0),
    )

    scheduler.start()
    frames.offer(_frame())
    _wait_for(lambda: scheduler.metrics.persistence_queued == 1)
    _wait_for(lambda: scheduler.metrics.persistence_accepted == 1)
    assert scheduler.stop(timeout_seconds=1.0)

    metrics = scheduler.metrics
    assert metrics.persistence_queued == 1
    assert metrics.persistence_accepted == 1
    assert metrics.persistence_pending_count == 0
    assert metrics.persistence_pending_bytes == 0
    assert metrics.writer_start_failures == 0
    assert metrics.writer_stop_failures == 0
    assert writer.state is WriterState.STOPPED
    with SQLiteStore(database, volume_reserve_bytes=0, now_unix_ns=lambda: 10) as reopened:
        assert reopened.health().row_count == 1


def test_scheduler_exposes_writer_backpressure_without_faking_durability() -> None:
    entered = Event()
    release = Event()

    class BlockingStore:
        def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
            entered.set()
            release.wait()
            return PersistenceReceipt(
                status="accepted", store_sequence=observation.source_sequence + 1
            )

        def close(self) -> None:
            return None

    writer = ObservationWriter(
        BlockingStore,
        max_pending_count=1,
        max_pending_bytes=16_384,
    )
    frames = LatestFrameSlot()
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        writer=writer,
        policy=_policy(shutdown_timeout_seconds=1.0),
    )
    scheduler.start()
    frames.offer(_frame(0))
    assert entered.wait(1.0)
    frames.offer(_frame(1))
    _wait_for(lambda: scheduler.metrics.persistence_queued == 2)
    assert scheduler.metrics.persistence_accepted == 0
    assert scheduler.metrics.persistence_pending_count == 1
    frames.offer(_frame(2))
    _wait_for(lambda: scheduler.metrics.persistence_queue_rejected == 1)

    metrics_under_pressure = scheduler.metrics
    assert metrics_under_pressure.persistence_rejected == 1
    assert metrics_under_pressure.persistence_accepted == 0
    release.set()
    _wait_for(lambda: scheduler.metrics.persistence_accepted == 2)
    assert scheduler.stop(timeout_seconds=1.0)
    assert scheduler.metrics.persistence_pending_count == 0


def test_writer_factory_failure_fails_scheduler_start_with_sanitized_error() -> None:
    def fail_factory() -> MemoryStore:
        raise ValueError("secret factory detail")

    scheduler = _scheduler(
        frames=LatestFrameSlot(),
        clock=VirtualClock(100),
        writer=ObservationWriter(fail_factory),
    )
    with pytest.raises(
        RuntimeError, match="observation writer failed to start: RuntimeError"
    ) as info:
        scheduler.start()
    assert "secret" not in str(info.value)
    assert scheduler.state is LiveSchedulerState.FAILED
    assert scheduler.metrics.writer_start_failures == 1


def test_async_writer_put_failure_closes_admission_and_fails_owned_shutdown() -> None:
    class FailingStore:
        def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
            raise OSError("secret persistence detail")

        def close(self) -> None:
            return None

    writer = ObservationWriter(FailingStore)
    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=VirtualClock(100), writer=writer)
    scheduler.start()
    frames.offer(_frame(0))
    _wait_for(lambda: writer.state is WriterState.FAILED)
    assert scheduler.metrics.persistence_failures == 1

    frames.offer(_frame(1))
    _wait_for(lambda: scheduler.metrics.persistence_queue_closed == 1)
    assert scheduler.metrics.persistence_failures == 2
    assert not scheduler.stop(timeout_seconds=1)
    assert scheduler.state is LiveSchedulerState.FAILED
    assert scheduler.metrics.writer_stop_failures == 1


@pytest.mark.parametrize(
    ("status", "metric"),
    [
        ("coalesced", "persistence_coalesced"),
        ("failed", "persistence_failures"),
        ("rejected", "persistence_rejected"),
    ],
)
def test_direct_store_receipt_outcomes_remain_compatible(status: str, metric: str) -> None:
    class ReceiptStore:
        def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
            return PersistenceReceipt(status=status)  # type: ignore[arg-type]

        def close(self) -> None:
            return None

    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=VirtualClock(100), store=ReceiptStore())
    scheduler.start()
    frames.offer(_frame())
    _wait_for(lambda: getattr(scheduler.metrics, metric) == 1)
    assert scheduler.stop()


def test_scheduler_marks_writer_shutdown_stuck_when_put_does_not_cooperate() -> None:
    entered = Event()
    release = Event()

    class BlockingStore:
        def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
            entered.set()
            release.wait()
            return PersistenceReceipt(status="accepted", store_sequence=1)

        def close(self) -> None:
            return None

    writer = ObservationWriter(BlockingStore)
    frames = LatestFrameSlot()
    scheduler = _scheduler(
        frames=frames,
        clock=VirtualClock(100),
        writer=writer,
        policy=_policy(shutdown_timeout_seconds=0.05),
    )
    scheduler.start()
    frames.offer(_frame())
    assert entered.wait(1.0)

    assert not scheduler.stop(timeout_seconds=0.05)
    assert scheduler.state is LiveSchedulerState.STUCK
    assert scheduler.metrics.writer_stop_failures == 1
    assert writer.state is WriterState.STUCK
    release.set()
    _wait_for(lambda: writer.metrics().in_flight is False)


def test_running_external_writer_remains_owned_by_caller() -> None:
    writer = ObservationWriter(lambda: MemoryStore(max_observations=2, max_bytes=8_192))
    assert writer.start(ready_timeout=1).ready
    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=VirtualClock(100), writer=writer)
    scheduler.start()
    frames.offer(_frame())
    _wait_for(lambda: scheduler.metrics.persistence_accepted == 1)

    assert scheduler.stop()
    assert writer.state is WriterState.RUNNING
    assert writer.stop(drain=True, timeout=1).clean


def test_stop_closes_owned_writer_after_scheduler_worker_already_stopped() -> None:
    writer = ObservationWriter(lambda: MemoryStore(max_observations=2, max_bytes=8_192))
    frames = LatestFrameSlot()
    scheduler = _scheduler(frames=frames, clock=VirtualClock(100), writer=writer)
    scheduler.start()
    frames.close()
    _wait_for(lambda: scheduler.state is LiveSchedulerState.STOPPED)
    assert writer.state is WriterState.STOPPED
    assert scheduler.stop(timeout_seconds=1)


def test_concurrent_stop_waits_for_complete_start_and_closes_owned_writer() -> None:
    factory_entered = Barrier(2)
    stop_attempting = Barrier(2)
    release_factory = Event()
    stop_finished = Event()
    start_errors: list[BaseException] = []
    stop_results: list[bool] = []

    def delayed_factory() -> MemoryStore:
        factory_entered.wait(timeout=1)
        if not release_factory.wait(timeout=1):
            raise RuntimeError("test factory was not released")
        return MemoryStore(max_observations=2, max_bytes=8_192)

    writer = ObservationWriter(delayed_factory)
    scheduler = _scheduler(
        frames=LatestFrameSlot(),
        clock=VirtualClock(100),
        writer=writer,
        policy=_policy(
            shutdown_timeout_seconds=1.0,
            writer_ready_timeout_seconds=1.0,
        ),
    )

    def start_scheduler() -> None:
        try:
            scheduler.start()
        except BaseException as error:  # pragma: no cover - asserted below
            start_errors.append(error)

    def stop_scheduler() -> None:
        stop_attempting.wait(timeout=1)
        stop_results.append(scheduler.stop(timeout_seconds=1.0))
        stop_finished.set()

    starter = Thread(target=start_scheduler)
    starter.start()
    factory_entered.wait(timeout=1)
    stopper = Thread(target=stop_scheduler)
    stopper.start()
    stop_attempting.wait(timeout=1)

    assert not stop_finished.wait(0.05)
    release_factory.set()
    starter.join(timeout=2)
    stopper.join(timeout=2)

    assert not starter.is_alive()
    assert not stopper.is_alive()
    assert start_errors == []
    assert stop_results == [True]
    assert scheduler.state is LiveSchedulerState.STOPPED
    assert writer.state is WriterState.STOPPED


def test_stop_does_not_wait_unbounded_for_start_lifecycle_lock() -> None:
    factory_entered = Event()
    release_factory = Event()
    start_errors: list[BaseException] = []

    def delayed_factory() -> MemoryStore:
        factory_entered.set()
        release_factory.wait()
        return MemoryStore(max_observations=2, max_bytes=8_192)

    writer = ObservationWriter(delayed_factory)
    scheduler = _scheduler(
        frames=LatestFrameSlot(),
        clock=VirtualClock(100),
        writer=writer,
        policy=_policy(
            shutdown_timeout_seconds=1.0,
            writer_ready_timeout_seconds=1.0,
        ),
    )

    def start_scheduler() -> None:
        try:
            scheduler.start()
        except BaseException as error:  # pragma: no cover - asserted below
            start_errors.append(error)

    starter = Thread(target=start_scheduler)
    starter.start()
    assert factory_entered.wait(1.0)

    started = time.monotonic()
    assert scheduler.stop(timeout_seconds=0.01) is False
    assert time.monotonic() - started < 0.2
    assert scheduler.state is LiveSchedulerState.RUNNING

    release_factory.set()
    starter.join(timeout=2)
    assert not starter.is_alive()
    assert start_errors == []
    assert scheduler.stop(timeout_seconds=1.0) is True
    assert writer.state is WriterState.STOPPED
