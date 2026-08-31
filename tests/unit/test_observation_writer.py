from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import cast

import pytest

from viskium.core import FrameEnvelope, ObservationEnvelope, PersistenceReceipt
from viskium.core.serialization import observation_size
from viskium.storage import SQLiteStore
from viskium.storage.writer import (
    ObservationWriter,
    SubmissionResult,
    SubmissionStatus,
    WriterReceiptMetadata,
    WriterState,
    WriterStopReport,
)


def _observation(sequence: int = 0) -> ObservationEnvelope:
    return ObservationEnvelope(
        session_id="writer-session",
        source_id="writer-source",
        stream_epoch=0,
        source_sequence=sequence,
        observed_monotonic_ns=sequence,
        producer_id="writer-test",
        producer_version="1",
        schema_id="viskium.writer-test",
        schema_version=1,
        payload={"sequence": sequence},
        idempotency_key=f"writer:{sequence}",
        trace_id=f"trace:{sequence}",
        ttl_ns=1_000_000_000,
    )


def _wait_for(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition did not become true before the test timeout")
        time.sleep(0.002)


class RecordingStore:
    def __init__(self, receipts: deque[PersistenceReceipt] | None = None) -> None:
        self.created_thread = threading.get_ident()
        self.put_threads: list[int] = []
        self.close_thread: int | None = None
        self.observations: list[ObservationEnvelope] = []
        self._receipts = receipts

    def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
        self.put_threads.append(threading.get_ident())
        self.observations.append(observation)
        if self._receipts is not None:
            return self._receipts.popleft()
        return PersistenceReceipt(status="accepted", store_sequence=len(self.observations))

    def close(self) -> None:
        self.close_thread = threading.get_ident()


class BlockingStore(RecordingStore):
    def __init__(self, entered: threading.Event, release: threading.Event) -> None:
        super().__init__()
        self._entered = entered
        self._release = release

    def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
        self._entered.set()
        self._release.wait()
        return super().put(observation)


def test_store_lifecycle_and_sqlite_affinity_belong_to_writer_thread(tmp_path: Path) -> None:
    caller_thread = threading.get_ident()
    stores: list[RecordingStore] = []

    def factory() -> RecordingStore:
        store = RecordingStore()
        stores.append(store)
        return store

    writer = ObservationWriter(factory)
    start = writer.start(ready_timeout=1)
    assert start.ready
    assert start.state is WriterState.RUNNING
    assert all(writer.submit(_observation(index)).queued for index in range(3))
    stopped = writer.stop(drain=True, timeout=2)

    assert stopped.clean
    assert stopped.state is WriterState.STOPPED
    assert len(stores) == 1
    store = stores[0]
    assert store.created_thread != caller_thread
    assert store.put_threads == [store.created_thread] * 3
    assert store.close_thread == store.created_thread
    metrics = writer.metrics()
    assert metrics.queued == 3
    assert metrics.persisted_accepted == 3
    assert metrics.pending_count == metrics.pending_bytes == 0
    with pytest.raises(FrozenInstanceError):
        metrics.queued = 99  # type: ignore[misc]

    database = tmp_path / "writer.sqlite3"
    sqlite_writer = ObservationWriter(
        lambda: SQLiteStore(database, volume_reserve_bytes=0, now_unix_ns=lambda: 10)
    )
    assert sqlite_writer.start(ready_timeout=1).ready
    assert sqlite_writer.submit(_observation(10)).queued
    assert sqlite_writer.stop(drain=True, timeout=2).clean
    with SQLiteStore(database, volume_reserve_bytes=0, now_unix_ns=lambda: 10) as reopened:
        assert reopened.health().row_count == 1


def test_submit_is_non_blocking_and_queue_is_bounded_by_count_and_bytes() -> None:
    entered = threading.Event()
    release = threading.Event()
    writer = ObservationWriter(
        lambda: BlockingStore(entered, release),
        max_pending_count=1,
        max_pending_bytes=16_384,
    )
    assert writer.start(ready_timeout=1).ready
    assert writer.submit(_observation(0)).queued
    assert entered.wait(1)
    second = writer.submit(_observation(1))
    full = writer.submit(_observation(2))

    assert second.status is SubmissionStatus.QUEUED
    assert full == SubmissionResult(
        status=SubmissionStatus.REJECTED,
        reason="pending_count_limit_reached",
    )
    metrics = writer.metrics()
    assert metrics.pending_count == 1
    assert metrics.pending_bytes == second.canonical_bytes
    release.set()
    assert writer.stop(drain=True, timeout=2).clean

    size = observation_size(_observation(), stop_after=16_384)
    assert size is not None
    byte_writer = ObservationWriter(lambda: RecordingStore(), max_pending_bytes=size - 1)
    assert byte_writer.start(ready_timeout=1).ready
    rejected = byte_writer.submit(_observation())
    assert rejected.reason == "observation_exceeds_pending_byte_limit"
    assert byte_writer.stop(drain=True, timeout=1).clean


def test_submit_never_accepts_frames_blobs_or_closed_admission() -> None:
    writer = ObservationWriter(lambda: RecordingStore())
    before_start = writer.submit(_observation())
    assert before_start.status is SubmissionStatus.CLOSED
    assert before_start.reason == "writer_not_started"
    assert writer.start(ready_timeout=1).ready

    frame = FrameEnvelope(
        source_id="camera",
        stream_epoch=0,
        sequence=0,
        received_monotonic_ns=0,
        payload=b"raw-frame",
    )
    frame_result = writer.submit(cast(ObservationEnvelope, frame))
    blob_result = writer.submit(cast(ObservationEnvelope, b"raw-blob"))
    assert frame_result.reason == blob_result.reason == "observation_required"
    assert writer.metrics().pending_count == 0

    assert writer.stop(drain=True, timeout=1).clean
    after_stop = writer.submit(_observation(1))
    assert after_stop.status is SubmissionStatus.CLOSED
    assert after_stop.reason == "writer_stopped"
    assert not isinstance(after_stop, PersistenceReceipt)


def test_drain_persists_every_queued_item_and_history_is_bounded() -> None:
    writer = ObservationWriter(
        lambda: RecordingStore(),
        max_pending_count=64,
        receipt_history_limit=32,
    )
    assert writer.start(ready_timeout=1).ready
    assert all(writer.submit(_observation(index)).queued for index in range(40))

    assert writer.stop(drain=True, timeout=2).clean
    metrics = writer.metrics()
    assert metrics.persisted_accepted == 40
    assert metrics.discarded == 0
    assert metrics.recent_receipts == 32
    history = writer.recent_receipts()
    assert len(history) == 32
    assert all(not hasattr(item, "observation") for item in history)


def test_non_drain_stop_discards_pending_explicitly() -> None:
    entered = threading.Event()
    release = threading.Event()
    writer = ObservationWriter(lambda: BlockingStore(entered, release), max_pending_count=4)
    assert writer.start(ready_timeout=1).ready
    assert writer.submit(_observation(0)).queued
    assert entered.wait(1)
    assert writer.submit(_observation(1)).queued
    assert writer.submit(_observation(2)).queued
    reports: list[WriterStopReport] = []

    stopper = threading.Thread(
        target=lambda: reports.append(writer.stop(drain=False, timeout=2)),
    )
    stopper.start()
    _wait_for(lambda: writer.state is WriterState.DRAINING)
    release.set()
    stopper.join(timeout=2)

    assert not stopper.is_alive()
    report = reports[0]
    assert report.clean is True
    assert report.discarded == 2
    metrics = writer.metrics()
    assert metrics.persisted_accepted == 1
    assert metrics.discarded == 2
    assert metrics.pending_count == 0


def test_factory_and_put_failures_are_sanitized_and_never_retried() -> None:
    factory_calls = 0

    def failing_factory() -> RecordingStore:
        nonlocal factory_calls
        factory_calls += 1
        raise ValueError("secret factory detail")

    factory_writer = ObservationWriter(failing_factory)
    start = factory_writer.start(ready_timeout=1)
    assert not start.ready
    assert start.state is WriterState.FAILED
    assert start.reason == "store_factory_failed:ValueError"
    assert "secret" not in cast(str, start.reason)
    assert factory_calls == 1

    closed = threading.Event()

    class FailingPutStore(RecordingStore):
        def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
            raise RuntimeError("secret observation detail")

        def close(self) -> None:
            super().close()
            closed.set()

    put_writer = ObservationWriter(FailingPutStore)
    assert put_writer.start(ready_timeout=1).ready
    assert put_writer.submit(_observation()).queued
    assert closed.wait(1)
    assert put_writer.state is WriterState.FAILED
    metrics = put_writer.metrics()
    assert metrics.persisted_failed == 1
    assert metrics.failure_reason == "store_put_failed:RuntimeError"
    assert "secret" not in cast(str, metrics.failure_reason)


def test_close_failure_is_sanitized_and_shutdown_is_not_reported_clean() -> None:
    class FailingCloseStore(RecordingStore):
        def close(self) -> None:
            raise OSError("secret close detail")

    writer = ObservationWriter(FailingCloseStore)
    assert writer.start(ready_timeout=1).ready
    assert writer.submit(_observation()).queued
    report = writer.stop(drain=True, timeout=1)
    assert not report.clean
    assert report.state is WriterState.FAILED
    assert report.reason == "store_close_failed:OSError"
    assert "secret" not in cast(str, report.reason)


def test_receipt_status_metrics_remain_distinct() -> None:
    receipts = deque(
        [
            PersistenceReceipt(status="accepted", store_sequence=1),
            PersistenceReceipt(status="coalesced", store_sequence=1),
            PersistenceReceipt(status="rejected", reason="policy"),
            PersistenceReceipt(status="failed", reason="disk"),
            PersistenceReceipt(status="gap", reason="best_effort"),
        ]
    )
    writer = ObservationWriter(lambda: RecordingStore(receipts))
    assert writer.start(ready_timeout=1).ready
    assert all(writer.submit(_observation(index)).queued for index in range(5))
    assert writer.stop(drain=True, timeout=2).clean

    metrics = writer.metrics()
    assert metrics.persisted_accepted == 1
    assert metrics.persisted_coalesced == 1
    assert metrics.persisted_rejected == 1
    assert metrics.persisted_failed == 2


def test_non_cooperative_put_uses_daemon_fallback_and_becomes_stuck() -> None:
    entered = threading.Event()
    release = threading.Event()
    writer = ObservationWriter(lambda: BlockingStore(entered, release))
    assert writer.start(ready_timeout=1).ready
    assert writer.submit(_observation()).queued
    assert entered.wait(1)

    report = writer.stop(drain=False, timeout=0.01)
    assert report.state is WriterState.STUCK
    assert not report.clean
    assert report.reason == "stop_timeout"
    assert writer._thread is not None
    assert writer._thread.daemon
    assert writer.submit(_observation(1)).reason == "writer_stuck"
    assert writer.stop(drain=False, timeout=0).state is WriterState.STUCK

    release.set()
    writer._thread.join(timeout=1)
    assert not writer._thread.is_alive()
    assert writer.state is WriterState.STUCK


def test_failed_writer_stop_joins_blocked_close_and_retains_store() -> None:
    close_entered = threading.Event()
    release_close = threading.Event()

    class FailedCloseStore(RecordingStore):
        def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
            raise OSError("injected put failure")

        def close(self) -> None:
            close_entered.set()
            release_close.wait()

    writer = ObservationWriter(FailedCloseStore)
    assert writer.start(ready_timeout=1).ready
    assert writer.submit(_observation()).queued
    assert close_entered.wait(1)
    _wait_for(lambda: writer.state is WriterState.FAILED)

    report = writer.stop(drain=True, timeout=0.01)
    assert report.state is WriterState.STUCK
    assert not report.clean
    assert writer.has_retained_store

    release_close.set()
    assert writer._thread is not None
    writer._thread.join(timeout=1)
    assert writer.state is WriterState.STUCK
    assert writer.stop(drain=True, timeout=1).state is WriterState.STUCK


def test_concurrent_writer_stop_has_a_bounded_lock_wait() -> None:
    close_entered = threading.Event()
    release_close = threading.Event()
    first_finished = threading.Event()

    class BlockingCloseStore(RecordingStore):
        def close(self) -> None:
            close_entered.set()
            release_close.wait()

    writer = ObservationWriter(BlockingCloseStore)
    assert writer.start(ready_timeout=1).ready

    def first_stop() -> None:
        try:
            writer.stop(drain=True, timeout=1)
        finally:
            first_finished.set()

    stopper = threading.Thread(target=first_stop)
    stopper.start()
    assert close_entered.wait(1)

    started = time.monotonic()
    report = writer.stop(drain=True, timeout=0.01)
    assert time.monotonic() - started < 0.2
    assert not report.clean
    assert report.reason == "stop_in_progress"

    release_close.set()
    stopper.join(timeout=1)
    assert first_finished.is_set()


def test_writer_retained_reason_fields_are_bounded() -> None:
    with pytest.raises(ValueError, match="reason"):
        WriterReceiptMetadata(
            status="rejected",
            reason="x" * 129,
            store_sequence=None,
            bytes_accepted=0,
            submitted_bytes=1,
        )


def test_factory_ready_timeout_is_bounded_and_late_store_is_closed_in_owner_thread() -> None:
    release = threading.Event()
    closed = threading.Event()
    store = RecordingStore()

    def slow_factory() -> RecordingStore:
        release.wait()
        store.created_thread = threading.get_ident()
        return store

    original_close = store.close

    def close_and_signal() -> None:
        original_close()
        closed.set()

    store.close = close_and_signal  # type: ignore[method-assign]
    writer = ObservationWriter(slow_factory)
    report = writer.start(ready_timeout=0.01)
    assert report.state is WriterState.STUCK
    assert report.reason == "factory_ready_timeout"
    release.set()
    assert closed.wait(1)
    assert store.close_thread == store.created_thread


def test_stop_before_start_and_repeated_stop_are_idempotent() -> None:
    writer = ObservationWriter(lambda: RecordingStore())
    first = writer.stop(drain=False, timeout=0)
    second = writer.stop(drain=True, timeout=0)
    assert first.clean
    assert second.clean
    assert first.state is second.state is WriterState.STOPPED
    with pytest.raises(RuntimeError, match="started once"):
        writer.start(ready_timeout=1)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_pending_count": 0}, "max_pending_count"),
        ({"max_pending_bytes": True}, "max_pending_bytes"),
        ({"receipt_history_limit": -1}, "receipt_history_limit"),
        ({"thread_name": ""}, "thread_name"),
    ],
)
def test_writer_configuration_is_bounded(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ObservationWriter(lambda: RecordingStore(), **kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), 31])
def test_start_ready_timeout_is_positive_finite_and_bounded(timeout: float) -> None:
    writer = ObservationWriter(lambda: RecordingStore())
    with pytest.raises(ValueError, match="ready_timeout"):
        writer.start(ready_timeout=timeout)
