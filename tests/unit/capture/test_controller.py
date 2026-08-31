from __future__ import annotations

import time
from dataclasses import FrozenInstanceError, replace
from threading import Event, get_ident
from threading import Thread as NativeThread
from typing import Any

import pytest

from viskium.adapters.fake_camera import FakeCameraBackend
from viskium.capture import (
    BackendFrame,
    CameraController,
    CameraPolicy,
    CameraState,
    CaptureCapabilities,
    CaptureDeadlineExceeded,
    CaptureRead,
    CaptureRequest,
    CaptureStateError,
    DeadlineCapability,
    LatestFrameSlot,
    NegotiatedStream,
    ReadStatus,
)
from viskium.capture import controller as controller_module
from viskium.resources.budget import BudgetDecision


class InMemoryLease:
    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.acquire_calls = 0
        self.release_calls = 0
        self.held = False

    def acquire(self) -> bool:
        self.acquire_calls += 1
        if not self.available:
            return False
        self.held = True
        return True

    def release(self) -> None:
        self.release_calls += 1
        self.held = False


class SharedLeaseState:
    def __init__(self) -> None:
        self.held = False


class SharedLease(InMemoryLease):
    def __init__(self, state: SharedLeaseState) -> None:
        super().__init__()
        self._shared_state = state

    def acquire(self) -> bool:
        self.acquire_calls += 1
        if self._shared_state.held:
            return False
        self._shared_state.held = True
        self.held = True
        return True

    def release(self) -> None:
        self.release_calls += 1
        self._shared_state.held = False
        self.held = False


def _policy(**overrides: Any) -> CameraPolicy:
    values: dict[str, Any] = {
        "max_frame_bytes": 1_024,
        "open_timeout_ns": 100_000_000,
        "read_timeout_ns": 100_000_000,
        "stale_after_ns": 100_000_000,
        "shutdown_timeout_ns": 200_000_000,
        "initial_cooldown_ns": 1_000_000,
        "maximum_cooldown_ns": 10_000_000,
        "minimum_reopen_interval_ns": 1_000_000,
        "stable_reset_after_ns": 100_000_000,
        "max_reopen_attempts": 3,
        "warmup_frames": 0,
    }
    values.update(overrides)
    return CameraPolicy(**values)


def _request(*, max_frame_bytes: int = 16) -> CaptureRequest:
    return CaptureRequest(0, 2, 2, 15.0, max_frame_bytes)


def _frame(value: int, *, width: int = 2, height: int = 2) -> BackendFrame:
    stride = width
    return BackendFrame(
        payload=bytes((value,)) * (stride * height),
        received_monotonic_ns=time.monotonic_ns(),
        width=width,
        height=height,
        stride=stride,
        pixel_format="GRAY8",
    )


def _controller(
    backend_factory: Any,
    *,
    frames: LatestFrameSlot | None = None,
    policy: CameraPolicy | None = None,
    request: CaptureRequest | None = None,
    monotonic_ns: Any = time.monotonic_ns,
    lease: Any | None = None,
    resource_gate: Any | None = None,
    estimated_backend_bytes: int = controller_module.DEFAULT_ESTIMATED_BACKEND_BYTES,
) -> CameraController:
    return CameraController(
        backend_factory=backend_factory,
        request=_request() if request is None else request,
        policy=_policy() if policy is None else policy,
        frames=LatestFrameSlot() if frames is None else frames,
        source_id="camera-main",
        monotonic_ns=monotonic_ns,
        lease=InMemoryLease() if lease is None else lease,
        resource_gate=resource_gate,
        estimated_backend_bytes=estimated_backend_bytes,
    )


def _wait_until(predicate: Any, *, timeout_seconds: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.001)
    return bool(predicate())


def test_controller_warms_up_then_replaces_with_latest_frame_on_one_owner_thread() -> None:
    slot = LatestFrameSlot()
    backend = FakeCameraBackend(
        reads=[
            CaptureRead(ReadStatus.FRAME, frame=_frame(0)),
            CaptureRead(ReadStatus.FRAME, frame=_frame(1)),
            CaptureRead(ReadStatus.FRAME, frame=_frame(2)),
            CaptureRead(ReadStatus.FRAME, frame=_frame(3)),
        ]
    )
    factory_thread_ids: list[int] = []

    def factory() -> FakeCameraBackend:
        factory_thread_ids.append(get_ident())
        return backend

    controller = _controller(factory, frames=slot, policy=_policy(warmup_frames=2))
    controller.start()

    assert _wait_until(lambda: controller.metrics.frames_offered == 2)
    latest = slot.take(timeout_seconds=0.0)
    assert latest is not None
    assert latest.stream_epoch == 0
    assert latest.sequence == 1
    assert latest.payload == b"\x03" * 4
    assert latest.buffer_id == "fake-camera:0:1"
    assert controller.stream_epoch == 0
    assert controller.negotiated_stream is not None
    assert controller.worker_thread_id == backend.owner_thread_id
    assert factory_thread_ids == [controller.worker_thread_id]

    assert controller.stop()
    assert controller.state is CameraState.CLOSED
    assert not controller.is_alive
    assert backend.close_calls == 1
    metrics = controller.metrics
    assert metrics.frames_read == 4
    assert metrics.warmup_frames_discarded == 2
    assert metrics.frames_offered == 2
    assert metrics.frames_replaced == 1
    assert slot.closed
    with pytest.raises(CaptureStateError, match="only be started once"):
        controller.start()


def test_controller_does_not_create_a_backend_when_the_device_lease_is_busy() -> None:
    read_entered = Event()
    release_read = Event()

    class BlockingFake(FakeCameraBackend):
        def read(self, *, deadline_monotonic_ns: int) -> CaptureRead:
            read_entered.set()
            release_read.wait(timeout=2)
            return super().read(deadline_monotonic_ns=deadline_monotonic_ns)

    shared_state = SharedLeaseState()
    first_backend = BlockingFake(reads=[CaptureRead(ReadStatus.FRAME, frame=_frame(1))])
    first = _controller(lambda: first_backend, lease=SharedLease(shared_state))
    first.start()
    assert read_entered.wait(timeout=1)

    second_backend = FakeCameraBackend(reads=[CaptureRead(ReadStatus.FRAME, frame=_frame(2))])
    second = _controller(lambda: second_backend, lease=SharedLease(shared_state))
    second.start()
    assert second.wait_for_state(CameraState.FAILED, timeout_seconds=1)
    assert _wait_until(lambda: not second.is_alive)
    assert second.metrics.last_reason_code == "camera_lease_busy"
    assert second_backend.open_calls == 0

    release_read.set()
    assert first.stop()


def test_controller_admits_once_before_lease_factory_and_open() -> None:
    events: list[str] = []

    class OrderedLease(InMemoryLease):
        def acquire(self) -> bool:
            events.append("lease.acquire")
            return super().acquire()

    class OrderedBackend(FakeCameraBackend):
        def open(
            self,
            request: CaptureRequest,
            *,
            deadline_monotonic_ns: int,
        ) -> NegotiatedStream:
            events.append("backend.open")
            return super().open(request, deadline_monotonic_ns=deadline_monotonic_ns)

    class OrderedGate:
        def evaluate(self, *, stage: str, estimated_bytes: int) -> BudgetDecision:
            events.append(f"gate.evaluate:{stage}:{estimated_bytes}")
            return BudgetDecision(True, True, True, "normal")

    backend = OrderedBackend(
        reads=[CaptureRead(ReadStatus.FATAL_ERROR, reason_code="stop_after_open")]
    )
    lease = OrderedLease()

    def factory() -> OrderedBackend:
        events.append("factory")
        return backend

    controller = _controller(
        factory,
        lease=lease,
        resource_gate=OrderedGate(),
        estimated_backend_bytes=123_456,
        policy=_policy(max_reopen_attempts=0),
    )
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert events == [
        "gate.evaluate:processing:123488",
        "lease.acquire",
        "factory",
        "backend.open",
    ]
    assert controller.metrics.last_reason_code == "stop_after_open"
    assert backend.close_calls == 1
    assert controller.stop()


@pytest.mark.parametrize(
    ("decision", "reason"),
    [
        (
            BudgetDecision(False, True, True, "critical", ("capture_pressure",)),
            "resource_admission_denied",
        ),
        (
            BudgetDecision(True, False, True, "constrained", ("processing_pressure",)),
            "resource_admission_denied",
        ),
    ],
)
def test_controller_denied_admission_makes_no_hardware_calls(
    decision: BudgetDecision,
    reason: str,
) -> None:
    class Gate:
        def __init__(self) -> None:
            self.calls = 0

        def evaluate(self, *, stage: str, estimated_bytes: int) -> BudgetDecision:
            self.calls += 1
            return decision

    gate = Gate()
    lease = InMemoryLease()
    factory_calls = 0

    def factory() -> FakeCameraBackend:
        nonlocal factory_calls
        factory_calls += 1
        return FakeCameraBackend()

    controller = _controller(factory, lease=lease, resource_gate=gate)
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert gate.calls == 1
    assert lease.acquire_calls == 0
    assert factory_calls == 0
    assert controller.metrics.open_attempts == 0
    assert controller.metrics.last_reason_code == reason
    assert len(reason) <= controller_module.MAX_REASON_CODE_CHARS
    assert controller.stop()


@pytest.mark.parametrize(
    ("gate", "reason"),
    [
        (object(), "resource_admission_invalid"),
        (RuntimeError("untrusted resource details"), "resource_admission_failed"),
    ],
)
def test_controller_admission_contract_failures_make_no_hardware_calls(
    gate: object,
    reason: str,
) -> None:
    class BrokenGate:
        def evaluate(self, *, stage: str, estimated_bytes: int) -> object:
            if isinstance(gate, Exception):
                raise gate
            return gate

    lease = InMemoryLease()
    factory_calls = 0

    def factory() -> FakeCameraBackend:
        nonlocal factory_calls
        factory_calls += 1
        return FakeCameraBackend()

    controller = _controller(factory, lease=lease, resource_gate=BrokenGate())
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert lease.acquire_calls == 0
    assert factory_calls == 0
    assert controller.metrics.open_attempts == 0
    assert controller.metrics.last_reason_code == reason
    assert "untrusted" not in (controller.metrics.last_reason_code or "")
    assert controller.stop()


@pytest.mark.parametrize(
    ("status", "reason_code", "metric_name"),
    [
        (ReadStatus.TIMEOUT, "first_timeout", "read_timeouts"),
        (ReadStatus.DISCONNECTED, "unplugged", "disconnects"),
        (ReadStatus.RECOVERABLE_ERROR, "temporary_read_error", "recoverable_errors"),
    ],
)
def test_controller_closes_before_reconnect_and_increments_epoch(
    status: ReadStatus,
    reason_code: str,
    metric_name: str,
) -> None:
    slot = LatestFrameSlot()
    first = FakeCameraBackend(reads=[CaptureRead(status, reason_code=reason_code)])
    second = FakeCameraBackend(reads=[CaptureRead(ReadStatus.FRAME, frame=_frame(7))])
    instances = [first, second]
    factory_thread_ids: list[int] = []

    def factory() -> FakeCameraBackend:
        factory_thread_ids.append(get_ident())
        index = len(factory_thread_ids) - 1
        if index == 1:
            assert first.close_calls == 1
            assert not first.is_open
        return instances[index]

    controller = _controller(factory, frames=slot)
    controller.start()
    published = slot.take(timeout_seconds=1.0)

    assert published is not None
    assert published.stream_epoch == 1
    assert published.sequence == 0
    assert controller.stop()
    assert first.close_calls == 1
    assert second.close_calls == 1
    assert len(set(factory_thread_ids)) == 1
    assert factory_thread_ids[0] == controller.worker_thread_id
    metrics = controller.metrics
    assert metrics.open_successes == 2
    assert metrics.retries_scheduled == 1
    assert getattr(metrics, metric_name) >= 1


def test_controller_caps_open_retries_without_overlapping_backends() -> None:
    created: list[FakeCameraBackend] = []

    def factory() -> FakeCameraBackend:
        backend = FakeCameraBackend(open_failure_reasons=["busy"])
        created.append(backend)
        return backend

    controller = _controller(factory, policy=_policy(max_reopen_attempts=2))
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert not controller.is_alive
    assert controller.stop()
    assert len(created) == 3
    assert len({backend.owner_thread_id for backend in created}) == 1
    assert created[0].owner_thread_id == controller.worker_thread_id
    metrics = controller.metrics
    assert metrics.backend_instances == 3
    assert metrics.open_attempts == 3
    assert metrics.open_failures == 3
    assert metrics.retries_scheduled == 2
    assert metrics.last_reason_code == "open_failed"


def test_controller_cancels_a_long_cooldown_during_stop() -> None:
    backend = FakeCameraBackend(open_failure_reasons=["busy"])
    policy = _policy(
        open_timeout_ns=10_000_000,
        read_timeout_ns=10_000_000,
        stale_after_ns=10_000_000,
        shutdown_timeout_ns=50_000_000,
        initial_cooldown_ns=5_000_000_000,
        maximum_cooldown_ns=5_000_000_000,
        stable_reset_after_ns=5_000_000_000,
        max_reopen_attempts=1,
    )
    controller = _controller(lambda: backend, policy=policy)
    controller.start()
    assert controller.wait_for_state(CameraState.COOLDOWN, timeout_seconds=1.0)

    started = time.monotonic()
    assert controller.stop()
    elapsed = time.monotonic() - started

    assert elapsed < 0.5
    assert controller.state is CameraState.CLOSED
    assert controller.metrics.retries_scheduled == 1


def test_controller_rejects_unsafe_capabilities_before_open() -> None:
    factory_thread_ids: list[int] = []
    close_thread_ids: list[int] = []

    class UnsafeFake(FakeCameraBackend):
        @property
        def capabilities(self) -> CaptureCapabilities:
            return CaptureCapabilities(
                DeadlineCapability.BEST_EFFORT,
                DeadlineCapability.ENFORCED,
                True,
            )

        def close(self) -> None:
            close_thread_ids.append(get_ident())
            super().close()

    backend = UnsafeFake()

    def factory() -> UnsafeFake:
        factory_thread_ids.append(get_ident())
        return backend

    controller = _controller(factory)
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert controller.stop()
    assert backend.open_calls == 0
    assert factory_thread_ids == close_thread_ids
    assert factory_thread_ids == [controller.worker_thread_id]
    assert controller.metrics.unsafe_backends == 1
    assert controller.metrics.last_reason_code == "unsafe_backend_capabilities"


def test_controller_marks_stuck_when_a_fake_backend_blocks_past_stop_deadline() -> None:
    read_entered = Event()
    release_read = Event()

    class BlockingFake(FakeCameraBackend):
        def read(self, *, deadline_monotonic_ns: int) -> CaptureRead:
            read_entered.set()
            release_read.wait()
            return super().read(deadline_monotonic_ns=deadline_monotonic_ns)

    backend = BlockingFake(reads=[CaptureRead(ReadStatus.FRAME, frame=_frame(1))])
    policy = _policy(
        open_timeout_ns=10_000_000,
        read_timeout_ns=10_000_000,
        stale_after_ns=10_000_000,
        shutdown_timeout_ns=20_000_000,
    )
    controller = _controller(lambda: backend, policy=policy)
    controller.start()
    assert read_entered.wait(1.0)

    try:
        assert not controller.stop()
        assert controller.state is CameraState.STUCK
        assert controller.metrics.stop_timeouts == 1
        assert not controller.stop()
        assert controller.metrics.stop_timeouts == 1
    finally:
        release_read.set()

    assert _wait_until(lambda: not controller.is_alive)
    assert controller.state is CameraState.STUCK
    assert backend.close_calls == 1


def test_controller_never_opens_backend_returned_after_stop_deadline() -> None:
    factory_entered = Event()
    release_factory = Event()
    backend = FakeCameraBackend()

    def factory() -> FakeCameraBackend:
        factory_entered.set()
        release_factory.wait()
        return backend

    controller = _controller(
        factory,
        policy=_policy(
            open_timeout_ns=5_000_000,
            read_timeout_ns=5_000_000,
            stale_after_ns=5_000_000,
            shutdown_timeout_ns=10_000_000,
        ),
    )
    controller.start()
    assert factory_entered.wait(1.0)

    try:
        assert not controller.stop()
        assert controller.state is CameraState.STUCK
    finally:
        release_factory.set()

    assert _wait_until(lambda: not controller.is_alive)
    assert backend.open_calls == 0
    assert backend.owner_thread_id is None


def test_controller_fatal_read_fails_closed_without_retry() -> None:
    backend = FakeCameraBackend(
        reads=[CaptureRead(ReadStatus.FATAL_ERROR, reason_code="device_corrupt")]
    )
    slot = LatestFrameSlot()
    controller = _controller(lambda: backend, frames=slot)
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert controller.stop()
    assert backend.close_calls == 1
    assert slot.closed
    metrics = controller.metrics
    assert metrics.fatal_errors == 1
    assert metrics.retries_scheduled == 0
    assert metrics.last_reason_code == "device_corrupt"


def test_controller_rejects_a_frame_that_does_not_match_negotiated_stream() -> None:
    backend = FakeCameraBackend(
        reads=[CaptureRead(ReadStatus.FRAME, frame=_frame(1, width=1, height=1))]
    )
    controller = _controller(lambda: backend)
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert controller.metrics.contract_violations == 1
    assert controller.metrics.frames_offered == 0
    assert controller.metrics.last_reason_code == "backend_frame_contract_invalid"


def test_controller_detects_a_fake_open_call_returning_after_its_deadline() -> None:
    class LateOpenFake(FakeCameraBackend):
        def open(
            self,
            request: CaptureRequest,
            *,
            deadline_monotonic_ns: int,
        ) -> NegotiatedStream:
            stream = super().open(request, deadline_monotonic_ns=deadline_monotonic_ns)
            time.sleep(0.02)
            return stream

    backend = LateOpenFake()
    controller = _controller(
        lambda: backend,
        policy=_policy(
            open_timeout_ns=1_000_000,
            read_timeout_ns=1_000_000,
            stale_after_ns=1_000_000,
            shutdown_timeout_ns=50_000_000,
        ),
    )
    controller.start()

    assert controller.wait_for_state(CameraState.STUCK, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert controller.metrics.contract_violations == 1
    assert controller.metrics.last_reason_code == "backend_open_deadline_violated"


def test_controller_keeps_the_same_epoch_after_a_non_stale_timeout() -> None:
    slot = LatestFrameSlot()
    backend = FakeCameraBackend(
        reads=[
            CaptureRead(ReadStatus.TIMEOUT, reason_code="short_timeout"),
            CaptureRead(ReadStatus.FRAME, frame=_frame(8)),
        ]
    )
    controller = _controller(
        lambda: backend,
        frames=slot,
        policy=_policy(
            open_timeout_ns=10_000_000,
            read_timeout_ns=10_000_000,
            stale_after_ns=100_000_000,
            shutdown_timeout_ns=50_000_000,
        ),
    )
    controller.start()
    published = slot.take(timeout_seconds=1.0)

    assert published is not None
    assert published.stream_epoch == 0
    assert published.sequence == 0
    assert controller.metrics.read_timeouts >= 1
    assert controller.metrics.retries_scheduled == 0
    assert controller.stop()


def test_controller_stops_when_slot_closes_during_an_in_flight_fake_read() -> None:
    read_entered = Event()
    release_read = Event()

    class GatedFake(FakeCameraBackend):
        def read(self, *, deadline_monotonic_ns: int) -> CaptureRead:
            read_entered.set()
            release_read.wait()
            return super().read(deadline_monotonic_ns=deadline_monotonic_ns)

    slot = LatestFrameSlot()
    backend = GatedFake(reads=[CaptureRead(ReadStatus.FRAME, frame=_frame(5))])
    controller = _controller(lambda: backend, frames=slot)
    controller.start()
    assert read_entered.wait(1.0)

    slot.close()
    release_read.set()

    assert _wait_until(lambda: not controller.is_alive)
    assert controller.state is CameraState.CLOSED
    assert backend.close_calls == 1
    assert controller.metrics.frames_rejected_closed == 1
    assert controller.metrics.last_reason_code == "frame_slot_closed"


@pytest.mark.parametrize("mode", ["raises", "wrong_type"])
def test_controller_rejects_invalid_capabilities_contracts(mode: str) -> None:
    class InvalidCapabilitiesFake(FakeCameraBackend):
        @property
        def capabilities(self) -> CaptureCapabilities:
            if mode == "raises":
                raise RuntimeError("scripted capability failure")
            return object()  # type: ignore[return-value]

    backend = InvalidCapabilitiesFake()
    controller = _controller(lambda: backend)
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    if mode == "raises":
        assert controller.metrics.backend_failures == 1
        assert controller.metrics.last_reason_code == "backend_capabilities_failed"
    else:
        assert controller.metrics.contract_violations == 1
        assert controller.metrics.last_reason_code == "backend_capabilities_invalid"


@pytest.mark.parametrize(
    ("error", "expected_reason", "backend_failures"),
    [
        (CaptureDeadlineExceeded("scripted"), "open_deadline_exceeded", 0),
        (CaptureStateError("scripted"), "backend_open_failed", 0),
        (RuntimeError("scripted"), "backend_open_unexpected_error", 1),
    ],
)
def test_controller_classifies_timely_open_errors_without_leaking_backend(
    error: Exception,
    expected_reason: str,
    backend_failures: int,
) -> None:
    class OpenErrorFake(FakeCameraBackend):
        def open(
            self,
            request: CaptureRequest,
            *,
            deadline_monotonic_ns: int,
        ) -> NegotiatedStream:
            raise error

    backend = OpenErrorFake()
    controller = _controller(lambda: backend, policy=_policy(max_reopen_attempts=0))
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert controller.metrics.open_failures == 1
    assert controller.metrics.backend_failures == backend_failures
    assert controller.metrics.last_reason_code == expected_reason


@pytest.mark.parametrize(
    ("mode", "expected_reason", "contract_violations"),
    [
        ("capture_error", "backend_read_failed", 0),
        ("unexpected_error", "backend_read_unexpected_error", 0),
        ("invalid_result", "backend_read_result_invalid", 1),
    ],
)
def test_controller_rejects_fake_read_exceptions_and_invalid_results(
    mode: str,
    expected_reason: str,
    contract_violations: int,
) -> None:
    class InvalidReadFake(FakeCameraBackend):
        def read(self, *, deadline_monotonic_ns: int) -> CaptureRead:
            if mode == "capture_error":
                raise CaptureStateError("scripted")
            if mode == "unexpected_error":
                raise RuntimeError("scripted")
            return object()  # type: ignore[return-value]

    backend = InvalidReadFake()
    controller = _controller(lambda: backend, policy=_policy(max_reopen_attempts=0))
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert controller.metrics.contract_violations == contract_violations
    assert controller.metrics.last_reason_code == expected_reason
    if mode != "invalid_result":
        assert controller.metrics.backend_failures == 1


@pytest.mark.parametrize("mode", ["wrong_type", "over_budget"])
def test_controller_rejects_invalid_negotiated_streams(mode: str) -> None:
    class InvalidStreamFake(FakeCameraBackend):
        def open(
            self,
            request: CaptureRequest,
            *,
            deadline_monotonic_ns: int,
        ) -> NegotiatedStream:
            super().open(request, deadline_monotonic_ns=deadline_monotonic_ns)
            if mode == "wrong_type":
                return object()  # type: ignore[return-value]
            return NegotiatedStream("malicious-fake", 4, 4, 15.0, "GRAY8", 4)

    backend = InvalidStreamFake()
    controller = _controller(lambda: backend, request=_request(max_frame_bytes=4))
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert backend.close_calls == 1
    assert controller.metrics.contract_violations == 1
    expected = (
        "negotiated_stream_invalid" if mode == "wrong_type" else "negotiated_stream_exceeds_limit"
    )
    assert controller.metrics.last_reason_code == expected


@pytest.mark.parametrize("mode", ["raises", "late"])
def test_controller_fails_closed_when_backend_close_violates_contract(mode: str) -> None:
    class InvalidCloseFake(FakeCameraBackend):
        def close(self) -> None:
            super().close()
            if mode == "raises":
                raise RuntimeError("scripted close failure")
            time.sleep(0.02)

    backend = InvalidCloseFake(
        reads=[CaptureRead(ReadStatus.FATAL_ERROR, reason_code="fatal_before_close")]
    )
    lease = InMemoryLease()
    factory_calls = 0

    def factory() -> FakeCameraBackend:
        nonlocal factory_calls
        factory_calls += 1
        return backend

    slot = LatestFrameSlot()
    controller = _controller(
        factory,
        frames=slot,
        policy=_policy(
            open_timeout_ns=1_000_000,
            read_timeout_ns=1_000_000,
            stale_after_ns=1_000_000,
            shutdown_timeout_ns=5_000_000,
        ),
        lease=lease,
    )
    controller.start()

    terminal = CameraState.FAILED if mode == "raises" else CameraState.STUCK
    assert controller.wait_for_state(terminal, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert controller.has_retained_backend
    assert controller._retained_backend is backend
    assert factory_calls == 1
    assert backend.close_calls == 1
    assert controller.negotiated_stream is None
    assert slot.closed
    assert slot.pending_count == 0
    assert controller.stop()
    assert backend.close_calls == 1
    with pytest.raises(CaptureStateError, match="only be started once"):
        controller.start()
    assert factory_calls == 1
    if mode == "raises":
        assert controller.metrics.backend_failures == 1
        assert controller.metrics.last_reason_code == "backend_close_failed"
    else:
        assert controller.metrics.contract_violations == 1
        assert controller.metrics.last_reason_code == "backend_close_deadline_violated"
    assert lease.release_calls == 0


def test_controller_resets_retry_count_after_a_stable_epoch() -> None:
    class StableThenDisconnectFake(FakeCameraBackend):
        def read(self, *, deadline_monotonic_ns: int) -> CaptureRead:
            time.sleep(0.005)
            return super().read(deadline_monotonic_ns=deadline_monotonic_ns)

    first = FakeCameraBackend(
        reads=[CaptureRead(ReadStatus.DISCONNECTED, reason_code="first_disconnect")]
    )
    second = StableThenDisconnectFake(
        reads=[CaptureRead(ReadStatus.DISCONNECTED, reason_code="stable_disconnect")]
    )
    third = FakeCameraBackend(reads=[CaptureRead(ReadStatus.FRAME, frame=_frame(9))])
    backends = [first, second, third]
    factory_calls = 0

    def factory() -> FakeCameraBackend:
        nonlocal factory_calls
        backend = backends[factory_calls]
        factory_calls += 1
        return backend

    slot = LatestFrameSlot()
    controller = _controller(
        factory,
        frames=slot,
        policy=_policy(
            max_reopen_attempts=1,
            stable_reset_after_ns=1_000_000,
        ),
    )
    controller.start()
    published = slot.take(timeout_seconds=1.0)

    assert published is not None
    assert published.stream_epoch == 2
    assert factory_calls == 3
    assert controller.metrics.retries_scheduled == 2
    assert controller.stop()


def test_controller_rejects_invalid_factory_products_and_factory_failures() -> None:
    invalid_product = _controller(lambda: object())
    invalid_product.start()
    assert invalid_product.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not invalid_product.is_alive)
    assert invalid_product.metrics.contract_violations == 1
    assert invalid_product.metrics.last_reason_code == "backend_protocol_invalid"

    def failing_factory() -> FakeCameraBackend:
        raise RuntimeError("scripted factory failure")

    failing = _controller(failing_factory, policy=_policy(max_reopen_attempts=0))
    failing.start()
    assert failing.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not failing.is_alive)
    assert failing.metrics.backend_failures == 1
    assert failing.metrics.last_reason_code == "backend_factory_failed"


def test_controller_retries_after_one_factory_failure_without_spending_an_epoch() -> None:
    slot = LatestFrameSlot()
    backend = FakeCameraBackend(reads=[CaptureRead(ReadStatus.FRAME, frame=_frame(4))])
    calls = 0

    def factory() -> FakeCameraBackend:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("scripted first factory failure")
        return backend

    controller = _controller(factory, frames=slot, policy=_policy(max_reopen_attempts=1))
    controller.start()
    published = slot.take(timeout_seconds=1.0)

    assert published is not None
    assert published.stream_epoch == 0
    assert calls == 2
    assert controller.metrics.backend_failures == 1
    assert controller.metrics.retries_scheduled == 1
    assert controller.stop()


def test_controller_treats_reported_read_deadline_as_timeout_not_contract_violation() -> None:
    class DeadlineReadFake(FakeCameraBackend):
        def read(self, *, deadline_monotonic_ns: int) -> CaptureRead:
            raise CaptureDeadlineExceeded("scripted")

    backend = DeadlineReadFake()
    controller = _controller(
        lambda: backend,
        policy=_policy(
            open_timeout_ns=1_000_000,
            read_timeout_ns=1_000_000,
            stale_after_ns=1_000_000,
            shutdown_timeout_ns=10_000_000,
            max_reopen_attempts=0,
        ),
    )
    controller.start()

    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert controller.metrics.read_timeouts == 1
    assert controller.metrics.contract_violations == 0


def test_controller_fails_closed_when_worker_thread_cannot_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StartFailureThread:
        def __init__(self, **kwargs: Any) -> None:
            del kwargs

        def start(self) -> None:
            raise RuntimeError("scripted thread start failure")

    slot = LatestFrameSlot()
    controller = _controller(FakeCameraBackend, frames=slot)
    monkeypatch.setattr(controller_module, "Thread", StartFailureThread)

    with pytest.raises(RuntimeError, match="thread start failure"):
        controller.start()

    assert controller.state is CameraState.FAILED
    assert controller.metrics.last_reason_code == "worker_start_failed"
    assert slot.closed


def test_start_and_stop_cannot_join_before_worker_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start_entered = Event()
    release_start = Event()

    class DelayedStartThread:
        def __init__(self, *, target: Any, name: str, daemon: bool) -> None:
            self._inner = NativeThread(target=target, name=name, daemon=daemon)

        @property
        def ident(self) -> int | None:
            return self._inner.ident

        def start(self) -> None:
            start_entered.set()
            release_start.wait()
            self._inner.start()

        def join(self, timeout: float | None = None) -> None:
            self._inner.join(timeout)

        def is_alive(self) -> bool:
            return self._inner.is_alive()

    controller = _controller(FakeCameraBackend)
    monkeypatch.setattr(controller_module, "Thread", DelayedStartThread)
    start_errors: list[Exception] = []
    stop_results: list[bool] = []

    def start_controller() -> None:
        try:
            controller.start()
        except Exception as error:
            start_errors.append(error)

    starter = NativeThread(target=start_controller)
    stopper = NativeThread(target=lambda: stop_results.append(controller.stop()))
    starter.start()
    assert start_entered.wait(1.0)
    stopper.start()
    try:
        assert _wait_until(stopper.is_alive)
    finally:
        release_start.set()
    starter.join(1.0)
    stopper.join(1.0)

    assert not starter.is_alive()
    assert not stopper.is_alive()
    assert start_errors == []
    assert stop_results == [True]
    assert controller.state is CameraState.CLOSED


def test_controller_stop_before_start_is_idempotent_and_closes_slot() -> None:
    slot = LatestFrameSlot()
    controller = _controller(FakeCameraBackend, frames=slot)

    assert controller.stop()
    assert controller.stop()
    assert controller.state is CameraState.CLOSED
    assert slot.closed
    with pytest.raises(CaptureStateError, match="closed latest-frame slot"):
        controller.start()


def test_controller_validates_inputs_waits_deadlines_and_frozen_metrics() -> None:
    slot = LatestFrameSlot()
    controller = _controller(FakeCameraBackend, frames=slot)
    metrics = controller.metrics

    with pytest.raises(FrozenInstanceError):
        metrics.open_attempts = 1  # type: ignore[misc]
    with pytest.raises(ValueError, match="signed int64"):
        replace(metrics, open_attempts=-1)
    with pytest.raises(ValueError, match="exceeds"):
        replace(metrics, last_reason_code="x" * 129)
    with pytest.raises(TypeError, match="string"):
        replace(metrics, last_reason_code=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="CameraState"):
        controller.wait_for_state("closed", timeout_seconds=0.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="number"):
        controller.wait_for_state(CameraState.CLOSED, timeout_seconds=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="finite"):
        controller.wait_for_state(CameraState.CLOSED, timeout_seconds=float("nan"))
    with pytest.raises(TypeError, match="integer"):
        controller.stop(deadline_monotonic_ns=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="signed int64"):
        controller.stop(deadline_monotonic_ns=-1)


@pytest.mark.parametrize(
    ("override", "match"),
    [
        ({"backend_factory": None}, "backend_factory"),
        ({"request": object()}, "CaptureRequest"),
        ({"policy": object()}, "CameraPolicy"),
        ({"frames": object()}, "LatestFrameSlot"),
        ({"source_id": ""}, "must not be empty"),
        ({"source_id": 1}, "must be a string"),
        ({"source_id": "x" * 129}, "exceeds"),
        ({"monotonic_ns": None}, "monotonic_ns"),
    ],
)
def test_controller_constructor_rejects_invalid_inputs(
    override: dict[str, object],
    match: str,
) -> None:
    values: dict[str, object] = {
        "backend_factory": FakeCameraBackend,
        "request": _request(),
        "policy": _policy(),
        "frames": LatestFrameSlot(),
        "source_id": "camera-main",
        "monotonic_ns": time.monotonic_ns,
    }
    values.update(override)
    with pytest.raises((TypeError, ValueError), match=match):
        CameraController(**values)  # type: ignore[arg-type]


def test_controller_rejects_request_budget_above_policy_and_invalid_clock_result() -> None:
    with pytest.raises(ValueError, match="camera policy"):
        _controller(
            FakeCameraBackend,
            request=_request(max_frame_bytes=16),
            policy=_policy(max_frame_bytes=8),
        )

    backend = FakeCameraBackend()
    controller = _controller(lambda: backend, monotonic_ns=lambda: -1)
    controller.start()
    assert controller.wait_for_state(CameraState.FAILED, timeout_seconds=1.0)
    assert _wait_until(lambda: not controller.is_alive)
    assert backend.close_calls == 0
    assert controller.metrics.last_reason_code == "controller_internal_error"
