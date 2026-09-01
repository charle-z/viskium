from __future__ import annotations

import math
from dataclasses import FrozenInstanceError
from threading import Event, Thread, get_ident
from typing import Any

import pytest

from viskium.agent import (
    CameraSnapshotMetrics,
    CameraSnapshotProvider,
    SnapshotCaptureResult,
    SnapshotProvider,
)
from viskium.agent import camera_snapshot as camera_snapshot_module
from viskium.capture import (
    BackendFrame,
    CaptureBackend,
    CaptureCapabilities,
    CaptureDeadlineExceeded,
    CaptureOpenError,
    CaptureRead,
    CaptureRequest,
    DeadlineCapability,
    NegotiatedStream,
    ReadStatus,
)
from viskium.core import FrameEnvelope
from viskium.resources.budget import BudgetDecision
from viskium.snapshots import (
    MAX_SNAPSHOT_BYTES,
    MAX_SNAPSHOT_EDGE_PX,
    MAX_SNAPSHOT_WAIT_SECONDS,
    SnapshotEnvelope,
)


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


class MutableClock:
    def __init__(self, value: int = 0) -> None:
        self.value = value

    def __call__(self) -> int:
        return self.value


class SequenceClock:
    def __init__(self, values: list[int]) -> None:
        self._values = list(values)
        self._last = values[-1]

    def __call__(self) -> int:
        if self._values:
            self._last = self._values.pop(0)
        return self._last


class ScriptedBackend:
    def __init__(
        self,
        reads: list[CaptureRead | object | Exception],
        *,
        capabilities: CaptureCapabilities | object | None = None,
        open_result: NegotiatedStream | object | None = None,
        open_error: Exception | None = None,
        close_error: BaseException | None = None,
        worker_state: object | None = None,
        read_entered: Event | None = None,
        read_release: Event | None = None,
        read_hook: Any = None,
    ) -> None:
        self._capabilities = (
            CaptureCapabilities(
                DeadlineCapability.ENFORCED,
                DeadlineCapability.ENFORCED,
                True,
            )
            if capabilities is None
            else capabilities
        )
        self._open_result = _stream() if open_result is None else open_result
        self._open_error = open_error
        self._close_error = close_error
        self._worker_state = worker_state
        self._read_entered = read_entered
        self._read_release = read_release
        self._read_hook = read_hook
        self.reads = list(reads)
        self.events: list[tuple[str, int, int | None]] = []
        self.open_requests: list[CaptureRequest] = []
        self.close_calls = 0

    @property
    def capabilities(self) -> CaptureCapabilities:
        return self._capabilities  # type: ignore[return-value]

    @property
    def worker_state(self) -> object | None:
        return self._worker_state

    def open(
        self,
        request: CaptureRequest,
        *,
        deadline_monotonic_ns: int,
    ) -> NegotiatedStream:
        self.events.append(("open", get_ident(), deadline_monotonic_ns))
        self.open_requests.append(request)
        if self._open_error is not None:
            raise self._open_error
        return self._open_result  # type: ignore[return-value]

    def read(self, *, deadline_monotonic_ns: int) -> CaptureRead:
        self.events.append(("read", get_ident(), deadline_monotonic_ns))
        if self._read_entered is not None:
            self._read_entered.set()
        if self._read_release is not None:
            assert self._read_release.wait(timeout=2)
        if self._read_hook is not None:
            self._read_hook()
        result = self.reads.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result  # type: ignore[return-value]

    def close(self) -> None:
        self.events.append(("close", get_ident(), None))
        self.close_calls += 1
        if self._close_error is not None:
            raise self._close_error


class BackendFactory:
    def __init__(
        self,
        backends: list[object],
        *,
        error: Exception | None = None,
    ) -> None:
        self.backends = list(backends)
        self.error = error
        self.calls: list[int] = []

    def __call__(self) -> CaptureBackend:
        self.calls.append(get_ident())
        if self.error is not None:
            raise self.error
        return self.backends.pop(0)  # type: ignore[return-value]


class AdmissionGate:
    def __init__(
        self,
        decision: BudgetDecision | object,
        *,
        error: Exception | None = None,
    ) -> None:
        self.decision = decision
        self.error = error
        self.calls: list[tuple[str, int]] = []

    def evaluate(self, *, stage: str, estimated_bytes: int) -> BudgetDecision:
        self.calls.append((stage, estimated_bytes))
        if self.error is not None:
            raise self.error
        return self.decision  # type: ignore[return-value]


def _request() -> CaptureRequest:
    return CaptureRequest(
        device_index=0,
        requested_width=2,
        requested_height=2,
        requested_fps=15.0,
        max_frame_bytes=12,
    )


def _stream() -> NegotiatedStream:
    return NegotiatedStream(
        backend_id="test-camera",
        width=2,
        height=2,
        fps=15.0,
        pixel_format="bgr24",
        stride=6,
    )


def _frame(*, received_monotonic_ns: int = 0) -> BackendFrame:
    return BackendFrame(
        payload=bytes(range(12)),
        received_monotonic_ns=received_monotonic_ns,
        width=2,
        height=2,
        stride=6,
        pixel_format="bgr24",
    )


def _frame_read(*, received_monotonic_ns: int = 0) -> CaptureRead:
    return CaptureRead(ReadStatus.FRAME, frame=_frame(received_monotonic_ns=received_monotonic_ns))


def _provider(
    factory: BackendFactory,
    *,
    clock: Any = None,
    gate: AdmissionGate | None = None,
    warmup_frames: int = 0,
    max_attempts: int = 1,
    minimum_interval: float = 0.0,
    estimated_backend_bytes: int = camera_snapshot_module.DEFAULT_ESTIMATED_BACKEND_BYTES,
    lease: Any | None = None,
) -> CameraSnapshotProvider:
    return CameraSnapshotProvider(
        backend_factory=factory,
        capture_request=_request(),
        resource_gate=gate,  # type: ignore[arg-type]
        warmup_frames=warmup_frames,
        max_attempts=max_attempts,
        minimum_open_interval_seconds=minimum_interval,
        estimated_backend_bytes=estimated_backend_bytes,
        monotonic_ns=MutableClock() if clock is None else clock,
        lease=InMemoryLease() if lease is None else lease,
    )


def _capture(provider: CameraSnapshotProvider, *, timeout: float = 1.0) -> SnapshotCaptureResult:
    return provider.capture(max_edge_px=64, max_bytes=1_024, timeout_seconds=timeout)


def _allowed() -> BudgetDecision:
    return BudgetDecision(True, True, True, "normal")


def _denied() -> BudgetDecision:
    return BudgetDecision(False, False, True, "critical", ("memory_reserve",))


def test_one_shot_opens_reads_encodes_and_closes_on_the_calling_thread() -> None:
    backend = ScriptedBackend([_frame_read()])
    factory = BackendFactory([backend])
    provider = _provider(factory)
    owner = get_ident()

    result = _capture(provider)

    assert result.outcome == "ok"
    assert result.snapshot is not None
    assert result.snapshot.source_id == "camera-0"
    assert result.snapshot.stream_epoch == 1
    assert result.snapshot.source_sequence == 0
    assert result.snapshot.sensitivity_class == "identifiable"
    assert result.snapshot.encoded_bytes <= 1_024
    assert isinstance(provider, SnapshotProvider)
    assert factory.calls == [owner]
    assert [event[0] for event in backend.events] == ["open", "read", "close"]
    assert {event[1] for event in backend.events} == {owner}
    assert backend.close_calls == 1
    assert provider.metrics == CameraSnapshotMetrics(
        requests=1,
        busy=0,
        throttled=0,
        resource_denied=0,
        backend_instances=1,
        opens=1,
        reads=1,
        delivered=1,
        timeouts=0,
        failures=0,
        close_failures=0,
        backend_retained=False,
        close_stuck=False,
        worker_state="not_reported",
        active=False,
    )


def test_default_warmup_is_practical_bounded_and_discards_prior_frames() -> None:
    backend = ScriptedBackend([_frame_read() for _ in range(4)])
    provider = CameraSnapshotProvider(
        backend_factory=BackendFactory([backend]),
        capture_request=_request(),
        minimum_open_interval_seconds=0.0,
        monotonic_ns=MutableClock(),
    )

    result = _capture(provider)

    assert result.outcome == "ok"
    assert result.snapshot is not None
    assert result.snapshot.source_sequence == 3
    assert [event[0] for event in backend.events].count("read") == 4
    assert backend.reads == []


def test_zero_timeout_and_resource_denial_never_create_or_open_backend() -> None:
    gate = AdmissionGate(_denied())
    zero_factory = BackendFactory([])
    zero = _provider(zero_factory, gate=gate)

    assert _capture(zero, timeout=0).outcome == "timeout"
    assert zero_factory.calls == []
    assert gate.calls == []

    denied_factory = BackendFactory([])
    denied = _provider(denied_factory, gate=gate)
    result = _capture(denied)

    assert result.outcome == "unavailable"
    assert denied_factory.calls == []
    assert gate.calls == [
        (
            "processing",
            camera_snapshot_module.DEFAULT_ESTIMATED_BACKEND_BYTES + 12 + 2 * 1_024,
        )
    ]
    assert denied.metrics.resource_denied == 1
    assert denied.capture(max_edge_px=64, max_bytes=1_024, timeout_seconds=1.0).reason_code == (
        "resource_denied"
    )


def test_provider_normalizes_untrusted_read_reason_without_leaking_it() -> None:
    backend = ScriptedBackend(
        [CaptureRead(ReadStatus.DISCONNECTED, reason_code=r"driver failure at C:\\private")]
    )
    result = _capture(_provider(BackendFactory([backend])))

    assert result.outcome == "unavailable"
    assert result.reason_code == "generic"
    assert "private" not in repr(result)


@pytest.mark.parametrize(
    "gate",
    [
        AdmissionGate(object()),
        AdmissionGate(_allowed(), error=RuntimeError(r"private C:\resource")),
    ],
)
def test_resource_gate_failure_fails_closed_without_hardware(gate: AdmissionGate) -> None:
    factory = BackendFactory([])
    provider = _provider(factory, gate=gate)

    result = _capture(provider)

    assert result.outcome == "unavailable"
    assert result.reason_code == "resource_gate_failed"
    assert result.snapshot is None
    assert factory.calls == []


def test_backend_memory_estimate_is_configurable_bounded_and_admission_only() -> None:
    gate = AdmissionGate(_denied())
    factory = BackendFactory([])
    provider = _provider(factory, gate=gate, estimated_backend_bytes=0)

    assert _capture(provider).outcome == "unavailable"
    assert gate.calls == [("processing", 12 + 2 * 1_024)]
    assert factory.calls == []

    maximum_gate = AdmissionGate(_denied())
    maximum_factory = BackendFactory([])
    maximum = _provider(
        maximum_factory,
        gate=maximum_gate,
        estimated_backend_bytes=camera_snapshot_module.MAX_ESTIMATED_BACKEND_BYTES,
    )

    assert _capture(maximum).outcome == "unavailable"
    assert maximum_gate.calls == [
        (
            "processing",
            camera_snapshot_module.MAX_ESTIMATED_BACKEND_BYTES + 12 + 2 * 1_024,
        )
    ]
    assert maximum_factory.calls == []


def test_concurrent_request_is_busy_and_never_queues_an_open() -> None:
    entered = Event()
    release = Event()
    backend = ScriptedBackend(
        [_frame_read(received_monotonic_ns=time_ns())],
        read_entered=entered,
        read_release=release,
    )
    factory = BackendFactory([backend])
    provider = _provider(factory, clock=time_ns)
    outcomes: list[str] = []

    def capture() -> None:
        outcomes.append(_capture(provider, timeout=5.0).outcome)

    worker = Thread(target=capture)
    worker.start()
    assert entered.wait(timeout=2)
    assert provider.metrics.active

    concurrent = _capture(provider, timeout=5.0)
    release.set()
    worker.join(timeout=2)

    assert concurrent.outcome == "busy"
    assert outcomes == ["ok"]
    assert len(factory.calls) == 1
    assert backend.close_calls == 1
    assert provider.metrics.busy == 1
    assert not provider.metrics.active


def test_provider_does_not_create_a_backend_when_the_device_lease_is_busy() -> None:
    shared_state = SharedLeaseState()
    first_lease = SharedLease(shared_state)
    second_lease = SharedLease(shared_state)
    entered = Event()
    release = Event()
    first_backend = ScriptedBackend(
        [_frame_read()],
        read_entered=entered,
        read_release=release,
    )
    first_factory = BackendFactory([first_backend])
    first_provider = _provider(first_factory, lease=first_lease)

    outcomes: list[str] = []

    def capture() -> None:
        outcomes.append(_capture(first_provider, timeout=5.0).outcome)

    worker = Thread(target=capture)
    worker.start()
    assert entered.wait(timeout=2)

    unopened_backend = ScriptedBackend([_frame_read()])
    second_factory = BackendFactory([unopened_backend])
    second_provider = _provider(second_factory, lease=second_lease)
    assert _capture(second_provider, timeout=5.0).outcome == "busy"
    assert second_factory.calls == []

    release.set()
    worker.join(timeout=2)
    assert outcomes == ["ok"]
    assert first_lease.release_calls == 1


def time_ns() -> int:
    import time

    return time.monotonic_ns()


def test_minimum_interval_prevents_camera_reopen_without_sleeping() -> None:
    clock = MutableClock()
    first_backend = ScriptedBackend([_frame_read()])
    second_backend = ScriptedBackend([_frame_read(received_monotonic_ns=500_000_000)])
    factory = BackendFactory([first_backend, second_backend])
    provider = _provider(factory, clock=clock, minimum_interval=0.5)

    first = _capture(provider)
    throttled = _capture(provider)
    clock.value = 500_000_000
    second = _capture(provider)

    assert first.outcome == "ok"
    assert throttled.outcome == "busy"
    assert second.outcome == "ok"
    assert first.snapshot is not None
    assert second.snapshot is not None
    assert first.snapshot.stream_epoch == 1
    assert second.snapshot.stream_epoch == 2
    assert len(factory.calls) == 2
    assert first_backend.close_calls == second_backend.close_calls == 1
    assert provider.metrics.throttled == 1


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (CaptureDeadlineExceeded("deadline"), "timeout"),
        (CaptureOpenError(r"device C:\private"), "unavailable"),
        (RuntimeError("driver secret"), "failed"),
    ],
)
def test_open_errors_are_sanitized_and_backend_is_always_closed(
    error: Exception,
    expected: str,
) -> None:
    backend = ScriptedBackend([], open_error=error)
    result = _capture(_provider(BackendFactory([backend])))

    assert result.outcome == expected
    assert result.snapshot is None
    assert "private" not in repr(result)
    assert "secret" not in repr(result)
    assert backend.close_calls == 1


def test_camera_open_timeout_is_a_timeout_with_a_safe_reason() -> None:
    backend = ScriptedBackend([], open_error=CaptureOpenError("camera_open_timeout"))
    provider = _provider(BackendFactory([backend]))

    result = _capture(provider)

    assert result.outcome == "timeout"
    assert result.reason_code == "camera_open_timeout"
    assert provider.metrics.timeouts == 1
    assert backend.close_calls == 1


def test_open_error_reason_extraction_never_calls_error_str() -> None:
    class MaliciousOpenError(CaptureOpenError):
        def __str__(self) -> str:
            raise AssertionError("provider must not format backend errors")

    backend = ScriptedBackend([], open_error=MaliciousOpenError(r"driver C:\\private"))
    result = _capture(_provider(BackendFactory([backend])))

    assert result.outcome == "unavailable"
    assert result.reason_code == "generic"
    assert backend.close_calls == 1


@pytest.mark.parametrize(
    ("read", "expected"),
    [
        (CaptureRead(ReadStatus.TIMEOUT, reason_code="timeout"), "timeout"),
        (CaptureRead(ReadStatus.DISCONNECTED, reason_code="gone"), "unavailable"),
        (CaptureRead(ReadStatus.FATAL_ERROR, reason_code="fatal"), "failed"),
        (CaptureRead(ReadStatus.RECOVERABLE_ERROR, reason_code="temporary"), "failed"),
        (RuntimeError(r"read C:\secret"), "failed"),
        (object(), "failed"),
    ],
)
def test_read_failures_are_sanitized_and_closed(
    read: CaptureRead | Exception | object,
    expected: str,
) -> None:
    backend = ScriptedBackend([read])
    result = _capture(_provider(BackendFactory([backend])))

    assert result.outcome == expected
    assert result.snapshot is None
    assert "secret" not in repr(result)
    assert backend.close_calls == 1


def test_exactly_one_recoverable_retry_is_optional_and_never_reopens() -> None:
    retry_backend = ScriptedBackend(
        [
            CaptureRead(ReadStatus.RECOVERABLE_ERROR, reason_code="temporary"),
            _frame_read(),
        ]
    )
    retry_factory = BackendFactory([retry_backend])
    recovered = _capture(_provider(retry_factory, max_attempts=2))

    assert recovered.outcome == "ok"
    assert recovered.snapshot is not None
    assert recovered.snapshot.source_sequence == 1
    assert len(retry_factory.calls) == 1
    assert [event[0] for event in retry_backend.events].count("open") == 1
    assert [event[0] for event in retry_backend.events].count("read") == 2
    assert retry_backend.close_calls == 1

    no_retry_backend = ScriptedBackend(
        [CaptureRead(ReadStatus.RECOVERABLE_ERROR, reason_code="temporary"), _frame_read()]
    )
    no_retry = _capture(_provider(BackendFactory([no_retry_backend]), max_attempts=1))
    assert no_retry.outcome == "failed"
    assert len(no_retry_backend.reads) == 1
    assert no_retry_backend.close_calls == 1


@pytest.mark.parametrize(
    ("max_attempts", "reads"),
    [
        (1, [CaptureRead(ReadStatus.RECOVERABLE_ERROR, reason_code="capture_read_error")]),
        (
            2,
            [
                CaptureRead(ReadStatus.RECOVERABLE_ERROR, reason_code="capture_read_error"),
                CaptureRead(ReadStatus.RECOVERABLE_ERROR, reason_code="capture_read_error"),
            ],
        ),
    ],
)
def test_exhausted_recoverable_attempts_preserve_compatible_reason(
    max_attempts: int,
    reads: list[CaptureRead],
) -> None:
    provider = _provider(
        BackendFactory([ScriptedBackend(reads)]),
        max_attempts=max_attempts,
    )

    result = _capture(provider)

    assert result.outcome == "failed"
    assert result.reason_code == "capture_read_error"


def test_exhausted_recoverable_attempt_rejects_incompatible_or_malicious_reason() -> None:
    class MaliciousString(str):
        def __eq__(self, _other: object) -> bool:
            raise AssertionError("reason normalization must not compare subclasses")

    incompatible = _capture(
        _provider(
            BackendFactory(
                [
                    ScriptedBackend(
                        [
                            CaptureRead(
                                ReadStatus.RECOVERABLE_ERROR, reason_code="device_open_failed"
                            )
                        ]
                    )
                ]
            )
        )
    )
    malicious = _capture(
        _provider(
            BackendFactory(
                [
                    ScriptedBackend(
                        [
                            CaptureRead(
                                ReadStatus.RECOVERABLE_ERROR,
                                reason_code=MaliciousString("capture_read_error"),  # type: ignore[arg-type]
                            )
                        ]
                    )
                ]
            )
        )
    )

    assert incompatible.outcome == "failed"
    assert incompatible.reason_code == "generic"
    assert malicious.outcome == "failed"
    assert malicious.reason_code == "generic"


def test_normal_lease_release_failure_invalidates_success_and_remains_fail_closed() -> None:
    class FailingReleaseLease(InMemoryLease):
        def release(self) -> None:
            self.release_calls += 1
            raise RuntimeError("private lease detail")

    lease = FailingReleaseLease()
    backend = ScriptedBackend([_frame_read()])
    unopened = ScriptedBackend([_frame_read()])
    factory = BackendFactory([backend, unopened])
    provider = _provider(factory, lease=lease)

    first = _capture(provider)
    second = _capture(provider)

    assert first.outcome == "failed"
    assert first.reason_code == "lease_release_failed"
    assert first.snapshot is None
    assert second.outcome == "failed"
    assert second.reason_code == "lease_release_failed"
    assert lease.release_calls == 2
    assert lease.held
    assert len(factory.calls) == 1
    assert backend.close_calls == 1
    assert unopened.events == []
    assert not provider.metrics.active


def test_lease_release_failure_does_not_swallow_pending_interrupt() -> None:
    class InjectedInterrupt(BaseException):
        pass

    class FailingReleaseLease(InMemoryLease):
        def release(self) -> None:
            self.release_calls += 1
            raise RuntimeError("private lease detail")

    lease = FailingReleaseLease()
    backend = ScriptedBackend([InjectedInterrupt()])
    provider = _provider(BackendFactory([backend]), lease=lease)

    with pytest.raises(InjectedInterrupt):
        _capture(provider)

    assert lease.release_calls == 1
    assert lease.held
    assert not provider.metrics.active
    assert provider.metrics.failures == 1
    assert not provider.metrics.backend_retained


def test_encode_and_close_errors_are_sanitized_and_discard_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encode_backend = ScriptedBackend([_frame_read()])

    def fail_encode(*_args: object, **_kwargs: object) -> SnapshotEnvelope:
        raise RuntimeError(r"encoder secret C:\frame")

    monkeypatch.setattr(camera_snapshot_module, "encode_png_snapshot", fail_encode)
    encoded = _capture(_provider(BackendFactory([encode_backend])))
    assert encoded.outcome == "failed"
    assert encoded.snapshot is None
    assert encode_backend.close_calls == 1

    monkeypatch.undo()
    close_backend = ScriptedBackend(
        [_frame_read()],
        close_error=RuntimeError(r"close secret C:\camera"),
    )
    unopened_backend = ScriptedBackend([_frame_read()])
    factory = BackendFactory([close_backend, unopened_backend])
    gate = AdmissionGate(_allowed())
    close_lease = InMemoryLease()
    close_provider = _provider(factory, gate=gate, lease=close_lease)
    closed = _capture(close_provider)
    assert closed.outcome == "failed"
    assert closed.snapshot is None
    assert close_backend.close_calls == 1
    metrics = close_provider.metrics
    assert metrics.close_failures == 1
    assert metrics.delivered == 0
    assert metrics.backend_retained
    assert metrics.close_stuck
    assert metrics.worker_state == "unknown"
    assert close_lease.release_calls == 0

    repeated = _capture(close_provider)

    assert repeated.outcome == "failed"
    assert repeated.snapshot is None
    assert len(factory.calls) == 1
    assert unopened_backend.events == []
    assert len(gate.calls) == 1
    assert close_provider.metrics.failures == 2


def test_successful_close_with_unresolved_worker_state_latches_fail_closed() -> None:
    backend = ScriptedBackend([_frame_read()], worker_state="running")
    unopened_backend = ScriptedBackend([_frame_read()])
    factory = BackendFactory([backend, unopened_backend])
    provider = _provider(factory)

    result = _capture(provider)

    assert result.outcome == "failed"
    assert result.snapshot is None
    assert backend.close_calls == 1
    assert provider.metrics.delivered == 0
    assert provider.metrics.close_failures == 1
    assert provider.metrics.close_stuck
    assert provider.metrics.worker_state == "running"
    assert _capture(provider).outcome == "failed"
    assert len(factory.calls) == 1
    assert unopened_backend.events == []


def test_finally_closes_backend_even_for_non_application_interrupt() -> None:
    class InjectedInterrupt(BaseException):
        pass

    backend = ScriptedBackend([InjectedInterrupt()])
    provider = _provider(BackendFactory([backend]))

    with pytest.raises(InjectedInterrupt):
        _capture(provider)

    assert backend.close_calls == 1
    assert not provider.metrics.active


def test_lease_release_interrupt_never_strands_operation_lock() -> None:
    class InjectedInterrupt(BaseException):
        pass

    class InterruptingLease(InMemoryLease):
        def __init__(self) -> None:
            super().__init__()
            self._interrupt_pending = True

        def release(self) -> None:
            self.release_calls += 1
            if self._interrupt_pending:
                self._interrupt_pending = False
                raise InjectedInterrupt()
            self.held = False

    lease = InterruptingLease()
    backend = ScriptedBackend([_frame_read()])
    unopened_backend = ScriptedBackend([_frame_read()])
    factory = BackendFactory([backend, unopened_backend])
    provider = _provider(factory, lease=lease)

    with pytest.raises(InjectedInterrupt):
        _capture(provider)

    assert backend.close_calls == 1
    assert lease.held
    assert not provider.metrics.active

    assert _capture(provider).outcome == "failed"
    assert lease.release_calls == 2
    assert not lease.held
    assert not provider.metrics.active
    assert len(factory.calls) == 1
    assert unopened_backend.events == []


def test_close_interrupt_is_propagated_but_latches_the_backend() -> None:
    class InjectedInterrupt(BaseException):
        pass

    backend = ScriptedBackend([_frame_read()], close_error=InjectedInterrupt())
    unopened_backend = ScriptedBackend([_frame_read()])
    factory = BackendFactory([backend, unopened_backend])
    provider = _provider(factory)

    with pytest.raises(InjectedInterrupt):
        _capture(provider)

    metrics = provider.metrics
    assert metrics.delivered == 0
    assert metrics.close_failures == 1
    assert metrics.backend_retained
    assert metrics.close_stuck
    assert metrics.worker_state == "unknown"
    assert _capture(provider).outcome == "failed"
    assert len(factory.calls) == 1
    assert unopened_backend.events == []


def test_single_deadline_is_shared_by_open_warmup_retry_and_target_reads() -> None:
    clock = MutableClock(10)
    backend = ScriptedBackend(
        [
            _frame_read(),
            CaptureRead(ReadStatus.RECOVERABLE_ERROR, reason_code="temporary"),
            _frame_read(),
        ]
    )
    provider = _provider(
        BackendFactory([backend]),
        clock=clock,
        warmup_frames=1,
        max_attempts=2,
    )

    result = _capture(provider, timeout=5.0)

    assert result.outcome == "ok"
    deadlines = [deadline for action, _, deadline in backend.events if action != "close"]
    assert deadlines == [5_000_000_010] * 4


def test_deadline_expiry_before_open_or_after_encode_returns_timeout_and_closes() -> None:
    before_factory = BackendFactory([])
    before = _provider(before_factory, clock=SequenceClock([0, 1_000_000_000]))
    assert _capture(before, timeout=1.0).outcome == "timeout"
    assert before_factory.calls == []

    clock = MutableClock()

    def expire_after_read() -> None:
        clock.value = 1_000_000_000

    backend = ScriptedBackend([_frame_read()], read_hook=expire_after_read)
    after = _capture(_provider(BackendFactory([backend]), clock=clock), timeout=1.0)
    assert after.outcome == "timeout"
    assert after.snapshot is None
    assert backend.close_calls == 1


def test_unsafe_backend_invalid_stream_and_changed_frame_fail_closed() -> None:
    unsafe = CaptureCapabilities(
        DeadlineCapability.BEST_EFFORT,
        DeadlineCapability.ENFORCED,
        True,
    )
    unsafe_backend = ScriptedBackend([], capabilities=unsafe)
    assert _capture(_provider(BackendFactory([unsafe_backend]))).outcome == "failed"
    assert unsafe_backend.events == [("close", get_ident(), None)]

    invalid_stream = ScriptedBackend([], open_result=object())
    assert _capture(_provider(BackendFactory([invalid_stream]))).outcome == "failed"
    assert invalid_stream.close_calls == 1

    changed = BackendFrame(
        payload=bytes(18),
        received_monotonic_ns=0,
        width=3,
        height=2,
        stride=9,
        pixel_format="bgr24",
    )
    changed_backend = ScriptedBackend([CaptureRead(ReadStatus.FRAME, frame=changed)])
    assert _capture(_provider(BackendFactory([changed_backend]))).outcome == "failed"
    assert changed_backend.close_calls == 1


def test_factory_failure_or_invalid_backend_is_explicit_and_memory_only() -> None:
    failed_factory = BackendFactory([], error=RuntimeError(r"factory C:\secret"))
    failed = _capture(_provider(failed_factory))
    assert failed.outcome == "failed"
    assert failed.snapshot is None

    invalid = _capture(_provider(BackendFactory([object()])))
    assert invalid.outcome == "failed"
    assert invalid.snapshot is None


def test_provider_supports_reviewed_global_limits_without_a_smaller_hidden_ceiling() -> None:
    backend = ScriptedBackend([_frame_read()])
    provider = _provider(BackendFactory([backend]))

    result = provider.capture(
        max_edge_px=MAX_SNAPSHOT_EDGE_PX,
        max_bytes=MAX_SNAPSHOT_BYTES,
        timeout_seconds=MAX_SNAPSHOT_WAIT_SECONDS,
    )

    assert result.outcome == "ok"
    assert backend.open_requests == [_request()]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_edge_px", 0),
        ("max_edge_px", MAX_SNAPSHOT_EDGE_PX + 1),
        ("max_edge_px", True),
        ("max_bytes", 0),
        ("max_bytes", MAX_SNAPSHOT_BYTES + 1),
        ("timeout_seconds", -1),
        ("timeout_seconds", MAX_SNAPSHOT_WAIT_SECONDS + 0.1),
        ("timeout_seconds", float("inf")),
        ("timeout_seconds", True),
    ],
)
def test_capture_inputs_are_strict_and_never_touch_factory(field: str, value: object) -> None:
    factory = BackendFactory([])
    provider = _provider(factory)
    values: dict[str, object] = {
        "max_edge_px": 64,
        "max_bytes": 1_024,
        "timeout_seconds": 1.0,
    }
    values[field] = value

    with pytest.raises((TypeError, ValueError)):
        provider.capture(**values)  # type: ignore[arg-type]

    assert factory.calls == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"backend_factory": object()},
        {"capture_request": object()},
        {"resource_gate": object()},
        {"source_id": ""},
        {"source_id": "x" * 257},
        {"source_id": 1},
        {"sensitivity_class": "prohibited"},
        {"warmup_frames": -1},
        {"warmup_frames": 17},
        {"warmup_frames": True},
        {"max_attempts": 0},
        {"max_attempts": 3},
        {"max_attempts": True},
        {"minimum_open_interval_seconds": -1},
        {"minimum_open_interval_seconds": 301},
        {"minimum_open_interval_seconds": math.inf},
        {"minimum_open_interval_seconds": True},
        {"estimated_backend_bytes": -1},
        {"estimated_backend_bytes": (camera_snapshot_module.MAX_ESTIMATED_BACKEND_BYTES + 1)},
        {"estimated_backend_bytes": True},
        {"monotonic_ns": 0},
    ],
)
def test_constructor_rejects_unbounded_or_unsafe_configuration(
    overrides: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "backend_factory": BackendFactory([]),
        "capture_request": _request(),
        "minimum_open_interval_seconds": 0.0,
        "monotonic_ns": MutableClock(),
    }
    with pytest.raises((TypeError, ValueError)):
        CameraSnapshotProvider(**(values | overrides))  # type: ignore[arg-type]


def test_clock_regression_fails_before_backend_creation() -> None:
    factory = BackendFactory([])
    provider = _provider(factory, clock=SequenceClock([10, 9]))

    result = _capture(provider)

    assert result.outcome == "failed"
    assert factory.calls == []


def test_provider_retains_no_frame_or_encoded_bytes_after_return() -> None:
    backend = ScriptedBackend([_frame_read()])
    factory = BackendFactory([backend])
    provider = _provider(factory)

    result = _capture(provider)

    assert result.outcome == "ok"
    assert result.snapshot is not None
    assert not hasattr(provider, "__dict__")
    dependency_fields = {"_backend_factory", "_resource_gate"}
    forbidden = (bytes, bytearray, BackendFrame, FrameEnvelope, SnapshotEnvelope, CaptureRead)
    for field_name in CameraSnapshotProvider.__slots__:
        if field_name not in dependency_fields:
            assert not isinstance(getattr(provider, field_name), forbidden)
    assert backend.reads == []
    assert provider.metrics.active is False


def test_metrics_are_frozen_slotted_and_content_free() -> None:
    metrics = CameraSnapshotMetrics(
        requests=0,
        busy=0,
        throttled=0,
        resource_denied=0,
        backend_instances=0,
        opens=0,
        reads=0,
        delivered=0,
        timeouts=0,
        failures=0,
        close_failures=0,
        backend_retained=False,
        close_stuck=False,
        worker_state="absent",
        active=False,
    )

    with pytest.raises(FrozenInstanceError):
        metrics.requests = 1  # type: ignore[misc]
    assert not hasattr(metrics, "__dict__")
    with pytest.raises(TypeError, match="active"):
        CameraSnapshotMetrics(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            False,
            False,
            "absent",
            1,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="close_failures"):
        CameraSnapshotMetrics(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            False,
            False,
            "absent",
            False,
        )
    with pytest.raises(ValueError, match="unresolved worker"):
        CameraSnapshotMetrics(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            False,
            False,
            "running",
            False,
        )
