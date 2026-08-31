from __future__ import annotations

from threading import Thread, get_ident

import pytest

from viskium.adapters import fake_camera as fake_camera_module
from viskium.adapters.fake_camera import FakeCameraBackend
from viskium.capture import (
    BackendFrame,
    CaptureBackend,
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


def _request(*, max_frame_bytes: int = 16) -> CaptureRequest:
    return CaptureRequest(0, 2, 2, 15.0, max_frame_bytes)


def _frame(sequence: int = 0) -> BackendFrame:
    return BackendFrame(
        payload=bytes((sequence, sequence, sequence, sequence)),
        received_monotonic_ns=sequence,
        width=2,
        height=2,
        stride=2,
        pixel_format="GRAY8",
    )


def test_fake_backend_satisfies_port_and_enforces_deadlines() -> None:
    backend = FakeCameraBackend(now_ns=lambda: 10)

    assert isinstance(backend, CaptureBackend)
    assert backend.capabilities.open_deadline is DeadlineCapability.ENFORCED
    assert backend.capabilities.read_deadline is DeadlineCapability.ENFORCED
    assert backend.capabilities.safe_in_process
    with pytest.raises(CaptureDeadlineExceeded):
        backend.open(_request(), deadline_monotonic_ns=10)


def test_fake_backend_open_read_close_lifecycle_is_deterministic() -> None:
    frame = _frame()
    backend = FakeCameraBackend(
        reads=[
            CaptureRead(ReadStatus.FRAME, frame=frame),
            CaptureRead(ReadStatus.DISCONNECTED, reason_code="unplugged"),
        ],
        now_ns=lambda: 0,
    )

    stream = backend.open(_request(), deadline_monotonic_ns=1)
    assert stream == NegotiatedStream("fake-camera", 2, 2, 15.0, "GRAY8", 2)
    assert backend.owner_thread_id == get_ident()
    assert backend.is_open
    assert backend.read(deadline_monotonic_ns=1).frame is frame
    assert backend.read(deadline_monotonic_ns=1).status is ReadStatus.DISCONNECTED
    exhausted = backend.read(deadline_monotonic_ns=1)
    assert exhausted.status is ReadStatus.TIMEOUT
    assert exhausted.reason_code == "fake_script_exhausted"

    backend.close()
    backend.close()
    assert not backend.is_open
    assert backend.open_calls == 1
    assert backend.read_calls == 3
    assert backend.close_calls == 1


def test_fake_backend_programs_open_failures_and_allows_owner_retry() -> None:
    backend = FakeCameraBackend(
        open_failure_reasons=["busy", "temporarily_unavailable"],
        now_ns=lambda: 0,
    )

    with pytest.raises(CaptureOpenError, match="busy"):
        backend.open(_request(), deadline_monotonic_ns=1)
    with pytest.raises(CaptureOpenError, match="temporarily_unavailable"):
        backend.open(_request(), deadline_monotonic_ns=1)
    backend.open(_request(), deadline_monotonic_ns=1)

    assert backend.open_calls == 3
    backend.close()


def test_fake_backend_rejects_wrong_thread_access() -> None:
    backend = FakeCameraBackend(now_ns=lambda: 0)
    backend.open(_request(), deadline_monotonic_ns=1)
    failures: list[Exception] = []

    def access_from_non_owner() -> None:
        try:
            backend.read(deadline_monotonic_ns=1)
        except Exception as error:  # test captures the cross-thread failure for assertion
            failures.append(error)

    intruder = Thread(target=access_from_non_owner, name="test-camera-intruder")
    intruder.start()
    intruder.join(timeout=1.0)

    assert not intruder.is_alive()
    assert len(failures) == 1
    assert isinstance(failures[0], CaptureOwnershipError)
    backend.close()


def test_fake_backend_rejects_invalid_lifecycle_operations() -> None:
    backend = FakeCameraBackend(now_ns=lambda: 0)

    with pytest.raises(CaptureStateError, match="not open"):
        backend.read(deadline_monotonic_ns=1)
    backend.open(_request(), deadline_monotonic_ns=1)
    with pytest.raises(CaptureStateError, match="already open"):
        backend.open(_request(), deadline_monotonic_ns=1)
    backend.close()
    with pytest.raises(CaptureStateError, match="not open"):
        backend.read(deadline_monotonic_ns=1)


def test_fake_backend_rejects_negotiated_mode_above_request_budget() -> None:
    negotiated = NegotiatedStream("fake", 4, 4, 15.0, "GRAY8", 4)
    backend = FakeCameraBackend(negotiated_stream=negotiated, now_ns=lambda: 0)

    with pytest.raises(CaptureOpenError, match="exceeds"):
        backend.open(_request(max_frame_bytes=4), deadline_monotonic_ns=1)
    assert not backend.is_open


@pytest.mark.parametrize(
    "kwargs",
    [
        {"reads": [object()]},
        {"open_failure_reasons": [1]},
        {"open_failure_reasons": [""]},
        {"negotiated_stream": object()},
        {"now_ns": 1},
    ],
)
def test_fake_backend_rejects_invalid_script_configuration(kwargs: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        FakeCameraBackend(**kwargs)  # type: ignore[arg-type]


def test_fake_backend_bounds_script_counts_payload_bytes_and_failure_reason_length(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timeout = CaptureRead(ReadStatus.TIMEOUT, reason_code="scripted_timeout")
    monkeypatch.setattr(fake_camera_module, "_MAX_SCRIPTED_READS", 1)
    with pytest.raises(ValueError, match="reads exceeds"):
        FakeCameraBackend(reads=[timeout, timeout])

    monkeypatch.setattr(fake_camera_module, "_MAX_SCRIPTED_PAYLOAD_BYTES", 3)
    with pytest.raises(ValueError, match="payloads exceed"):
        FakeCameraBackend(reads=[CaptureRead(ReadStatus.FRAME, frame=_frame())])

    with pytest.raises(ValueError, match="must not exceed"):
        FakeCameraBackend(open_failure_reasons=["x" * 129])


def test_fake_backend_validates_deadline_and_clock_types() -> None:
    backend = FakeCameraBackend(now_ns=lambda: 0)
    with pytest.raises(TypeError, match="CaptureRequest"):
        backend.open(object(), deadline_monotonic_ns=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="integer"):
        backend.open(_request(), deadline_monotonic_ns=1.0)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-negative"):
        backend.open(_request(), deadline_monotonic_ns=-1)

    invalid_clock = FakeCameraBackend(now_ns=lambda: -1)
    with pytest.raises(ValueError, match="now_ns"):
        invalid_clock.open(_request(), deadline_monotonic_ns=1)
    non_integer_clock = FakeCameraBackend(now_ns=lambda: 1.0)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="now_ns"):
        non_integer_clock.open(_request(), deadline_monotonic_ns=2)


def test_close_before_open_is_a_noop_without_claiming_ownership() -> None:
    backend = FakeCameraBackend()

    backend.close()

    assert backend.owner_thread_id is None
    assert backend.close_calls == 0
