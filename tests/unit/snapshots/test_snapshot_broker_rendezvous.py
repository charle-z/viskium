from __future__ import annotations

import math
import time
from dataclasses import FrozenInstanceError
from threading import Thread
from typing import Any

import pytest

from viskium.core import FrameEnvelope
from viskium.snapshots import SnapshotBroker, SnapshotRequestResult, encode_png_snapshot


def _frame(sequence: int = 0) -> FrameEnvelope:
    return FrameEnvelope(
        source_id="camera",
        stream_epoch=1,
        sequence=sequence,
        received_monotonic_ns=10,
        width=1,
        height=1,
        stride=1,
        pixel_format="gray8",
        payload=bytes((sequence % 256,)),
    )


def _wait_until_pending(broker: SnapshotBroker) -> None:
    deadline = time.monotonic() + 1.0
    while not broker.metrics.pending:
        if time.monotonic() >= deadline:
            pytest.fail("snapshot request did not become pending")
        time.sleep(0.001)


def test_frames_are_not_cached_without_existing_demand() -> None:
    broker = SnapshotBroker()

    assert broker.offer(_frame()) == "no_demand"
    assert broker.request(timeout_seconds=0).outcome == "timeout"
    assert broker.metrics.frames_no_demand == 1
    assert broker.metrics.frames_delivered == 0


def test_one_pending_request_receives_exactly_one_immutable_frame() -> None:
    broker = SnapshotBroker()
    results: list[SnapshotRequestResult] = []
    reader = Thread(target=lambda: results.append(broker.request(timeout_seconds=1.0)))
    reader.start()
    _wait_until_pending(broker)
    first = _frame(1)

    assert broker.offer(first) == "delivered"
    assert broker.offer(_frame(2)) == "no_demand"
    reader.join(timeout=1.0)

    assert not reader.is_alive()
    assert results == [SnapshotRequestResult(outcome="ok", frame=first)]
    assert results[0].frame is first
    with pytest.raises(FrozenInstanceError):
        first.sequence = 9  # type: ignore[misc]


def test_second_pending_request_is_rejected_as_busy() -> None:
    broker = SnapshotBroker()
    results: list[SnapshotRequestResult] = []
    reader = Thread(target=lambda: results.append(broker.request(timeout_seconds=1.0)))
    reader.start()
    _wait_until_pending(broker)

    assert broker.request(timeout_seconds=1.0).outcome == "busy"
    assert broker.offer(_frame()) == "delivered"
    reader.join(timeout=1.0)

    assert results[0].outcome == "ok"
    assert broker.metrics.requests_busy == 1


def test_timeout_releases_capacity_for_a_later_request() -> None:
    broker = SnapshotBroker()

    assert broker.request(timeout_seconds=0.01).outcome == "timeout"
    results: list[SnapshotRequestResult] = []
    reader = Thread(target=lambda: results.append(broker.request(timeout_seconds=1.0)))
    reader.start()
    _wait_until_pending(broker)
    assert broker.offer(_frame()) == "delivered"
    reader.join(timeout=1.0)

    assert results[0].outcome == "ok"
    assert broker.metrics.requests_timed_out == 1


def test_close_is_idempotent_discards_and_wakes_pending_request() -> None:
    broker = SnapshotBroker()
    results: list[SnapshotRequestResult] = []
    reader = Thread(target=lambda: results.append(broker.request(timeout_seconds=1.0)))
    reader.start()
    _wait_until_pending(broker)

    broker.close()
    broker.close()
    reader.join(timeout=1.0)

    assert not reader.is_alive()
    assert results[0].outcome == "closed"
    assert broker.closed is True
    assert broker.offer(_frame()) == "closed"
    assert broker.request(timeout_seconds=0).outcome == "closed"
    assert broker.metrics.pending is False


def test_encoding_failure_after_delivery_does_not_poison_broker() -> None:
    broker = SnapshotBroker()
    first_results: list[SnapshotRequestResult] = []
    first_reader = Thread(target=lambda: first_results.append(broker.request(timeout_seconds=1.0)))
    first_reader.start()
    _wait_until_pending(broker)
    invalid_for_png = FrameEnvelope(
        source_id="camera",
        stream_epoch=0,
        sequence=0,
        received_monotonic_ns=0,
        width=1,
        height=1,
        stride=1,
        pixel_format="unsupported",
        payload=b"x",
    )
    broker.offer(invalid_for_png)
    first_reader.join(timeout=1.0)

    with pytest.raises(ValueError, match="supports only"):
        encode_png_snapshot(first_results[0].frame)  # type: ignore[arg-type]

    second_results: list[SnapshotRequestResult] = []
    second_reader = Thread(
        target=lambda: second_results.append(broker.request(timeout_seconds=1.0))
    )
    second_reader.start()
    _wait_until_pending(broker)
    broker.offer(_frame(2))
    second_reader.join(timeout=1.0)
    assert second_results[0].outcome == "ok"


@pytest.mark.parametrize(
    ("timeout", "error_type"),
    [
        (-0.1, ValueError),
        (15.01, ValueError),
        (math.inf, ValueError),
        (True, TypeError),
        ("1", TypeError),
    ],
)
def test_request_rejects_invalid_or_overlong_timeout(
    timeout: Any, error_type: type[Exception]
) -> None:
    with pytest.raises(error_type):
        SnapshotBroker().request(timeout_seconds=timeout)


def test_offer_requires_frame_envelope() -> None:
    with pytest.raises(TypeError, match="FrameEnvelope"):
        SnapshotBroker().offer(object())  # type: ignore[arg-type]
