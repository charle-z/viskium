from __future__ import annotations

from threading import Thread

import pytest

from viskium.capture.latest import (
    FrameOffer,
    LatestFrameSlot,
    OfferStatus,
)
from viskium.core import FrameEnvelope


def _frame(sequence: int) -> FrameEnvelope:
    return FrameEnvelope(
        source_id="fake-camera",
        stream_epoch=0,
        sequence=sequence,
        received_monotonic_ns=sequence,
        payload=bytes((sequence % 256,)),
        width=1,
        height=1,
        pixel_format="GRAY8",
        stride=1,
        buffer_id="fake:0",
        generation=sequence,
    )


def test_slot_replaces_obsolete_pending_frame_without_growing() -> None:
    slot = LatestFrameSlot()

    assert slot.offer(_frame(0)).status is OfferStatus.ACCEPTED
    for sequence in range(1, 1_000):
        receipt = slot.offer(_frame(sequence))
        assert receipt.status is OfferStatus.REPLACED
        assert receipt.replaced_sequence == sequence - 1

    assert slot.pending_count == 1
    assert slot.offered_count == 1_000
    assert slot.replaced_count == 999
    latest = slot.take(timeout_seconds=0.0)
    assert latest is not None
    assert latest.sequence == 999
    assert slot.pending_count == 0
    assert slot.taken_count == 1


def test_waiting_take_is_woken_by_offer() -> None:
    slot = LatestFrameSlot()
    received: list[FrameEnvelope | None] = []
    consumer = Thread(
        target=lambda: received.append(slot.take(timeout_seconds=1.0)),
        name="test-latest-consumer",
    )

    consumer.start()
    slot.offer(_frame(7))
    consumer.join(timeout=1.0)

    assert not consumer.is_alive()
    assert received
    assert received[0] is not None
    assert received[0].sequence == 7


def test_take_timeout_and_close_discard_are_bounded() -> None:
    slot = LatestFrameSlot()

    assert slot.take(timeout_seconds=0.0) is None
    slot.offer(_frame(1))
    slot.close()
    assert slot.closed
    assert slot.pending_count == 0
    assert slot.take(timeout_seconds=0.0) is None
    rejected = slot.offer(_frame(2))
    assert rejected.status is OfferStatus.CLOSED
    assert slot.rejected_count == 1


def test_close_always_releases_one_pending_frame() -> None:
    slot = LatestFrameSlot()
    slot.offer(_frame(3))

    slot.close()
    slot.close()

    assert slot.pending_count == 0
    assert slot.take(timeout_seconds=0.0) is None


@pytest.mark.parametrize("timeout", [-1.0, float("nan"), float("inf"), 60.1])
def test_take_rejects_invalid_or_unbounded_timeout(timeout: float) -> None:
    with pytest.raises(ValueError, match=r"finite|non-negative|exceed"):
        LatestFrameSlot().take(timeout_seconds=timeout)


def test_slot_rejects_wrong_runtime_types() -> None:
    slot = LatestFrameSlot()
    with pytest.raises(TypeError, match="FrameEnvelope"):
        slot.offer(object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="float"):
        slot.take(timeout_seconds=1)  # type: ignore[arg-type]


def test_frame_offer_enforces_status_payload_consistency() -> None:
    with pytest.raises(ValueError, match="requires"):
        FrameOffer(OfferStatus.REPLACED)
    with pytest.raises(ValueError, match="only"):
        FrameOffer(OfferStatus.ACCEPTED, 1)
    with pytest.raises(TypeError, match="OfferStatus"):
        FrameOffer("accepted")  # type: ignore[arg-type]
