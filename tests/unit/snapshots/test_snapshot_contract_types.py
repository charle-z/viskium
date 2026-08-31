from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from viskium.core import FrameEnvelope
from viskium.snapshots import (
    MAX_SNAPSHOT_BYTES,
    MAX_SNAPSHOT_EDGE_PX,
    SnapshotBrokerMetrics,
    SnapshotEnvelope,
    SnapshotRequestResult,
)
from viskium.snapshots.contracts import PNG_SIGNATURE


def _frame() -> FrameEnvelope:
    return FrameEnvelope(
        source_id="camera",
        stream_epoch=1,
        sequence=2,
        received_monotonic_ns=3,
        width=1,
        height=1,
        stride=1,
        pixel_format="gray8",
        payload=b"\x00",
    )


def _snapshot(**overrides: Any) -> SnapshotEnvelope:
    values: dict[str, Any] = {
        "source_id": "camera",
        "stream_epoch": 1,
        "source_sequence": 2,
        "received_monotonic_ns": 3,
        "width": 1,
        "height": 1,
        "sensitivity_class": "identifiable",
        "png_bytes": PNG_SIGNATURE,
    }
    values.update(overrides)
    return SnapshotEnvelope(**values)


def test_snapshot_envelope_is_frozen_slotted_and_reports_encoded_bytes() -> None:
    snapshot = _snapshot()

    assert snapshot.encoded_bytes == len(PNG_SIGNATURE)
    assert snapshot.mime_type == "image/png"
    assert not hasattr(snapshot, "__dict__")
    with pytest.raises(FrozenInstanceError):
        snapshot.width = 2  # type: ignore[misc]


@pytest.mark.parametrize(
    ("overrides", "error_type"),
    [
        ({"source_id": ""}, ValueError),
        ({"source_id": 1}, TypeError),
        ({"stream_epoch": -1}, ValueError),
        ({"source_sequence": True}, TypeError),
        ({"received_monotonic_ns": 2**63}, ValueError),
        ({"width": 0}, ValueError),
        ({"height": MAX_SNAPSHOT_EDGE_PX + 1}, ValueError),
        ({"sensitivity_class": "prohibited"}, ValueError),
        ({"sensitivity_class": "unknown"}, ValueError),
        ({"png_bytes": bytearray(PNG_SIGNATURE)}, TypeError),
        ({"png_bytes": b"not-png"}, ValueError),
        ({"png_bytes": PNG_SIGNATURE + b"x" * MAX_SNAPSHOT_BYTES}, ValueError),
        ({"mime_type": "image/jpeg"}, ValueError),
    ],
)
def test_snapshot_envelope_rejects_invalid_or_unbounded_metadata(
    overrides: dict[str, Any], error_type: type[Exception]
) -> None:
    with pytest.raises(error_type):
        _snapshot(**overrides)


def test_snapshot_request_result_only_exposes_frame_on_success() -> None:
    frame = _frame()

    assert SnapshotRequestResult(outcome="ok", frame=frame).frame is frame
    with pytest.raises(TypeError, match="FrameEnvelope"):
        SnapshotRequestResult(outcome="ok")
    with pytest.raises(ValueError, match="must not expose"):
        SnapshotRequestResult(outcome="timeout", frame=frame)
    with pytest.raises(ValueError, match="unsupported"):
        SnapshotRequestResult(outcome="unknown")  # type: ignore[arg-type]


def test_snapshot_broker_metrics_are_frozen_slotted_and_bounded() -> None:
    values: dict[str, Any] = {
        "requests_started": 0,
        "requests_busy": 0,
        "requests_timed_out": 0,
        "requests_closed": 0,
        "frames_delivered": 0,
        "frames_no_demand": 0,
        "offers_closed": 0,
        "pending": False,
        "closed": False,
    }
    metrics = SnapshotBrokerMetrics(**values)

    assert not hasattr(metrics, "__dict__")
    with pytest.raises(FrozenInstanceError):
        metrics.frames_delivered = 1  # type: ignore[misc]
    with pytest.raises(ValueError, match="int64"):
        SnapshotBrokerMetrics(**(values | {"requests_started": 2**63}))
    with pytest.raises(TypeError, match="integer"):
        SnapshotBrokerMetrics(**(values | {"requests_busy": True}))
    with pytest.raises(TypeError, match="booleans"):
        SnapshotBrokerMetrics(**(values | {"pending": 1}))
    with pytest.raises(ValueError, match="closed"):
        SnapshotBrokerMetrics(**(values | {"pending": True, "closed": True}))
