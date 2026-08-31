"""Ephemeral, bounded snapshot rendezvous and PNG encoding."""

from .broker import SnapshotBroker, SnapshotBrokerMetrics, SnapshotRequestResult
from .contracts import (
    MAX_SNAPSHOT_BYTES,
    MAX_SNAPSHOT_EDGE_PX,
    MAX_SNAPSHOT_WAIT_SECONDS,
    PNG_MIME_TYPE,
    SnapshotEnvelope,
    SnapshotOfferOutcome,
    SnapshotRequestOutcome,
)
from .png import SnapshotEncodingError, SnapshotTooLargeError, encode_png_snapshot

__all__ = [
    "MAX_SNAPSHOT_BYTES",
    "MAX_SNAPSHOT_EDGE_PX",
    "MAX_SNAPSHOT_WAIT_SECONDS",
    "PNG_MIME_TYPE",
    "SnapshotBroker",
    "SnapshotBrokerMetrics",
    "SnapshotEncodingError",
    "SnapshotEnvelope",
    "SnapshotOfferOutcome",
    "SnapshotRequestOutcome",
    "SnapshotRequestResult",
    "SnapshotTooLargeError",
    "encode_png_snapshot",
]
