"""Immutable contracts for bounded, ephemeral snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from viskium.core.contracts import SensitivityClass

type SnapshotRequestOutcome = Literal["ok", "busy", "timeout", "closed"]
type SnapshotOfferOutcome = Literal["delivered", "no_demand", "closed"]

MAX_SNAPSHOT_BYTES = 8 * 1_024 * 1_024
MAX_SNAPSHOT_EDGE_PX = 1_920
MAX_SNAPSHOT_WAIT_SECONDS = 15.0
PNG_MIME_TYPE: Literal["image/png"] = "image/png"
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_MAX_INT64 = 2**63 - 1
_MAX_IDENTIFIER_CHARS = 256


def _bounded_integer(value: object, field_name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not minimum <= value <= _MAX_INT64:
        raise ValueError(f"{field_name} must be between {minimum} and signed int64 max")
    return value


@dataclass(frozen=True, slots=True)
class SnapshotEnvelope:
    """One PNG snapshot held only in immutable memory."""

    source_id: str
    stream_epoch: int
    source_sequence: int
    received_monotonic_ns: int
    width: int
    height: int
    sensitivity_class: SensitivityClass
    png_bytes: bytes
    mime_type: Literal["image/png"] = PNG_MIME_TYPE

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str):
            raise TypeError("source_id must be a string")
        if not self.source_id or not self.source_id.strip():
            raise ValueError("source_id must not be empty")
        if len(self.source_id) > _MAX_IDENTIFIER_CHARS:
            raise ValueError(f"source_id exceeds {_MAX_IDENTIFIER_CHARS} characters")
        _bounded_integer(self.stream_epoch, "stream_epoch")
        _bounded_integer(self.source_sequence, "source_sequence")
        _bounded_integer(self.received_monotonic_ns, "received_monotonic_ns")
        _bounded_integer(self.width, "width", minimum=1)
        _bounded_integer(self.height, "height", minimum=1)
        if max(self.width, self.height) > MAX_SNAPSHOT_EDGE_PX:
            raise ValueError(f"snapshot edge must not exceed {MAX_SNAPSHOT_EDGE_PX} pixels")
        if self.sensitivity_class not in {
            "public",
            "operational",
            "sensitive",
            "identifiable",
        }:
            raise ValueError("snapshot sensitivity cannot be prohibited")
        if not isinstance(self.png_bytes, bytes):
            raise TypeError("png_bytes must be immutable bytes")
        if not self.png_bytes.startswith(PNG_SIGNATURE):
            raise ValueError("png_bytes must contain a PNG image")
        if len(self.png_bytes) > MAX_SNAPSHOT_BYTES:
            raise ValueError(f"png_bytes must not exceed {MAX_SNAPSHOT_BYTES} bytes")
        if self.mime_type != PNG_MIME_TYPE:
            raise ValueError(f"mime_type must be {PNG_MIME_TYPE}")

    @property
    def encoded_bytes(self) -> int:
        return len(self.png_bytes)


__all__ = [
    "MAX_SNAPSHOT_BYTES",
    "MAX_SNAPSHOT_EDGE_PX",
    "MAX_SNAPSHOT_WAIT_SECONDS",
    "PNG_MIME_TYPE",
    "PNG_SIGNATURE",
    "SnapshotEnvelope",
    "SnapshotOfferOutcome",
    "SnapshotRequestOutcome",
]
