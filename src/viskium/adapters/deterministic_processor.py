"""Small deterministic processor used to verify Viskium's contracts."""

from __future__ import annotations

import zlib

from viskium.core import FrameEnvelope, ObservationEnvelope


class DeterministicProcessor:
    """Convert frame bytes into a compact, structured checksum observation."""

    producer_id = "viskium.deterministic.crc32"
    producer_version = "1"
    schema_id = "viskium.synthetic.digest"
    schema_version = 1

    def process(self, frame: FrameEnvelope, *, session_id: str) -> ObservationEnvelope:
        if not session_id or not session_id.strip():
            raise ValueError("session_id must not be empty")

        crc32 = zlib.crc32(frame.payload) & 0xFFFFFFFF
        frame_identity = f"{frame.source_id}:{frame.stream_epoch}:{frame.sequence}:{crc32:08x}"
        return ObservationEnvelope(
            session_id=session_id,
            source_id=frame.source_id,
            stream_epoch=frame.stream_epoch,
            source_sequence=frame.sequence,
            observed_monotonic_ns=frame.received_monotonic_ns,
            producer_id=self.producer_id,
            producer_version=self.producer_version,
            schema_id=self.schema_id,
            schema_version=self.schema_version,
            payload={
                "byte_count": len(frame.payload),
                "byte_sum": sum(frame.payload),
                "crc32": crc32,
                "crc32_hex": f"{crc32:08x}",
                "pixel_format": frame.pixel_format,
            },
            confidence=1.0,
            provenance=(f"frame:{frame.source_id}:{frame.stream_epoch}:{frame.sequence}",),
            sensitivity_class="operational",
            persistence_class="routine",
            ttl_ns=300_000_000_000,
            idempotency_key=f"digest:{frame_identity}",
            trace_id=f"synthetic:{frame_identity}",
        )
