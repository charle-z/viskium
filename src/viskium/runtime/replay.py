"""Deterministic synthetic replay modes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from viskium.adapters.deterministic_processor import DeterministicProcessor
from viskium.adapters.memory_store import MemoryStore
from viskium.adapters.synthetic import SyntheticSource
from viskium.core import FrameEnvelope, PersistenceReceipt
from viskium.limits import MAX_SYNTHETIC_REPLAY_FRAMES
from viskium.runtime.clocks import VirtualClock

type ReplayMode = Literal["exhaustive", "faithful"]

_FRAME_INTERVAL_NS = 10_000_000
_PROCESSING_TIME_NS = 25_000_000


@dataclass(frozen=True, slots=True)
class ReplayReport:
    """Stable, JSON-compatible summary of a synthetic replay."""

    mode: ReplayMode
    requested_frames: int
    produced_frames: int
    processed_frames: int
    dropped_frames: int
    accepted_observations: int
    coalesced_observations: int
    rejected_observations: int
    processed_sequences: tuple[int, ...]
    dropped_sequences: tuple[int, ...]
    observation_crc32: tuple[str, ...]
    virtual_duration_ns: int
    store_bytes: int

    def __post_init__(self) -> None:
        if self.mode not in {"exhaustive", "faithful"}:
            raise ValueError("unsupported replay mode")
        counts = (
            self.requested_frames,
            self.produced_frames,
            self.processed_frames,
            self.dropped_frames,
            self.accepted_observations,
            self.coalesced_observations,
            self.rejected_observations,
            self.virtual_duration_ns,
            self.store_bytes,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in counts):
            raise TypeError("replay report counts must be integers")
        if any(value < 0 for value in counts):
            raise ValueError("replay report counts must be non-negative")
        if self.requested_frames != self.produced_frames:
            raise ValueError("synthetic replay must produce every requested frame")
        if self.produced_frames != self.processed_frames + self.dropped_frames:
            raise ValueError("every produced frame must be processed or dropped")
        if len(self.processed_sequences) != self.processed_frames:
            raise ValueError("processed sequence count must match processed_frames")
        if len(self.dropped_sequences) != self.dropped_frames:
            raise ValueError("dropped sequence count must match dropped_frames")
        all_sequences = self.processed_sequences + self.dropped_sequences
        if sorted(all_sequences) != list(range(self.produced_frames)):
            raise ValueError("processed and dropped sequences must partition produced frames")
        if (
            self.accepted_observations + self.coalesced_observations + self.rejected_observations
            != self.processed_frames
        ):
            raise ValueError("every processed frame must have one persistence outcome")
        if len(self.observation_crc32) != self.accepted_observations:
            raise ValueError("CRC count must match newly accepted observations")
        if any(
            len(value) != 8
            or value != value.lower()
            or any(char not in "0123456789abcdef" for char in value)
            for value in self.observation_crc32
        ):
            raise ValueError("observation CRC values must be lowercase hexadecimal")

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "requested_frames": self.requested_frames,
            "produced_frames": self.produced_frames,
            "processed_frames": self.processed_frames,
            "dropped_frames": self.dropped_frames,
            "accepted_observations": self.accepted_observations,
            "coalesced_observations": self.coalesced_observations,
            "rejected_observations": self.rejected_observations,
            "processed_sequences": list(self.processed_sequences),
            "dropped_sequences": list(self.dropped_sequences),
            "observation_crc32": list(self.observation_crc32),
            "virtual_duration_ns": self.virtual_duration_ns,
            "store_bytes": self.store_bytes,
        }


def run_synthetic_replay(mode: ReplayMode, frame_count: int) -> ReplayReport:
    """Run a dependency-free replay with deterministic admission decisions.

    ``exhaustive`` processes every frame. ``faithful`` simulates a processor that
    takes 25 ms while frames arrive every 10 ms, retaining only the newest pending
    frame. No wall clock, sleep, thread, file, or random global state is involved.
    """

    if mode not in {"exhaustive", "faithful"}:
        raise ValueError("mode must be 'exhaustive' or 'faithful'")
    if isinstance(frame_count, bool) or not isinstance(frame_count, int) or frame_count < 0:
        raise ValueError("frame_count must be a non-negative integer")
    if frame_count > MAX_SYNTHETIC_REPLAY_FRAMES:
        raise ValueError(f"frame_count must not exceed {MAX_SYNTHETIC_REPLAY_FRAMES}")

    clock = VirtualClock()
    source = SyntheticSource(
        frame_count,
        seed=0,
        frame_interval_ns=_FRAME_INTERVAL_NS,
        clock=clock,
    )
    processor = DeterministicProcessor()
    store = MemoryStore(
        max_observations=max(1, frame_count),
        max_bytes=max(4_096, frame_count * 2_048),
    )
    session_id = f"synthetic-{mode}-v1"
    processed_sequences: list[int] = []
    dropped_sequences: list[int] = []
    receipts: list[PersistenceReceipt] = []
    busy_until_ns = 0

    def process_frame(frame: FrameEnvelope) -> None:
        observation = processor.process(frame, session_id=session_id)
        receipts.append(store.put(observation))
        processed_sequences.append(frame.sequence)

    try:
        if mode == "exhaustive":
            for frame in source:
                process_frame(frame)
            if frame_count:
                busy_until_ns = (frame_count - 1) * _FRAME_INTERVAL_NS
        else:
            pending: FrameEnvelope | None = None
            processor_active = False

            for frame in source:
                frame_time_ns = frame.received_monotonic_ns
                if not processor_active:
                    process_frame(frame)
                    busy_until_ns = frame_time_ns + _PROCESSING_TIME_NS
                    processor_active = True
                    continue

                if frame_time_ns >= busy_until_ns and pending is not None:
                    process_frame(pending)
                    pending = None
                    busy_until_ns += _PROCESSING_TIME_NS

                if frame_time_ns >= busy_until_ns:
                    process_frame(frame)
                    busy_until_ns = frame_time_ns + _PROCESSING_TIME_NS
                else:
                    if pending is not None:
                        dropped_sequences.append(pending.sequence)
                    pending = frame

            if pending is not None:
                process_frame(pending)
                busy_until_ns += _PROCESSING_TIME_NS
        accepted = sum(receipt.status == "accepted" for receipt in receipts)
        coalesced = sum(receipt.status == "coalesced" for receipt in receipts)
        rejected = sum(receipt.status in {"rejected", "failed", "gap"} for receipt in receipts)
        observations = store.observations
        report = ReplayReport(
            mode=mode,
            requested_frames=frame_count,
            produced_frames=source.produced_count,
            processed_frames=len(processed_sequences),
            dropped_frames=len(dropped_sequences),
            accepted_observations=accepted,
            coalesced_observations=coalesced,
            rejected_observations=rejected,
            processed_sequences=tuple(processed_sequences),
            dropped_sequences=tuple(dropped_sequences),
            observation_crc32=tuple(str(item.payload["crc32_hex"]) for item in observations),
            virtual_duration_ns=max(clock.monotonic_ns(), busy_until_ns),
            store_bytes=store.bytes_used,
        )
    finally:
        source.close()
        store.close()
    return report
