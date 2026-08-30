"""Deterministic in-memory frame source used before hardware integration."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

from viskium.core import FrameEnvelope


class _AdvancingClock(Protocol):
    def monotonic_ns(self) -> int: ...

    def advance_ns(self, delta_ns: int) -> int: ...


class SyntheticSource(Iterator[FrameEnvelope]):
    """Produce a finite sequence of compact, deterministic grayscale frames."""

    _MAX_FRAME_BYTES = 1_048_576

    def __init__(
        self,
        frame_count: int,
        *,
        seed: int = 0,
        source_id: str = "synthetic",
        stream_epoch: int = 0,
        width: int = 8,
        height: int = 8,
        frame_interval_ns: int = 10_000_000,
        clock: _AdvancingClock | None = None,
    ) -> None:
        integer_fields = {
            "frame_count": frame_count,
            "seed": seed,
            "stream_epoch": stream_epoch,
            "width": width,
            "height": height,
            "frame_interval_ns": frame_interval_ns,
        }
        for field_name, value in integer_fields.items():
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{field_name} must be an integer")
        if frame_count < 0:
            raise ValueError("frame_count must be a non-negative integer")
        if not source_id or not source_id.strip():
            raise ValueError("source_id must not be empty")
        if stream_epoch < 0:
            raise ValueError("stream_epoch must be non-negative")
        if width <= 0 or height <= 0:
            raise ValueError("width and height must be positive")
        if width * height > self._MAX_FRAME_BYTES:
            raise ValueError("synthetic frame exceeds the bounded payload limit")
        if frame_interval_ns <= 0:
            raise ValueError("frame_interval_ns must be positive")

        self._frame_count = frame_count
        self._seed = seed
        self._source_id = source_id
        self._stream_epoch = stream_epoch
        self._width = width
        self._height = height
        self._frame_interval_ns = frame_interval_ns
        self._clock = clock
        self._next_timestamp_ns = 0
        self._next_sequence = 0
        self._closed = False

    @property
    def source_id(self) -> str:
        return self._source_id

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def produced_count(self) -> int:
        return self._next_sequence

    def next_frame(self) -> FrameEnvelope | None:
        if self._closed or self._next_sequence >= self._frame_count:
            return None

        sequence = self._next_sequence
        timestamp_ns = (
            self._clock.monotonic_ns() if self._clock is not None else self._next_timestamp_ns
        )
        frame_size = self._width * self._height
        payload = bytes(
            (self._seed * 17 + sequence * 31 + offset * 13) & 0xFF for offset in range(frame_size)
        )
        frame = FrameEnvelope(
            source_id=self._source_id,
            stream_epoch=self._stream_epoch,
            sequence=sequence,
            source_timestamp_ns=timestamp_ns,
            received_monotonic_ns=timestamp_ns,
            timestamp_quality="synthetic",
            width=self._width,
            height=self._height,
            pixel_format="GRAY8",
            stride=self._width,
            buffer_id=f"{self._source_id}:synthetic-buffer",
            generation=sequence,
            quality_flags=(),
            payload=payload,
        )
        self._next_sequence += 1
        if self._clock is not None:
            self._clock.advance_ns(self._frame_interval_ns)
        else:
            self._next_timestamp_ns += self._frame_interval_ns
        return frame

    def close(self) -> None:
        self._closed = True

    def __iter__(self) -> SyntheticSource:
        return self

    def __next__(self) -> FrameEnvelope:
        frame = self.next_frame()
        if frame is None:
            raise StopIteration
        return frame

    def __enter__(self) -> SyntheticSource:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
