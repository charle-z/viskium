"""A thread-safe, capacity-one latest-frame handoff."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from enum import StrEnum
from threading import Condition

from viskium.core import FrameEnvelope

MAX_TAKE_TIMEOUT_SECONDS = 60.0


class OfferStatus(StrEnum):
    ACCEPTED = "accepted"
    REPLACED = "replaced"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class FrameOffer:
    status: OfferStatus
    replaced_sequence: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, OfferStatus):
            raise TypeError("status must be an OfferStatus")
        if self.status is OfferStatus.REPLACED:
            if (
                isinstance(self.replaced_sequence, bool)
                or not isinstance(self.replaced_sequence, int)
                or self.replaced_sequence < 0
            ):
                raise ValueError("a replaced offer requires a non-negative sequence")
        elif self.replaced_sequence is not None:
            raise ValueError("only a replaced offer may include a replaced sequence")


class LatestFrameSlot:
    """Retain at most one pending frame and replace obsolete work atomically."""

    def __init__(self) -> None:
        self._condition = Condition()
        self._pending: FrameEnvelope | None = None
        self._closed = False
        self._offered_count = 0
        self._replaced_count = 0
        self._taken_count = 0
        self._rejected_count = 0

    def offer(self, frame: FrameEnvelope) -> FrameOffer:
        if not isinstance(frame, FrameEnvelope):
            raise TypeError("frame must be a FrameEnvelope")
        with self._condition:
            if self._closed:
                self._rejected_count += 1
                return FrameOffer(OfferStatus.CLOSED)
            self._offered_count += 1
            replaced = self._pending
            self._pending = frame
            self._condition.notify()
            if replaced is None:
                return FrameOffer(OfferStatus.ACCEPTED)
            self._replaced_count += 1
            return FrameOffer(OfferStatus.REPLACED, replaced.sequence)

    def take(self, *, timeout_seconds: float) -> FrameEnvelope | None:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, float):
            raise TypeError("timeout_seconds must be a float")
        if not math.isfinite(timeout_seconds) or timeout_seconds < 0.0:
            raise ValueError("timeout_seconds must be finite and non-negative")
        if timeout_seconds > MAX_TAKE_TIMEOUT_SECONDS:
            raise ValueError(f"timeout_seconds must not exceed {MAX_TAKE_TIMEOUT_SECONDS}")

        deadline = time.monotonic() + timeout_seconds
        with self._condition:
            while self._pending is None and not self._closed:
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return None
                self._condition.wait(remaining)
            frame = self._pending
            if frame is not None:
                self._pending = None
                self._taken_count += 1
            return frame

    def close(self) -> None:
        """Close the handoff and immediately release any pending raw frame."""

        with self._condition:
            self._closed = True
            self._pending = None
            self._condition.notify_all()

    @property
    def closed(self) -> bool:
        with self._condition:
            return self._closed

    @property
    def pending_count(self) -> int:
        with self._condition:
            return int(self._pending is not None)

    @property
    def offered_count(self) -> int:
        with self._condition:
            return self._offered_count

    @property
    def replaced_count(self) -> int:
        with self._condition:
            return self._replaced_count

    @property
    def taken_count(self) -> int:
        with self._condition:
            return self._taken_count

    @property
    def rejected_count(self) -> int:
        with self._condition:
            return self._rejected_count


__all__ = [
    "MAX_TAKE_TIMEOUT_SECONDS",
    "FrameOffer",
    "LatestFrameSlot",
    "OfferStatus",
]
