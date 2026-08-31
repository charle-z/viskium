"""Capacity-one rendezvous between snapshot demand and live frames."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from threading import Condition

from viskium.core import FrameEnvelope

from .contracts import MAX_SNAPSHOT_WAIT_SECONDS, SnapshotOfferOutcome, SnapshotRequestOutcome

_MAX_INT64 = 2**63 - 1
_REQUEST_OUTCOMES = frozenset({"ok", "busy", "timeout", "closed"})


def _normalize_timeout(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("timeout_seconds must be a number")
    timeout = float(value)
    if not math.isfinite(timeout):
        raise ValueError("timeout_seconds must be finite")
    if not 0.0 <= timeout <= MAX_SNAPSHOT_WAIT_SECONDS:
        raise ValueError(f"timeout_seconds must be between zero and {MAX_SNAPSHOT_WAIT_SECONDS:g}")
    return timeout


def _counter(value: object, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not 0 <= value <= _MAX_INT64:
        raise ValueError(f"{field_name} must fit signed int64")


@dataclass(frozen=True, slots=True)
class SnapshotRequestResult:
    """Result returned after the broker lock has been released."""

    outcome: SnapshotRequestOutcome
    frame: FrameEnvelope | None = None

    def __post_init__(self) -> None:
        if self.outcome not in _REQUEST_OUTCOMES:
            raise ValueError("unsupported snapshot request outcome")
        if self.outcome == "ok":
            if not isinstance(self.frame, FrameEnvelope):
                raise TypeError("ok snapshot requests require a FrameEnvelope")
        elif self.frame is not None:
            raise ValueError("non-ok snapshot requests must not expose a frame")


@dataclass(frozen=True, slots=True)
class SnapshotBrokerMetrics:
    """Immutable counters with no frame metadata or payloads."""

    requests_started: int
    requests_busy: int
    requests_timed_out: int
    requests_closed: int
    frames_delivered: int
    frames_no_demand: int
    offers_closed: int
    pending: bool
    closed: bool

    def __post_init__(self) -> None:
        for field_name in (
            "requests_started",
            "requests_busy",
            "requests_timed_out",
            "requests_closed",
            "frames_delivered",
            "frames_no_demand",
            "offers_closed",
        ):
            _counter(getattr(self, field_name), field_name)
        if not isinstance(self.pending, bool) or not isinstance(self.closed, bool):
            raise TypeError("pending and closed must be booleans")
        if self.closed and self.pending:
            raise ValueError("a closed broker cannot retain a pending request")


class SnapshotBroker:
    """Deliver at most one immutable frame to at most one pending request."""

    __slots__ = (
        "_closed",
        "_condition",
        "_frame",
        "_frames_delivered",
        "_frames_no_demand",
        "_offers_closed",
        "_pending",
        "_requests_busy",
        "_requests_closed",
        "_requests_started",
        "_requests_timed_out",
    )

    def __init__(self) -> None:
        self._condition = Condition()
        self._closed = False
        self._pending = False
        self._frame: FrameEnvelope | None = None
        self._requests_started = 0
        self._requests_busy = 0
        self._requests_timed_out = 0
        self._requests_closed = 0
        self._frames_delivered = 0
        self._frames_no_demand = 0
        self._offers_closed = 0

    def request(self, *, timeout_seconds: float = 0.0) -> SnapshotRequestResult:
        """Wait for one frame; callers perform any encoding after this returns."""

        timeout = _normalize_timeout(timeout_seconds)
        with self._condition:
            if self._closed:
                self._increment("_requests_closed")
                result = SnapshotRequestResult(outcome="closed")
            elif self._pending:
                self._increment("_requests_busy")
                result = SnapshotRequestResult(outcome="busy")
            else:
                self._pending = True
                self._increment("_requests_started")
                deadline = time.monotonic() + timeout
                while self._frame is None and not self._closed:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0.0:
                        break
                    self._condition.wait(timeout=remaining)

                if self._closed:
                    self._pending = False
                    self._frame = None
                    self._increment("_requests_closed")
                    result = SnapshotRequestResult(outcome="closed")
                elif self._frame is None:
                    self._pending = False
                    self._increment("_requests_timed_out")
                    result = SnapshotRequestResult(outcome="timeout")
                else:
                    frame = self._frame
                    self._frame = None
                    self._pending = False
                    self._increment("_frames_delivered")
                    result = SnapshotRequestResult(outcome="ok", frame=frame)

        # Leaving the condition before returning guarantees that encoding and
        # any other caller work happens outside the broker lock.
        return result

    def offer(self, frame: FrameEnvelope) -> SnapshotOfferOutcome:
        """Deliver a frame only when an unsatisfied request already exists."""

        if not isinstance(frame, FrameEnvelope):
            raise TypeError("frame must be a FrameEnvelope")
        with self._condition:
            if self._closed:
                self._increment("_offers_closed")
                return "closed"
            if not self._pending or self._frame is not None:
                self._increment("_frames_no_demand")
                return "no_demand"
            self._frame = frame
            self._condition.notify_all()
            return "delivered"

    def close(self) -> None:
        """Idempotently discard a reserved frame and wake a pending caller."""

        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._pending = False
            self._frame = None
            self._condition.notify_all()

    @property
    def closed(self) -> bool:
        with self._condition:
            return self._closed

    @property
    def metrics(self) -> SnapshotBrokerMetrics:
        with self._condition:
            return SnapshotBrokerMetrics(
                requests_started=self._requests_started,
                requests_busy=self._requests_busy,
                requests_timed_out=self._requests_timed_out,
                requests_closed=self._requests_closed,
                frames_delivered=self._frames_delivered,
                frames_no_demand=self._frames_no_demand,
                offers_closed=self._offers_closed,
                pending=self._pending,
                closed=self._closed,
            )

    def _increment(self, field_name: str) -> None:
        current = getattr(self, field_name)
        if current < _MAX_INT64:
            setattr(self, field_name, current + 1)


__all__ = ["SnapshotBroker", "SnapshotBrokerMetrics", "SnapshotRequestResult"]
