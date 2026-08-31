"""Thread-safe latest-value slot for immutable Viskium observations."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Collection
from dataclasses import dataclass
from threading import Condition
from typing import Literal

from viskium.core import ObservationEnvelope

type OfferOutcome = Literal["accepted", "replaced", "closed"]
type ReadOutcome = Literal[
    "ok",
    "empty",
    "stale",
    "schema_mismatch",
    "timeout",
    "closed",
    "future_timestamp",
]

MAX_OBSERVATION_WAIT_SECONDS = 15.0
MAX_OBSERVATION_SCHEMA_IDS = 8
_MAX_SCHEMA_ID_CHARS = 256
_READ_OUTCOMES: frozenset[str] = frozenset(
    {
        "ok",
        "empty",
        "stale",
        "schema_mismatch",
        "timeout",
        "closed",
        "future_timestamp",
    }
)


def _require_non_negative_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def _normalize_wait_seconds(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("wait_seconds must be a number")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError("wait_seconds must be finite")
    if not 0.0 <= normalized <= MAX_OBSERVATION_WAIT_SECONDS:
        raise ValueError(f"wait_seconds must be between 0 and {MAX_OBSERVATION_WAIT_SECONDS:g}")
    return normalized


def _normalize_schema_ids(schema_ids: Collection[str] | None) -> frozenset[str] | None:
    if schema_ids is None:
        return None
    if isinstance(schema_ids, (str, bytes)):
        raise TypeError("schema_ids must be a collection of strings")
    if not isinstance(schema_ids, Collection):
        raise TypeError("schema_ids must be a sized collection of strings")
    if len(schema_ids) > MAX_OBSERVATION_SCHEMA_IDS:
        raise ValueError(f"schema_ids must not exceed {MAX_OBSERVATION_SCHEMA_IDS} entries")

    normalized: set[str] = set()
    for schema_id in schema_ids:
        if not isinstance(schema_id, str):
            raise TypeError("schema_ids entries must be strings")
        if not schema_id or not schema_id.strip():
            raise ValueError("schema_ids entries must not be empty")
        if len(schema_id) > _MAX_SCHEMA_ID_CHARS:
            raise ValueError(
                f"schema_ids entries must not exceed {_MAX_SCHEMA_ID_CHARS} characters"
            )
        normalized.add(schema_id)
    return frozenset(normalized)


@dataclass(frozen=True, slots=True)
class LatestObservationRead:
    """One bounded read result; only ``ok`` exposes an observation."""

    outcome: ReadOutcome
    observation: ObservationEnvelope | None = None
    age_ns: int | None = None

    def __post_init__(self) -> None:
        if self.outcome not in _READ_OUTCOMES:
            raise ValueError("unsupported latest-observation read outcome")
        if self.age_ns is not None:
            _require_non_negative_integer(self.age_ns, "age_ns")

        if self.outcome == "ok":
            if not isinstance(self.observation, ObservationEnvelope):
                raise TypeError("ok reads require an ObservationEnvelope")
            if self.age_ns is None:
                raise ValueError("ok reads require age_ns")
            return

        if self.observation is not None:
            raise ValueError("non-ok reads must not expose an observation")
        if self.outcome in {"stale", "schema_mismatch"}:
            if self.age_ns is None:
                raise ValueError(f"{self.outcome} reads require age_ns")
        elif self.age_ns is not None:
            raise ValueError(f"{self.outcome} reads must not report age_ns")


@dataclass(frozen=True, slots=True)
class LatestObservationMetrics:
    """Immutable snapshot of slot outcomes and current occupancy."""

    offers_accepted: int
    offers_replaced: int
    offers_closed: int
    reads_ok: int
    reads_empty: int
    reads_stale: int
    reads_schema_mismatch: int
    reads_timeout: int
    reads_closed: int
    reads_future_timestamp: int
    occupied: bool
    closed: bool

    def __post_init__(self) -> None:
        for field_name in (
            "offers_accepted",
            "offers_replaced",
            "offers_closed",
            "reads_ok",
            "reads_empty",
            "reads_stale",
            "reads_schema_mismatch",
            "reads_timeout",
            "reads_closed",
            "reads_future_timestamp",
        ):
            _require_non_negative_integer(getattr(self, field_name), field_name)
        if not isinstance(self.occupied, bool) or not isinstance(self.closed, bool):
            raise TypeError("occupied and closed must be booleans")
        if self.closed and self.occupied:
            raise ValueError("a closed slot cannot remain occupied")


class LatestObservationSlot:
    """A condition-backed slot that retains exactly zero or one observation.

    ``offer`` never waits for capacity: a present value is replaced immediately.
    ``read`` waits only while the slot is empty and never consumes the value.
    """

    __slots__ = (
        "_closed",
        "_condition",
        "_latest",
        "_monotonic_ns",
        "_offers_accepted",
        "_offers_closed",
        "_offers_replaced",
        "_reads_closed",
        "_reads_empty",
        "_reads_future_timestamp",
        "_reads_ok",
        "_reads_schema_mismatch",
        "_reads_stale",
        "_reads_timeout",
    )

    def __init__(self, *, monotonic_ns: Callable[[], int] | None = None) -> None:
        if monotonic_ns is not None and not callable(monotonic_ns):
            raise TypeError("monotonic_ns must be callable")
        self._condition = Condition()
        self._monotonic_ns = time.monotonic_ns if monotonic_ns is None else monotonic_ns
        self._latest: ObservationEnvelope | None = None
        self._closed = False
        self._offers_accepted = 0
        self._offers_replaced = 0
        self._offers_closed = 0
        self._reads_ok = 0
        self._reads_empty = 0
        self._reads_stale = 0
        self._reads_schema_mismatch = 0
        self._reads_timeout = 0
        self._reads_closed = 0
        self._reads_future_timestamp = 0

    @property
    def capacity(self) -> int:
        return 1

    @property
    def closed(self) -> bool:
        with self._condition:
            return self._closed

    def __len__(self) -> int:
        with self._condition:
            return int(self._latest is not None)

    def offer(self, observation: ObservationEnvelope) -> OfferOutcome:
        """Offer one observation without queueing or waiting for free capacity."""

        if not isinstance(observation, ObservationEnvelope):
            raise TypeError("observation must be an ObservationEnvelope")
        with self._condition:
            if self._closed:
                self._offers_closed += 1
                return "closed"
            if self._latest is None:
                outcome: OfferOutcome = "accepted"
                self._offers_accepted += 1
            else:
                outcome = "replaced"
                self._offers_replaced += 1
            self._latest = observation
            self._condition.notify_all()
            return outcome

    def read(
        self,
        *,
        now_monotonic_ns: int,
        max_age_ns: int,
        wait_seconds: float = 0.0,
        schema_ids: Collection[str] | None = None,
    ) -> LatestObservationRead:
        """Read the latest admissible observation without consuming it.

        The caller-provided monotonic time keeps freshness decisions replayable.
        Waiting is only for an empty slot; an existing invalid value returns its
        explicit outcome immediately.
        """

        now_ns = _require_non_negative_integer(now_monotonic_ns, "now_monotonic_ns")
        maximum_age_ns = _require_non_negative_integer(max_age_ns, "max_age_ns")
        maximum_wait = _normalize_wait_seconds(wait_seconds)
        accepted_schemas = _normalize_schema_ids(schema_ids)
        effective_now_ns = now_ns

        with self._condition:
            if self._closed:
                return self._finish_read("closed")
            if self._latest is None and maximum_wait == 0.0:
                return self._finish_read("empty")
            if self._latest is None:
                wait_started_ns = _require_non_negative_integer(
                    self._monotonic_ns(), "monotonic clock value"
                )
                deadline = time.monotonic() + maximum_wait
                while self._latest is None and not self._closed:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0.0:
                        return self._finish_read("timeout")
                    self._condition.wait(timeout=remaining)
                if self._closed:
                    return self._finish_read("closed")
                if self._latest is None:
                    return self._finish_read("timeout")
                wait_finished_ns = _require_non_negative_integer(
                    self._monotonic_ns(), "monotonic clock value"
                )
                effective_now_ns += max(0, wait_finished_ns - wait_started_ns)

            observation = self._latest
            if observation.observed_monotonic_ns > effective_now_ns:
                return self._finish_read("future_timestamp")

            age_ns = effective_now_ns - observation.observed_monotonic_ns
            effective_max_age_ns = maximum_age_ns
            if observation.ttl_ns is not None:
                effective_max_age_ns = min(effective_max_age_ns, observation.ttl_ns)
            if age_ns > effective_max_age_ns:
                return self._finish_read("stale", age_ns=age_ns)
            if accepted_schemas is not None and observation.schema_id not in accepted_schemas:
                return self._finish_read("schema_mismatch", age_ns=age_ns)
            return self._finish_read("ok", observation=observation, age_ns=age_ns)

    def close(self) -> None:
        """Idempotently close the slot, discard its value, and wake readers."""

        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._latest = None
            self._condition.notify_all()

    @property
    def metrics(self) -> LatestObservationMetrics:
        return self.metrics_snapshot()

    def metrics_snapshot(self) -> LatestObservationMetrics:
        """Return counters without exposing mutable slot state."""

        with self._condition:
            return LatestObservationMetrics(
                offers_accepted=self._offers_accepted,
                offers_replaced=self._offers_replaced,
                offers_closed=self._offers_closed,
                reads_ok=self._reads_ok,
                reads_empty=self._reads_empty,
                reads_stale=self._reads_stale,
                reads_schema_mismatch=self._reads_schema_mismatch,
                reads_timeout=self._reads_timeout,
                reads_closed=self._reads_closed,
                reads_future_timestamp=self._reads_future_timestamp,
                occupied=self._latest is not None,
                closed=self._closed,
            )

    def _finish_read(
        self,
        outcome: ReadOutcome,
        *,
        observation: ObservationEnvelope | None = None,
        age_ns: int | None = None,
    ) -> LatestObservationRead:
        if outcome == "ok":
            self._reads_ok += 1
        elif outcome == "empty":
            self._reads_empty += 1
        elif outcome == "stale":
            self._reads_stale += 1
        elif outcome == "schema_mismatch":
            self._reads_schema_mismatch += 1
        elif outcome == "timeout":
            self._reads_timeout += 1
        elif outcome == "closed":
            self._reads_closed += 1
        else:
            self._reads_future_timestamp += 1
        return LatestObservationRead(outcome=outcome, observation=observation, age_ns=age_ns)


__all__ = [
    "MAX_OBSERVATION_SCHEMA_IDS",
    "MAX_OBSERVATION_WAIT_SECONDS",
    "LatestObservationMetrics",
    "LatestObservationRead",
    "LatestObservationSlot",
    "OfferOutcome",
    "ReadOutcome",
]
