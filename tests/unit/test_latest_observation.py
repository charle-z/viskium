from __future__ import annotations

import math
from dataclasses import FrozenInstanceError
from threading import Event, Thread
from typing import Any

import pytest

from viskium.core import ObservationEnvelope
from viskium.observations import (
    LatestObservationMetrics,
    LatestObservationRead,
    LatestObservationSlot,
)


class _WaitClock:
    def __init__(self, started: Event, *, start_ns: int, finish_ns: int) -> None:
        self._started = started
        self._values = iter((start_ns, finish_ns))

    def __call__(self) -> int:
        value = next(self._values)
        self._started.set()
        return value


def _observation(
    *,
    sequence: int = 0,
    observed_monotonic_ns: int = 100,
    schema_id: str = "viskium.test",
    ttl_ns: int | None = None,
) -> ObservationEnvelope:
    return ObservationEnvelope(
        session_id="session",
        source_id="source",
        stream_epoch=0,
        source_sequence=sequence,
        observed_monotonic_ns=observed_monotonic_ns,
        producer_id="test",
        producer_version="1",
        schema_id=schema_id,
        schema_version=1,
        payload={"sequence": sequence},
        idempotency_key=f"observation-{sequence}",
        trace_id=f"trace-{sequence}",
        ttl_ns=ttl_ns,
    )


def test_slot_has_exact_capacity_one_and_offer_replaces_without_backlog() -> None:
    slot = LatestObservationSlot()
    first = _observation(sequence=1)
    second = _observation(sequence=2)

    assert slot.capacity == 1
    assert len(slot) == 0
    assert slot.offer(first) == "accepted"
    assert len(slot) == 1
    assert slot.offer(second) == "replaced"
    assert len(slot) == 1

    result = slot.read(now_monotonic_ns=100, max_age_ns=0)
    assert result == LatestObservationRead(outcome="ok", observation=second, age_ns=0)
    assert result.observation is second


def test_empty_and_timeout_are_distinct() -> None:
    slot = LatestObservationSlot()

    assert slot.read(now_monotonic_ns=0, max_age_ns=0).outcome == "empty"
    assert slot.read(now_monotonic_ns=0, max_age_ns=0, wait_seconds=0.01).outcome == "timeout"


def test_waiting_reader_is_woken_by_offer() -> None:
    slot = LatestObservationSlot()
    started = Event()
    finished = Event()
    results: list[LatestObservationRead] = []

    def read_once() -> None:
        started.set()
        results.append(slot.read(now_monotonic_ns=100, max_age_ns=1_000_000_000, wait_seconds=1.0))
        finished.set()

    reader = Thread(target=read_once)
    reader.start()
    assert started.wait(timeout=1.0)
    assert slot.offer(_observation()) == "accepted"
    assert finished.wait(timeout=1.0)
    reader.join(timeout=1.0)

    assert not reader.is_alive()
    assert results[0].outcome == "ok"


def _read_with_offer_during_wait(
    *,
    observation: ObservationEnvelope,
    now_monotonic_ns: int,
    elapsed_ns: int,
    max_age_ns: int,
) -> LatestObservationRead:
    wait_started = Event()
    clock = _WaitClock(wait_started, start_ns=10_000, finish_ns=10_000 + elapsed_ns)
    slot = LatestObservationSlot(monotonic_ns=clock)

    def offer_after_wait_starts() -> None:
        assert wait_started.wait(timeout=1.0)
        assert slot.offer(observation) == "accepted"

    producer = Thread(target=offer_after_wait_starts)
    producer.start()
    result = slot.read(
        now_monotonic_ns=now_monotonic_ns,
        max_age_ns=max_age_ns,
        wait_seconds=1.0,
    )
    producer.join(timeout=1.0)
    assert not producer.is_alive()
    return result


def test_observation_offered_from_another_thread_during_wait_is_not_artificially_future() -> None:
    observation = _observation(observed_monotonic_ns=110)

    result = _read_with_offer_during_wait(
        observation=observation,
        now_monotonic_ns=100,
        elapsed_ns=10,
        max_age_ns=0,
    )

    assert result == LatestObservationRead(outcome="ok", observation=observation, age_ns=0)


def test_observation_can_be_stale_after_effective_wait_time_advances() -> None:
    result = _read_with_offer_during_wait(
        observation=_observation(observed_monotonic_ns=100),
        now_monotonic_ns=100,
        elapsed_ns=11,
        max_age_ns=10,
    )

    assert result == LatestObservationRead(outcome="stale", age_ns=11)


def test_genuinely_future_observation_remains_future_after_wait() -> None:
    result = _read_with_offer_during_wait(
        observation=_observation(observed_monotonic_ns=112),
        now_monotonic_ns=100,
        elapsed_ns=11,
        max_age_ns=1_000,
    )

    assert result == LatestObservationRead(outcome="future_timestamp")


def test_close_discards_latest_rejects_offers_and_wakes_reader() -> None:
    slot = LatestObservationSlot()
    assert slot.offer(_observation()) == "accepted"
    slot.close()
    slot.close()

    assert slot.closed is True
    assert len(slot) == 0
    assert slot.offer(_observation(sequence=2)) == "closed"
    assert slot.read(now_monotonic_ns=100, max_age_ns=0).outcome == "closed"

    waiting_slot = LatestObservationSlot()
    started = Event()
    results: list[LatestObservationRead] = []

    def wait_until_closed() -> None:
        started.set()
        results.append(waiting_slot.read(now_monotonic_ns=100, max_age_ns=0, wait_seconds=1.0))

    reader = Thread(target=wait_until_closed)
    reader.start()
    assert started.wait(timeout=1.0)
    waiting_slot.close()
    reader.join(timeout=1.0)

    assert not reader.is_alive()
    assert results[0].outcome == "closed"


def test_age_and_ttl_apply_the_tighter_inclusive_ceiling() -> None:
    max_age_slot = LatestObservationSlot()
    max_age_slot.offer(_observation(observed_monotonic_ns=100, ttl_ns=1_000))
    assert max_age_slot.read(now_monotonic_ns=111, max_age_ns=10) == LatestObservationRead(
        outcome="stale", age_ns=11
    )

    ttl_slot = LatestObservationSlot()
    observation = _observation(observed_monotonic_ns=100, ttl_ns=10)
    ttl_slot.offer(observation)
    assert ttl_slot.read(now_monotonic_ns=110, max_age_ns=1_000).outcome == "ok"
    assert ttl_slot.read(now_monotonic_ns=111, max_age_ns=1_000) == LatestObservationRead(
        outcome="stale", age_ns=11
    )


def test_future_timestamp_is_explicit_and_never_exposes_observation() -> None:
    slot = LatestObservationSlot()
    slot.offer(_observation(observed_monotonic_ns=101))

    result = slot.read(now_monotonic_ns=100, max_age_ns=1_000)

    assert result == LatestObservationRead(outcome="future_timestamp")
    assert result.observation is None
    assert result.age_ns is None


def test_schema_filter_returns_only_the_latest_matching_schema() -> None:
    slot = LatestObservationSlot()
    observation = _observation(schema_id="viskium.alpha")
    slot.offer(observation)

    mismatch = slot.read(
        now_monotonic_ns=100,
        max_age_ns=0,
        schema_ids={"viskium.beta"},
    )
    accepted = slot.read(
        now_monotonic_ns=100,
        max_age_ns=0,
        schema_ids={"viskium.alpha"},
    )
    no_filter = slot.read(now_monotonic_ns=100, max_age_ns=0, schema_ids=set())

    assert mismatch == LatestObservationRead(outcome="schema_mismatch", age_ns=0)
    assert accepted.observation is observation
    assert no_filter.outcome == "schema_mismatch"


@pytest.mark.parametrize(
    ("kwargs", "error_type"),
    [
        ({"now_monotonic_ns": -1}, ValueError),
        ({"now_monotonic_ns": True}, TypeError),
        ({"max_age_ns": -1}, ValueError),
        ({"max_age_ns": False}, TypeError),
        ({"wait_seconds": -0.1}, ValueError),
        ({"wait_seconds": 15.01}, ValueError),
        ({"wait_seconds": math.inf}, ValueError),
        ({"wait_seconds": True}, TypeError),
        ({"schema_ids": "viskium.test"}, TypeError),
        ({"schema_ids": (item for item in ["viskium.test"])}, TypeError),
        ({"schema_ids": {str(index) for index in range(9)}}, ValueError),
        ({"schema_ids": {""}}, ValueError),
        ({"schema_ids": {"x" * 257}}, ValueError),
        ({"schema_ids": {1}}, TypeError),
    ],
)
def test_read_validates_all_control_plane_inputs(
    kwargs: dict[str, Any], error_type: type[Exception]
) -> None:
    values: dict[str, Any] = {
        "now_monotonic_ns": 100,
        "max_age_ns": 0,
    }
    values.update(kwargs)
    with pytest.raises(error_type):
        LatestObservationSlot().read(**values)


def test_offer_requires_an_observation_envelope() -> None:
    with pytest.raises(TypeError, match="ObservationEnvelope"):
        LatestObservationSlot().offer(object())  # type: ignore[arg-type]


def test_metrics_are_frozen_slotted_and_count_stable_outcomes() -> None:
    slot = LatestObservationSlot()
    assert slot.read(now_monotonic_ns=100, max_age_ns=0).outcome == "empty"
    slot.offer(_observation(schema_id="viskium.alpha"))
    slot.offer(_observation(sequence=2, schema_id="viskium.alpha"))
    assert (
        slot.read(now_monotonic_ns=100, max_age_ns=0, schema_ids={"viskium.beta"}).outcome
        == "schema_mismatch"
    )
    assert slot.read(now_monotonic_ns=101, max_age_ns=0).outcome == "stale"
    assert slot.read(now_monotonic_ns=99, max_age_ns=10).outcome == "future_timestamp"
    slot.close()
    assert slot.offer(_observation(sequence=3)) == "closed"
    assert slot.read(now_monotonic_ns=100, max_age_ns=0).outcome == "closed"

    metrics = slot.metrics
    assert metrics == LatestObservationMetrics(
        offers_accepted=1,
        offers_replaced=1,
        offers_closed=1,
        reads_ok=0,
        reads_empty=1,
        reads_stale=1,
        reads_schema_mismatch=1,
        reads_timeout=0,
        reads_closed=1,
        reads_future_timestamp=1,
        occupied=False,
        closed=True,
    )
    assert metrics == slot.metrics_snapshot()
    assert not hasattr(metrics, "__dict__")
    with pytest.raises(FrozenInstanceError):
        metrics.reads_ok = 1  # type: ignore[misc]


@pytest.mark.parametrize(
    "values",
    [
        {"offers_accepted": -1},
        {"reads_ok": True},
        {"occupied": 1},
        {"closed": "yes"},
        {"occupied": True, "closed": True},
    ],
)
def test_metrics_reject_impossible_values(values: dict[str, Any]) -> None:
    defaults: dict[str, Any] = {
        "offers_accepted": 0,
        "offers_replaced": 0,
        "offers_closed": 0,
        "reads_ok": 0,
        "reads_empty": 0,
        "reads_stale": 0,
        "reads_schema_mismatch": 0,
        "reads_timeout": 0,
        "reads_closed": 0,
        "reads_future_timestamp": 0,
        "occupied": False,
        "closed": False,
    }
    defaults.update(values)
    with pytest.raises((TypeError, ValueError)):
        LatestObservationMetrics(**defaults)


@pytest.mark.parametrize(
    "values",
    [
        {"outcome": "unknown"},
        {"outcome": "ok"},
        {"outcome": "ok", "observation": _observation()},
        {"outcome": "empty", "observation": _observation()},
        {"outcome": "empty", "age_ns": 0},
        {"outcome": "stale"},
        {"outcome": "future_timestamp", "age_ns": 0},
        {"outcome": "stale", "age_ns": -1},
    ],
)
def test_read_result_rejects_impossible_states(values: dict[str, Any]) -> None:
    with pytest.raises((TypeError, ValueError)):
        LatestObservationRead(**values)
