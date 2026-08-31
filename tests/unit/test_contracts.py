from __future__ import annotations

import math
from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from viskium.adapters import DeterministicProcessor, MemoryStore, SyntheticSource
from viskium.core import (
    FrameEnvelope,
    FrameSource,
    ObservationEnvelope,
    ObservationStore,
    PersistenceReceipt,
    Processor,
)
from viskium.runtime.clocks import SystemClock, VirtualClock


def _frame(**overrides: Any) -> FrameEnvelope:
    values: dict[str, Any] = {
        "source_id": "fixture",
        "stream_epoch": 0,
        "sequence": 0,
        "received_monotonic_ns": 1,
        "payload": b"x",
    }
    values.update(overrides)
    return FrameEnvelope(**values)


def _observation(sequence: int = 0) -> ObservationEnvelope:
    return DeterministicProcessor().process(_frame(sequence=sequence), session_id="test-session")


@pytest.mark.parametrize(
    "overrides",
    [
        {"stream_epoch": 2**63},
        {"sequence": 2**63},
        {"received_monotonic_ns": 2**63},
        {"source_timestamp_ns": 2**63},
        {"width": 2**63, "height": 1},
        {"height": 2**63, "width": 1},
        {"stride": 2**63},
        {"generation": 2**63},
    ],
)
def test_frame_integer_fields_reject_signed_64_bit_overflow(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="signed 64 bits"):
        _frame(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"stream_epoch": 2**63},
        {"source_sequence": 2**63},
        {"observed_monotonic_ns": 2**63},
        {"schema_version": 2**63},
        {"ttl_ns": 2**63},
    ],
)
def test_observation_integer_fields_reject_signed_64_bit_overflow(
    overrides: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match="signed 64 bits"):
        replace(_observation(), **overrides)


@pytest.mark.parametrize("field_name", ["store_sequence", "bytes_accepted"])
def test_receipt_integer_fields_reject_signed_64_bit_overflow(field_name: str) -> None:
    values: dict[str, Any] = {"status": "accepted", field_name: 2**63}
    with pytest.raises(ValueError, match="signed 64 bits"):
        PersistenceReceipt(**values)


@pytest.mark.parametrize("reason", ["", " ", "x" * 129, object()])
def test_persistence_receipt_reason_is_bounded(reason: object) -> None:
    with pytest.raises((TypeError, ValueError), match="reason"):
        PersistenceReceipt(status="failed", reason=reason)  # type: ignore[arg-type]


def test_contracts_are_frozen_and_payload_is_read_only() -> None:
    frame = next(SyntheticSource(1))
    observation = DeterministicProcessor().process(frame, session_id="test-session")

    with pytest.raises(FrozenInstanceError):
        frame.sequence = 9  # type: ignore[misc]
    with pytest.raises(TypeError):
        observation.payload["crc32"] = 0  # type: ignore[index]


def test_dependency_free_adapters_satisfy_ports() -> None:
    assert isinstance(SyntheticSource(0), FrameSource)
    assert isinstance(DeterministicProcessor(), Processor)
    assert isinstance(MemoryStore(), ObservationStore)


def test_processor_emits_structured_repeatable_digest() -> None:
    frame = FrameEnvelope(
        source_id="fixture",
        stream_epoch=2,
        sequence=3,
        received_monotonic_ns=40,
        payload=bytes((1, 2, 3, 4)),
    )
    processor = DeterministicProcessor()

    first = processor.process(frame, session_id="session")
    second = processor.process(frame, session_id="session")

    assert first == second
    assert first.payload["byte_count"] == 4
    assert first.payload["byte_sum"] == 10
    assert first.payload["crc32_hex"] == "b63cfbcd"


def test_memory_store_enforces_idempotency_and_count_bound() -> None:
    processor = DeterministicProcessor()
    source = SyntheticSource(2)
    first = processor.process(next(source), session_id="session")
    second = processor.process(next(source), session_id="session")
    store = MemoryStore(max_observations=1, max_bytes=4_096)

    accepted = store.put(first)
    duplicate = store.put(first)
    full = store.put(second)

    assert accepted.status == "accepted"
    assert accepted.bytes_accepted == store.bytes_used
    assert duplicate.status == "coalesced"
    assert duplicate.store_sequence == accepted.store_sequence
    assert full.status == "rejected"
    assert full.reason == "count_limit_reached"
    assert len(store) == 1


def test_memory_store_enforces_byte_bound_without_partial_acceptance() -> None:
    observation = DeterministicProcessor().process(next(SyntheticSource(1)), session_id="session")
    store = MemoryStore(max_observations=4, max_bytes=16)

    receipt = store.put(observation)

    assert receipt.status == "rejected"
    assert receipt.reason == "observation_exceeds_byte_limit"
    assert store.bytes_used == 0
    assert store.observations == ()


@pytest.mark.parametrize(
    ("overrides", "error_type"),
    [
        ({"source_id": ""}, ValueError),
        ({"stream_epoch": -1}, ValueError),
        ({"sequence": -1}, ValueError),
        ({"received_monotonic_ns": -1}, ValueError),
        ({"source_timestamp_ns": -1}, ValueError),
        ({"payload": bytearray(b"x")}, TypeError),
        ({"width": -1}, ValueError),
        ({"width": 0, "height": 1}, ValueError),
        ({"width": 2, "stride": 1}, ValueError),
        ({"generation": -1}, ValueError),
        ({"timestamp_quality": "guess"}, ValueError),
    ],
)
def test_frame_contract_rejects_invalid_metadata(
    overrides: dict[str, Any], error_type: type[Exception]
) -> None:
    with pytest.raises(error_type):
        _frame(**overrides)


def test_frame_contract_normalizes_quality_flags() -> None:
    frame = _frame(quality_flags=["synthetic-warning"])

    assert frame.quality_flags == ("synthetic-warning",)


@pytest.mark.parametrize(
    ("field_name", "value", "error_type"),
    [
        ("session_id", " ", ValueError),
        ("stream_epoch", -1, ValueError),
        ("source_sequence", -1, ValueError),
        ("observed_monotonic_ns", -1, ValueError),
        ("schema_version", 0, ValueError),
        ("confidence", -0.1, ValueError),
        ("confidence", 1.1, ValueError),
        ("ttl_ns", 0, ValueError),
        ("sensitivity_class", "secret", ValueError),
        ("persistence_class", "forever", ValueError),
        ("payload", [], TypeError),
        ("payload", {1: "non-string-key"}, TypeError),
    ],
)
def test_observation_contract_rejects_invalid_fields(
    field_name: str, value: object, error_type: type[Exception]
) -> None:
    with pytest.raises(error_type):
        replace(_observation(), **{field_name: value})


def test_observation_deep_freezes_payload_and_normalizes_provenance() -> None:
    payload = {"nested": {"values": [1, 2]}}
    observation = replace(_observation(), payload=payload, provenance=["frame:1"])

    payload["nested"]["values"].append(3)  # type: ignore[index,union-attr]
    nested = observation.payload["nested"]
    assert nested["values"] == (1, 2)  # type: ignore[index]
    with pytest.raises(TypeError):
        nested["other"] = True  # type: ignore[index]
    assert observation.provenance == ("frame:1",)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, 2**63, -(2**63) - 1])
def test_observation_rejects_noncanonical_numeric_payloads(value: float | int) -> None:
    with pytest.raises(ValueError, match="payload"):
        replace(_observation(), payload={"value": value})


@pytest.mark.parametrize(
    "payload",
    [
        {"text": "x" * 65_537},
        {("k" * 250) + f"{index:06}": "x" for index in range(256)},
    ],
)
def test_observation_rejects_payloads_with_excessive_total_text(payload: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="text characters"):
        replace(_observation(), payload=payload)


def test_observation_rejects_cycles_and_excessive_nesting() -> None:
    cyclic: dict[str, Any] = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValueError, match="cycles"):
        replace(_observation(), payload=cyclic)

    nested: object = "leaf"
    for _ in range(34):
        nested = [nested]
    with pytest.raises(ValueError, match="nesting"):
        replace(_observation(), payload={"nested": nested})

    with pytest.raises(ValueError, match="structured values"):
        replace(_observation(), payload={"many": [None] * 4_096})


@pytest.mark.parametrize(
    "overrides",
    [
        {"stream_epoch": 1.5},
        {"sequence": True},
        {"received_monotonic_ns": 1.0},
        {"width": True},
        {"quality_flags": "not-a-sequence-of-flags"},
    ],
)
def test_frame_contract_rejects_wrong_runtime_types(overrides: dict[str, Any]) -> None:
    with pytest.raises(TypeError, match=r".+"):
        _frame(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"schema_version": 1.0},
        {"source_sequence": False},
        {"confidence": 1},
        {"ttl_ns": True},
        {"provenance": "not-a-sequence-of-provenance"},
    ],
)
def test_observation_contract_rejects_wrong_runtime_types(overrides: dict[str, Any]) -> None:
    with pytest.raises(TypeError, match=r".+"):
        replace(_observation(), **overrides)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"status": "unknown"},
        {"status": "accepted", "store_sequence": 0},
        {"status": "accepted", "bytes_accepted": -1},
        {"status": "rejected", "bytes_accepted": 1},
    ],
)
def test_persistence_receipt_rejects_inconsistent_outcomes(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match=r".+"):
        PersistenceReceipt(**kwargs)


def test_persistence_receipt_accepted_property_is_explicit() -> None:
    assert PersistenceReceipt(status="accepted").accepted
    assert PersistenceReceipt(status="coalesced").accepted
    assert not PersistenceReceipt(status="rejected").accepted


def test_virtual_and_system_clocks_obey_monotonic_contract() -> None:
    clock = VirtualClock(10)

    assert clock.monotonic_ns() == 10
    assert clock.advance_ns(5) == 15
    assert clock.advance_to_ns(20) == 20
    with pytest.raises(ValueError, match="backwards"):
        clock.advance_ns(-1)
    with pytest.raises(ValueError, match="backwards"):
        clock.advance_to_ns(19)
    with pytest.raises(ValueError, match="non-negative"):
        VirtualClock(-1)
    assert SystemClock().monotonic_ns() > 0


@pytest.mark.parametrize(
    ("kwargs", "error_type"),
    [
        ({"frame_count": -1}, ValueError),
        ({"frame_count": True}, TypeError),
        ({"frame_count": 1.5}, TypeError),
        ({"frame_count": 1, "seed": 1.5}, TypeError),
        ({"frame_count": 1, "source_id": ""}, ValueError),
        ({"frame_count": 1, "stream_epoch": -1}, ValueError),
        ({"frame_count": 1, "width": 0}, ValueError),
        ({"frame_count": 1, "height": 0}, ValueError),
        ({"frame_count": 1, "width": 1_048_577, "height": 1}, ValueError),
        ({"frame_count": 1, "frame_interval_ns": 0}, ValueError),
    ],
)
def test_synthetic_source_rejects_unbounded_or_invalid_configuration(
    kwargs: dict[str, Any],
    error_type: type[Exception],
) -> None:
    with pytest.raises(error_type, match=r".+"):
        SyntheticSource(**kwargs)


def test_synthetic_source_lifecycle_and_internal_clock_are_bounded() -> None:
    source = SyntheticSource(2, frame_interval_ns=7)

    first = source.next_frame()
    second = source.next_frame()
    assert first is not None
    assert first.received_monotonic_ns == 0
    assert second is not None
    assert second.received_monotonic_ns == 7
    assert source.source_id == "synthetic"
    assert source.produced_count == 2
    assert source.next_frame() is None
    with pytest.raises(StopIteration):
        next(source)

    with SyntheticSource(1) as managed:
        assert not managed.closed
    assert managed.closed
    assert managed.next_frame() is None


def test_processor_rejects_empty_session_identity() -> None:
    with pytest.raises(ValueError, match="session_id"):
        DeterministicProcessor().process(_frame(), session_id="")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_observations": 0},
        {"max_observations": True},
        {"max_bytes": 0},
        {"max_bytes": True},
    ],
)
def test_memory_store_rejects_invalid_limits(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        MemoryStore(**kwargs)


def test_memory_store_rejects_conflict_and_unserializable_payload() -> None:
    first = _observation()
    conflicting = replace(first, payload={"different": True})
    store = MemoryStore(max_observations=4, max_bytes=8_192)

    assert store.put(first).status == "accepted"
    conflict = store.put(conflicting)
    assert conflict.status == "rejected"
    assert conflict.reason == "idempotency_conflict"
    assert conflict.store_sequence == 1

    with pytest.raises(TypeError, match="unsupported"):
        replace(_observation(1), payload={"object": object()})


def test_memory_store_stops_sizing_at_byte_ceiling() -> None:
    observation = replace(_observation(), payload={"large": "x" * 10_000})
    store = MemoryStore(max_observations=1, max_bytes=1_024)

    receipt = store.put(observation)

    assert receipt.status == "rejected"
    assert receipt.reason == "observation_exceeds_byte_limit"
    assert store.bytes_used == 0


def test_memory_store_enforces_cumulative_bytes_then_can_clear() -> None:
    first = _observation(0)
    second = _observation(1)
    probe = MemoryStore(max_observations=2, max_bytes=8_192)
    first_size = probe.put(first).bytes_accepted
    second_size = probe.put(second).bytes_accepted
    bounded = MemoryStore(
        max_observations=2,
        max_bytes=max(first_size, second_size, first_size + second_size - 1),
    )

    assert bounded.max_observations == 2
    assert bounded.max_bytes >= first_size
    assert bounded.put(first).status == "accepted"
    receipt = bounded.put(second)
    assert receipt.status == "rejected"
    assert receipt.reason == "byte_limit_reached"
    bounded.clear()
    assert bounded.bytes_used == 0
    assert len(bounded) == 0


def test_memory_store_context_closes_and_rejects_later_writes() -> None:
    with MemoryStore() as store:
        assert not store.closed
        assert store.put(_observation()).accepted
        assert len(store) == 1

    assert store.closed
    assert store.observations == ()
    assert store.bytes_used == 0
    receipt = store.put(_observation())
    assert receipt.status == "failed"
    assert receipt.reason == "store_closed"
