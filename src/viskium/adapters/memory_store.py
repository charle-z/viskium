"""Bounded, in-memory observation store for deterministic slices."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from typing import Any

from viskium.core import ObservationEnvelope, PersistenceReceipt

_STRING_CHUNK_CHARS = 4_096


def _json_string_tokens(value: str) -> Iterator[str]:
    """Yield an escaped JSON string without materializing an unbounded copy."""

    yield '"'
    for offset in range(0, len(value), _STRING_CHUNK_CHARS):
        escaped = json.dumps(value[offset : offset + _STRING_CHUNK_CHARS], ensure_ascii=False)
        yield escaped[1:-1]
    yield '"'


def _json_tokens(value: Any) -> Iterator[str]:
    if value is None:
        yield "null"
        return
    if value is True:
        yield "true"
        return
    if value is False:
        yield "false"
        return
    if isinstance(value, str):
        yield from _json_string_tokens(value)
        return
    if isinstance(value, int):
        yield str(value)
        return
    if isinstance(value, float):
        yield json.dumps(value, allow_nan=False, separators=(",", ":"))
        return
    if isinstance(value, Mapping):
        yield "{"
        for index, key in enumerate(sorted(value)):
            if index:
                yield ","
            yield from _json_string_tokens(key)
            yield ":"
            yield from _json_tokens(value[key])
        yield "}"
        return
    if isinstance(value, tuple):
        yield "["
        for index, item in enumerate(value):
            if index:
                yield ","
            yield from _json_tokens(item)
        yield "]"
        return
    raise TypeError(f"unsupported observation payload type: {type(value).__name__}")


def _encoded_size(observation: ObservationEnvelope, *, stop_after: int) -> int | None:
    """Return canonical JSON bytes, or ``None`` once the ceiling is crossed."""

    document = {
        "session_id": observation.session_id,
        "source_id": observation.source_id,
        "stream_epoch": observation.stream_epoch,
        "source_sequence": observation.source_sequence,
        "observed_monotonic_ns": observation.observed_monotonic_ns,
        "producer_id": observation.producer_id,
        "producer_version": observation.producer_version,
        "schema_id": observation.schema_id,
        "schema_version": observation.schema_version,
        "payload": dict(observation.payload),
        "idempotency_key": observation.idempotency_key,
        "trace_id": observation.trace_id,
        "confidence": observation.confidence,
        "provenance": observation.provenance,
        "sensitivity_class": observation.sensitivity_class,
        "persistence_class": observation.persistence_class,
        "ttl_ns": observation.ttl_ns,
        "wall_utc": observation.wall_utc,
    }
    encoded_size = 0
    for token in _json_tokens(document):
        encoded_size += len(token.encode("utf-8"))
        if encoded_size > stop_after:
            return None
    return encoded_size


class MemoryStore:
    """Keep observations within strict count and serialized-byte limits.

    The store never evicts silently. A full store returns an explicit rejection,
    preserving deterministic replay and making backpressure visible to callers.
    """

    def __init__(self, *, max_observations: int = 1_024, max_bytes: int = 1_048_576):
        if (
            isinstance(max_observations, bool)
            or not isinstance(max_observations, int)
            or max_observations <= 0
        ):
            raise ValueError("max_observations must be a positive integer")
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
            raise ValueError("max_bytes must be a positive integer")
        self._max_observations = max_observations
        self._max_bytes = max_bytes
        self._observations: list[ObservationEnvelope] = []
        self._sequences_by_key: dict[str, int] = {}
        self._observations_by_key: dict[str, ObservationEnvelope] = {}
        self._bytes_used = 0
        self._closed = False

    @property
    def max_observations(self) -> int:
        return self._max_observations

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def bytes_used(self) -> int:
        return self._bytes_used

    @property
    def observations(self) -> tuple[ObservationEnvelope, ...]:
        return tuple(self._observations)

    @property
    def closed(self) -> bool:
        return self._closed

    def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
        if self._closed:
            return PersistenceReceipt(status="failed", reason="store_closed")

        existing = self._observations_by_key.get(observation.idempotency_key)
        if existing is not None:
            store_sequence = self._sequences_by_key[observation.idempotency_key]
            if existing == observation:
                return PersistenceReceipt(
                    status="coalesced",
                    reason="duplicate_idempotency_key",
                    store_sequence=store_sequence,
                )
            return PersistenceReceipt(
                status="rejected",
                reason="idempotency_conflict",
                store_sequence=store_sequence,
            )

        try:
            observation_size = _encoded_size(observation, stop_after=self._max_bytes)
        except (RecursionError, TypeError, ValueError):
            return PersistenceReceipt(status="rejected", reason="payload_not_serializable")

        if observation_size is None:
            return PersistenceReceipt(status="rejected", reason="observation_exceeds_byte_limit")
        if len(self._observations) >= self._max_observations:
            return PersistenceReceipt(status="rejected", reason="count_limit_reached")
        if self._bytes_used + observation_size > self._max_bytes:
            return PersistenceReceipt(status="rejected", reason="byte_limit_reached")

        self._observations.append(observation)
        store_sequence = len(self._observations)
        self._sequences_by_key[observation.idempotency_key] = store_sequence
        self._observations_by_key[observation.idempotency_key] = observation
        self._bytes_used += observation_size
        return PersistenceReceipt(
            status="accepted",
            store_sequence=store_sequence,
            bytes_accepted=observation_size,
        )

    def clear(self) -> None:
        self._observations.clear()
        self._sequences_by_key.clear()
        self._observations_by_key.clear()
        self._bytes_used = 0

    def close(self) -> None:
        if self._closed:
            return
        self.clear()
        self._closed = True

    def __len__(self) -> int:
        return len(self._observations)

    def __enter__(self) -> MemoryStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
