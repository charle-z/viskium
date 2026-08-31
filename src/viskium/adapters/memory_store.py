"""Bounded, in-memory observation store for deterministic slices."""

from __future__ import annotations

from viskium.core import ObservationEnvelope, PersistenceReceipt
from viskium.core.serialization import observation_size


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
            encoded_size = observation_size(observation, stop_after=self._max_bytes)
        except (RecursionError, TypeError, ValueError):
            return PersistenceReceipt(status="rejected", reason="payload_not_serializable")

        if encoded_size is None:
            return PersistenceReceipt(status="rejected", reason="observation_exceeds_byte_limit")
        if len(self._observations) >= self._max_observations:
            return PersistenceReceipt(status="rejected", reason="count_limit_reached")
        if self._bytes_used + encoded_size > self._max_bytes:
            return PersistenceReceipt(status="rejected", reason="byte_limit_reached")

        self._observations.append(observation)
        store_sequence = len(self._observations)
        self._sequences_by_key[observation.idempotency_key] = store_sequence
        self._observations_by_key[observation.idempotency_key] = observation
        self._bytes_used += encoded_size
        return PersistenceReceipt(
            status="accepted",
            store_sequence=store_sequence,
            bytes_accepted=encoded_size,
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
