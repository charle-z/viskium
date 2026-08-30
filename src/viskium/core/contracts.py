"""Immutable contracts shared by Viskium's neutral execution core."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, cast

type TimestampQuality = Literal["synthetic", "source", "received"]
type PersistenceStatus = Literal["accepted", "coalesced", "rejected", "gap", "failed"]
type SensitivityClass = Literal["public", "operational", "sensitive", "identifiable", "prohibited"]
type PersistenceClass = Literal["routine", "important", "diagnostic", "visual"]

_MAX_IDENTIFIER_CHARS = 256
_MAX_OBSERVATION_NESTING = 32
_MAX_OBSERVATION_NODES = 4_096
_MAX_PROVENANCE_ENTRIES = 64
_MIN_JSON_INTEGER = -(2**63)
_MAX_JSON_INTEGER = 2**63 - 1


def _require_non_empty(value: str, field_name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    if len(value) > _MAX_IDENTIFIER_CHARS:
        raise ValueError(f"{field_name} exceeds {_MAX_IDENTIFIER_CHARS} characters")


def _require_integer(value: object, field_name: str, *, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum:
        qualifier = "positive" if minimum == 1 else "non-negative"
        raise ValueError(f"{field_name} must be {qualifier}")


def _normalize_string_tuple(
    value: object,
    field_name: str,
    *,
    max_entries: int,
) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise TypeError(f"{field_name} must be an iterable of strings")
    try:
        candidates: tuple[object, ...] = tuple(value)  # type: ignore[arg-type]
    except TypeError as error:
        raise TypeError(f"{field_name} must be an iterable of strings") from error
    if len(candidates) > max_entries:
        raise ValueError(f"{field_name} exceeds {max_entries} entries")
    normalized: list[str] = []
    for entry in candidates:
        if not isinstance(entry, str):
            raise TypeError(f"{field_name} entries must be strings")
        _require_non_empty(entry, f"{field_name} entry")
        normalized.append(entry)
    return tuple(normalized)


def _freeze_json_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    """Deep-copy JSON-shaped payloads into immutable containers.

    A bounded traversal prevents hostile nesting or collections from turning a
    supposedly small observation into unbounded validation work.
    """

    active_containers: set[int] = set()
    visited_nodes = 0

    def freeze(value: Any, *, depth: int) -> Any:
        nonlocal visited_nodes
        visited_nodes += 1
        if visited_nodes > _MAX_OBSERVATION_NODES:
            raise ValueError(f"payload exceeds {_MAX_OBSERVATION_NODES} structured values")
        if depth > _MAX_OBSERVATION_NESTING:
            raise ValueError(f"payload exceeds {_MAX_OBSERVATION_NESTING} nesting levels")

        if value is None or isinstance(value, (str, bool)):
            return value
        if isinstance(value, int):
            if not _MIN_JSON_INTEGER <= value <= _MAX_JSON_INTEGER:
                raise ValueError("payload integers must fit in signed 64 bits")
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("payload floats must be finite")
            return value

        if isinstance(value, Mapping):
            container_id = id(value)
            if container_id in active_containers:
                raise ValueError("payload must not contain reference cycles")
            active_containers.add(container_id)
            try:
                frozen_mapping: dict[str, Any] = {}
                for key, item in value.items():
                    if not isinstance(key, str):
                        raise TypeError("payload keys must be strings")
                    _require_non_empty(key, "payload key")
                    frozen_mapping[key] = freeze(item, depth=depth + 1)
                return MappingProxyType(frozen_mapping)
            finally:
                active_containers.remove(container_id)

        if isinstance(value, (list, tuple)):
            container_id = id(value)
            if container_id in active_containers:
                raise ValueError("payload must not contain reference cycles")
            active_containers.add(container_id)
            try:
                return tuple(freeze(item, depth=depth + 1) for item in value)
            finally:
                active_containers.remove(container_id)

        raise TypeError(f"unsupported observation payload type: {type(value).__name__}")

    return cast(Mapping[str, Any], freeze(payload, depth=0))


@dataclass(frozen=True, slots=True)
class FrameEnvelope:
    """Metadata plus a small, ephemeral frame payload.

    F2 intentionally uses immutable ``bytes`` payloads. Hardware adapters may later
    add explicit buffer leases without changing the semantic identity fields.
    """

    source_id: str
    stream_epoch: int
    sequence: int
    received_monotonic_ns: int
    payload: bytes
    source_timestamp_ns: int | None = None
    timestamp_quality: TimestampQuality = "received"
    width: int = 0
    height: int = 0
    pixel_format: str = "opaque"
    stride: int = 0
    buffer_id: str = ""
    generation: int = 0
    quality_flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_non_empty(self.source_id, "source_id")
        _require_integer(self.stream_epoch, "stream_epoch", minimum=0)
        _require_integer(self.sequence, "sequence", minimum=0)
        _require_integer(self.received_monotonic_ns, "received_monotonic_ns", minimum=0)
        if self.source_timestamp_ns is not None:
            _require_integer(self.source_timestamp_ns, "source_timestamp_ns", minimum=0)
        if not isinstance(self.payload, bytes):
            raise TypeError("payload must be immutable bytes")
        _require_integer(self.width, "width", minimum=0)
        _require_integer(self.height, "height", minimum=0)
        _require_integer(self.stride, "stride", minimum=0)
        if bool(self.width) != bool(self.height):
            raise ValueError("width and height must either both be zero or both be positive")
        if self.width and self.stride and self.stride < self.width:
            raise ValueError("stride cannot be smaller than width")
        _require_integer(self.generation, "generation", minimum=0)
        if self.timestamp_quality not in {"synthetic", "source", "received"}:
            raise ValueError("unsupported timestamp_quality")
        _require_non_empty(self.pixel_format, "pixel_format")
        if self.buffer_id:
            _require_non_empty(self.buffer_id, "buffer_id")
        object.__setattr__(
            self,
            "quality_flags",
            _normalize_string_tuple(self.quality_flags, "quality_flags", max_entries=32),
        )


@dataclass(frozen=True, slots=True)
class ObservationEnvelope:
    """A bounded structured observation derived from a frame.

    The top-level mapping is copied and made read-only so caller mutation cannot
    alter an accepted observation after processing.
    """

    session_id: str
    source_id: str
    stream_epoch: int
    source_sequence: int
    observed_monotonic_ns: int
    producer_id: str
    producer_version: str
    schema_id: str
    schema_version: int
    payload: Mapping[str, Any]
    idempotency_key: str
    trace_id: str
    confidence: float | None = None
    provenance: tuple[str, ...] = ()
    sensitivity_class: SensitivityClass = "operational"
    persistence_class: PersistenceClass = "routine"
    ttl_ns: int | None = None
    wall_utc: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "session_id",
            "source_id",
            "producer_id",
            "producer_version",
            "schema_id",
            "idempotency_key",
            "trace_id",
        ):
            _require_non_empty(getattr(self, field_name), field_name)
        _require_integer(self.stream_epoch, "stream_epoch", minimum=0)
        _require_integer(self.source_sequence, "source_sequence", minimum=0)
        _require_integer(self.observed_monotonic_ns, "observed_monotonic_ns", minimum=0)
        _require_integer(self.schema_version, "schema_version", minimum=1)
        if self.confidence is not None:
            if isinstance(self.confidence, bool) or not isinstance(self.confidence, float):
                raise TypeError("confidence must be a float when provided")
            if not math.isfinite(self.confidence) or not 0.0 <= self.confidence <= 1.0:
                raise ValueError("confidence must be finite and between 0 and 1")
        if self.ttl_ns is not None:
            _require_integer(self.ttl_ns, "ttl_ns", minimum=1)
        if self.sensitivity_class not in {
            "public",
            "operational",
            "sensitive",
            "identifiable",
            "prohibited",
        }:
            raise ValueError("unsupported sensitivity_class")
        if self.persistence_class not in {
            "routine",
            "important",
            "diagnostic",
            "visual",
        }:
            raise ValueError("unsupported persistence_class")
        if not isinstance(self.payload, Mapping):
            raise TypeError("payload must be a mapping")
        if self.wall_utc is not None:
            _require_non_empty(self.wall_utc, "wall_utc")
        object.__setattr__(self, "payload", _freeze_json_payload(self.payload))
        object.__setattr__(
            self,
            "provenance",
            _normalize_string_tuple(
                self.provenance,
                "provenance",
                max_entries=_MAX_PROVENANCE_ENTRIES,
            ),
        )


@dataclass(frozen=True, slots=True)
class PersistenceReceipt:
    """Explicit outcome of one observation-store request."""

    status: PersistenceStatus
    reason: str | None = None
    store_sequence: int | None = None
    bytes_accepted: int = 0

    def __post_init__(self) -> None:
        if self.status not in {
            "accepted",
            "coalesced",
            "rejected",
            "gap",
            "failed",
        }:
            raise ValueError("unsupported persistence status")
        if self.store_sequence is not None:
            _require_integer(self.store_sequence, "store_sequence", minimum=1)
        _require_integer(self.bytes_accepted, "bytes_accepted", minimum=0)
        if self.status != "accepted" and self.bytes_accepted:
            raise ValueError("only accepted receipts may report accepted bytes")

    @property
    def accepted(self) -> bool:
        """Whether the store retained the observation, including a duplicate."""

        return self.status in {"accepted", "coalesced"}
