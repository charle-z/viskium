"""Bounded canonical JSON encoding for Viskium contracts."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from typing import Any

from .contracts import ObservationEnvelope

_STRING_CHUNK_CHARS = 4_096


def _require_positive_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _json_string_tokens(value: str) -> Iterator[str]:
    """Yield one escaped JSON string without materializing a large copy."""

    yield '"'
    for offset in range(0, len(value), _STRING_CHUNK_CHARS):
        escaped = json.dumps(value[offset : offset + _STRING_CHUNK_CHARS], ensure_ascii=False)
        yield escaped[1:-1]
    yield '"'


def canonical_json_tokens(value: Any) -> Iterator[str]:
    """Yield deterministic JSON tokens for an already validated JSON-shaped value."""

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
            if not isinstance(key, str):
                raise TypeError("JSON object keys must be strings")
            if index:
                yield ","
            yield from _json_string_tokens(key)
            yield ":"
            yield from canonical_json_tokens(value[key])
        yield "}"
        return
    if isinstance(value, (list, tuple)):
        yield "["
        for index, item in enumerate(value):
            if index:
                yield ","
            yield from canonical_json_tokens(item)
        yield "]"
        return
    raise TypeError(f"unsupported canonical JSON type: {type(value).__name__}")


def bounded_canonical_json_bytes(value: Any, *, max_bytes: int) -> bytes | None:
    """Encode canonical UTF-8 JSON and stop before the byte ceiling is exceeded."""

    limit = _require_positive_integer(max_bytes, "max_bytes")
    encoded = bytearray()
    for token in canonical_json_tokens(value):
        chunk = token.encode("utf-8")
        if len(encoded) + len(chunk) > limit:
            return None
        encoded.extend(chunk)
    return bytes(encoded)


def canonical_json_size(value: Any, *, stop_after: int) -> int | None:
    """Return encoded size, or None as soon as the ceiling is crossed."""

    limit = _require_positive_integer(stop_after, "stop_after")
    encoded_size = 0
    for token in canonical_json_tokens(value):
        encoded_size += len(token.encode("utf-8"))
        if encoded_size > limit:
            return None
    return encoded_size


def observation_document(observation: ObservationEnvelope) -> dict[str, Any]:
    """Return the complete stable document used by observation stores and APIs."""

    return {
        "session_id": observation.session_id,
        "source_id": observation.source_id,
        "stream_epoch": observation.stream_epoch,
        "source_sequence": observation.source_sequence,
        "observed_monotonic_ns": observation.observed_monotonic_ns,
        "producer_id": observation.producer_id,
        "producer_version": observation.producer_version,
        "schema_id": observation.schema_id,
        "schema_version": observation.schema_version,
        "payload": observation.payload,
        "idempotency_key": observation.idempotency_key,
        "trace_id": observation.trace_id,
        "confidence": observation.confidence,
        "provenance": observation.provenance,
        "sensitivity_class": observation.sensitivity_class,
        "persistence_class": observation.persistence_class,
        "ttl_ns": observation.ttl_ns,
        "wall_utc": observation.wall_utc,
    }


def bounded_observation_bytes(
    observation: ObservationEnvelope,
    *,
    max_bytes: int,
) -> bytes | None:
    """Encode one observation within a hard byte ceiling."""

    return bounded_canonical_json_bytes(observation_document(observation), max_bytes=max_bytes)


def observation_size(
    observation: ObservationEnvelope,
    *,
    stop_after: int,
) -> int | None:
    """Measure one observation without retaining its serialized representation."""

    return canonical_json_size(observation_document(observation), stop_after=stop_after)


__all__ = [
    "bounded_canonical_json_bytes",
    "bounded_observation_bytes",
    "canonical_json_size",
    "canonical_json_tokens",
    "observation_document",
    "observation_size",
]
