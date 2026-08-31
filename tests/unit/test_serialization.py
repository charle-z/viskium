from __future__ import annotations

import json
from typing import Any

import pytest

from viskium.adapters import DeterministicProcessor
from viskium.core import FrameEnvelope
from viskium.core.serialization import (
    bounded_canonical_json_bytes,
    bounded_observation_bytes,
    canonical_json_size,
    canonical_json_tokens,
    observation_document,
    observation_size,
)


def _observation() -> Any:
    frame = FrameEnvelope(
        source_id="fixture",
        stream_epoch=0,
        sequence=3,
        received_monotonic_ns=4,
        payload=b"abc",
    )
    return DeterministicProcessor().process(frame, session_id="serialization")


def test_canonical_json_is_sorted_compact_and_utf8() -> None:
    value = {"z": [None, True, False, 1, 1.5], "a": "café"}

    encoded = bounded_canonical_json_bytes(value, max_bytes=1_024)

    assert encoded == '{"a":"café","z":[null,true,false,1,1.5]}'.encode()
    assert canonical_json_size(value, stop_after=1_024) == len(encoded)


def test_bounded_encoder_honors_exact_limit_without_partial_output() -> None:
    value = {"value": "x" * 10_000}
    exact = bounded_canonical_json_bytes(value, max_bytes=20_000)
    assert exact is not None

    assert bounded_canonical_json_bytes(value, max_bytes=len(exact)) == exact
    assert bounded_canonical_json_bytes(value, max_bytes=len(exact) - 1) is None
    assert canonical_json_size(value, stop_after=len(exact) - 1) is None


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_serialization_limits_must_be_positive_integers(limit: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        bounded_canonical_json_bytes({}, max_bytes=limit)  # type: ignore[arg-type]
    with pytest.raises((TypeError, ValueError)):
        canonical_json_size({}, stop_after=limit)  # type: ignore[arg-type]


def test_canonical_tokens_reject_unsupported_values_and_non_string_keys() -> None:
    with pytest.raises(TypeError, match="unsupported"):
        tuple(canonical_json_tokens(object()))
    with pytest.raises(TypeError, match="keys"):
        tuple(canonical_json_tokens({1: "value"}))


def test_observation_document_round_trips_and_size_matches_bytes() -> None:
    observation = _observation()
    encoded = bounded_observation_bytes(observation, max_bytes=8_192)

    assert encoded is not None
    decoded = json.loads(encoded)
    assert decoded["payload"] == dict(observation.payload)
    assert decoded["source_sequence"] == observation.source_sequence
    assert decoded["provenance"] == list(observation.provenance)
    assert set(decoded) == set(observation_document(observation))
    assert observation_size(observation, stop_after=8_192) == len(encoded)
    assert bounded_observation_bytes(observation, max_bytes=len(encoded) - 1) is None
