from __future__ import annotations

from dataclasses import replace

from hypothesis import given, settings
from hypothesis import strategies as st

from viskium.adapters import DeterministicProcessor, MemoryStore, SyntheticSource
from viskium.core import FrameEnvelope
from viskium.runtime import run_synthetic_replay

_JSON_SCALARS = (
    st.none()
    | st.booleans()
    | st.integers(min_value=-(2**63), max_value=2**63 - 1)
    | st.floats(allow_nan=False, allow_infinity=False, width=64)
    | st.text(max_size=32)
)
_JSON_VALUES = st.recursive(
    _JSON_SCALARS,
    lambda children: (
        st.lists(children, max_size=6)
        | st.dictionaries(
            st.from_regex(r"[A-Za-z][A-Za-z0-9_]{0,15}", fullmatch=True),
            children,
            max_size=6,
        )
    ),
    max_leaves=32,
)


@given(
    mode=st.sampled_from(["exhaustive", "faithful"]),
    frame_count=st.integers(min_value=0, max_value=200),
)
@settings(max_examples=50, deadline=None)
def test_replay_partitions_every_generated_sequence(mode: str, frame_count: int) -> None:
    report = run_synthetic_replay(mode, frame_count)  # type: ignore[arg-type]

    assert report == run_synthetic_replay(mode, frame_count)  # type: ignore[arg-type]
    assert sorted(report.processed_sequences + report.dropped_sequences) == list(range(frame_count))
    assert report.processed_frames + report.dropped_frames == frame_count


@given(payload=_JSON_VALUES)
@settings(max_examples=50, deadline=None)
def test_canonical_payload_size_and_idempotency_are_stable(payload: object) -> None:
    frame = FrameEnvelope(
        source_id="property",
        stream_epoch=0,
        sequence=0,
        received_monotonic_ns=0,
        payload=b"property",
    )
    base = DeterministicProcessor().process(frame, session_id="property")
    observation = replace(base, payload={"value": payload})
    first_store = MemoryStore(max_observations=2, max_bytes=1_048_576)
    second_store = MemoryStore(max_observations=2, max_bytes=1_048_576)

    first = first_store.put(observation)
    duplicate = first_store.put(observation)
    independent = second_store.put(observation)

    assert first.status == independent.status == "accepted"
    assert first.bytes_accepted == independent.bytes_accepted
    assert duplicate.status == "coalesced"
    first_store.close()
    second_store.close()


@given(
    frame_count=st.integers(min_value=0, max_value=64),
    seed=st.integers(min_value=-(2**31), max_value=2**31 - 1),
)
@settings(max_examples=50, deadline=None)
def test_synthetic_source_sequences_and_bytes_are_repeatable(frame_count: int, seed: int) -> None:
    first = list(SyntheticSource(frame_count, seed=seed))
    second = list(SyntheticSource(frame_count, seed=seed))

    assert first == second
    assert [frame.sequence for frame in first] == list(range(frame_count))
