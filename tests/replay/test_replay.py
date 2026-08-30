from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from viskium.adapters import MemoryStore, SyntheticSource
from viskium.limits import MAX_SYNTHETIC_REPLAY_FRAMES
from viskium.runtime import ReplayReport, run_synthetic_replay
from viskium.runtime import replay as replay_module


@pytest.mark.parametrize("mode", ["exhaustive", "faithful"])
def test_replay_is_deterministic(mode: str) -> None:
    first = run_synthetic_replay(mode, 20)  # type: ignore[arg-type]
    second = run_synthetic_replay(mode, 20)  # type: ignore[arg-type]

    assert first == second
    assert first.to_dict() == second.to_dict()


def test_exhaustive_replay_processes_every_frame() -> None:
    report = run_synthetic_replay("exhaustive", 12)

    assert report.produced_frames == 12
    assert report.processed_frames == 12
    assert report.dropped_frames == 0
    assert report.processed_sequences == tuple(range(12))
    assert report.accepted_observations == 12
    assert report.rejected_observations == 0


def test_faithful_replay_replaces_obsolete_pending_frames() -> None:
    report = run_synthetic_replay("faithful", 12)

    assert report.produced_frames == 12
    assert 0 < report.processed_frames < report.produced_frames
    assert report.processed_frames + report.dropped_frames == report.produced_frames
    assert set(report.processed_sequences).isdisjoint(report.dropped_sequences)
    assert sorted(report.processed_sequences + report.dropped_sequences) == list(range(12))
    assert report.rejected_observations == 0


def test_empty_replay_has_a_stable_zero_report() -> None:
    report = run_synthetic_replay("faithful", 0)

    assert report.produced_frames == 0
    assert report.processed_frames == 0
    assert report.dropped_frames == 0
    assert report.virtual_duration_ns == 0
    assert report.observation_crc32 == ()


@pytest.mark.parametrize("mode", ["unknown", "live", ""])
def test_replay_rejects_unknown_modes(mode: str) -> None:
    with pytest.raises(ValueError, match="mode must be"):
        run_synthetic_replay(mode, 1)  # type: ignore[arg-type]


@pytest.mark.parametrize("frame_count", [-1, True, 1.5])
def test_replay_rejects_invalid_frame_counts(frame_count: object) -> None:
    with pytest.raises(ValueError, match="frame_count"):
        run_synthetic_replay("exhaustive", frame_count)  # type: ignore[arg-type]


def test_replay_rejects_count_above_safety_ceiling() -> None:
    with pytest.raises(ValueError, match=str(MAX_SYNTHETIC_REPLAY_FRAMES)):
        run_synthetic_replay("exhaustive", MAX_SYNTHETIC_REPLAY_FRAMES + 1)


def test_empty_exhaustive_replay_covers_zero_frame_boundary() -> None:
    report = run_synthetic_replay("exhaustive", 0)

    assert report.virtual_duration_ns == 0
    assert report.to_dict()["processed_sequences"] == []


def test_replay_report_rejects_negative_or_unaccounted_counts() -> None:
    values = {
        "mode": "exhaustive",
        "requested_frames": 1,
        "produced_frames": 1,
        "processed_frames": 1,
        "dropped_frames": 0,
        "accepted_observations": 1,
        "coalesced_observations": 0,
        "rejected_observations": 0,
        "processed_sequences": (0,),
        "dropped_sequences": (),
        "observation_crc32": ("00000000",),
        "virtual_duration_ns": 1,
        "store_bytes": 1,
    }
    with pytest.raises(ValueError, match="non-negative"):
        ReplayReport(**(values | {"store_bytes": -1}))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="processed or dropped"):
        ReplayReport(**(values | {"processed_frames": 0}))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"mode": "live"}, "mode"),
        ({"requested_frames": 2}, "requested"),
        ({"processed_sequences": ()}, "processed sequence"),
        ({"dropped_sequences": (0,)}, "dropped sequence"),
        ({"accepted_observations": 0}, "persistence outcome"),
        ({"observation_crc32": ()}, "CRC count"),
        ({"observation_crc32": ("NOT-CRC!",)}, "lowercase hexadecimal"),
    ],
)
def test_replay_report_rejects_impossible_states(
    overrides: dict[str, object], message: str
) -> None:
    values: dict[str, object] = {
        "mode": "exhaustive",
        "requested_frames": 1,
        "produced_frames": 1,
        "processed_frames": 1,
        "dropped_frames": 0,
        "accepted_observations": 1,
        "coalesced_observations": 0,
        "rejected_observations": 0,
        "processed_sequences": (0,),
        "dropped_sequences": (),
        "observation_crc32": ("00000000",),
        "virtual_duration_ns": 1,
        "store_bytes": 1,
    }

    with pytest.raises((TypeError, ValueError), match=message):
        ReplayReport(**(values | overrides))  # type: ignore[arg-type]


def test_replay_is_stable_across_process_hash_seeds() -> None:
    command = [
        sys.executable,
        "-m",
        "viskium",
        "replay",
        "--mode",
        "faithful",
        "--frames",
        "12",
        "--json",
    ]
    outputs: list[dict[str, object]] = []
    for hash_seed in ("1", "8675309"):
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = hash_seed
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
            env=environment,
        )
        outputs.append(json.loads(completed.stdout))

    assert outputs[0] == outputs[1] == run_synthetic_replay("faithful", 12).to_dict()


def test_replay_closes_and_clears_resources_on_processor_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources: list[SyntheticSource] = []
    stores: list[MemoryStore] = []

    class TrackingSource(SyntheticSource):
        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, **kwargs)  # type: ignore[arg-type]
            sources.append(self)

    class TrackingStore(MemoryStore):
        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, **kwargs)  # type: ignore[arg-type]
            stores.append(self)

    class FailingProcessor:
        def process(self, *_args: object, **_kwargs: object) -> object:
            raise RuntimeError("injected processor failure")

    monkeypatch.setattr(replay_module, "SyntheticSource", TrackingSource)
    monkeypatch.setattr(replay_module, "MemoryStore", TrackingStore)
    monkeypatch.setattr(replay_module, "DeterministicProcessor", FailingProcessor)

    with pytest.raises(RuntimeError, match="injected"):
        run_synthetic_replay("exhaustive", 1)

    assert len(sources) == len(stores) == 1
    assert sources[0].closed
    assert stores[0].closed
    assert stores[0].observations == ()
