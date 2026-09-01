from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from pathlib import Path
from threading import Barrier, Lock, Thread
from typing import Any

import pytest

from viskium.agent import (
    AGENT_READ_CONTRACT,
    AgentLimits,
    AgentObservationResult,
    AgentReadService,
    AgentServiceMetrics,
    AgentSnapshotResult,
    AgentStatusResult,
    ConsentLedger,
    SnapshotCaptureResult,
    normalize_snapshot_reason,
)
from viskium.agent import service as service_module
from viskium.core import FrameEnvelope, ObservationEnvelope
from viskium.observations import LatestObservationSlot
from viskium.snapshots import SnapshotEnvelope
from viskium.snapshots.contracts import PNG_SIGNATURE
from viskium.storage import initialize_data_root


class RecordingSnapshotProvider:
    def __init__(
        self,
        result: SnapshotCaptureResult | object,
        *,
        error: Exception | None = None,
        sensitivity_class: str = "public",
    ) -> None:
        self._result = result
        self._error = error
        self._lock = Lock()
        self.sensitivity_class = sensitivity_class
        self.calls: list[tuple[int, int, float]] = []

    def capture(
        self,
        *,
        max_edge_px: int,
        max_bytes: int,
        timeout_seconds: float,
    ) -> SnapshotCaptureResult:
        with self._lock:
            self.calls.append((max_edge_px, max_bytes, timeout_seconds))
        if self._error is not None:
            raise self._error
        return self._result  # type: ignore[return-value]


def _ledger(
    tmp_path: Path,
    *,
    scopes: frozenset[str] | None = None,
    quota: int = 4,
    ceiling: str = "identifiable",
    duration_seconds: int = 60,
) -> ConsentLedger:
    ledger = ConsentLedger(initialize_data_root(tmp_path / "data"))
    if scopes is not None:
        ledger.grant(
            scopes=scopes,  # type: ignore[arg-type]
            duration_seconds=duration_seconds,
            snapshot_quota=quota,
            sensitivity_ceiling=ceiling,  # type: ignore[arg-type]
            now_unix_ns=1_000_000_000,
        )
    return ledger


def _observation(
    *,
    payload: dict[str, Any] | None = None,
    sensitivity: str = "operational",
    observed_monotonic_ns: int = 5_000_000_000,
    schema_id: str = "viskium.test",
) -> ObservationEnvelope:
    return ObservationEnvelope(
        session_id="session",
        source_id="fixture",
        stream_epoch=1,
        source_sequence=2,
        observed_monotonic_ns=observed_monotonic_ns,
        producer_id="test",
        producer_version="1",
        schema_id=schema_id,
        schema_version=1,
        payload={"value": 7} if payload is None else payload,
        idempotency_key="fixture:1:2:test",
        trace_id="trace",
        sensitivity_class=sensitivity,  # type: ignore[arg-type]
    )


def _snapshot(
    *,
    sensitivity: str = "public",
    width: int = 32,
    height: int = 24,
    extra_bytes: int = 0,
) -> SnapshotEnvelope:
    return SnapshotEnvelope(
        source_id="fixture",
        stream_epoch=1,
        source_sequence=2,
        received_monotonic_ns=5_000_000_000,
        width=width,
        height=height,
        sensitivity_class=sensitivity,  # type: ignore[arg-type]
        png_bytes=PNG_SIGNATURE + (b"x" * extra_bytes),
    )


def _service(
    *,
    ledger: ConsentLedger,
    slot: LatestObservationSlot | None = None,
    provider: RecordingSnapshotProvider | None = None,
    status_provider: Any = lambda: {"state": "ready"},
    limits: AgentLimits | None = None,
    unix_time_ns: Any = lambda: 2_000_000_000,
    monotonic_ns: Any = lambda: 5_000_000_000,
) -> AgentReadService:
    return AgentReadService(
        observations=LatestObservationSlot() if slot is None else slot,
        consent=ledger,
        snapshot_provider=(
            RecordingSnapshotProvider(SnapshotCaptureResult("unavailable"))
            if provider is None
            else provider
        ),
        status_provider=status_provider,
        limits=limits,
        unix_time_ns=unix_time_ns,
        monotonic_ns=monotonic_ns,
    )


def test_status_is_canonical_bounded_and_has_no_camera_or_consent_side_effects(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    provider = RecordingSnapshotProvider(
        SnapshotCaptureResult("failed"),
        error=AssertionError("status must not call snapshot provider"),
    )
    calls = 0

    def status_provider() -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {"z": 1, "state": "ready", "features": ["observations", "snapshots"]}

    service = _service(ledger=ledger, provider=provider, status_provider=status_provider)

    result = service.status()

    assert result.outcome == "ok"
    assert result.contract == AGENT_READ_CONTRACT
    assert result.metadata_json is not None
    assert (
        result.metadata_json
        == json.dumps(
            json.loads(result.metadata_json),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )
    assert calls == 1
    assert provider.calls == []
    assert not ledger.path.exists()


@pytest.mark.parametrize(
    "status_provider",
    [
        lambda: {"api_token": "should-never-leave"},
        lambda: {"detail": r"C:\private\camera.json"},
        lambda: {"value": float("nan")},
        lambda: ["not", "a", "mapping"],
    ],
)
def test_status_rejects_non_public_or_invalid_metadata(
    tmp_path: Path,
    status_provider: Any,
) -> None:
    result = _service(ledger=_ledger(tmp_path), status_provider=status_provider).status()

    assert result.outcome == "status_invalid"
    assert result.metadata_json is None
    assert "should-never-leave" not in repr(result)
    assert "private" not in repr(result)


def test_status_provider_exception_is_sanitized(tmp_path: Path) -> None:
    def fail() -> dict[str, object]:
        raise RuntimeError(r"token=private at C:\users\person\status.json")

    result = _service(ledger=_ledger(tmp_path), status_provider=fail).status()

    assert result.outcome == "status_unavailable"
    assert result.metadata_json is None
    assert "token" not in repr(result)
    assert "users" not in repr(result)


def test_status_enforces_metadata_byte_ceiling(tmp_path: Path) -> None:
    metadata = {f"metric_{index}": "x" * 4_096 for index in range(20)}

    result = _service(ledger=_ledger(tmp_path), status_provider=lambda: metadata).status()

    assert result.outcome == "metadata_too_large"
    assert result.metadata_json is None


@pytest.mark.parametrize(
    "document",
    [
        {"credential": "x"},
        {"value": "Bearer credential"},
        {"value": "sk-private"},
        {"value": "/private/status"},
        {"value": "\\\\server\\share"},
        {"value": "x" * 4_097},
        {"value": 2**63},
        {"items": [0] * 129},
        {f"key_{index}": index for index in range(129)},
        {"": "empty-key"},
        {"x" * 129: "long-key"},
        {"unsupported": object()},
    ],
)
def test_status_sanitizer_fails_closed_for_adversarial_shapes(document: Any) -> None:
    with pytest.raises((TypeError, ValueError)):
        service_module._safe_status_document(document)  # type: ignore[arg-type]


def test_status_sanitizer_bounds_depth_nodes_and_json_scalar_types() -> None:
    nested: dict[str, Any] = {"value": True}
    for _ in range(9):
        nested = {"nested": nested}
    with pytest.raises(ValueError, match="nesting"):
        service_module._safe_status_document(nested)

    wide = {f"group_{index}": [None] * 128 for index in range(9)}
    with pytest.raises(ValueError, match="node"):
        service_module._safe_status_document(wide)

    assert service_module._safe_status_document(
        {"none": None, "bool": False, "integer": -1, "float": 1.5}
    ) == {"none": None, "bool": False, "integer": -1, "float": 1.5}


def test_unexpected_status_sanitizer_failure_is_still_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(_value: object) -> dict[str, object]:
        raise RuntimeError(r"private C:\status\provider")

    monkeypatch.setattr(service_module, "_safe_status_document", fail)

    result = _service(ledger=_ledger(tmp_path)).status()

    assert result.outcome == "status_unavailable"
    assert result.metadata_json is None


def test_observation_is_authorized_then_returned_as_bounded_canonical_json(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"observation.read"}))
    slot = LatestObservationSlot()
    observation = _observation(payload={"z": 2, "a": "café"})
    assert slot.offer(observation) == "accepted"
    service = _service(ledger=ledger, slot=slot)

    result = service.latest_observation(
        max_age_ms=500,
        schema_ids={"viskium.test"},
    )

    assert result.outcome == "ok"
    assert result.age_ns == 0
    assert result.observation_json is not None
    assert len(result.observation_json) <= service.limits.max_observation_bytes
    document = json.loads(result.observation_json)
    assert document["payload"] == {"a": "café", "z": 2}
    assert document["sensitivity_class"] == "operational"


@pytest.mark.parametrize(
    ("scopes", "duration_seconds", "now_unix_ns", "expected"),
    [
        (None, 60, 2_000_000_000, "grant_missing"),
        (frozenset({"snapshot.read"}), 60, 2_000_000_000, "scope_missing"),
        (frozenset({"observation.read"}), 1, 3_000_000_000, "grant_expired"),
    ],
)
def test_observation_denial_never_reads_or_exposes_payload(
    tmp_path: Path,
    scopes: frozenset[str] | None,
    duration_seconds: int,
    now_unix_ns: int,
    expected: str,
) -> None:
    ledger = _ledger(tmp_path, scopes=scopes, duration_seconds=duration_seconds)
    slot = LatestObservationSlot()
    slot.offer(_observation(payload={"secret": "must-not-leave"}))
    service = _service(ledger=ledger, slot=slot, unix_time_ns=lambda: now_unix_ns)

    result = service.latest_observation(max_age_ms=100)

    assert result.outcome == expected
    assert result.observation_json is None
    assert "must-not-leave" not in repr(result)
    assert slot.metrics.reads_ok == 0


@pytest.mark.parametrize(
    ("sensitivity", "expected"),
    [("identifiable", "sensitivity_denied"), ("prohibited", "prohibited")],
)
def test_observation_revalidates_actual_sensitivity_without_payload(
    tmp_path: Path,
    sensitivity: str,
    expected: str,
) -> None:
    ledger = _ledger(
        tmp_path,
        scopes=frozenset({"observation.read"}),
        ceiling="operational",
    )
    slot = LatestObservationSlot()
    slot.offer(_observation(payload={"face": "private"}, sensitivity=sensitivity))

    result = _service(ledger=ledger, slot=slot).latest_observation(max_age_ms=100)

    assert result.outcome == expected
    assert result.observation_json is None
    assert "private" not in repr(result)


def test_observation_rechecks_expiration_after_the_read(tmp_path: Path) -> None:
    ledger = _ledger(
        tmp_path,
        scopes=frozenset({"observation.read"}),
        duration_seconds=1,
    )
    slot = LatestObservationSlot()
    slot.offer(_observation())
    times = iter((1_500_000_000, 2_000_000_000))

    result = _service(
        ledger=ledger, slot=slot, unix_time_ns=lambda: next(times)
    ).latest_observation(max_age_ms=100)

    assert result.outcome == "grant_expired"
    assert result.observation_json is None


def test_observation_enforces_size_wait_schema_and_age_limits(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"observation.read"}))
    slot = LatestObservationSlot()
    slot.offer(_observation(payload={"text": "x" * 2_000}))
    limits = AgentLimits(
        max_observation_bytes=512,
        max_wait_ms=10,
        max_age_ms=20,
        max_schema_ids=2,
    )
    service = _service(ledger=ledger, slot=slot, limits=limits)

    assert service.latest_observation(max_age_ms=20).outcome == "observation_too_large"
    with pytest.raises(ValueError, match="max_age_ms"):
        service.latest_observation(max_age_ms=21)
    with pytest.raises(ValueError, match="wait_ms"):
        service.latest_observation(max_age_ms=20, wait_ms=11)
    with pytest.raises(ValueError, match="schema_ids"):
        service.latest_observation(max_age_ms=20, schema_ids={"a", "b", "c"})

    empty_service = _service(ledger=ledger, limits=limits)
    assert empty_service.latest_observation(max_age_ms=20, wait_ms=5).outcome == "timeout"


@pytest.mark.parametrize(
    ("setup", "now_monotonic_ns", "schema_ids", "expected", "has_age"),
    [
        ("empty", 5_000_000_000, None, "empty", False),
        ("stale", 5_100_000_000, None, "stale", True),
        ("schema", 5_000_000_000, {"different"}, "schema_mismatch", True),
        ("future", 4_999_999_999, None, "future_timestamp", False),
        ("closed", 5_000_000_000, None, "closed", False),
    ],
)
def test_observation_slot_outcomes_remain_explicit_without_payload(
    tmp_path: Path,
    setup: str,
    now_monotonic_ns: int,
    schema_ids: set[str] | None,
    expected: str,
    has_age: bool,
) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"observation.read"}))
    slot = LatestObservationSlot()
    if setup != "empty":
        slot.offer(_observation())
    if setup == "closed":
        slot.close()
    result = _service(
        ledger=ledger,
        slot=slot,
        monotonic_ns=lambda: now_monotonic_ns,
    ).latest_observation(max_age_ms=1, schema_ids=schema_ids)

    assert result.outcome == expected
    assert result.observation_json is None
    assert (result.age_ns is not None) is has_age


@pytest.mark.parametrize(
    "schema_ids",
    ["one", [1], [""], ["x" * 257]],
)
def test_observation_schema_filter_is_strictly_typed_and_bounded(
    tmp_path: Path,
    schema_ids: Any,
) -> None:
    service = _service(ledger=_ledger(tmp_path))

    with pytest.raises((TypeError, ValueError)):
        service.latest_observation(max_age_ms=1, schema_ids=schema_ids)


def test_observation_clock_and_slot_failures_are_sanitized(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"observation.read"}))

    def broken_clock() -> int:
        raise RuntimeError(r"clock failed at C:\private")

    clock_result = _service(ledger=ledger, unix_time_ns=broken_clock).latest_observation(
        max_age_ms=1
    )
    assert clock_result.outcome == "clock_unavailable"

    class BrokenSlot:
        def read(self, **_kwargs: object) -> object:
            raise RuntimeError("payload=private")

    slot_result = _service(ledger=ledger, slot=BrokenSlot()).latest_observation(  # type: ignore[arg-type]
        max_age_ms=1
    )
    assert slot_result.outcome == "observation_invalid"
    assert slot_result.observation_json is None


def test_observation_invalid_slot_result_and_serialization_failure_are_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"observation.read"}))

    class InvalidSlot:
        def read(self, **_kwargs: object) -> object:
            return {"payload": "not-an-envelope"}

    invalid = _service(ledger=ledger, slot=InvalidSlot()).latest_observation(  # type: ignore[arg-type]
        max_age_ms=1
    )
    assert invalid.outcome == "observation_invalid"

    slot = LatestObservationSlot()
    slot.offer(_observation())

    def fail_serialization(_observation: object, *, max_bytes: int) -> bytes:
        del max_bytes
        raise RuntimeError("secret serialization failure")

    monkeypatch.setattr(service_module, "bounded_observation_bytes", fail_serialization)
    serialization = _service(ledger=ledger, slot=slot).latest_observation(max_age_ms=1)
    assert serialization.outcome == "observation_invalid"
    assert serialization.observation_json is None


@pytest.mark.parametrize(
    ("action", "expected"),
    [("revoke", "grant_missing"), ("replace", "grant_changed")],
)
def test_observation_revalidation_detects_revocation_and_grant_change(
    tmp_path: Path,
    action: str,
    expected: str,
) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"observation.read"}))
    slot = LatestObservationSlot()
    slot.offer(_observation())
    original_load = ledger.load
    loads = 0

    def load() -> Any:
        nonlocal loads
        loads += 1
        if loads == 2:
            if action == "revoke":
                ledger.revoke()
            else:
                ledger.grant(
                    scopes=frozenset({"observation.read"}),
                    duration_seconds=60,
                    snapshot_quota=0,
                    sensitivity_ceiling="identifiable",
                    now_unix_ns=1_000_000_000,
                )
        return original_load()

    ledger.load = load  # type: ignore[method-assign]
    result = _service(ledger=ledger, slot=slot).latest_observation(max_age_ms=1)

    assert result.outcome == expected
    assert result.observation_json is None


def test_snapshot_reserves_before_provider_and_failed_attempt_consumes_quota(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"snapshot.read"}), quota=1)
    provider = RecordingSnapshotProvider(
        SnapshotCaptureResult("failed"),
        error=RuntimeError(r"secret at C:\private\device"),
    )
    service = _service(ledger=ledger, provider=provider)

    first = service.snapshot(max_edge_px=320, wait_ms=25)
    second = service.snapshot(max_edge_px=320, wait_ms=25)

    assert first.outcome == "provider_failure"
    assert first.snapshot is None
    assert "secret" not in repr(first)
    assert second.outcome == "snapshot_quota_exhausted"
    assert provider.calls == [(320, service.limits.max_snapshot_bytes, 0.025)]
    state = ledger.load()
    assert state is not None
    assert state.snapshot_attempts == 1


def test_snapshot_propagates_only_a_safe_provider_reason_code(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"snapshot.read"}), quota=1)
    provider = RecordingSnapshotProvider(
        SnapshotCaptureResult("unavailable", reason_code=r"driver failure at C:\\private")
    )

    result = _service(ledger=ledger, provider=provider).snapshot(max_edge_px=64)

    assert result.outcome == "unavailable"
    assert result.reason_code == "generic"
    assert result.explicit_reason_code == "generic"
    assert "private" not in repr(result)


@pytest.mark.parametrize(
    ("scopes", "quota", "expected"),
    [
        (None, 1, "grant_missing"),
        (frozenset({"observation.read"}), 1, "scope_missing"),
        (frozenset({"snapshot.read"}), 0, "snapshot_quota_exhausted"),
    ],
)
def test_snapshot_denials_never_call_provider(
    tmp_path: Path,
    scopes: frozenset[str] | None,
    quota: int,
    expected: str,
) -> None:
    ledger = _ledger(tmp_path, scopes=scopes, quota=quota)
    provider = RecordingSnapshotProvider(SnapshotCaptureResult("ok", _snapshot()))
    service = _service(ledger=ledger, provider=provider)

    result = service.snapshot(max_edge_px=64)

    assert result.outcome == expected
    assert result.snapshot is None
    assert provider.calls == []


def test_snapshot_returns_only_png_and_revalidates_sensitivity_and_limits(
    tmp_path: Path,
) -> None:
    allowed_ledger = _ledger(
        tmp_path / "allowed",
        scopes=frozenset({"snapshot.read"}),
        quota=1,
    )
    allowed_provider = RecordingSnapshotProvider(
        SnapshotCaptureResult("ok", _snapshot(width=16, height=12))
    )
    allowed = _service(ledger=allowed_ledger, provider=allowed_provider).snapshot(max_edge_px=16)
    assert allowed.outcome == "ok"
    assert allowed.snapshot is not None
    assert allowed.snapshot.png_bytes.startswith(PNG_SIGNATURE)
    assert not isinstance(allowed.snapshot, FrameEnvelope)

    sensitivity_ledger = _ledger(
        tmp_path / "sensitivity",
        scopes=frozenset({"snapshot.read"}),
        quota=1,
        ceiling="public",
    )
    sensitive_provider = RecordingSnapshotProvider(
        SnapshotCaptureResult("ok", _snapshot(sensitivity="operational")),
        sensitivity_class="operational",
    )
    denied = _service(ledger=sensitivity_ledger, provider=sensitive_provider).snapshot(
        max_edge_px=64
    )
    assert denied.outcome == "sensitivity_denied"
    assert denied.snapshot is None
    assert sensitive_provider.calls == []
    sensitive_state = sensitivity_ledger.load()
    assert sensitive_state is not None
    assert sensitive_state.snapshot_attempts == 0

    limit_ledger = _ledger(
        tmp_path / "limit",
        scopes=frozenset({"snapshot.read"}),
        quota=1,
    )
    limit_provider = RecordingSnapshotProvider(
        SnapshotCaptureResult("ok", _snapshot(width=32, height=24))
    )
    limited = _service(
        ledger=limit_ledger,
        provider=limit_provider,
        limits=AgentLimits(max_snapshot_edge_px=16),
    ).snapshot(max_edge_px=16)
    assert limited.outcome == "snapshot_limit_exceeded"
    assert limited.snapshot is None


@pytest.mark.parametrize("provider_outcome", ["busy", "timeout", "closed", "unavailable", "failed"])
def test_snapshot_preserves_explicit_provider_outcomes(
    tmp_path: Path,
    provider_outcome: str,
) -> None:
    ledger = _ledger(
        tmp_path / provider_outcome,
        scopes=frozenset({"snapshot.read"}),
        quota=1,
    )
    provider = RecordingSnapshotProvider(
        SnapshotCaptureResult(provider_outcome)  # type: ignore[arg-type]
    )

    result = _service(ledger=ledger, provider=provider).snapshot(max_edge_px=64)

    assert result.outcome == provider_outcome
    assert result.snapshot is None
    state = ledger.load()
    assert state is not None
    assert state.snapshot_attempts == 1


def test_snapshot_clock_and_consent_failures_do_not_call_provider(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"snapshot.read"}), quota=1)
    provider = RecordingSnapshotProvider(SnapshotCaptureResult("ok", _snapshot()))

    def fail_clock() -> int:
        raise RuntimeError("private clock")

    clock_result = _service(
        ledger=ledger,
        provider=provider,
        unix_time_ns=fail_clock,
    ).snapshot(max_edge_px=64)
    assert clock_result.outcome == "clock_unavailable"

    def fail_reservation(*, now_unix_ns: int | None = None) -> Any:
        del now_unix_ns
        raise RuntimeError(r"secret C:\consent")

    ledger.reserve_snapshot = fail_reservation  # type: ignore[method-assign]
    consent_result = _service(ledger=ledger, provider=provider).snapshot(max_edge_px=64)
    assert consent_result.outcome == "consent_unavailable"
    assert provider.calls == []


@pytest.mark.parametrize(
    ("action", "expected_outcome"),
    [
        ("revoke", "grant_missing"),
        ("replace", "grant_changed"),
        ("clock", "clock_unavailable"),
    ],
)
def test_snapshot_revalidation_detects_revocation_change_and_clock_failure(
    tmp_path: Path,
    action: str,
    expected_outcome: str,
) -> None:
    ledger = _ledger(
        tmp_path,
        scopes=frozenset({"snapshot.read"}),
        quota=1,
    )
    clock_calls = 0

    def unix_clock() -> int:
        nonlocal clock_calls
        clock_calls += 1
        if action == "clock" and clock_calls == 2:
            raise RuntimeError("private clock")
        return 2_000_000_000

    class MutatingProvider(RecordingSnapshotProvider):
        def capture(
            self,
            *,
            max_edge_px: int,
            max_bytes: int,
            timeout_seconds: float,
        ) -> SnapshotCaptureResult:
            result = super().capture(
                max_edge_px=max_edge_px,
                max_bytes=max_bytes,
                timeout_seconds=timeout_seconds,
            )
            if action == "revoke":
                ledger.revoke()
            elif action == "replace":
                ledger.grant(
                    scopes=frozenset({"snapshot.read"}),
                    duration_seconds=60,
                    snapshot_quota=1,
                    sensitivity_ceiling="identifiable",
                    now_unix_ns=1_000_000_000,
                )
            return result

    provider = MutatingProvider(SnapshotCaptureResult("ok", _snapshot()))
    result = _service(
        ledger=ledger,
        provider=provider,
        unix_time_ns=unix_clock,
    ).snapshot(max_edge_px=64)

    assert result.outcome == expected_outcome
    assert result.snapshot is None


def test_invalid_provider_result_is_explicit_and_never_leaks_raw_frame(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"snapshot.read"}), quota=1)
    raw_frame = FrameEnvelope(
        source_id="fixture",
        stream_epoch=1,
        sequence=1,
        received_monotonic_ns=1,
        payload=b"raw-camera-data",
    )
    provider = RecordingSnapshotProvider(raw_frame)

    result = _service(ledger=ledger, provider=provider).snapshot(max_edge_px=64)

    assert result.outcome == "provider_invalid"
    assert result.snapshot is None
    assert "raw-camera-data" not in repr(result)


def test_concurrent_snapshot_requests_never_exceed_atomic_quota(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, scopes=frozenset({"snapshot.read"}), quota=3)
    provider = RecordingSnapshotProvider(SnapshotCaptureResult("ok", _snapshot()))
    service = _service(ledger=ledger, provider=provider)
    barrier = Barrier(9)
    result_lock = Lock()
    outcomes: list[str] = []

    def request() -> None:
        barrier.wait(timeout=2)
        result = service.snapshot(max_edge_px=64)
        with result_lock:
            outcomes.append(result.outcome)

    workers = [Thread(target=request) for _ in range(8)]
    for worker in workers:
        worker.start()
    barrier.wait(timeout=2)
    for worker in workers:
        worker.join(timeout=2)

    assert all(not worker.is_alive() for worker in workers)
    assert outcomes.count("ok") == 3
    assert outcomes.count("snapshot_quota_exhausted") == 5
    assert len(provider.calls) == 3
    assert service.metrics.snapshot_attempts_reserved == 3
    assert service.metrics.snapshots_delivered == 3


def test_results_and_metrics_are_frozen_and_contain_only_counters(tmp_path: Path) -> None:
    service = _service(ledger=_ledger(tmp_path))
    status = service.status()
    metrics = service.metrics

    assert metrics == AgentServiceMetrics(1, 0, 0, 0, 0, 0, 0, 0, 0)
    with pytest.raises(FrozenInstanceError):
        metrics.status_requests = 2  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        status.outcome = "status_unavailable"  # type: ignore[misc]
    assert not hasattr(metrics, "__dict__")
    assert not hasattr(status, "__dict__")


def test_public_result_contracts_reject_payload_mixing_and_invalid_values() -> None:
    snapshot = _snapshot()
    with pytest.raises(ValueError, match="outcome"):
        SnapshotCaptureResult("unknown")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="requires"):
        SnapshotCaptureResult("ok")
    with pytest.raises(ValueError, match="must not expose"):
        SnapshotCaptureResult("failed", snapshot)

    with pytest.raises(ValueError, match="contract"):
        AgentStatusResult("ok", b"{}", contract="wrong")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="outcome"):
        AgentStatusResult("unknown")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="requires"):
        AgentStatusResult("ok")
    with pytest.raises(ValueError, match="must not expose"):
        AgentStatusResult("status_invalid", b"{}")
    with pytest.raises(ValueError, match="hard byte"):
        AgentStatusResult("ok", b"x" * (64 * 1_024 + 1))

    with pytest.raises(ValueError, match="contract"):
        AgentObservationResult("empty", contract="wrong")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="outcome"):
        AgentObservationResult("unknown")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="requires"):
        AgentObservationResult("ok", age_ns=0)
    with pytest.raises(ValueError, match="hard byte"):
        AgentObservationResult("ok", b"x" * (32 * 1_024 + 1), age_ns=0)
    with pytest.raises(ValueError, match="requires age"):
        AgentObservationResult("ok", b"{}")
    with pytest.raises(ValueError, match="must not expose observation"):
        AgentObservationResult("empty", b"{}")
    with pytest.raises(ValueError, match="requires age"):
        AgentObservationResult("stale")
    with pytest.raises(ValueError, match="must not expose age"):
        AgentObservationResult("empty", age_ns=1)

    with pytest.raises(ValueError, match="contract"):
        AgentSnapshotResult("ok", snapshot, contract="wrong")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="outcome"):
        AgentSnapshotResult("unknown")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="requires"):
        AgentSnapshotResult("ok")
    with pytest.raises(ValueError, match="must not expose"):
        AgentSnapshotResult("failed", snapshot)


def test_snapshot_reason_codes_are_canonical_and_semantically_bounded() -> None:
    snapshot = _snapshot()

    class MaliciousString(str):
        def __eq__(self, _other: object) -> bool:
            raise AssertionError("reason normalization must not compare subclasses")

    assert normalize_snapshot_reason("device_open_failed") == "device_open_failed"
    assert normalize_snapshot_reason(MaliciousString("device_open_failed")) == "generic"
    assert normalize_snapshot_reason(r"driver failure at C:\\private") == "generic"

    capture = SnapshotCaptureResult(
        "unavailable",
        reason_code=MaliciousString("device_open_failed"),  # type: ignore[arg-type]
    )
    assert capture.reason_code == "generic"
    result = AgentSnapshotResult(
        "unavailable",
        reason_code=MaliciousString("device_open_failed"),  # type: ignore[arg-type]
    )
    assert result.reason_code == "generic"

    with pytest.raises(ValueError, match="incompatible"):
        SnapshotCaptureResult("timeout", reason_code="device_open_failed")
    with pytest.raises(ValueError, match="incompatible"):
        AgentSnapshotResult("failed", reason_code="device_open_failed")
    with pytest.raises(ValueError, match="must not include"):
        SnapshotCaptureResult("ok", snapshot, reason_code="generic")
    with pytest.raises(ValueError, match="must not include"):
        AgentSnapshotResult("ok", snapshot, reason_code="generic")


def test_result_reason_codes_are_stable() -> None:
    assert AgentStatusResult("status_invalid").reason_code == "status_invalid"
    assert AgentStatusResult("ok", b"{}").reason_code is None
    assert AgentObservationResult("empty").reason_code == "empty"
    assert AgentObservationResult("ok", b"{}", age_ns=0).reason_code is None
    assert AgentSnapshotResult("failed").reason_code == "failed"
    assert AgentSnapshotResult("ok", _snapshot()).reason_code is None


def test_service_constructor_rejects_non_callable_dependencies(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    provider = RecordingSnapshotProvider(SnapshotCaptureResult("failed"))
    kwargs = {
        "observations": LatestObservationSlot(),
        "consent": ledger,
        "snapshot_provider": provider,
        "status_provider": dict,
    }
    with pytest.raises(TypeError, match="status_provider"):
        AgentReadService(**(kwargs | {"status_provider": object()}))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="SnapshotProvider"):
        AgentReadService(**(kwargs | {"snapshot_provider": object()}))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="unix_time_ns"):
        AgentReadService(**(kwargs | {"unix_time_ns": object()}))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="monotonic_ns"):
        AgentReadService(**(kwargs | {"monotonic_ns": object()}))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("method", "kwargs"),
    [
        ("latest_observation", {"max_age_ms": True}),
        ("latest_observation", {"max_age_ms": 1, "wait_ms": -1}),
        ("snapshot", {"max_edge_px": 0}),
        ("snapshot", {"max_edge_px": 1, "wait_ms": 10_001}),
    ],
)
def test_request_values_are_strictly_bounded_before_dependencies(
    tmp_path: Path,
    method: str,
    kwargs: dict[str, object],
) -> None:
    provider = RecordingSnapshotProvider(SnapshotCaptureResult("failed"))
    service = _service(ledger=_ledger(tmp_path), provider=provider)

    with pytest.raises((TypeError, ValueError)):
        getattr(service, method)(**kwargs)

    assert provider.calls == []
