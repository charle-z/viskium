from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from viskium.agent import AgentLimits, ConsentGrant, GrantDecision, evaluate_grant


def _grant(**overrides: Any) -> ConsentGrant:
    values: dict[str, Any] = {
        "public_id": "grant-public-1",
        "scopes": frozenset({"observation.read", "snapshot.read"}),
        "expires_unix_ns": 2_000,
        "snapshot_quota": 2,
        "sensitivity_ceiling": "sensitive",
    }
    values.update(overrides)
    return ConsentGrant(**values)


def test_agent_limits_match_reviewed_defensive_ceilings() -> None:
    limits = AgentLimits()

    assert limits.to_dict() == {
        "max_request_bytes": 16 * 1_024,
        "max_wire_bytes": 256 * 1_024,
        "max_inflight_requests": 4,
        "max_metadata_bytes": 64 * 1_024,
        "max_observation_bytes": 32 * 1_024,
        "max_snapshot_bytes": 4 * 1_024 * 1_024,
        "max_snapshot_edge_px": 1_280,
        "max_wait_ms": 10_000,
        "max_age_ms": 10_000,
        "max_schema_ids": 8,
    }


def test_agent_limits_are_frozen_slotted_and_tunable_below_absolute_ceilings() -> None:
    limits = AgentLimits(max_request_bytes=1_024)

    assert limits.max_request_bytes == 1_024
    assert not hasattr(limits, "__dict__")
    with pytest.raises(FrozenInstanceError):
        limits.max_wait_ms = 1  # type: ignore[misc]
    with pytest.raises(ValueError, match="must not exceed"):
        AgentLimits(max_request_bytes=16 * 1_024 + 1)
    expanded = AgentLimits(
        max_wire_bytes=1 * 1_024 * 1_024,
        max_inflight_requests=32,
        max_snapshot_bytes=8 * 1_024 * 1_024,
        max_snapshot_edge_px=1_920,
        max_wait_ms=15_000,
    )
    assert expanded.max_snapshot_bytes == 8 * 1_024 * 1_024
    assert expanded.max_inflight_requests == 32


@pytest.mark.parametrize(
    "overrides",
    [
        {"max_request_bytes": 0},
        {"max_wire_bytes": 1 * 1_024 * 1_024 + 1},
        {"max_inflight_requests": 33},
        {"max_metadata_bytes": True},
        {"max_observation_bytes": 32 * 1_024 + 1},
        {"max_snapshot_bytes": 8 * 1_024 * 1_024 + 1},
        {"max_snapshot_edge_px": 1_921},
        {"max_wait_ms": 15_001},
        {"max_age_ms": 10_001},
        {"max_schema_ids": 9},
        {"max_metadata_bytes": 1_024, "max_observation_bytes": 2_048},
        {"max_request_bytes": 2_048, "max_wire_bytes": 1_024},
    ],
)
def test_agent_limits_reject_invalid_or_raised_values(overrides: dict[str, Any]) -> None:
    with pytest.raises((TypeError, ValueError)):
        AgentLimits(**overrides)


def test_consent_grant_is_frozen_slotted_and_has_no_secret_representation() -> None:
    grant = _grant()
    public = grant.to_public_dict()

    assert not hasattr(grant, "__dict__")
    assert set(public) == {
        "public_id",
        "scopes",
        "expires_unix_ns",
        "snapshot_quota",
        "sensitivity_ceiling",
    }
    assert "token" not in repr(grant).lower()
    assert "secret" not in repr(grant).lower()
    assert public["scopes"] == ["observation.read", "snapshot.read"]
    with pytest.raises(FrozenInstanceError):
        grant.snapshot_quota = 10  # type: ignore[misc]


def test_consent_grant_normalizes_scopes_to_an_immutable_set() -> None:
    grant = _grant(scopes=["snapshot.read"])  # type: ignore[arg-type]

    assert grant.scopes == frozenset({"snapshot.read"})


@pytest.mark.parametrize(
    ("overrides", "error_type"),
    [
        ({"public_id": ""}, ValueError),
        ({"public_id": 1}, TypeError),
        ({"public_id": "x" * 129}, ValueError),
        ({"scopes": "snapshot.read"}, TypeError),
        ({"scopes": object()}, TypeError),
        ({"scopes": frozenset({"camera.open"})}, ValueError),
        ({"expires_unix_ns": 0}, ValueError),
        ({"expires_unix_ns": True}, TypeError),
        ({"snapshot_quota": -1}, ValueError),
        ({"snapshot_quota": False}, TypeError),
        ({"expires_unix_ns": 2**63}, ValueError),
        ({"snapshot_quota": 2**63}, ValueError),
        ({"sensitivity_ceiling": "prohibited"}, ValueError),
    ],
)
def test_consent_grant_rejects_invalid_public_metadata(
    overrides: dict[str, Any], error_type: type[Exception]
) -> None:
    with pytest.raises(error_type):
        _grant(**overrides)


def test_prohibited_is_always_denied_even_without_a_grant() -> None:
    decision = evaluate_grant(
        None,
        scope="observation.read",
        sensitivity="prohibited",
        now_unix_ns=1,
    )

    assert decision == GrantDecision(allowed=False, reason="prohibited")


@pytest.mark.parametrize(
    ("grant", "scope", "sensitivity", "now_unix_ns", "snapshots_used", "reason"),
    [
        (None, "observation.read", "public", 1, 0, "grant_missing"),
        (_grant(), "observation.read", "public", 2_000, 0, "grant_expired"),
        (
            _grant(scopes=frozenset({"observation.read"})),
            "snapshot.read",
            "public",
            1_000,
            0,
            "scope_missing",
        ),
        (_grant(), "observation.read", "identifiable", 1_000, 0, "sensitivity_denied"),
        (_grant(), "snapshot.read", "public", 1_000, 2, "snapshot_quota_exhausted"),
    ],
)
def test_evaluate_grant_distinguishes_denial_reasons(
    grant: ConsentGrant | None,
    scope: Any,
    sensitivity: Any,
    now_unix_ns: int,
    snapshots_used: int,
    reason: str,
) -> None:
    decision = evaluate_grant(
        grant,
        scope=scope,
        sensitivity=sensitivity,
        now_unix_ns=now_unix_ns,
        snapshots_used=snapshots_used,
    )

    assert decision.allowed is False
    assert decision.reason == reason
    assert decision.reason_code == reason


def test_evaluate_grant_allows_exact_ceiling_and_available_snapshot_quota() -> None:
    grant = _grant()

    decision = evaluate_grant(
        grant,
        scope="snapshot.read",
        sensitivity="sensitive",
        now_unix_ns=1_999,
        snapshots_used=1,
    )

    assert decision == GrantDecision(allowed=True, reason="allowed")


def test_observation_scope_does_not_consume_or_require_snapshot_quota() -> None:
    decision = evaluate_grant(
        _grant(snapshot_quota=0),
        scope="observation.read",
        sensitivity="operational",
        now_unix_ns=1_000,
        snapshots_used=99,
    )

    assert decision.allowed is True


def test_evaluate_grant_is_pure_and_cannot_renew_the_grant() -> None:
    grant = _grant()
    original = replace(grant)

    evaluate_grant(
        grant,
        scope="snapshot.read",
        sensitivity="public",
        now_unix_ns=1_000,
        snapshots_used=0,
    )

    assert grant == original
    assert grant.expires_unix_ns == 2_000
    assert grant.snapshot_quota == 2


@pytest.mark.parametrize(
    ("kwargs", "error_type"),
    [
        ({"scope": "camera.open"}, ValueError),
        ({"sensitivity": "unknown"}, ValueError),
        ({"now_unix_ns": -1}, ValueError),
        ({"now_unix_ns": True}, TypeError),
        ({"snapshots_used": -1}, ValueError),
        ({"snapshots_used": False}, TypeError),
    ],
)
def test_evaluate_grant_rejects_invalid_inputs(
    kwargs: dict[str, Any], error_type: type[Exception]
) -> None:
    values: dict[str, Any] = {
        "scope": "observation.read",
        "sensitivity": "public",
        "now_unix_ns": 1,
        "snapshots_used": 0,
    }
    values.update(kwargs)

    with pytest.raises(error_type):
        evaluate_grant(_grant(), **values)


@pytest.mark.parametrize(
    ("allowed", "reason"),
    [(True, "scope_missing"), (False, "allowed"), (False, "unknown")],
)
def test_grant_decision_rejects_inconsistent_state(allowed: bool, reason: Any) -> None:
    with pytest.raises(ValueError, match=r"allowed|reason"):
        GrantDecision(allowed=allowed, reason=reason)
