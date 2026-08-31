"""Pure, bounded contracts for agent access to Viskium observations.

This module deliberately contains no camera or session lifecycle operations.  A
consent grant can only be evaluated; it cannot be created, renewed, or used to
open hardware from this boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from viskium.core.contracts import SensitivityClass

type AgentScope = Literal["observation.read", "snapshot.read"]
type GrantDecisionReason = Literal[
    "allowed",
    "grant_missing",
    "grant_expired",
    "scope_missing",
    "sensitivity_denied",
    "snapshot_quota_exhausted",
    "prohibited",
]

_ALLOWED_SCOPES: frozenset[str] = frozenset({"observation.read", "snapshot.read"})
_DECISION_REASONS: frozenset[str] = frozenset(
    {
        "allowed",
        "grant_missing",
        "grant_expired",
        "scope_missing",
        "sensitivity_denied",
        "snapshot_quota_exhausted",
        "prohibited",
    }
)
_SENSITIVITY_RANK: dict[str, int] = {
    "public": 0,
    "operational": 1,
    "sensitive": 2,
    "identifiable": 3,
}
_MAX_PUBLIC_ID_CHARS = 128
_MAX_INT64 = 2**63 - 1


def _require_integer(value: object, field_name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    if value > _MAX_INT64:
        raise ValueError(f"{field_name} must fit in signed 64 bits")
    return value


@dataclass(frozen=True, slots=True)
class AgentLimits:
    """Defensive ceilings for the local agent-read boundary.

    Defaults suit a modest host. Instances may tune them up or down without
    crossing the reviewed absolute ceilings.
    """

    max_request_bytes: int = 16 * 1_024
    max_wire_bytes: int = 256 * 1_024
    max_inflight_requests: int = 4
    max_metadata_bytes: int = 64 * 1_024
    max_observation_bytes: int = 32 * 1_024
    max_snapshot_bytes: int = 4 * 1_024 * 1_024
    max_snapshot_edge_px: int = 1_280
    max_wait_ms: int = 10_000
    max_age_ms: int = 10_000
    max_schema_ids: int = 8

    def __post_init__(self) -> None:
        ceilings = {
            "max_request_bytes": 16 * 1_024,
            "max_wire_bytes": 1 * 1_024 * 1_024,
            "max_inflight_requests": 32,
            "max_metadata_bytes": 64 * 1_024,
            "max_observation_bytes": 32 * 1_024,
            "max_snapshot_bytes": 8 * 1_024 * 1_024,
            "max_snapshot_edge_px": 1_920,
            "max_wait_ms": 15_000,
            "max_age_ms": 10_000,
            "max_schema_ids": 8,
        }
        for field_name, ceiling in ceilings.items():
            value = _require_integer(getattr(self, field_name), field_name, minimum=1)
            if value > ceiling:
                raise ValueError(f"{field_name} must not exceed {ceiling}")
        if self.max_observation_bytes > self.max_metadata_bytes:
            raise ValueError("max_observation_bytes must not exceed max_metadata_bytes")
        if self.max_request_bytes > self.max_wire_bytes:
            raise ValueError("max_request_bytes must not exceed max_wire_bytes")

    def to_dict(self) -> dict[str, int]:
        """Return a stable JSON-shaped representation of the effective limits."""

        return {
            "max_request_bytes": self.max_request_bytes,
            "max_wire_bytes": self.max_wire_bytes,
            "max_inflight_requests": self.max_inflight_requests,
            "max_metadata_bytes": self.max_metadata_bytes,
            "max_observation_bytes": self.max_observation_bytes,
            "max_snapshot_bytes": self.max_snapshot_bytes,
            "max_snapshot_edge_px": self.max_snapshot_edge_px,
            "max_wait_ms": self.max_wait_ms,
            "max_age_ms": self.max_age_ms,
            "max_schema_ids": self.max_schema_ids,
        }


@dataclass(frozen=True, slots=True)
class ConsentGrant:
    """Public, immutable view of an out-of-band consent grant.

    Authentication material intentionally has no field in this contract.  The
    local transport binds credentials to a connection outside the model-visible
    request and response schemas.
    """

    public_id: str
    scopes: frozenset[AgentScope]
    expires_unix_ns: int
    snapshot_quota: int
    sensitivity_ceiling: SensitivityClass

    def __post_init__(self) -> None:
        if not isinstance(self.public_id, str):
            raise TypeError("public_id must be a string")
        if not self.public_id or not self.public_id.strip():
            raise ValueError("public_id must not be empty")
        if len(self.public_id) > _MAX_PUBLIC_ID_CHARS:
            raise ValueError(f"public_id exceeds {_MAX_PUBLIC_ID_CHARS} characters")

        if isinstance(self.scopes, (str, bytes)):
            raise TypeError("scopes must be an iterable of agent scopes")
        try:
            normalized_scopes = frozenset(self.scopes)
        except TypeError as error:
            raise TypeError("scopes must be an iterable of agent scopes") from error
        invalid_scopes = normalized_scopes - _ALLOWED_SCOPES
        if invalid_scopes:
            names = ", ".join(sorted(str(scope) for scope in invalid_scopes))
            raise ValueError(f"unsupported agent scope(s): {names}")
        object.__setattr__(self, "scopes", normalized_scopes)

        _require_integer(self.expires_unix_ns, "expires_unix_ns", minimum=1)
        _require_integer(self.snapshot_quota, "snapshot_quota")
        if self.sensitivity_ceiling not in _SENSITIVITY_RANK:
            raise ValueError("sensitivity_ceiling cannot authorize prohibited content")

    def to_public_dict(self) -> dict[str, Any]:
        """Serialize only model-safe grant metadata, never credentials."""

        return {
            "public_id": self.public_id,
            "scopes": sorted(self.scopes),
            "expires_unix_ns": self.expires_unix_ns,
            "snapshot_quota": self.snapshot_quota,
            "sensitivity_ceiling": self.sensitivity_ceiling,
        }


@dataclass(frozen=True, slots=True)
class GrantDecision:
    """Pure authorization result with a stable machine-readable reason."""

    allowed: bool
    reason: GrantDecisionReason

    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool):
            raise TypeError("allowed must be a boolean")
        if self.reason not in _DECISION_REASONS:
            raise ValueError("unsupported grant decision reason")
        if self.allowed != (self.reason == "allowed"):
            raise ValueError("allowed must be true exactly when reason is 'allowed'")

    @property
    def reason_code(self) -> GrantDecisionReason:
        """Alias suitable for JSON contracts that call the field ``reason_code``."""

        return self.reason


def evaluate_grant(
    grant: ConsentGrant | None,
    *,
    scope: AgentScope,
    sensitivity: SensitivityClass,
    now_unix_ns: int,
    snapshots_used: int = 0,
) -> GrantDecision:
    """Evaluate a public grant without mutating state or touching hardware.

    Prohibited content is denied before all other considerations.  Expiration is
    inclusive: a grant is no longer valid when ``now_unix_ns`` reaches its
    expiration instant.
    """

    if scope not in _ALLOWED_SCOPES:
        raise ValueError("unsupported agent scope")
    if sensitivity not in {*_SENSITIVITY_RANK, "prohibited"}:
        raise ValueError("unsupported sensitivity class")
    _require_integer(now_unix_ns, "now_unix_ns")
    _require_integer(snapshots_used, "snapshots_used")

    if sensitivity == "prohibited":
        return GrantDecision(allowed=False, reason="prohibited")
    if grant is None:
        return GrantDecision(allowed=False, reason="grant_missing")
    if now_unix_ns >= grant.expires_unix_ns:
        return GrantDecision(allowed=False, reason="grant_expired")
    if scope not in grant.scopes:
        return GrantDecision(allowed=False, reason="scope_missing")
    if _SENSITIVITY_RANK[sensitivity] > _SENSITIVITY_RANK[grant.sensitivity_ceiling]:
        return GrantDecision(allowed=False, reason="sensitivity_denied")
    if scope == "snapshot.read" and snapshots_used >= grant.snapshot_quota:
        return GrantDecision(allowed=False, reason="snapshot_quota_exhausted")
    return GrantDecision(allowed=True, reason="allowed")


__all__ = [
    "AgentLimits",
    "AgentScope",
    "ConsentGrant",
    "GrantDecision",
    "GrantDecisionReason",
    "evaluate_grant",
]
