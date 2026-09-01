"""Consent-gated, bounded reads for local agents.

This module is an in-process application boundary, not a transport and not a
camera controller.  It can read a latest observation or request one encoded
still image from injected dependencies.  It never opens, renews, or owns a
capture session.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Literal, Protocol, cast, runtime_checkable

from viskium.agent.consent import ConsentLedger, ConsentState, SnapshotReservation
from viskium.agent.contracts import (
    AgentLimits,
    AgentScope,
    GrantDecisionReason,
    evaluate_grant,
)
from viskium.core.contracts import SensitivityClass
from viskium.core.serialization import (
    bounded_canonical_json_bytes,
    bounded_observation_bytes,
)
from viskium.observations.latest import LatestObservationRead, LatestObservationSlot
from viskium.snapshots.contracts import SnapshotEnvelope

AGENT_READ_CONTRACT: Literal["urn:viskium:agent-read:1"] = "urn:viskium:agent-read:1"

type SnapshotCaptureOutcome = Literal[
    "ok",
    "busy",
    "timeout",
    "closed",
    "unavailable",
    "failed",
]
type SnapshotReasonCode = Literal[
    "generic",
    "resource_denied",
    "resource_gate_failed",
    "busy",
    "throttled",
    "lease_busy",
    "timeout",
    "closed",
    "close_stuck",
    "unavailable",
    "failed",
    "opencv_unavailable",
    "device_open_failed",
    "negotiated_mode_exceeds_limit",
    "capture_read_error",
    "capture_read_failed",
    "invalid_backend_frame",
    "invalid_worker_command",
    "camera_open_timeout",
    "camera_open_failed",
    "camera_worker_exited",
    "camera_worker_disconnected",
    "camera_worker_start_failed",
    "opencv_worker_error",
    "camera_worker_error",
    "camera_read_deadline_expired",
    "camera_read_timeout",
    "invalid_worker_response",
    "snapshot_deadline_expired",
    "close_failed",
    "lease_release_failed",
    "invalid_open_response",
]
type AgentStatusOutcome = Literal[
    "ok",
    "status_unavailable",
    "status_invalid",
    "metadata_too_large",
]
type AgentObservationOutcome = Literal[
    "ok",
    "empty",
    "stale",
    "schema_mismatch",
    "timeout",
    "closed",
    "future_timestamp",
    "grant_missing",
    "grant_expired",
    "scope_missing",
    "sensitivity_denied",
    "prohibited",
    "grant_changed",
    "consent_unavailable",
    "clock_unavailable",
    "observation_invalid",
    "observation_too_large",
]
type AgentSnapshotOutcome = Literal[
    "ok",
    "busy",
    "timeout",
    "closed",
    "unavailable",
    "failed",
    "grant_missing",
    "grant_expired",
    "scope_missing",
    "sensitivity_denied",
    "snapshot_quota_exhausted",
    "prohibited",
    "grant_changed",
    "consent_unavailable",
    "clock_unavailable",
    "provider_failure",
    "provider_invalid",
    "snapshot_limit_exceeded",
]
type StatusScalar = bool | int | float | str | None
type StatusValue = StatusScalar | Mapping[str, "StatusValue"] | Sequence["StatusValue"]
type StatusProvider = Callable[[], Mapping[str, StatusValue]]
type _AuthorizationReason = (
    GrantDecisionReason | Literal["clock_unavailable", "grant_changed", "consent_unavailable"]
)

_MAX_INT64 = 2**63 - 1
_MAX_SCHEMA_ID_CHARS = 256
_MAX_STATUS_DEPTH = 8
_MAX_STATUS_NODES = 1_024
_MAX_STATUS_COLLECTION_ITEMS = 128
_MAX_STATUS_KEY_CHARS = 128
_MAX_STATUS_STRING_CHARS = 4_096
_FORBIDDEN_STATUS_KEY_FRAGMENTS: tuple[str, ...] = (
    "authorization",
    "credential",
    "password",
    "secret",
    "token",
    "apikey",
    "api_key",
    "filepath",
    "filename",
    "directory",
    "path",
    "root",
    "cwd",
    "home",
)
_OBSERVATION_OUTCOMES: frozenset[str] = frozenset(
    {
        "ok",
        "empty",
        "stale",
        "schema_mismatch",
        "timeout",
        "closed",
        "future_timestamp",
        "grant_missing",
        "grant_expired",
        "scope_missing",
        "sensitivity_denied",
        "prohibited",
        "grant_changed",
        "consent_unavailable",
        "clock_unavailable",
        "observation_invalid",
        "observation_too_large",
    }
)
_SNAPSHOT_OUTCOMES: frozenset[str] = frozenset(
    {
        "ok",
        "busy",
        "timeout",
        "closed",
        "unavailable",
        "failed",
        "grant_missing",
        "grant_expired",
        "scope_missing",
        "sensitivity_denied",
        "snapshot_quota_exhausted",
        "prohibited",
        "grant_changed",
        "consent_unavailable",
        "clock_unavailable",
        "provider_failure",
        "provider_invalid",
        "snapshot_limit_exceeded",
    }
)
_CAPTURE_OUTCOMES: frozenset[str] = frozenset(
    {"ok", "busy", "timeout", "closed", "unavailable", "failed"}
)
_STATUS_OUTCOMES: frozenset[str] = frozenset(
    {"ok", "status_unavailable", "status_invalid", "metadata_too_large"}
)
_SNAPSHOT_REASON_CODES: frozenset[str] = frozenset(
    {
        "generic",
        "resource_denied",
        "resource_gate_failed",
        "busy",
        "throttled",
        "lease_busy",
        "timeout",
        "closed",
        "close_stuck",
        "unavailable",
        "failed",
        "opencv_unavailable",
        "device_open_failed",
        "negotiated_mode_exceeds_limit",
        "capture_read_error",
        "capture_read_failed",
        "invalid_backend_frame",
        "invalid_worker_command",
        "camera_open_timeout",
        "camera_open_failed",
        "camera_worker_exited",
        "camera_worker_disconnected",
        "camera_worker_start_failed",
        "opencv_worker_error",
        "camera_worker_error",
        "camera_read_deadline_expired",
        "camera_read_timeout",
        "invalid_worker_response",
        "snapshot_deadline_expired",
        "close_failed",
        "lease_release_failed",
        "invalid_open_response",
    }
)
_CANONICAL_SNAPSHOT_REASON_CODES: dict[str, SnapshotReasonCode] = {
    code: cast(SnapshotReasonCode, code) for code in _SNAPSHOT_REASON_CODES
}
_SNAPSHOT_REASON_OUTCOMES: dict[str, frozenset[str]] = {
    "resource_denied": frozenset({"unavailable"}),
    "resource_gate_failed": frozenset({"unavailable"}),
    "busy": frozenset({"busy"}),
    "throttled": frozenset({"busy"}),
    "lease_busy": frozenset({"busy"}),
    "timeout": frozenset({"timeout"}),
    "camera_open_timeout": frozenset({"timeout"}),
    "camera_read_deadline_expired": frozenset({"timeout"}),
    "camera_read_timeout": frozenset({"timeout"}),
    "snapshot_deadline_expired": frozenset({"timeout"}),
    "closed": frozenset({"closed", "failed"}),
    "close_stuck": frozenset({"failed"}),
    "close_failed": frozenset({"failed"}),
    "lease_release_failed": frozenset({"failed"}),
    "unavailable": frozenset({"unavailable"}),
    "opencv_unavailable": frozenset({"unavailable"}),
    "device_open_failed": frozenset({"unavailable"}),
    "negotiated_mode_exceeds_limit": frozenset({"unavailable"}),
    "camera_open_failed": frozenset({"unavailable"}),
    "invalid_open_response": frozenset({"unavailable"}),
    "camera_worker_exited": frozenset({"unavailable"}),
    "camera_worker_disconnected": frozenset({"unavailable"}),
    "capture_read_failed": frozenset({"unavailable"}),
    "failed": frozenset({"failed"}),
    "capture_read_error": frozenset({"failed"}),
    "invalid_backend_frame": frozenset({"failed"}),
    "invalid_worker_command": frozenset({"failed"}),
    "camera_worker_start_failed": frozenset({"failed"}),
    "opencv_worker_error": frozenset({"failed"}),
    "camera_worker_error": frozenset({"failed"}),
    "invalid_worker_response": frozenset({"failed"}),
}


def normalize_snapshot_reason(value: object) -> SnapshotReasonCode:
    """Return only a reviewed reason code, replacing untrusted text with generic."""

    if type(value) is str:
        return _CANONICAL_SNAPSHOT_REASON_CODES.get(value, "generic")
    return "generic"


def normalize_snapshot_reason_for_outcome(
    value: object,
    *,
    outcome: str,
) -> SnapshotReasonCode:
    """Normalize a reason and replace semantically incompatible codes with generic."""

    normalized = normalize_snapshot_reason(value)
    if normalized == "generic":
        return normalized
    if outcome not in _SNAPSHOT_REASON_OUTCOMES.get(normalized, frozenset()):
        return "generic"
    return normalized


def _validate_snapshot_reason_for_outcome(
    outcome: str,
    reason_code: SnapshotReasonCode | None,
    *,
    allowed_outcomes: frozenset[str],
) -> None:
    if reason_code is None:
        return
    if reason_code == "generic":
        if outcome not in allowed_outcomes - {"ok"}:
            raise ValueError("generic reason_code requires a non-ok snapshot outcome")
        return
    if outcome not in _SNAPSHOT_REASON_OUTCOMES.get(reason_code, frozenset()):
        raise ValueError("reason_code is incompatible with snapshot outcome")


def _require_bounded_integer(
    value: object,
    field_name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
    return value


def _clock_value(clock: Callable[[], int]) -> int:
    return _require_bounded_integer(
        clock(),
        "clock value",
        minimum=0,
        maximum=_MAX_INT64,
    )


def _normalize_schema_ids(
    schema_ids: Collection[str] | None,
    *,
    maximum: int,
) -> frozenset[str] | None:
    if schema_ids is None:
        return None
    if isinstance(schema_ids, (str, bytes)):
        raise TypeError("schema_ids must be a collection of strings")
    if not isinstance(schema_ids, Collection):
        raise TypeError("schema_ids must be a sized collection of strings")
    if len(schema_ids) > maximum:
        raise ValueError(f"schema_ids must not exceed {maximum} entries")
    normalized: set[str] = set()
    for schema_id in schema_ids:
        if not isinstance(schema_id, str):
            raise TypeError("schema_ids entries must be strings")
        if not schema_id or not schema_id.strip():
            raise ValueError("schema_ids entries must not be empty")
        if len(schema_id) > _MAX_SCHEMA_ID_CHARS:
            raise ValueError(
                f"schema_ids entries must not exceed {_MAX_SCHEMA_ID_CHARS} characters"
            )
        normalized.add(schema_id)
    return frozenset(normalized)


def _looks_private_status_string(value: str) -> bool:
    stripped = value.strip()
    lowered = stripped.lower()
    if lowered.startswith(("bearer ", "sk-")):
        return True
    if stripped.startswith(("/", "\\\\")):
        return True
    return (
        len(stripped) >= 3
        and stripped[0].isalpha()
        and stripped[1] == ":"
        and stripped[2]
        in {
            "/",
            "\\",
        }
    )


def _safe_status_document(value: Mapping[str, StatusValue]) -> dict[str, Any]:
    """Copy a small JSON status tree while rejecting secret/path-shaped fields."""

    visited_nodes = 0

    def copy_node(node: StatusValue, *, depth: int) -> Any:
        nonlocal visited_nodes
        visited_nodes += 1
        if visited_nodes > _MAX_STATUS_NODES:
            raise ValueError("status exceeds its node limit")
        if depth > _MAX_STATUS_DEPTH:
            raise ValueError("status exceeds its nesting limit")

        if node is None or isinstance(node, bool):
            return node
        if isinstance(node, int):
            if not -(2**63) <= node <= _MAX_INT64:
                raise ValueError("status integers must fit signed int64")
            return node
        if isinstance(node, float):
            if not math.isfinite(node):
                raise ValueError("status floats must be finite")
            return node
        if isinstance(node, str):
            if len(node) > _MAX_STATUS_STRING_CHARS:
                raise ValueError("status strings exceed their character limit")
            if _looks_private_status_string(node):
                raise ValueError("status contains private-looking text")
            return node
        if isinstance(node, Mapping):
            if len(node) > _MAX_STATUS_COLLECTION_ITEMS:
                raise ValueError("status mapping exceeds its item limit")
            result: dict[str, Any] = {}
            for key, item in node.items():
                if not isinstance(key, str):
                    raise TypeError("status keys must be strings")
                if not key or not key.strip() or len(key) > _MAX_STATUS_KEY_CHARS:
                    raise ValueError("status keys must be short and non-empty")
                lowered_key = key.casefold()
                if any(fragment in lowered_key for fragment in _FORBIDDEN_STATUS_KEY_FRAGMENTS):
                    raise ValueError("status contains a private field")
                result[key] = copy_node(item, depth=depth + 1)
            return result
        if isinstance(node, Sequence) and not isinstance(node, (str, bytes, bytearray)):
            if len(node) > _MAX_STATUS_COLLECTION_ITEMS:
                raise ValueError("status sequence exceeds its item limit")
            return tuple(copy_node(item, depth=depth + 1) for item in node)
        raise TypeError("status values must be JSON-shaped")

    copied = copy_node(value, depth=0)
    if not isinstance(copied, dict):
        raise TypeError("status provider must return a mapping")
    return copied


@dataclass(frozen=True, slots=True)
class SnapshotCaptureResult:
    """Explicit provider result containing a PNG envelope, never a raw frame."""

    outcome: SnapshotCaptureOutcome
    snapshot: SnapshotEnvelope | None = None
    reason_code: SnapshotReasonCode | None = None

    def __post_init__(self) -> None:
        if self.outcome not in _CAPTURE_OUTCOMES:
            raise ValueError("unsupported snapshot capture outcome")
        normalized_reason = (
            None if self.reason_code is None else normalize_snapshot_reason(self.reason_code)
        )
        object.__setattr__(self, "reason_code", normalized_reason)
        if self.outcome == "ok":
            if not isinstance(self.snapshot, SnapshotEnvelope):
                raise TypeError("ok snapshot capture requires a SnapshotEnvelope")
            if normalized_reason is not None:
                raise ValueError("ok snapshot capture must not include a reason_code")
        elif self.snapshot is not None:
            raise ValueError("non-ok snapshot capture must not expose a snapshot")
        _validate_snapshot_reason_for_outcome(
            self.outcome,
            normalized_reason,
            allowed_outcomes=_CAPTURE_OUTCOMES,
        )


@runtime_checkable
class SnapshotProvider(Protocol):
    """One-shot encoded snapshot provider; no streaming or lifecycle methods."""

    @property
    def sensitivity_class(self) -> SensitivityClass:
        """Return the highest sensitivity this provider may emit."""

    def capture(
        self,
        *,
        max_edge_px: int,
        max_bytes: int,
        timeout_seconds: float,
    ) -> SnapshotCaptureResult:
        """Return at most one bounded, already encoded still image."""


@dataclass(frozen=True, slots=True)
class AgentStatusResult:
    """Bounded status result encoded as deterministic JSON."""

    outcome: AgentStatusOutcome
    metadata_json: bytes | None = None
    contract: Literal["urn:viskium:agent-read:1"] = AGENT_READ_CONTRACT

    def __post_init__(self) -> None:
        if self.contract != AGENT_READ_CONTRACT:
            raise ValueError("unsupported agent-read contract")
        if self.outcome not in _STATUS_OUTCOMES:
            raise ValueError("unsupported status outcome")
        if self.outcome == "ok":
            if not isinstance(self.metadata_json, bytes):
                raise TypeError("ok status requires immutable JSON bytes")
            if len(self.metadata_json) > 64 * 1_024:
                raise ValueError("status metadata exceeds the hard byte ceiling")
        elif self.metadata_json is not None:
            raise ValueError("non-ok status must not expose metadata")

    @property
    def reason_code(self) -> AgentStatusOutcome | None:
        return None if self.outcome == "ok" else self.outcome


@dataclass(frozen=True, slots=True)
class AgentObservationResult:
    """Observation read result; only ``ok`` carries canonical JSON bytes."""

    outcome: AgentObservationOutcome
    observation_json: bytes | None = None
    age_ns: int | None = None
    contract: Literal["urn:viskium:agent-read:1"] = AGENT_READ_CONTRACT

    def __post_init__(self) -> None:
        if self.contract != AGENT_READ_CONTRACT:
            raise ValueError("unsupported agent-read contract")
        if self.outcome not in _OBSERVATION_OUTCOMES:
            raise ValueError("unsupported observation outcome")
        if self.age_ns is not None:
            _require_bounded_integer(
                self.age_ns,
                "age_ns",
                minimum=0,
                maximum=_MAX_INT64,
            )
        if self.outcome == "ok":
            if not isinstance(self.observation_json, bytes):
                raise TypeError("ok observation requires immutable JSON bytes")
            if len(self.observation_json) > 32 * 1_024:
                raise ValueError("observation exceeds the hard byte ceiling")
            if self.age_ns is None:
                raise ValueError("ok observation requires age_ns")
            return
        if self.observation_json is not None:
            raise ValueError("non-ok observation must not expose observation JSON")
        if self.outcome in {"stale", "schema_mismatch"}:
            if self.age_ns is None:
                raise ValueError(f"{self.outcome} observation requires age_ns")
        elif self.age_ns is not None:
            raise ValueError(f"{self.outcome} observation must not expose age_ns")

    @property
    def reason_code(self) -> AgentObservationOutcome | None:
        return None if self.outcome == "ok" else self.outcome


@dataclass(frozen=True, slots=True, init=False)
class AgentSnapshotResult:
    """One encoded still-image result; raw frame contracts cannot appear here."""

    outcome: AgentSnapshotOutcome
    snapshot: SnapshotEnvelope | None
    contract: Literal["urn:viskium:agent-read:1"]
    _explicit_reason_code: SnapshotReasonCode | None = field(default=None, repr=False)

    def __init__(
        self,
        outcome: AgentSnapshotOutcome,
        snapshot: SnapshotEnvelope | None = None,
        contract: Literal["urn:viskium:agent-read:1"] = AGENT_READ_CONTRACT,
        reason_code: SnapshotReasonCode | None = None,
    ) -> None:
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "snapshot", snapshot)
        object.__setattr__(self, "contract", contract)
        object.__setattr__(
            self,
            "_explicit_reason_code",
            None if reason_code is None else normalize_snapshot_reason(reason_code),
        )
        self.__post_init__()

    def __post_init__(self) -> None:
        if self.contract != AGENT_READ_CONTRACT:
            raise ValueError("unsupported agent-read contract")
        if self.outcome not in _SNAPSHOT_OUTCOMES:
            raise ValueError("unsupported snapshot outcome")
        if self.outcome == "ok":
            if not isinstance(self.snapshot, SnapshotEnvelope):
                raise TypeError("ok snapshot result requires a SnapshotEnvelope")
            if self._explicit_reason_code is not None:
                raise ValueError("ok snapshot result must not include a reason_code")
        elif self.snapshot is not None:
            raise ValueError("non-ok snapshot result must not expose a snapshot")
        _validate_snapshot_reason_for_outcome(
            self.outcome,
            self._explicit_reason_code,
            allowed_outcomes=_SNAPSHOT_OUTCOMES,
        )

    @property
    def reason_code(self) -> AgentSnapshotOutcome | SnapshotReasonCode | None:
        """Return an explicit safe code, or the legacy outcome alias."""

        if self._explicit_reason_code is not None:
            return self._explicit_reason_code
        return None if self.outcome == "ok" else self.outcome

    @property
    def explicit_reason_code(self) -> SnapshotReasonCode | None:
        """Return only an explicitly supplied provider reason, if present."""

        return self._explicit_reason_code


@dataclass(frozen=True, slots=True)
class AgentServiceMetrics:
    """Immutable counters that contain neither content nor identifiers."""

    status_requests: int
    observation_requests: int
    snapshot_requests: int
    authorization_denials: int
    snapshot_attempts_reserved: int
    observations_delivered: int
    snapshots_delivered: int
    dependency_failures: int
    limit_rejections: int

    def __post_init__(self) -> None:
        for field_name in (
            "status_requests",
            "observation_requests",
            "snapshot_requests",
            "authorization_denials",
            "snapshot_attempts_reserved",
            "observations_delivered",
            "snapshots_delivered",
            "dependency_failures",
            "limit_rejections",
        ):
            _require_bounded_integer(
                getattr(self, field_name),
                field_name,
                minimum=0,
                maximum=_MAX_INT64,
            )


@dataclass(frozen=True, slots=True)
class _AuthorizationCheck:
    reason: _AuthorizationReason
    state: ConsentState | None

    @property
    def allowed(self) -> bool:
        return self.reason == "allowed" and self.state is not None


class AgentReadService:
    """Small synchronous application service for bounded local agent reads."""

    __slots__ = (
        "_consent",
        "_limits",
        "_metrics_counts",
        "_metrics_lock",
        "_monotonic_ns",
        "_observations",
        "_snapshot_provider",
        "_status_provider",
        "_unix_time_ns",
    )

    _STATUS_REQUEST = 0
    _OBSERVATION_REQUEST = 1
    _SNAPSHOT_REQUEST = 2
    _AUTHORIZATION_DENIAL = 3
    _SNAPSHOT_RESERVED = 4
    _OBSERVATION_DELIVERED = 5
    _SNAPSHOT_DELIVERED = 6
    _DEPENDENCY_FAILURE = 7
    _LIMIT_REJECTION = 8

    def __init__(
        self,
        *,
        observations: LatestObservationSlot,
        consent: ConsentLedger,
        snapshot_provider: SnapshotProvider,
        status_provider: StatusProvider,
        limits: AgentLimits | None = None,
        unix_time_ns: Callable[[], int] | None = None,
        monotonic_ns: Callable[[], int] | None = None,
    ) -> None:
        if not callable(status_provider):
            raise TypeError("status_provider must be callable")
        if not isinstance(snapshot_provider, SnapshotProvider):
            raise TypeError("snapshot_provider must implement SnapshotProvider")
        if unix_time_ns is not None and not callable(unix_time_ns):
            raise TypeError("unix_time_ns must be callable")
        if monotonic_ns is not None and not callable(monotonic_ns):
            raise TypeError("monotonic_ns must be callable")
        self._observations = observations
        self._consent = consent
        self._snapshot_provider = snapshot_provider
        self._status_provider = status_provider
        self._limits = AgentLimits() if limits is None else limits
        self._unix_time_ns = time.time_ns if unix_time_ns is None else unix_time_ns
        self._monotonic_ns = time.monotonic_ns if monotonic_ns is None else monotonic_ns
        self._metrics_lock = Lock()
        self._metrics_counts = [0] * 9

    @property
    def limits(self) -> AgentLimits:
        return self._limits

    @property
    def metrics(self) -> AgentServiceMetrics:
        return self.metrics_snapshot()

    def metrics_snapshot(self) -> AgentServiceMetrics:
        with self._metrics_lock:
            counts = tuple(self._metrics_counts)
        return AgentServiceMetrics(*counts)

    def status(self) -> AgentStatusResult:
        """Return public status without consulting consent, frames, or camera providers."""

        self._count(self._STATUS_REQUEST)
        try:
            supplied = self._status_provider()
        except Exception:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentStatusResult("status_unavailable")
        if not isinstance(supplied, Mapping):
            self._count(self._DEPENDENCY_FAILURE)
            return AgentStatusResult("status_invalid")
        try:
            safe_status = _safe_status_document(supplied)
            document = {
                "contract": AGENT_READ_CONTRACT,
                "limits": self._limits.to_dict(),
                "status": safe_status,
            }
            encoded = bounded_canonical_json_bytes(
                document,
                max_bytes=self._limits.max_metadata_bytes,
            )
        except (TypeError, ValueError):
            self._count(self._DEPENDENCY_FAILURE)
            return AgentStatusResult("status_invalid")
        except Exception:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentStatusResult("status_unavailable")
        if encoded is None:
            self._count(self._LIMIT_REJECTION)
            return AgentStatusResult("metadata_too_large")
        return AgentStatusResult("ok", metadata_json=encoded)

    def latest_observation(
        self,
        *,
        max_age_ms: int,
        wait_ms: int = 0,
        schema_ids: Collection[str] | None = None,
    ) -> AgentObservationResult:
        """Read one latest observation after consent, freshness, and size checks."""

        self._count(self._OBSERVATION_REQUEST)
        maximum_age_ms = _require_bounded_integer(
            max_age_ms,
            "max_age_ms",
            minimum=0,
            maximum=self._limits.max_age_ms,
        )
        maximum_wait_ms = _require_bounded_integer(
            wait_ms,
            "wait_ms",
            minimum=0,
            maximum=self._limits.max_wait_ms,
        )
        accepted_schemas = _normalize_schema_ids(
            schema_ids,
            maximum=self._limits.max_schema_ids,
        )

        try:
            initial_now_unix_ns = _clock_value(self._unix_time_ns)
        except Exception:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentObservationResult("clock_unavailable")
        authorization = self._authorize(
            scope="observation.read",
            sensitivity="public",
            now_unix_ns=initial_now_unix_ns,
        )
        if not authorization.allowed:
            return self._observation_denied(authorization.reason)

        try:
            read = self._observations.read(
                now_monotonic_ns=_clock_value(self._monotonic_ns),
                max_age_ns=maximum_age_ms * 1_000_000,
                wait_seconds=maximum_wait_ms / 1_000,
                schema_ids=accepted_schemas,
            )
        except Exception:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentObservationResult("observation_invalid")
        if not isinstance(read, LatestObservationRead):
            self._count(self._DEPENDENCY_FAILURE)
            return AgentObservationResult("observation_invalid")
        if read.outcome != "ok":
            return AgentObservationResult(
                cast(AgentObservationOutcome, read.outcome),
                age_ns=read.age_ns,
            )
        observation = read.observation
        if observation is None:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentObservationResult("observation_invalid")

        reauthorization = self._reauthorize_same_grant(
            authorization.state,
            scope="observation.read",
            sensitivity=observation.sensitivity_class,
        )
        if not reauthorization.allowed:
            return self._observation_denied(reauthorization.reason)
        try:
            encoded = bounded_observation_bytes(
                observation,
                max_bytes=self._limits.max_observation_bytes,
            )
        except Exception:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentObservationResult("observation_invalid")
        if encoded is None:
            self._count(self._LIMIT_REJECTION)
            return AgentObservationResult("observation_too_large")
        self._count(self._OBSERVATION_DELIVERED)
        return AgentObservationResult("ok", observation_json=encoded, age_ns=read.age_ns)

    def snapshot(
        self,
        *,
        max_edge_px: int,
        wait_ms: int = 0,
    ) -> AgentSnapshotResult:
        """Reserve one attempt, then request at most one bounded encoded still image."""

        self._count(self._SNAPSHOT_REQUEST)
        maximum_edge_px = _require_bounded_integer(
            max_edge_px,
            "max_edge_px",
            minimum=1,
            maximum=self._limits.max_snapshot_edge_px,
        )
        maximum_wait_ms = _require_bounded_integer(
            wait_ms,
            "wait_ms",
            minimum=0,
            maximum=self._limits.max_wait_ms,
        )
        try:
            now_unix_ns = _clock_value(self._unix_time_ns)
        except Exception:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentSnapshotResult("clock_unavailable")
        try:
            reservation = self._consent.reserve_snapshot(
                sensitivity=self._snapshot_provider.sensitivity_class,
                now_unix_ns=now_unix_ns,
            )
        except Exception:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentSnapshotResult("consent_unavailable")
        if not isinstance(reservation, SnapshotReservation):
            self._count(self._DEPENDENCY_FAILURE)
            return AgentSnapshotResult("consent_unavailable")
        if not reservation.allowed:
            self._count(self._AUTHORIZATION_DENIAL)
            return AgentSnapshotResult(cast(AgentSnapshotOutcome, reservation.reason))
        reserved_state = reservation.state
        if reserved_state is None:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentSnapshotResult("consent_unavailable")
        self._count(self._SNAPSHOT_RESERVED)

        try:
            capture = self._snapshot_provider.capture(
                max_edge_px=maximum_edge_px,
                max_bytes=self._limits.max_snapshot_bytes,
                timeout_seconds=maximum_wait_ms / 1_000,
            )
        except Exception:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentSnapshotResult("provider_failure")
        if not isinstance(capture, SnapshotCaptureResult):
            self._count(self._DEPENDENCY_FAILURE)
            return AgentSnapshotResult("provider_invalid")
        if capture.outcome != "ok":
            if capture.outcome in {"unavailable", "failed"}:
                self._count(self._DEPENDENCY_FAILURE)
            return AgentSnapshotResult(
                cast(AgentSnapshotOutcome, capture.outcome),
                reason_code=capture.reason_code,
            )
        snapshot = capture.snapshot
        if snapshot is None:
            self._count(self._DEPENDENCY_FAILURE)
            return AgentSnapshotResult("provider_invalid")

        reauthorization = self._reauthorize_same_grant(
            reserved_state,
            scope="snapshot.read",
            sensitivity=snapshot.sensitivity_class,
        )
        if not reauthorization.allowed:
            return self._snapshot_denied(reauthorization.reason)
        if (
            snapshot.width > maximum_edge_px
            or snapshot.height > maximum_edge_px
            or snapshot.width > self._limits.max_snapshot_edge_px
            or snapshot.height > self._limits.max_snapshot_edge_px
            or snapshot.encoded_bytes > self._limits.max_snapshot_bytes
        ):
            self._count(self._LIMIT_REJECTION)
            return AgentSnapshotResult("snapshot_limit_exceeded")
        self._count(self._SNAPSHOT_DELIVERED)
        return AgentSnapshotResult("ok", snapshot=snapshot)

    def _authorize(
        self,
        *,
        scope: AgentScope,
        sensitivity: SensitivityClass,
        now_unix_ns: int,
    ) -> _AuthorizationCheck:
        try:
            state = self._consent.load()
            if state is not None and not isinstance(state, ConsentState):
                return _AuthorizationCheck("consent_unavailable", None)
            decision = evaluate_grant(
                None if state is None else state.grant,
                scope=scope,
                sensitivity=sensitivity,
                now_unix_ns=now_unix_ns,
                snapshots_used=0 if state is None else state.snapshot_attempts,
            )
        except Exception:
            return _AuthorizationCheck("consent_unavailable", None)
        return _AuthorizationCheck(decision.reason, state)

    def _reauthorize_same_grant(
        self,
        initial_state: ConsentState | None,
        *,
        scope: AgentScope,
        sensitivity: SensitivityClass,
    ) -> _AuthorizationCheck:
        if initial_state is None:
            return _AuthorizationCheck("consent_unavailable", None)
        try:
            now_unix_ns = _clock_value(self._unix_time_ns)
        except Exception:
            return _AuthorizationCheck("clock_unavailable", None)
        try:
            current_state = self._consent.load()
            if current_state is not None and not isinstance(current_state, ConsentState):
                return _AuthorizationCheck("consent_unavailable", None)
        except Exception:
            return _AuthorizationCheck("consent_unavailable", None)
        if current_state is None:
            return _AuthorizationCheck("grant_missing", None)
        if current_state.grant.public_id != initial_state.grant.public_id:
            return _AuthorizationCheck("grant_changed", current_state)
        try:
            decision = evaluate_grant(
                current_state.grant,
                scope=scope,
                sensitivity=sensitivity,
                now_unix_ns=now_unix_ns,
                snapshots_used=0,
            )
        except Exception:
            return _AuthorizationCheck("consent_unavailable", None)
        return _AuthorizationCheck(decision.reason, current_state)

    def _observation_denied(
        self,
        reason: _AuthorizationReason,
    ) -> AgentObservationResult:
        if reason in {"clock_unavailable", "consent_unavailable"}:
            self._count(self._DEPENDENCY_FAILURE)
        else:
            self._count(self._AUTHORIZATION_DENIAL)
        return AgentObservationResult(cast(AgentObservationOutcome, reason))

    def _snapshot_denied(self, reason: _AuthorizationReason) -> AgentSnapshotResult:
        if reason in {"clock_unavailable", "consent_unavailable"}:
            self._count(self._DEPENDENCY_FAILURE)
        else:
            self._count(self._AUTHORIZATION_DENIAL)
        return AgentSnapshotResult(cast(AgentSnapshotOutcome, reason))

    def _count(self, index: int) -> None:
        with self._metrics_lock:
            current = self._metrics_counts[index]
            if current < _MAX_INT64:
                self._metrics_counts[index] = current + 1


__all__ = [
    "AGENT_READ_CONTRACT",
    "AgentObservationOutcome",
    "AgentObservationResult",
    "AgentReadService",
    "AgentServiceMetrics",
    "AgentSnapshotOutcome",
    "AgentSnapshotResult",
    "AgentStatusOutcome",
    "AgentStatusResult",
    "SnapshotCaptureOutcome",
    "SnapshotCaptureResult",
    "SnapshotProvider",
    "SnapshotReasonCode",
    "StatusProvider",
    "normalize_snapshot_reason",
    "normalize_snapshot_reason_for_outcome",
]
