"""Explicit, bounded, local consent state for the agent-read boundary."""

from __future__ import annotations

import errno
import json
import os
import stat
import threading
import time
import uuid
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from viskium.agent.contracts import (
    AgentScope,
    ConsentGrant,
    GrantDecisionReason,
    evaluate_grant,
)
from viskium.core.contracts import SensitivityClass
from viskium.core.file_lock import FileLockLease
from viskium.storage import DataRootLayout, verify_data_root

CONSENT_FILE_NAME = "agent-consent.json"
CONSENT_SCHEMA_VERSION = 1
MAX_CONSENT_FILE_BYTES = 16_384
MAX_CONSENT_DURATION_SECONDS = 7 * 86_400
MAX_SNAPSHOT_QUOTA = 1_024
_MAX_INT64 = 2**63 - 1
_KIND = "viskium.agent-consent"
_LOCK_FILE_NAME = "agent-consent.lock"
_UNSUPPORTED_DIRECTORY_SYNC_ERRNOS = frozenset(
    getattr(errno, name) for name in ("EINVAL", "ENOTSUP", "EOPNOTSUPP") if hasattr(errno, name)
)


class ConsentLedgerError(RuntimeError):
    """Raised when consent state cannot be validated or changed safely."""


@dataclass(frozen=True, slots=True)
class ConsentState:
    """Validated grant plus its bounded local usage counters."""

    grant: ConsentGrant
    created_unix_ns: int
    snapshot_attempts: int
    generation: int

    def __post_init__(self) -> None:
        for field_name in ("created_unix_ns", "snapshot_attempts", "generation"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{field_name} must be an integer")
            if not 0 <= value <= _MAX_INT64:
                raise ValueError(f"{field_name} must fit in non-negative signed int64")
        if self.snapshot_attempts > self.grant.snapshot_quota:
            raise ValueError("snapshot_attempts cannot exceed snapshot_quota")
        if self.grant.snapshot_quota > MAX_SNAPSHOT_QUOTA:
            raise ValueError("snapshot_quota exceeds the persisted-state ceiling")
        duration_ns = self.grant.expires_unix_ns - self.created_unix_ns
        if not 1 <= duration_ns <= MAX_CONSENT_DURATION_SECONDS * 1_000_000_000:
            raise ValueError("persisted consent duration is outside its ceiling")
        if self.generation != self.snapshot_attempts + 1:
            raise ValueError("generation must match the bounded usage sequence")

    def to_public_dict(self) -> dict[str, Any]:
        return self.grant.to_public_dict() | {
            "created_unix_ns": self.created_unix_ns,
            "snapshot_attempts": self.snapshot_attempts,
            "generation": self.generation,
        }


@dataclass(frozen=True, slots=True)
class SnapshotReservation:
    """Atomic quota decision made before one hardware snapshot attempt."""

    allowed: bool
    reason: GrantDecisionReason
    state: ConsentState | None

    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool):
            raise TypeError("allowed must be a boolean")
        if self.allowed != (self.reason == "allowed"):
            raise ValueError("allowed must match the reservation reason")
        if self.allowed and self.state is None:
            raise ValueError("an allowed reservation requires consent state")


def _bounded_integer(
    value: object,
    field_name: str,
    *,
    minimum: int = 0,
    maximum: int = _MAX_INT64,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
    return value


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ConsentLedgerError(f"duplicate consent field: {key}")
        result[key] = value
    return result


def _is_link_or_reparse(path: Path) -> bool:
    metadata = path.lstat()
    attributes = getattr(metadata, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return stat.S_ISLNK(metadata.st_mode) or bool(attributes & reparse_flag)


def _sync_parent_directory(path: Path) -> None:
    """Durably publish a directory entry where the platform supports it."""

    if os.name == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path.parent, flags)
    except OSError as error:
        if error.errno in _UNSUPPORTED_DIRECTORY_SYNC_ERRNOS:
            return
        raise
    try:
        try:
            os.fsync(descriptor)
        except OSError as error:
            if error.errno in _UNSUPPORTED_DIRECTORY_SYNC_ERRNOS:
                return
            raise
    finally:
        os.close(descriptor)


class ConsentLedger:
    """Read and mutate consent only inside a verified Viskium data root.

    The file contains no bearer token or other credential.  It is an explicit
    same-user consent switch.  Mutations use atomic replacement plus a bounded
    cross-process advisory lock; a crashed writer releases it automatically
    while the sentinel remains as a stable rendezvous point.
    """

    def __init__(self, layout: DataRootLayout | str | os.PathLike[str]) -> None:
        selected = layout if isinstance(layout, DataRootLayout) else verify_data_root(layout)
        if verify_data_root(selected.root) != selected:
            raise ConsentLedgerError("data-root identity changed during consent setup")
        self._layout = selected
        self._path = selected.category("state") / CONSENT_FILE_NAME
        self._lock_path = selected.category("locks") / _LOCK_FILE_NAME
        self._thread_lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> ConsentState | None:
        """Read current consent without creating, repairing, or expiring it."""

        try:
            metadata = self._path.lstat()
        except FileNotFoundError:
            return None
        except OSError as error:
            raise ConsentLedgerError("consent state cannot be inspected") from error
        if _is_link_or_reparse(self._path) or not stat.S_ISREG(metadata.st_mode):
            raise ConsentLedgerError("consent state must be a regular local file")
        if metadata.st_size > MAX_CONSENT_FILE_BYTES:
            raise ConsentLedgerError("consent state exceeds its byte limit")
        try:
            raw = self._path.read_bytes()
            document = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
        except ConsentLedgerError:
            raise
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ConsentLedgerError("consent state cannot be decoded") from error
        return self._decode(document)

    def grant(
        self,
        *,
        scopes: frozenset[AgentScope],
        duration_seconds: int,
        snapshot_quota: int,
        sensitivity_ceiling: SensitivityClass,
        now_unix_ns: int | None = None,
    ) -> ConsentState:
        """Replace consent explicitly with a new, independently identified grant."""

        duration = _bounded_integer(
            duration_seconds,
            "duration_seconds",
            minimum=1,
            maximum=MAX_CONSENT_DURATION_SECONDS,
        )
        quota = _bounded_integer(
            snapshot_quota,
            "snapshot_quota",
            maximum=MAX_SNAPSHOT_QUOTA,
        )
        now = time.time_ns() if now_unix_ns is None else now_unix_ns
        created_ns = _bounded_integer(now, "now_unix_ns")
        duration_ns = duration * 1_000_000_000
        expires_ns = _bounded_integer(
            created_ns + duration_ns,
            "expires_unix_ns",
            minimum=created_ns + 1,
        )
        grant = ConsentGrant(
            public_id=str(uuid.uuid4()),
            scopes=scopes,
            expires_unix_ns=expires_ns,
            snapshot_quota=quota,
            sensitivity_ceiling=sensitivity_ceiling,
        )
        state = ConsentState(
            grant=grant,
            created_unix_ns=created_ns,
            snapshot_attempts=0,
            generation=1,
        )
        with self._mutation_lock():
            self._write(state)
        return state

    def revoke(self) -> bool:
        """Remove the exact consent file; no observation or other state is touched."""

        with self._mutation_lock():
            try:
                if _is_link_or_reparse(self._path):
                    raise ConsentLedgerError("refusing to revoke linked consent state")
                self._path.unlink()
                _sync_parent_directory(self._path)
            except FileNotFoundError:
                return False
            except OSError as error:
                raise ConsentLedgerError("consent state could not be revoked") from error
            return True

    def reserve_snapshot(
        self,
        *,
        sensitivity: SensitivityClass,
        now_unix_ns: int | None = None,
    ) -> SnapshotReservation:
        """Authorize declared sensitivity and atomically consume one attempt."""

        now = time.time_ns() if now_unix_ns is None else now_unix_ns
        checked_now = _bounded_integer(now, "now_unix_ns")
        with self._mutation_lock():
            state = self.load()
            grant = None if state is None else state.grant
            attempts = 0 if state is None else state.snapshot_attempts
            decision = evaluate_grant(
                grant,
                scope="snapshot.read",
                sensitivity=sensitivity,
                now_unix_ns=checked_now,
                snapshots_used=attempts,
            )
            if not decision.allowed or state is None:
                return SnapshotReservation(False, decision.reason, state)
            updated = ConsentState(
                grant=state.grant,
                created_unix_ns=state.created_unix_ns,
                snapshot_attempts=state.snapshot_attempts + 1,
                generation=state.generation + 1,
            )
            self._write(updated)
            return SnapshotReservation(True, "allowed", updated)

    @contextmanager
    def _mutation_lock(self) -> Any:
        with self._thread_lock:
            lease = FileLockLease(self._lock_path)
            deadline = time.monotonic() + 0.25
            while True:
                if lease.acquire():
                    break
                if time.monotonic() >= deadline:
                    raise ConsentLedgerError("consent mutation lock is unavailable") from None
                time.sleep(0.005)
            try:
                yield
            finally:
                try:
                    lease.release()
                except OSError as error:
                    raise ConsentLedgerError("consent mutation lock cannot be released") from error

    def _write(self, state: ConsentState) -> None:
        payload = (
            json.dumps(
                {
                    "created_unix_ns": state.created_unix_ns,
                    "expires_unix_ns": state.grant.expires_unix_ns,
                    "generation": state.generation,
                    "kind": _KIND,
                    "public_id": state.grant.public_id,
                    "schema_version": CONSENT_SCHEMA_VERSION,
                    "scopes": sorted(state.grant.scopes),
                    "sensitivity_ceiling": state.grant.sensitivity_ceiling,
                    "snapshot_attempts": state.snapshot_attempts,
                    "snapshot_quota": state.grant.snapshot_quota,
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        if len(payload) > MAX_CONSENT_FILE_BYTES:
            raise ConsentLedgerError("encoded consent state exceeds its byte limit")
        temporary = self._path.with_name(f".{CONSENT_FILE_NAME}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("xb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            with suppress(OSError):
                os.chmod(temporary, 0o600)
            if self._path.exists() and _is_link_or_reparse(self._path):
                raise ConsentLedgerError("refusing to replace linked consent state")
            os.replace(temporary, self._path)
            _sync_parent_directory(self._path)
        except ConsentLedgerError:
            raise
        except OSError as error:
            raise ConsentLedgerError("consent state could not be written atomically") from error
        finally:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)

    @staticmethod
    def _decode(document: object) -> ConsentState:
        expected = {
            "created_unix_ns",
            "expires_unix_ns",
            "generation",
            "kind",
            "public_id",
            "schema_version",
            "scopes",
            "sensitivity_ceiling",
            "snapshot_attempts",
            "snapshot_quota",
        }
        if not isinstance(document, dict) or set(document) != expected:
            raise ConsentLedgerError("consent state has unsupported fields")
        if document["kind"] != _KIND or document["schema_version"] != CONSENT_SCHEMA_VERSION:
            raise ConsentLedgerError("consent state kind or schema is unsupported")
        scopes_value = document["scopes"]
        if not isinstance(scopes_value, list) or len(scopes_value) > 2:
            raise ConsentLedgerError("consent scopes are malformed")
        try:
            grant = ConsentGrant(
                public_id=document["public_id"],
                scopes=frozenset(cast(list[AgentScope], scopes_value)),
                expires_unix_ns=document["expires_unix_ns"],
                snapshot_quota=document["snapshot_quota"],
                sensitivity_ceiling=cast(SensitivityClass, document["sensitivity_ceiling"]),
            )
            return ConsentState(
                grant=grant,
                created_unix_ns=document["created_unix_ns"],
                snapshot_attempts=document["snapshot_attempts"],
                generation=document["generation"],
            )
        except (TypeError, ValueError) as error:
            raise ConsentLedgerError("consent state values are invalid") from error


__all__ = [
    "CONSENT_FILE_NAME",
    "CONSENT_SCHEMA_VERSION",
    "MAX_CONSENT_DURATION_SECONDS",
    "MAX_CONSENT_FILE_BYTES",
    "MAX_SNAPSHOT_QUOTA",
    "ConsentLedger",
    "ConsentLedgerError",
    "ConsentState",
    "SnapshotReservation",
]
