from __future__ import annotations

import errno
import json
import os
import subprocess
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path
from threading import Barrier, Lock, Thread

import pytest

from viskium.agent import consent as consent_module
from viskium.agent.consent import (
    ConsentLedger,
    ConsentLedgerError,
    ConsentState,
    SnapshotReservation,
)
from viskium.storage import StorageLayoutError, initialize_data_root


def _ledger(tmp_path: Path) -> ConsentLedger:
    return ConsentLedger(initialize_data_root(tmp_path / "data"))


def _grant(ledger: ConsentLedger, **overrides: object) -> ConsentState:
    values: dict[str, object] = {
        "scopes": frozenset({"observation.read", "snapshot.read"}),
        "duration_seconds": 60,
        "snapshot_quota": 2,
        "sensitivity_ceiling": "identifiable",
        "now_unix_ns": 1_000,
    }
    return ledger.grant(**(values | overrides))  # type: ignore[arg-type]


def test_grant_is_atomic_strict_and_contains_no_secret(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)

    state = _grant(ledger)

    assert ledger.load() == state
    document = json.loads(ledger.path.read_text(encoding="utf-8"))
    assert document == state.to_public_dict() | {
        "kind": "viskium.agent-consent",
        "schema_version": 1,
    }
    assert all("token" not in key and "secret" not in key for key in document)
    assert not tuple(ledger.path.parent.glob(".agent-consent.json.*.tmp"))
    lock_path = ledger.path.parents[1] / "locks" / "agent-consent.lock"
    assert lock_path.is_file()
    assert lock_path.read_bytes() == b"\0"
    if os.name != "nt":
        assert lock_path.stat().st_mode & 0o777 == 0o600
    assert ledger.path.stat().st_size <= consent_module.MAX_CONSENT_FILE_BYTES
    if os.name != "nt":
        assert ledger.path.stat().st_mode & 0o777 == 0o600


def test_new_grant_replaces_identity_and_resets_usage(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    first = _grant(ledger)
    assert ledger.reserve_snapshot(sensitivity="identifiable", now_unix_ns=2_000).allowed

    second = _grant(ledger, now_unix_ns=3_000)

    assert second.grant.public_id != first.grant.public_id
    assert second.snapshot_attempts == 0
    assert second.generation == 1
    assert ledger.load() == second


def test_snapshot_reservation_is_atomic_bounded_and_persistent(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    initial = _grant(ledger, snapshot_quota=2)

    first = ledger.reserve_snapshot(sensitivity="identifiable", now_unix_ns=2_000)
    second = ledger.reserve_snapshot(sensitivity="identifiable", now_unix_ns=2_000)
    denied = ledger.reserve_snapshot(sensitivity="identifiable", now_unix_ns=2_000)

    assert first == SnapshotReservation(True, "allowed", first.state)
    assert second.allowed
    assert denied.reason == "snapshot_quota_exhausted"
    assert denied.state is not None
    assert denied.state.snapshot_attempts == 2
    assert denied.state.generation == initial.generation + 2
    assert ledger.load() == denied.state


def test_snapshot_reservation_denies_declared_sensitivity_before_consuming_quota(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    initial = _grant(ledger, sensitivity_ceiling="public")
    before = ledger.path.read_bytes()

    denied = ledger.reserve_snapshot(sensitivity="identifiable", now_unix_ns=2_000)

    assert denied.reason == "sensitivity_denied"
    assert denied.state == initial
    assert ledger.path.read_bytes() == before


def test_missing_expired_and_scope_missing_fail_without_writes(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    assert ledger.reserve_snapshot(sensitivity="public", now_unix_ns=1).reason == "grant_missing"
    assert not ledger.path.exists()

    observation_only = _grant(
        ledger,
        scopes=frozenset({"observation.read"}),
        snapshot_quota=0,
    )
    before = ledger.path.read_bytes()
    assert (
        ledger.reserve_snapshot(sensitivity="public", now_unix_ns=2_000).reason == "scope_missing"
    )
    assert ledger.load() == observation_only
    assert ledger.path.read_bytes() == before

    _grant(ledger, duration_seconds=1, now_unix_ns=1_000)
    before = ledger.path.read_bytes()
    assert (
        ledger.reserve_snapshot(sensitivity="public", now_unix_ns=1_000_001_000).reason
        == "grant_expired"
    )
    assert ledger.path.read_bytes() == before


def test_concurrent_reservations_never_exceed_quota(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _grant(ledger, snapshot_quota=4)
    barrier = Barrier(9)
    result_lock = Lock()
    outcomes: list[bool] = []

    def reserve() -> None:
        barrier.wait(timeout=2.0)
        result = ledger.reserve_snapshot(sensitivity="identifiable", now_unix_ns=2_000)
        with result_lock:
            outcomes.append(result.allowed)

    workers = [Thread(target=reserve) for _ in range(8)]
    for worker in workers:
        worker.start()
    barrier.wait(timeout=2.0)
    for worker in workers:
        worker.join(timeout=2.0)

    assert all(not worker.is_alive() for worker in workers)
    assert outcomes.count(True) == 4
    assert outcomes.count(False) == 4
    state = ledger.load()
    assert state is not None
    assert state.snapshot_attempts == 4


def test_revoke_is_exact_and_idempotent(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _grant(ledger)
    unrelated = ledger.path.parent / "unrelated.txt"
    unrelated.write_text("keep", encoding="utf-8")

    assert ledger.revoke()
    assert not ledger.path.exists()
    assert unrelated.read_text(encoding="utf-8") == "keep"
    assert not ledger.revoke()


def test_mutations_sync_parent_directory_after_entry_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = _ledger(tmp_path)
    synced: list[Path] = []
    monkeypatch.setattr(consent_module, "_sync_parent_directory", synced.append)

    _grant(ledger)
    assert ledger.revoke()

    assert synced == [ledger.path, ledger.path]


def test_parent_directory_sync_uses_directory_fsync_on_posix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: list[tuple[Path, int]] = []
    synced: list[int] = []
    closed: list[int] = []

    def fake_open(path: Path, flags: int) -> int:
        opened.append((path, flags))
        return 37

    monkeypatch.setattr(consent_module.os, "name", "posix")
    monkeypatch.setattr(consent_module.os, "open", fake_open)
    monkeypatch.setattr(consent_module.os, "fsync", synced.append)
    monkeypatch.setattr(consent_module.os, "close", closed.append)

    consent_module._sync_parent_directory(tmp_path / "state.json")

    assert opened == [
        (tmp_path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)),
    ]
    assert synced == [37]
    assert closed == [37]


def test_parent_directory_sync_is_noop_on_windows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_open(*_args: object) -> int:
        raise AssertionError("Windows must not open a directory for fsync")

    monkeypatch.setattr(consent_module.os, "name", "nt")
    monkeypatch.setattr(consent_module.os, "open", forbidden_open)

    consent_module._sync_parent_directory(tmp_path / "state.json")


def test_parent_directory_sync_ignores_unsupported_posix_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(consent_module.os, "name", "posix")

    def unsupported_open(*_args: object) -> int:
        raise OSError(errno.EINVAL, "directory fsync unsupported")

    monkeypatch.setattr(consent_module.os, "open", unsupported_open)

    consent_module._sync_parent_directory(tmp_path / "state.json")


def test_parent_directory_sync_ignores_unsupported_posix_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(consent_module.os, "name", "posix")
    monkeypatch.setattr(consent_module.os, "open", lambda *_args: 37)

    def unsupported_fsync(_descriptor: int) -> None:
        raise OSError(errno.ENOTSUP, "directory fsync unsupported")

    monkeypatch.setattr(consent_module.os, "fsync", unsupported_fsync)
    monkeypatch.setattr(consent_module.os, "close", lambda _descriptor: None)

    consent_module._sync_parent_directory(tmp_path / "state.json")


def test_parent_directory_sync_propagates_unexpected_posix_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(consent_module.os, "name", "posix")
    monkeypatch.setattr(consent_module.os, "open", lambda *_args: 37)

    def fail_fsync(_descriptor: int) -> None:
        raise OSError(errno.EIO, "injected")

    monkeypatch.setattr(consent_module.os, "fsync", fail_fsync)
    monkeypatch.setattr(consent_module.os, "close", lambda _descriptor: None)

    with pytest.raises(OSError, match="injected"):
        consent_module._sync_parent_directory(tmp_path / "state.json")


@pytest.mark.parametrize(
    ("overrides", "error_type", "message"),
    [
        ({"duration_seconds": 0}, ValueError, "duration_seconds"),
        ({"duration_seconds": 604_801}, ValueError, "duration_seconds"),
        ({"duration_seconds": True}, TypeError, "duration_seconds"),
        ({"snapshot_quota": -1}, ValueError, "snapshot_quota"),
        ({"snapshot_quota": 1_025}, ValueError, "snapshot_quota"),
        ({"now_unix_ns": -1}, ValueError, "now_unix_ns"),
        ({"sensitivity_ceiling": "prohibited"}, ValueError, "prohibited"),
        ({"scopes": frozenset({"camera.open"})}, ValueError, "scope"),
    ],
)
def test_grant_rejects_unbounded_or_unsupported_values(
    tmp_path: Path,
    overrides: dict[str, object],
    error_type: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error_type, match=message):
        _grant(_ledger(tmp_path), **overrides)


def test_corrupt_duplicate_oversized_and_unknown_documents_fail_closed(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _grant(ledger)

    ledger.path.write_text('{"kind":"a","kind":"b"}', encoding="utf-8")
    with pytest.raises(ConsentLedgerError, match="duplicate"):
        ledger.load()

    ledger.path.write_bytes(b"x" * (consent_module.MAX_CONSENT_FILE_BYTES + 1))
    with pytest.raises(ConsentLedgerError, match="byte limit"):
        ledger.load()

    ledger.path.write_text(json.dumps({"kind": "viskium.agent-consent"}), encoding="utf-8")
    with pytest.raises(ConsentLedgerError, match="unsupported fields"):
        ledger.load()


def test_malformed_values_and_non_file_are_rejected(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    state = _grant(ledger)
    document = state.to_public_dict() | {
        "kind": "viskium.agent-consent",
        "schema_version": 1,
    }
    document["snapshot_attempts"] = 3
    ledger.path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ConsentLedgerError, match="values"):
        ledger.load()

    ledger.path.unlink()
    ledger.path.mkdir()
    with pytest.raises(ConsentLedgerError, match="regular local file"):
        ledger.load()


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("snapshot_quota", consent_module.MAX_SNAPSHOT_QUOTA + 1),
        ("generation", 2),
        ("expires_unix_ns", 1_000),
        (
            "expires_unix_ns",
            1_000 + consent_module.MAX_CONSENT_DURATION_SECONDS * 1_000_000_000 + 1,
        ),
    ],
)
def test_loaded_consent_reapplies_all_persisted_state_ceilings(
    tmp_path: Path,
    field: str,
    replacement: int,
) -> None:
    ledger = _ledger(tmp_path)
    state = _grant(ledger)
    document = state.to_public_dict() | {
        "kind": "viskium.agent-consent",
        "schema_version": 1,
    }
    document[field] = replacement
    ledger.path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ConsentLedgerError, match="values"):
        ledger.load()


def test_atomic_replace_failure_preserves_previous_grant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = _ledger(tmp_path)
    first = _grant(ledger)

    def fail_replace(_source: Path, _destination: Path) -> None:
        raise OSError("injected")

    monkeypatch.setattr(consent_module.os, "replace", fail_replace)
    with pytest.raises(ConsentLedgerError, match="atomically"):
        _grant(ledger, now_unix_ns=2_000)

    assert ledger.load() == first
    assert not tuple(ledger.path.parent.glob(".agent-consent.json.*.tmp"))


def test_mutation_lock_is_reclaimed_after_contending_process_terminates(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    first = _grant(ledger)
    lock_path = ledger.path.parents[1] / "locks" / "agent-consent.lock"
    child_code = (
        "from viskium.core.file_lock import FileLockLease; "
        "import sys, time; "
        "lease = FileLockLease(sys.argv[1]); "
        "print(int(lease.acquire()), flush=True); time.sleep(30)"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", child_code, str(lock_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "1"
        with pytest.raises(ConsentLedgerError, match="lock is unavailable"):
            ledger.revoke()
        assert ledger.load() == first
        child.terminate()
        child.wait(timeout=5)
        assert ledger.revoke()
        assert not ledger.path.exists()
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=5)


def test_state_and_reservation_are_frozen_and_validate_invariants(tmp_path: Path) -> None:
    state = _grant(_ledger(tmp_path))
    with pytest.raises(FrozenInstanceError):
        state.generation = 2  # type: ignore[misc]
    with pytest.raises(ValueError, match="exceed"):
        ConsentState(state.grant, state.created_unix_ns, 3, state.generation)
    with pytest.raises(ValueError, match="match"):
        SnapshotReservation(True, "grant_missing", state)
    with pytest.raises(ValueError, match="requires"):
        SnapshotReservation(True, "allowed", None)


def test_ledger_requires_a_verified_unchanged_data_root(tmp_path: Path) -> None:
    with pytest.raises(StorageLayoutError, match=r"layout|directory|marker"):
        ConsentLedger(tmp_path / "missing")

    layout = initialize_data_root(tmp_path / "data")
    (layout.root / "state").rmdir()
    with pytest.raises(StorageLayoutError, match="layout path"):
        ConsentLedger(layout)
