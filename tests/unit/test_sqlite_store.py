from __future__ import annotations

import sqlite3
import threading
from dataclasses import replace
from pathlib import Path

import pytest

from viskium.core import ObservationEnvelope, ObservationStore
from viskium.core.serialization import bounded_observation_bytes
from viskium.storage import sqlite_store as sqlite_store_module
from viskium.storage.sqlite_store import (
    SQLiteStore,
    SQLiteStoreError,
    SQLiteStoreIntegrityError,
    SQLiteStoreReadOnlyError,
)


def _observation(sequence: int = 0, **changes: object) -> ObservationEnvelope:
    values: dict[str, object] = {
        "session_id": "session-a",
        "source_id": "source-a",
        "stream_epoch": 0,
        "source_sequence": sequence,
        "observed_monotonic_ns": 1_000 + sequence,
        "producer_id": "processor",
        "producer_version": "1",
        "schema_id": "viskium.test-observation",
        "schema_version": 1,
        "payload": {"sequence": sequence, "nested": [True, None]},
        "idempotency_key": f"observation:{sequence}",
        "trace_id": f"trace:{sequence}",
        "confidence": 1.0,
        "provenance": (f"frame:{sequence}",),
        "sensitivity_class": "operational",
        "persistence_class": "routine",
        "ttl_ns": 100,
        "wall_utc": None,
    }
    values.update(changes)
    return ObservationEnvelope(**values)  # type: ignore[arg-type]


def _store(path: Path, **kwargs: object) -> SQLiteStore:
    return SQLiteStore(path, volume_reserve_bytes=0, **kwargs)  # type: ignore[arg-type]


def test_store_applies_connection_policy_and_observation_contract(tmp_path: Path) -> None:
    store = _store(tmp_path / "observations.sqlite3")

    assert isinstance(store, ObservationStore)
    assert store.health().journal_mode == "delete"
    assert store._execute("PRAGMA synchronous").fetchone()[0] == 1
    assert store._execute("PRAGMA temp_store").fetchone()[0] == 2
    assert store._execute("PRAGMA trusted_schema").fetchone()[0] == 0
    assert store._execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert store._execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert not Path(f"{store.path}-wal").exists()
    store.close()


def test_put_uses_shared_canonical_bytes_and_global_idempotency(tmp_path: Path) -> None:
    store = _store(tmp_path / "observations.sqlite3", now_unix_ns=lambda: 10)
    first = _observation()
    expected = bounded_observation_bytes(first, max_bytes=65_536)
    assert expected is not None

    accepted = store.put(first)
    duplicate = store.put(first)
    conflict = store.put(replace(first, session_id="different-session"))

    assert accepted.status == "accepted"
    assert accepted.bytes_accepted == len(expected)
    assert duplicate.status == "coalesced"
    assert duplicate.store_sequence == accepted.store_sequence
    assert conflict.status == "rejected"
    assert conflict.reason == "idempotency_conflict"
    assert store.health().row_count == 1
    store.close()


def test_latest_is_bounded_ordered_and_ttl_aware(tmp_path: Path) -> None:
    now = [1_000]
    store = _store(
        tmp_path / "observations.sqlite3",
        now_unix_ns=lambda: now[0],
        max_query_bytes=65_536,
    )
    first = _observation(0, ttl_ns=100)
    second = _observation(1, ttl_ns=200)
    assert store.put(first).accepted
    assert store.put(second).accepted

    latest = store.query_latest(limit=2, now_unix_ns=1_050)
    assert [item.observation.source_sequence for item in latest] == [1, 0]
    assert store.query_latest(limit=2, max_bytes=1, now_unix_ns=1_050) == ()
    assert [
        item.observation.source_sequence for item in store.query_latest(limit=2, now_unix_ns=1_100)
    ] == [1]

    report = store.purge_expired(now_unix_ns=1_100)
    assert report.rows_deleted == 1
    assert report.logical_bytes_deleted > 0
    assert store.health().row_count == 1
    store.close()


def test_purge_is_explicit_bounded_and_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "observations.sqlite3"
    store = _store(path, now_unix_ns=lambda: 10, max_purge_rows=2)
    assert store.put(_observation(0, ttl_ns=1)).accepted
    assert store.put(_observation(1, ttl_ns=1)).accepted
    assert store.put(_observation(2, ttl_ns=100)).accepted
    with pytest.raises(ValueError, match="max_purge_rows"):
        store.purge_expired(limit=3, now_unix_ns=20)
    assert store.purge_expired(limit=1, now_unix_ns=20).rows_deleted == 1
    store.close()

    reopened = _store(path, now_unix_ns=lambda: 20)
    assert reopened.health().row_count == 2
    assert reopened.purge_expired(now_unix_ns=20).rows_deleted == 1
    assert [item.observation.source_sequence for item in reopened.query_latest(limit=3)] == [2]
    reopened.close()


def test_put_reclaims_only_expired_rows_when_count_limit_is_reached(tmp_path: Path) -> None:
    now = [10]
    store = _store(
        tmp_path / "pressure.sqlite3",
        max_rows=2,
        max_purge_rows=1,
        now_unix_ns=lambda: now[0],
    )
    assert store.put(_observation(0, ttl_ns=1)).accepted
    assert store.put(_observation(1, ttl_ns=100)).accepted

    now[0] = 20
    accepted = store.put(_observation(2, ttl_ns=100))

    assert accepted.accepted
    assert store.health().row_count == 2
    assert [item.observation.source_sequence for item in store.query_latest(limit=2)] == [2, 1]
    store.close()


def test_put_reclaims_expired_rows_before_the_logical_byte_ceiling(tmp_path: Path) -> None:
    now = [10]
    expired = _observation(0, ttl_ns=1, payload={"text": "a"})
    replacement = _observation(1, ttl_ns=100, payload={"text": "b" * 128})
    replacement_bytes = bounded_observation_bytes(replacement, max_bytes=65_536)
    assert replacement_bytes is not None
    store = _store(
        tmp_path / "logical-pressure.sqlite3",
        max_logical_bytes=len(replacement_bytes) + 8,
        now_unix_ns=lambda: now[0],
    )
    assert store.put(expired).accepted

    now[0] = 20
    assert store.put(replacement).accepted
    health = store.health()
    assert health.row_count == 1
    assert health.logical_bytes == len(replacement_bytes)
    assert health.max_logical_bytes == len(replacement_bytes) + 8
    store.close()


def test_logical_byte_ceiling_never_purges_fresh_rows(tmp_path: Path) -> None:
    now = 10
    first = _observation(0, ttl_ns=100, payload={"text": "a"})
    second = _observation(1, ttl_ns=100, payload={"text": "b" * 128})
    second_bytes = bounded_observation_bytes(second, max_bytes=65_536)
    assert second_bytes is not None
    store = _store(
        tmp_path / "logical-fresh.sqlite3",
        max_logical_bytes=len(second_bytes) + 8,
        now_unix_ns=lambda: now,
    )
    assert store.put(first).accepted

    receipt = store.put(second)

    assert receipt.status == "rejected"
    assert receipt.reason == "logical_byte_limit_reached"
    assert store.health().row_count == 1
    assert store.query_latest(limit=2) != ()
    store.close()


def test_row_size_reserve_and_int64_limits_reject_without_partial_rows(tmp_path: Path) -> None:
    count_store = _store(
        tmp_path / "count.sqlite3",
        max_rows=1,
        now_unix_ns=lambda: 10,
    )
    assert count_store.put(_observation(0)).accepted
    assert count_store.put(_observation(1)).reason == "count_limit_reached"
    assert count_store.health().row_count == 1
    count_store.close()

    size_store = _store(tmp_path / "size.sqlite3", max_observation_bytes=128)
    assert size_store.put(_observation()).reason == "observation_exceeds_byte_limit"
    assert size_store.health().row_count == 0
    size_store.close()

    reserve_store = SQLiteStore(
        tmp_path / "reserve.sqlite3",
        volume_reserve_bytes=100,
        free_bytes=lambda _: 100,
    )
    assert reserve_store.put(_observation()).reason == "volume_reserve_reached"
    assert reserve_store.health().row_count == 0
    reserve_store.close()

    integer_store = _store(tmp_path / "integer.sqlite3")
    oversized = _observation()
    object.__setattr__(oversized, "observed_monotonic_ns", 2**63)
    assert integer_store.put(oversized).reason == "integer_out_of_range"
    assert integer_store.health().row_count == 0
    integer_store.close()

    with pytest.raises(ValueError, match="max_logical_bytes"):
        _store(
            tmp_path / "invalid-logical-limit.sqlite3",
            max_db_bytes=64 * 1_024,
            max_logical_bytes=64 * 1_024 + 1,
        )


def test_policy_rejects_missing_ttl_prohibited_and_visual_content(tmp_path: Path) -> None:
    store = _store(tmp_path / "observations.sqlite3")

    assert store.put(replace(_observation(), ttl_ns=None)).reason == "ttl_required"
    assert (
        store.put(replace(_observation(), sensitivity_class="prohibited")).reason
        == "prohibited_content"
    )
    assert (
        store.put(replace(_observation(), persistence_class="visual")).reason
        == "visual_persistence_disabled"
    )
    assert store.health().row_count == 0
    store.close()


def test_put_handles_non_observations_and_serializer_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path / "invalid-input.sqlite3")
    invalid_input = store.put(object())  # type: ignore[arg-type]
    assert invalid_input.status == "rejected"
    assert invalid_input.reason == "observation_required"

    observation = _observation()
    for injected in (RecursionError(), TypeError(), ValueError(), UnicodeError()):

        def reject_invalid(*_: object, error: Exception = injected, **__: object) -> bytes:
            raise error

        monkeypatch.setattr(
            sqlite_store_module,
            "bounded_observation_bytes",
            reject_invalid,
        )
        receipt = store.put(observation)
        assert receipt.status == "rejected"
        assert receipt.reason == "observation_invalid"

    def fail_unexpected(*_: object, **__: object) -> bytes:
        raise RuntimeError("injected")

    monkeypatch.setattr(
        sqlite_store_module,
        "bounded_observation_bytes",
        fail_unexpected,
    )
    unexpected = store.put(observation)
    assert unexpected.status == "failed"
    assert unexpected.reason == "observation_serialization_failed"

    monkeypatch.setattr(
        sqlite_store_module,
        "bounded_observation_bytes",
        lambda *_args, **_kwargs: bytearray(b"not-bytes"),
    )
    invalid_return = store.put(observation)
    assert invalid_return.status == "failed"
    assert invalid_return.reason == "observation_serialization_failed"
    assert store.health().row_count == 0
    store.close()


def test_put_contains_unexpected_free_space_probe_failure(tmp_path: Path) -> None:
    def fail_probe(_: Path) -> int:
        raise RuntimeError("injected")

    store = _store(
        tmp_path / "space-probe.sqlite3",
        free_bytes=fail_probe,
    )
    receipt = store.put(_observation())

    assert receipt.status == "failed"
    assert receipt.reason == "volume_space_unavailable"
    assert store.health().state == "healthy"
    assert store.health().row_count == 0
    store.close()


@pytest.mark.parametrize(
    ("error_code", "reason"),
    [
        (sqlite3.SQLITE_FULL, "sqlite_full"),
        (sqlite3.SQLITE_IOERR, "sqlite_io_error"),
        (sqlite3.SQLITE_CORRUPT, "sqlite_corrupt"),
    ],
)
def test_fatal_write_errors_rollback_and_latch_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error_code: int,
    reason: str,
) -> None:
    class InjectedError(sqlite3.OperationalError):
        sqlite_errorcode = error_code

    store = _store(tmp_path / f"fault-{error_code}.sqlite3")

    def fail_commit() -> None:
        raise InjectedError("injected")

    monkeypatch.setattr(store, "_commit", fail_commit)
    receipt = store.put(_observation())

    assert receipt.status == "failed"
    assert receipt.reason == reason
    assert store.health().state == "read_only"
    assert store.health().row_count == 0
    assert store.put(_observation(1)).reason == reason
    with pytest.raises(SQLiteStoreReadOnlyError):
        store.purge_expired()
    store.close()


def test_busy_is_bounded_but_does_not_poison_the_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class BusyError(sqlite3.OperationalError):
        sqlite_errorcode = sqlite3.SQLITE_BUSY

    store = _store(tmp_path / "busy.sqlite3")

    def fail_begin() -> None:
        raise BusyError("injected")

    monkeypatch.setattr(store, "_begin", fail_begin)
    assert store.put(_observation()).reason == "sqlite_busy"
    assert store.health().state == "healthy"
    store.close()


def test_query_detects_record_corruption_without_mutating_or_quarantining(tmp_path: Path) -> None:
    store = _store(tmp_path / "observations.sqlite3", now_unix_ns=lambda: 10)
    assert store.put(_observation()).accepted
    store._execute("UPDATE observations SET document_json=?", (b"{}",))

    with pytest.raises(SQLiteStoreIntegrityError):
        store.query_latest(limit=1, max_bytes=1)
    assert store.health().state == "read_only"
    assert store.path.exists()
    store.close()


@pytest.mark.parametrize(
    ("case_name", "assignment"),
    [
        ("sequence", "store_sequence=0"),
        ("idempotency", "idempotency_key='different-key'"),
        ("session", "session_id='different-session'"),
        ("observed", "observed_monotonic_ns=observed_monotonic_ns+1"),
        ("stored", "stored_unix_ns=-1"),
        ("expiry", "expires_unix_ns=expires_unix_ns+1"),
        ("persistence", "persistence_class='important'"),
        ("sensitivity", "sensitivity_class='sensitive'"),
        ("digest_size", "document_sha256=zeroblob(33)"),
        ("logical_size", "logical_bytes=1"),
        ("document_size", "document_json=zeroblob(65537)"),
    ],
)
def test_query_rejects_each_corrupt_record_boundary(
    tmp_path: Path,
    case_name: str,
    assignment: str,
) -> None:
    store = _store(
        tmp_path / f"corrupt-{case_name}.sqlite3",
        now_unix_ns=lambda: 10,
    )
    assert store.put(_observation()).accepted
    store._execute("PRAGMA ignore_check_constraints=ON")
    store._execute(f"UPDATE observations SET {assignment}")

    with pytest.raises(SQLiteStoreIntegrityError):
        store.query_latest(limit=1)

    assert store.health().state == "read_only"
    assert store.health().reason == "record_integrity_error"
    assert store._execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
    store.close()


def test_decode_never_invokes_arbitrary_blob_conversion(tmp_path: Path) -> None:
    class ExplodingBlob:
        def __bytes__(self) -> bytes:
            raise AssertionError("arbitrary __bytes__ must not run")

    store = _store(tmp_path / "blob.sqlite3")
    observation = _observation()
    row = (
        1,
        observation.idempotency_key,
        observation.session_id,
        observation.observed_monotonic_ns,
        10,
        110,
        observation.persistence_class,
        observation.sensitivity_class,
        b"x" * 32,
        1,
        ExplodingBlob(),
    )

    with pytest.raises(SQLiteStoreIntegrityError):
        store._decode_row(row)
    store.close()


def test_corrupt_duplicate_metadata_fails_closed_without_new_row(tmp_path: Path) -> None:
    store = _store(tmp_path / "duplicate.sqlite3", now_unix_ns=lambda: 10)
    observation = _observation()
    assert store.put(observation).accepted
    store._execute("PRAGMA ignore_check_constraints=ON")
    store._execute("UPDATE observations SET document_sha256=zeroblob(33)")

    receipt = store.put(observation)

    assert receipt.status == "failed"
    assert receipt.reason == "record_integrity_error"
    assert store.health().state == "read_only"
    assert store._execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
    store.close()


def test_corrupt_purge_candidate_rolls_back_and_latches_read_only(tmp_path: Path) -> None:
    store = _store(tmp_path / "purge-corrupt.sqlite3", now_unix_ns=lambda: 10)
    assert store.put(_observation(ttl_ns=1)).accepted
    store._execute("PRAGMA ignore_check_constraints=ON")
    store._execute("UPDATE observations SET logical_bytes=0")

    with pytest.raises(SQLiteStoreIntegrityError):
        store.purge_expired(now_unix_ns=20)

    assert store.health().state == "read_only"
    assert store._execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
    store.close()


def test_pressure_purge_corruption_rejects_pending_write_without_mutation(
    tmp_path: Path,
) -> None:
    now = [10]
    store = _store(
        tmp_path / "pressure-corrupt.sqlite3",
        max_rows=1,
        now_unix_ns=lambda: now[0],
    )
    assert store.put(_observation(0, ttl_ns=1)).accepted
    store._execute("PRAGMA ignore_check_constraints=ON")
    store._execute("UPDATE observations SET logical_bytes=0")
    now[0] = 20

    receipt = store.put(_observation(1))

    assert receipt.status == "failed"
    assert receipt.reason == "record_integrity_error"
    assert store.health().state == "read_only"
    assert store._execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
    store.close()


def test_reopen_rejects_invalid_store_state_counter(tmp_path: Path) -> None:
    path = tmp_path / "invalid-state.sqlite3"
    store = _store(path, now_unix_ns=lambda: 10)
    assert store.put(_observation()).accepted
    store._execute("PRAGMA ignore_check_constraints=ON")
    store._execute("UPDATE store_state SET next_sequence=0")
    store.close()

    with pytest.raises(SQLiteStoreError, match="cannot open"):
        _store(path, now_unix_ns=lambda: 10)


def test_owner_thread_close_and_footprint_are_explicit(tmp_path: Path) -> None:
    store = _store(tmp_path / "observations.sqlite3")
    assert store.put(_observation()).accepted
    footprint = store.footprint()
    assert footprint.database_bytes > 0
    assert footprint.wal_bytes == 0
    assert footprint.total_bytes >= footprint.database_bytes

    errors: list[BaseException] = []

    def use_from_other_thread() -> None:
        try:
            store.health()
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=use_from_other_thread)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)

    store.close()
    store.close()
    assert store.closed
    assert store.health().state == "closed"
