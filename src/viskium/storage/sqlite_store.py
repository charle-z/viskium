"""Bounded SQLite persistence for structured Viskium observations."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import shutil
import sqlite3
import stat
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from viskium.core import ObservationEnvelope, PersistenceReceipt
from viskium.core.serialization import bounded_observation_bytes

_SCHEMA_VERSION = 1
_STORE_KIND = "viskium.observations"
_MIN_INT64 = -(2**63)
_MAX_INT64 = 2**63 - 1
_PAGE_SIZE = 4_096
_DEFAULT_MAX_ROWS = 100_000
_DEFAULT_MAX_DB_BYTES = 192 * 1_024 * 1_024
_DEFAULT_MAX_LOGICAL_BYTES = 128 * 1_024 * 1_024
_DEFAULT_MAX_OBSERVATION_BYTES = 65_536
_DEFAULT_VOLUME_RESERVE_BYTES = 512 * 1_024 * 1_024
_DEFAULT_BUSY_TIMEOUT_MS = 250
_DEFAULT_QUERY_ROWS = 256
_DEFAULT_QUERY_BYTES = 1_048_576
_DEFAULT_PURGE_ROWS = 512

type StoreState = Literal["healthy", "read_only", "closed"]


class SQLiteStoreError(RuntimeError):
    """Base error for SQLite store operations outside the receipt contract."""


class SQLiteStoreReadOnlyError(SQLiteStoreError):
    """Raised when a maintenance write is attempted after the store latched read-only."""


class SQLiteStoreIntegrityError(SQLiteStoreError):
    """Raised when persisted data does not match its recorded integrity metadata."""


@dataclass(frozen=True, slots=True)
class StoreFootprint:
    database_bytes: int
    journal_bytes: int
    wal_bytes: int
    shm_bytes: int

    @property
    def total_bytes(self) -> int:
        return self.database_bytes + self.journal_bytes + self.wal_bytes + self.shm_bytes


@dataclass(frozen=True, slots=True)
class StoreHealth:
    state: StoreState
    reason: str | None
    row_count: int
    logical_bytes: int
    max_rows: int
    max_db_bytes: int
    max_logical_bytes: int
    journal_mode: str
    sqlite_version: str


@dataclass(frozen=True, slots=True)
class StoredObservation:
    store_sequence: int
    stored_unix_ns: int
    expires_unix_ns: int
    observation: ObservationEnvelope


@dataclass(frozen=True, slots=True)
class PurgeReport:
    rows_deleted: int
    logical_bytes_deleted: int


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _signed_int64(value: object, name: str, *, minimum: int = _MIN_INT64) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < minimum or value > _MAX_INT64:
        raise ValueError(f"{name} must fit in signed 64 bits")
    return value


def _bounded_blob(
    value: object,
    name: str,
    *,
    maximum: int,
    exact: int | None = None,
) -> bytes:
    def validate_size(size: int) -> None:
        if exact is not None and size != exact:
            raise ValueError(f"{name} has an invalid length")
        if size > maximum:
            raise ValueError(f"{name} exceeds its byte ceiling")

    if type(value) is memoryview:
        view = cast(memoryview, value)
        validate_size(view.nbytes)
        return view.tobytes()
    if type(value) is bytes:
        immutable = value
        validate_size(len(immutable))
        return immutable
    if type(value) is bytearray:
        mutable = value
        validate_size(len(mutable))
        return bytes(mutable)
    raise TypeError(f"{name} must be a SQLite blob")


def _bounded_text(value: object, name: str, *, maximum: int) -> str:
    if type(value) is not str or not value or len(value) > maximum:
        raise ValueError(f"{name} must be a non-empty bounded string")
    return value


def _default_free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


class SQLiteStore:
    """A single-thread-owned, bounded implementation of ``ObservationStore``.

    The adapter deliberately uses rollback journal mode. It has no background
    retention, WAL, VACUUM, retry loop, or quarantine mutation. When the row
    ceiling is reached, one bounded batch of already-expired rows may be
    reclaimed inside the pending write transaction when either the row or
    logical-byte ceiling would be crossed.
    """

    def __init__(
        self,
        database_path: str | os.PathLike[str],
        *,
        max_rows: int = _DEFAULT_MAX_ROWS,
        max_db_bytes: int = _DEFAULT_MAX_DB_BYTES,
        max_logical_bytes: int | None = None,
        max_observation_bytes: int = _DEFAULT_MAX_OBSERVATION_BYTES,
        volume_reserve_bytes: int = _DEFAULT_VOLUME_RESERVE_BYTES,
        busy_timeout_ms: int = _DEFAULT_BUSY_TIMEOUT_MS,
        max_query_rows: int = _DEFAULT_QUERY_ROWS,
        max_query_bytes: int = _DEFAULT_QUERY_BYTES,
        max_purge_rows: int = _DEFAULT_PURGE_ROWS,
        now_unix_ns: Callable[[], int] = time.time_ns,
        free_bytes: Callable[[Path], int] = _default_free_bytes,
    ) -> None:
        self._max_rows = _positive_integer(max_rows, "max_rows")
        self._max_db_bytes = _positive_integer(max_db_bytes, "max_db_bytes")
        selected_logical_limit = (
            min(_DEFAULT_MAX_LOGICAL_BYTES, (self._max_db_bytes * 2) // 3)
            if max_logical_bytes is None
            else max_logical_bytes
        )
        self._max_logical_bytes = _positive_integer(
            selected_logical_limit,
            "max_logical_bytes",
        )
        if self._max_logical_bytes > self._max_db_bytes:
            raise ValueError("max_logical_bytes cannot exceed max_db_bytes")
        self._max_observation_bytes = _positive_integer(
            max_observation_bytes, "max_observation_bytes"
        )
        if isinstance(volume_reserve_bytes, bool) or not isinstance(volume_reserve_bytes, int):
            raise ValueError("volume_reserve_bytes must be a non-negative integer")
        if volume_reserve_bytes < 0:
            raise ValueError("volume_reserve_bytes must be a non-negative integer")
        self._volume_reserve_bytes = volume_reserve_bytes
        self._busy_timeout_ms = _positive_integer(busy_timeout_ms, "busy_timeout_ms")
        self._max_query_rows = _positive_integer(max_query_rows, "max_query_rows")
        self._max_query_bytes = _positive_integer(max_query_bytes, "max_query_bytes")
        self._max_purge_rows = _positive_integer(max_purge_rows, "max_purge_rows")
        if self._max_db_bytes < _PAGE_SIZE * 8:
            raise ValueError("max_db_bytes is too small for the SQLite schema")
        if not callable(now_unix_ns) or not callable(free_bytes):
            raise TypeError("clock and free-space providers must be callable")
        self._now_unix_ns = now_unix_ns
        self._free_bytes = free_bytes
        self._owner_thread_id = threading.get_ident()
        self._path = Path(os.path.abspath(Path(database_path).expanduser()))
        self._validate_database_path()
        self._state: StoreState = "healthy"
        self._reason: str | None = None
        self._row_count = 0
        self._logical_bytes = 0
        self._journal_mode = "unknown"
        self._connection: sqlite3.Connection | None = None
        try:
            self._connection = sqlite3.connect(
                self._path,
                timeout=self._busy_timeout_ms / 1_000,
                autocommit=True,
                check_same_thread=True,
            )
            self._configure_connection()
            self._initialize_or_verify_schema()
            self._load_and_verify_counters()
        except (OSError, sqlite3.Error, SQLiteStoreError, ValueError) as error:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            raise SQLiteStoreError(f"cannot open SQLite observation store: {self._path}") from error

    @property
    def path(self) -> Path:
        return self._path

    @property
    def closed(self) -> bool:
        return self._state == "closed"

    def _validate_database_path(self) -> None:
        raw = str(self._path).replace("/", "\\")
        if raw.startswith("\\\\"):
            raise SQLiteStoreError("remote and device namespace database paths are not supported")
        parent = self._path.parent
        if not parent.is_dir():
            raise SQLiteStoreError("database parent directory must already exist")
        for candidate in (parent, self._path):
            if not candidate.exists():
                continue
            metadata = candidate.lstat()
            attributes = getattr(metadata, "st_file_attributes", 0)
            reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
            if stat.S_ISLNK(metadata.st_mode) or bool(attributes & reparse_flag):
                raise SQLiteStoreError("database paths must not use links or reparse points")
        if self._path.exists() and not self._path.is_file():
            raise SQLiteStoreError("database path is not a regular file")

    def _assert_owner(self) -> None:
        if threading.get_ident() != self._owner_thread_id:
            raise RuntimeError("SQLiteStore may only be used by its owner thread")

    def _connection_or_raise(self) -> sqlite3.Connection:
        if self._connection is None or self._state == "closed":
            raise SQLiteStoreError("store is closed")
        return self._connection

    def _execute(self, sql: str, parameters: Sequence[object] = ()) -> sqlite3.Cursor:
        return self._connection_or_raise().execute(sql, parameters)

    def _begin(self) -> None:
        self._execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        self._execute("COMMIT")

    def _rollback(self) -> None:
        connection = self._connection
        if connection is not None and connection.in_transaction:
            connection.execute("ROLLBACK")

    def _configure_connection(self) -> None:
        connection = self._connection_or_raise()
        connection.setlimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
        connection.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 65_536)
        connection.setlimit(
            sqlite3.SQLITE_LIMIT_LENGTH, max(1_048_576, self._max_observation_bytes * 2)
        )
        connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 1_024)
        self._execute("PRAGMA foreign_keys=ON")
        self._execute("PRAGMA trusted_schema=OFF")
        self._execute("PRAGMA synchronous=NORMAL")
        self._execute("PRAGMA temp_store=MEMORY")
        self._execute("PRAGMA auto_vacuum=NONE")
        self._execute("PRAGMA secure_delete=FAST")
        self._execute("PRAGMA mmap_size=0")
        self._execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        page_count = cast(int, self._execute("PRAGMA page_count").fetchone()[0])
        if page_count == 0:
            self._execute(f"PRAGMA page_size={_PAGE_SIZE}")
        page_size = cast(int, self._execute("PRAGMA page_size").fetchone()[0])
        maximum_pages = self._max_db_bytes // page_size
        applied_pages = cast(
            int, self._execute(f"PRAGMA max_page_count={maximum_pages}").fetchone()[0]
        )
        if page_count > applied_pages or page_count * page_size > self._max_db_bytes:
            raise SQLiteStoreError("existing database exceeds max_db_bytes")
        mode = cast(str, self._execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower()
        if mode != "delete":
            raise SQLiteStoreError(f"SQLite refused journal_mode=DELETE: {mode}")
        self._journal_mode = mode
        foreign_keys = cast(int, self._execute("PRAGMA foreign_keys").fetchone()[0])
        trusted_schema = cast(int, self._execute("PRAGMA trusted_schema").fetchone()[0])
        synchronous = cast(int, self._execute("PRAGMA synchronous").fetchone()[0])
        temp_store = cast(int, self._execute("PRAGMA temp_store").fetchone()[0])
        if (foreign_keys, trusted_schema, synchronous, temp_store) != (1, 0, 1, 2):
            raise SQLiteStoreError("required SQLite connection policy was not applied")

    def _initialize_or_verify_schema(self) -> None:
        names = {
            cast(str, row[0])
            for row in self._execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        required = {"store_meta", "store_state", "observations"}
        if not names:
            self._create_schema()
            return
        if names != required:
            raise SQLiteStoreError("database is not a recognized Viskium observation store")
        row = self._execute(
            "SELECT store_kind, schema_version FROM store_meta WHERE singleton=1"
        ).fetchone()
        if row != (_STORE_KIND, _SCHEMA_VERSION):
            raise SQLiteStoreError("observation store metadata is not supported")

    def _create_schema(self) -> None:
        statements = (
            """
            CREATE TABLE store_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                store_kind TEXT NOT NULL CHECK (store_kind = 'viskium.observations'),
                schema_version INTEGER NOT NULL CHECK (schema_version = 1),
                created_unix_ns INTEGER NOT NULL CHECK (created_unix_ns >= 0)
            ) STRICT
            """,
            """
            CREATE TABLE store_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                next_sequence INTEGER NOT NULL CHECK (next_sequence >= 1),
                row_count INTEGER NOT NULL CHECK (row_count >= 0),
                logical_bytes INTEGER NOT NULL CHECK (logical_bytes >= 0)
            ) STRICT
            """,
            """
            CREATE TABLE observations (
                store_sequence INTEGER PRIMARY KEY CHECK (store_sequence >= 1),
                idempotency_key TEXT NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 256),
                session_id TEXT NOT NULL CHECK (length(session_id) BETWEEN 1 AND 256),
                observed_monotonic_ns INTEGER NOT NULL CHECK (observed_monotonic_ns >= 0),
                stored_unix_ns INTEGER NOT NULL CHECK (stored_unix_ns >= 0),
                expires_unix_ns INTEGER NOT NULL CHECK (expires_unix_ns >= stored_unix_ns),
                persistence_class TEXT NOT NULL CHECK (persistence_class IN ('routine','important','diagnostic')),
                sensitivity_class TEXT NOT NULL CHECK (sensitivity_class IN ('public','operational','sensitive','identifiable')),
                document_sha256 BLOB NOT NULL CHECK (length(document_sha256) = 32),
                logical_bytes INTEGER NOT NULL CHECK (logical_bytes > 0),
                document_json BLOB NOT NULL
            ) STRICT
            """,
            "CREATE INDEX observations_expiry_idx ON observations(expires_unix_ns, store_sequence)",
            "CREATE INDEX observations_latest_idx ON observations(session_id, store_sequence DESC)",
        )
        now = self._read_now()
        try:
            self._begin()
            for statement in statements:
                self._execute(statement)
            self._execute(
                "INSERT INTO store_meta(singleton, store_kind, schema_version, created_unix_ns) VALUES(1, ?, ?, ?)",
                (_STORE_KIND, _SCHEMA_VERSION, now),
            )
            self._execute(
                "INSERT INTO store_state(singleton, next_sequence, row_count, logical_bytes) VALUES(1, 1, 0, 0)"
            )
            self._commit()
        except sqlite3.Error:
            self._rollback()
            raise

    def _load_and_verify_counters(self) -> None:
        state = self._execute(
            "SELECT next_sequence, row_count, logical_bytes FROM store_state WHERE singleton=1"
        ).fetchone()
        actual = self._execute(
            "SELECT COUNT(*), COALESCE(SUM(logical_bytes), 0), COALESCE(MAX(store_sequence), 0) FROM observations"
        ).fetchone()
        if state is None or actual is None:
            raise SQLiteStoreError("observation store state is missing")
        try:
            next_sequence = _signed_int64(state[0], "next_sequence", minimum=1)
            row_count = _signed_int64(state[1], "row_count", minimum=0)
            logical_bytes = _signed_int64(state[2], "logical_bytes", minimum=0)
            actual_count = _signed_int64(actual[0], "actual_count", minimum=0)
            actual_bytes = _signed_int64(actual[1], "actual_bytes", minimum=0)
            maximum_sequence = _signed_int64(actual[2], "maximum_sequence", minimum=0)
        except (IndexError, TypeError, ValueError) as error:
            raise SQLiteStoreIntegrityError("observation store counters are invalid") from error
        if row_count != actual_count or logical_bytes != actual_bytes:
            raise SQLiteStoreError("observation store counters do not match persisted rows")
        if next_sequence <= maximum_sequence:
            raise SQLiteStoreError("observation store sequence would be reused")
        if row_count > self._max_rows or logical_bytes > self._max_logical_bytes:
            raise SQLiteStoreError("existing observation store exceeds configured bounds")
        self._row_count = row_count
        self._logical_bytes = logical_bytes

    @staticmethod
    def _decode_store_state(row: Sequence[object] | None) -> tuple[int, int, int]:
        if row is None:
            raise SQLiteStoreIntegrityError("store_state row is missing")
        try:
            return (
                _signed_int64(row[0], "next_sequence", minimum=1),
                _signed_int64(row[1], "row_count", minimum=0),
                _signed_int64(row[2], "logical_bytes", minimum=0),
            )
        except (IndexError, TypeError, ValueError) as error:
            raise SQLiteStoreIntegrityError("store_state row is invalid") from error

    @staticmethod
    def _decode_duplicate(row: Sequence[object]) -> tuple[int, bytes]:
        try:
            sequence = _signed_int64(row[0], "store_sequence", minimum=1)
            digest = _bounded_blob(
                row[1],
                "document_sha256",
                maximum=32,
                exact=32,
            )
        except (IndexError, TypeError, ValueError) as error:
            raise SQLiteStoreIntegrityError("duplicate record metadata is invalid") from error
        return sequence, digest

    @staticmethod
    def _decode_purge_candidates(
        rows: Sequence[Sequence[object]],
    ) -> tuple[list[int], int]:
        sequences: list[int] = []
        seen: set[int] = set()
        deleted_bytes = 0
        try:
            for row in rows:
                sequence = _signed_int64(row[0], "store_sequence", minimum=1)
                logical_bytes = _signed_int64(row[1], "logical_bytes", minimum=1)
                if sequence in seen:
                    raise ValueError("duplicate purge candidate")
                seen.add(sequence)
                sequences.append(sequence)
                deleted_bytes = _signed_int64(
                    deleted_bytes + logical_bytes,
                    "deleted_bytes",
                    minimum=0,
                )
        except (IndexError, TypeError, ValueError) as error:
            raise SQLiteStoreIntegrityError("purge candidate metadata is invalid") from error
        return sequences, deleted_bytes

    def _read_now(self) -> int:
        return _signed_int64(self._now_unix_ns(), "now_unix_ns", minimum=0)

    def _prevalidate_observation(self, observation: ObservationEnvelope) -> str | None:
        fields = (
            (observation.stream_epoch, "stream_epoch", 0),
            (observation.source_sequence, "source_sequence", 0),
            (observation.observed_monotonic_ns, "observed_monotonic_ns", 0),
            (observation.schema_version, "schema_version", 1),
        )
        try:
            for value, name, minimum in fields:
                _signed_int64(value, name, minimum=minimum)
            if observation.ttl_ns is None:
                return "ttl_required"
            _signed_int64(observation.ttl_ns, "ttl_ns", minimum=1)
        except (TypeError, ValueError):
            return "integer_out_of_range"
        if observation.sensitivity_class == "prohibited":
            return "prohibited_content"
        if observation.persistence_class == "visual":
            return "visual_persistence_disabled"
        return None

    def _latch_read_only(self, reason: str) -> None:
        if self._state != "closed":
            self._state = "read_only"
            self._reason = reason

    @staticmethod
    def _base_error_code(error: sqlite3.Error) -> int | None:
        code = getattr(error, "sqlite_errorcode", None)
        return None if not isinstance(code, int) else code & 0xFF

    def _handle_write_error(self, error: sqlite3.Error) -> PersistenceReceipt:
        try:
            self._rollback()
        except sqlite3.Error:
            self._latch_read_only("rollback_failed")
            return PersistenceReceipt(status="failed", reason="rollback_failed")
        code = self._base_error_code(error)
        if code == sqlite3.SQLITE_FULL:
            self._latch_read_only("sqlite_full")
            return PersistenceReceipt(status="failed", reason="sqlite_full")
        if code == sqlite3.SQLITE_IOERR:
            self._latch_read_only("sqlite_io_error")
            return PersistenceReceipt(status="failed", reason="sqlite_io_error")
        if code in {sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB}:
            self._latch_read_only("sqlite_corrupt")
            return PersistenceReceipt(status="failed", reason="sqlite_corrupt")
        if code in {sqlite3.SQLITE_READONLY, sqlite3.SQLITE_CANTOPEN}:
            self._latch_read_only("sqlite_read_only")
            return PersistenceReceipt(status="failed", reason="sqlite_read_only")
        if code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
            return PersistenceReceipt(status="failed", reason="sqlite_busy")
        self._latch_read_only("sqlite_database_error")
        return PersistenceReceipt(status="failed", reason="sqlite_database_error")

    def _handle_record_integrity_error(self) -> PersistenceReceipt:
        try:
            self._rollback()
        except sqlite3.Error:
            self._latch_read_only("rollback_failed")
            return PersistenceReceipt(status="failed", reason="rollback_failed")
        self._latch_read_only("record_integrity_error")
        return PersistenceReceipt(status="failed", reason="record_integrity_error")

    def put(self, observation: ObservationEnvelope) -> PersistenceReceipt:
        self._assert_owner()
        if self._state == "closed":
            return PersistenceReceipt(status="failed", reason="store_closed")
        if self._state == "read_only":
            return PersistenceReceipt(status="failed", reason=self._reason or "store_read_only")
        if not isinstance(observation, ObservationEnvelope):
            return PersistenceReceipt(status="rejected", reason="observation_required")
        invalid_reason = self._prevalidate_observation(observation)
        if invalid_reason is not None:
            return PersistenceReceipt(status="rejected", reason=invalid_reason)
        try:
            document = bounded_observation_bytes(
                observation,
                max_bytes=self._max_observation_bytes,
            )
        except (RecursionError, TypeError, ValueError, UnicodeError):
            return PersistenceReceipt(status="rejected", reason="observation_invalid")
        except Exception:
            return PersistenceReceipt(
                status="failed",
                reason="observation_serialization_failed",
            )
        if document is None:
            return PersistenceReceipt(status="rejected", reason="observation_exceeds_byte_limit")
        if type(document) is not bytes:
            return PersistenceReceipt(
                status="failed",
                reason="observation_serialization_failed",
            )
        digest = hashlib.sha256(document).digest()
        try:
            existing = self._execute(
                """
                SELECT store_sequence, substr(document_sha256, 1, 33)
                FROM observations
                WHERE idempotency_key=?
                """,
                (observation.idempotency_key,),
            ).fetchone()
        except sqlite3.Error as error:
            return self._handle_write_error(error)
        if existing is not None:
            try:
                sequence, existing_digest = self._decode_duplicate(existing)
            except SQLiteStoreIntegrityError:
                return self._handle_record_integrity_error()
            if hmac.compare_digest(existing_digest, digest):
                return PersistenceReceipt(
                    status="coalesced",
                    reason="duplicate_idempotency_key",
                    store_sequence=sequence,
                )
            return PersistenceReceipt(
                status="rejected",
                reason="idempotency_conflict",
                store_sequence=sequence,
            )
        try:
            stored_unix_ns = self._read_now()
            ttl_ns = cast(int, observation.ttl_ns)
            expires_unix_ns = _signed_int64(
                stored_unix_ns + ttl_ns,
                "expires_unix_ns",
                minimum=stored_unix_ns,
            )
        except (TypeError, ValueError):
            return PersistenceReceipt(status="rejected", reason="expiry_out_of_range")
        try:
            free_bytes = _signed_int64(self._free_bytes(self._path.parent), "free_bytes", minimum=0)
        except Exception:
            return PersistenceReceipt(status="failed", reason="volume_space_unavailable")
        if free_bytes - len(document) < self._volume_reserve_bytes:
            return PersistenceReceipt(status="rejected", reason="volume_reserve_reached")
        try:
            self._begin()
            duplicate = self._execute(
                """
                SELECT store_sequence, substr(document_sha256, 1, 33)
                FROM observations
                WHERE idempotency_key=?
                """,
                (observation.idempotency_key,),
            ).fetchone()
            if duplicate is not None:
                self._rollback()
                sequence, duplicate_digest = self._decode_duplicate(duplicate)
                if hmac.compare_digest(duplicate_digest, digest):
                    return PersistenceReceipt(
                        status="coalesced",
                        reason="duplicate_idempotency_key",
                        store_sequence=sequence,
                    )
                return PersistenceReceipt(
                    status="rejected",
                    reason="idempotency_conflict",
                    store_sequence=sequence,
                )
            state = self._execute(
                "SELECT next_sequence, row_count, logical_bytes FROM store_state WHERE singleton=1"
            ).fetchone()
            sequence, row_count, logical_bytes = self._decode_store_state(state)
            if row_count != self._row_count or logical_bytes != self._logical_bytes:
                raise SQLiteStoreIntegrityError("store_state counters changed unexpectedly")
            if sequence == _MAX_INT64:
                self._rollback()
                self._latch_read_only("sequence_exhausted")
                return PersistenceReceipt(status="failed", reason="sequence_exhausted")
            if (
                row_count >= self._max_rows
                or logical_bytes + len(document) > self._max_logical_bytes
            ):
                candidates = list(
                    self._execute(
                        """
                        SELECT store_sequence, logical_bytes
                        FROM observations
                        WHERE expires_unix_ns <= ?
                        ORDER BY expires_unix_ns, store_sequence
                        LIMIT ?
                        """,
                        (stored_unix_ns, self._max_purge_rows),
                    )
                )
                if candidates:
                    sequences, deleted_bytes = self._decode_purge_candidates(candidates)
                    if len(sequences) > row_count or deleted_bytes > logical_bytes:
                        raise SQLiteStoreIntegrityError(
                            "pressure purge candidates exceed store counters"
                        )
                    placeholders = ",".join("?" for _ in sequences)
                    cursor = self._execute(
                        f"DELETE FROM observations WHERE store_sequence IN ({placeholders})",
                        sequences,
                    )
                    if cursor.rowcount != len(sequences):
                        raise sqlite3.DatabaseError(
                            "pressure purge row count changed inside one transaction"
                        )
                    row_count -= len(sequences)
                    logical_bytes -= deleted_bytes
                if row_count >= self._max_rows:
                    self._rollback()
                    return PersistenceReceipt(
                        status="rejected",
                        reason="count_limit_reached",
                    )
                if logical_bytes + len(document) > self._max_logical_bytes:
                    self._rollback()
                    return PersistenceReceipt(
                        status="rejected",
                        reason="logical_byte_limit_reached",
                    )
            self._execute(
                """
                INSERT INTO observations(
                    store_sequence, idempotency_key, session_id, observed_monotonic_ns,
                    stored_unix_ns, expires_unix_ns, persistence_class, sensitivity_class,
                    document_sha256, logical_bytes, document_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sequence,
                    observation.idempotency_key,
                    observation.session_id,
                    observation.observed_monotonic_ns,
                    stored_unix_ns,
                    expires_unix_ns,
                    observation.persistence_class,
                    observation.sensitivity_class,
                    digest,
                    len(document),
                    document,
                ),
            )
            self._execute(
                "UPDATE store_state SET next_sequence=?, row_count=?, logical_bytes=? WHERE singleton=1",
                (sequence + 1, row_count + 1, logical_bytes + len(document)),
            )
            self._commit()
            self._row_count = row_count + 1
            self._logical_bytes = logical_bytes + len(document)
            return PersistenceReceipt(
                status="accepted",
                store_sequence=sequence,
                bytes_accepted=len(document),
            )
        except SQLiteStoreIntegrityError:
            return self._handle_record_integrity_error()
        except sqlite3.Error as error:
            return self._handle_write_error(error)

    def _decode_row(self, row: Sequence[object]) -> tuple[StoredObservation, int]:
        try:
            sequence = _signed_int64(row[0], "store_sequence", minimum=1)
            idempotency_key = _bounded_text(row[1], "idempotency_key", maximum=256)
            session_id = _bounded_text(row[2], "session_id", maximum=256)
            observed_monotonic_ns = _signed_int64(
                row[3],
                "observed_monotonic_ns",
                minimum=0,
            )
            stored_unix_ns = _signed_int64(row[4], "stored_unix_ns", minimum=0)
            expires_unix_ns = _signed_int64(row[5], "expires_unix_ns", minimum=0)
            persistence_class = _bounded_text(
                row[6],
                "persistence_class",
                maximum=32,
            )
            sensitivity_class = _bounded_text(
                row[7],
                "sensitivity_class",
                maximum=32,
            )
            expected_digest = _bounded_blob(
                row[8],
                "document_sha256",
                maximum=32,
                exact=32,
            )
            logical_bytes = _signed_int64(row[9], "logical_bytes", minimum=1)
            document_bytes = _bounded_blob(
                row[10],
                "document_json",
                maximum=self._max_observation_bytes,
            )
        except (IndexError, TypeError, ValueError) as error:
            raise SQLiteStoreIntegrityError("observation record metadata is invalid") from error
        if expires_unix_ns < stored_unix_ns:
            raise SQLiteStoreIntegrityError("observation expiry precedes storage time")
        if logical_bytes != len(document_bytes):
            raise SQLiteStoreIntegrityError("observation logical byte count is invalid")
        if not hmac.compare_digest(hashlib.sha256(document_bytes).digest(), expected_digest):
            raise SQLiteStoreIntegrityError("observation document digest mismatch")
        try:
            document = json.loads(document_bytes)
            if not isinstance(document, dict):
                raise TypeError("observation document is not an object")
            observation = ObservationEnvelope(**cast(dict[str, Any], document))
            canonical = bounded_observation_bytes(
                observation,
                max_bytes=self._max_observation_bytes,
            )
        except (
            MemoryError,
            RecursionError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as error:
            raise SQLiteStoreIntegrityError(
                "observation document cannot be reconstructed"
            ) from error
        if canonical is None or canonical != document_bytes:
            raise SQLiteStoreIntegrityError("observation document is not canonical")
        if (
            observation.idempotency_key != idempotency_key
            or observation.session_id != session_id
            or observation.observed_monotonic_ns != observed_monotonic_ns
            or observation.persistence_class != persistence_class
            or observation.sensitivity_class != sensitivity_class
        ):
            raise SQLiteStoreIntegrityError("observation metadata does not match its document")
        if observation.ttl_ns is None or stored_unix_ns + observation.ttl_ns != expires_unix_ns:
            raise SQLiteStoreIntegrityError("observation expiry does not match its TTL")
        return (
            StoredObservation(
                store_sequence=sequence,
                stored_unix_ns=stored_unix_ns,
                expires_unix_ns=expires_unix_ns,
                observation=observation,
            ),
            logical_bytes,
        )

    def query_latest(
        self,
        *,
        limit: int,
        session_id: str | None = None,
        max_bytes: int | None = None,
        now_unix_ns: int | None = None,
    ) -> tuple[StoredObservation, ...]:
        self._assert_owner()
        self._connection_or_raise()
        selected_limit = _positive_integer(limit, "limit")
        if selected_limit > self._max_query_rows:
            raise ValueError("limit exceeds max_query_rows")
        byte_limit = (
            self._max_query_bytes
            if max_bytes is None
            else _positive_integer(max_bytes, "max_bytes")
        )
        if byte_limit > self._max_query_bytes:
            raise ValueError("max_bytes exceeds the configured query ceiling")
        now = (
            self._read_now()
            if now_unix_ns is None
            else _signed_int64(now_unix_ns, "now_unix_ns", minimum=0)
        )
        parameters: list[object] = [now]
        where = "expires_unix_ns > ?"
        if session_id is not None:
            if not isinstance(session_id, str) or not session_id or len(session_id) > 256:
                raise ValueError("session_id must be a non-empty bounded string")
            where += " AND session_id = ?"
            parameters.append(session_id)
        parameters.append(selected_limit)
        try:
            cursor = self._execute(
                f"""
                SELECT store_sequence, idempotency_key, session_id,
                       observed_monotonic_ns, stored_unix_ns, expires_unix_ns,
                       persistence_class, sensitivity_class,
                       substr(document_sha256, 1, 33), logical_bytes,
                       substr(document_json, 1, {self._max_observation_bytes + 1})
                FROM observations
                WHERE {where}
                ORDER BY store_sequence DESC
                LIMIT ?
                """,
                parameters,
            )
            result: list[StoredObservation] = []
            consumed = 0
            for row in cursor:
                observation, row_bytes = self._decode_row(row)
                if consumed + row_bytes > byte_limit:
                    break
                result.append(observation)
                consumed += row_bytes
            return tuple(result)
        except SQLiteStoreIntegrityError:
            self._latch_read_only("record_integrity_error")
            raise
        except sqlite3.Error as error:
            code = self._base_error_code(error)
            if code in {sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB, sqlite3.SQLITE_IOERR}:
                self._latch_read_only("sqlite_read_error")
            raise SQLiteStoreError("bounded observation query failed") from error

    def purge_expired(
        self,
        *,
        limit: int = _DEFAULT_PURGE_ROWS,
        now_unix_ns: int | None = None,
    ) -> PurgeReport:
        self._assert_owner()
        self._connection_or_raise()
        if self._state == "read_only":
            raise SQLiteStoreReadOnlyError(self._reason or "store is read-only")
        selected_limit = _positive_integer(limit, "limit")
        if selected_limit > self._max_purge_rows:
            raise ValueError("limit exceeds max_purge_rows")
        now = (
            self._read_now()
            if now_unix_ns is None
            else _signed_int64(now_unix_ns, "now_unix_ns", minimum=0)
        )
        try:
            self._begin()
            state = self._execute(
                "SELECT next_sequence, row_count, logical_bytes FROM store_state WHERE singleton=1"
            ).fetchone()
            _, row_count, logical_bytes = self._decode_store_state(state)
            if row_count != self._row_count or logical_bytes != self._logical_bytes:
                raise SQLiteStoreIntegrityError("store_state counters changed unexpectedly")
            candidates = list(
                self._execute(
                    """
                    SELECT store_sequence, logical_bytes
                    FROM observations
                    WHERE expires_unix_ns <= ?
                    ORDER BY expires_unix_ns, store_sequence
                    LIMIT ?
                    """,
                    (now, selected_limit),
                )
            )
            if not candidates:
                self._rollback()
                return PurgeReport(rows_deleted=0, logical_bytes_deleted=0)
            sequences, deleted_bytes = self._decode_purge_candidates(candidates)
            if len(sequences) > row_count or deleted_bytes > logical_bytes:
                raise SQLiteStoreIntegrityError("purge candidates exceed store counters")
            placeholders = ",".join("?" for _ in sequences)
            cursor = self._execute(
                f"DELETE FROM observations WHERE store_sequence IN ({placeholders})",
                sequences,
            )
            if cursor.rowcount != len(sequences):
                raise sqlite3.DatabaseError("purge row count changed inside one transaction")
            next_row_count = row_count - len(sequences)
            next_logical_bytes = logical_bytes - deleted_bytes
            state_cursor = self._execute(
                "UPDATE store_state SET row_count=?, logical_bytes=? WHERE singleton=1",
                (next_row_count, next_logical_bytes),
            )
            if state_cursor.rowcount != 1:
                raise sqlite3.DatabaseError("store_state row changed inside one transaction")
            self._commit()
            self._row_count = next_row_count
            self._logical_bytes = next_logical_bytes
            return PurgeReport(
                rows_deleted=len(sequences),
                logical_bytes_deleted=deleted_bytes,
            )
        except SQLiteStoreIntegrityError as error:
            receipt = self._handle_record_integrity_error()
            if receipt.reason == "rollback_failed":
                raise SQLiteStoreError("rollback_failed") from error
            raise
        except sqlite3.Error as error:
            receipt = self._handle_write_error(error)
            raise SQLiteStoreError(receipt.reason or "purge failed") from error

    @staticmethod
    def _file_size(path: Path) -> int:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return 0
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise SQLiteStoreIntegrityError(f"owned SQLite path is not a regular file: {path}")
        return metadata.st_size

    def footprint(self) -> StoreFootprint:
        self._assert_owner()
        try:
            return StoreFootprint(
                database_bytes=self._file_size(self._path),
                journal_bytes=self._file_size(Path(f"{self._path}-journal")),
                wal_bytes=self._file_size(Path(f"{self._path}-wal")),
                shm_bytes=self._file_size(Path(f"{self._path}-shm")),
            )
        except SQLiteStoreIntegrityError:
            self._latch_read_only("owned_path_integrity_error")
            raise

    def health(self) -> StoreHealth:
        self._assert_owner()
        return StoreHealth(
            state=self._state,
            reason=self._reason,
            row_count=self._row_count,
            logical_bytes=self._logical_bytes,
            max_rows=self._max_rows,
            max_db_bytes=self._max_db_bytes,
            max_logical_bytes=self._max_logical_bytes,
            journal_mode=self._journal_mode,
            sqlite_version=sqlite3.sqlite_version,
        )

    def close(self) -> None:
        self._assert_owner()
        if self._state == "closed":
            return
        connection = self._connection
        self._connection = None
        if connection is not None:
            try:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
            finally:
                connection.close()
        self._state = "closed"

    def __enter__(self) -> SQLiteStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


__all__ = [
    "PurgeReport",
    "SQLiteStore",
    "SQLiteStoreError",
    "SQLiteStoreIntegrityError",
    "SQLiteStoreReadOnlyError",
    "StoreFootprint",
    "StoreHealth",
    "StoredObservation",
]
