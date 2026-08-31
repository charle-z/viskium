"""Dependency-free command-line interface for Viskium."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import stat
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

from viskium import __version__
from viskium.agent import (
    MAX_CONSENT_DURATION_SECONDS,
    MAX_SNAPSHOT_QUOTA,
    AgentLimits,
    ConsentLedger,
    ConsentLedgerError,
)
from viskium.capture.contracts import (
    MAX_CAPTURE_DIMENSION,
    MAX_CAPTURE_FPS,
    MAX_CAPTURE_FRAME_BYTES,
    CaptureRequest,
)
from viskium.config import ConfigError, EffectiveConfig, load_effective_config
from viskium.limits import MAX_SYNTHETIC_REPLAY_FRAMES
from viskium.resources import build_doctor_report
from viskium.snapshots import MAX_SNAPSHOT_BYTES, MAX_SNAPSHOT_EDGE_PX
from viskium.storage import (
    SQLiteStore,
    SQLiteStoreError,
    StorageLayoutError,
    initialize_data_root,
    verify_data_root,
)

_STORAGE_DATABASE_NAME = "observations.sqlite3"
_MAX_CLI_PURGE_ROWS = 512
_AGENT_SCOPES = ("observation.read", "snapshot.read")
_CONSENT_SENSITIVITIES = ("public", "operational", "sensitive", "identifiable")


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def _replay_frame_count(value: str) -> int:
    parsed = _non_negative_int(value)
    if parsed > MAX_SYNTHETIC_REPLAY_FRAMES:
        raise argparse.ArgumentTypeError(f"must not exceed {MAX_SYNTHETIC_REPLAY_FRAMES} frames")
    return parsed


def _purge_limit(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < 1 or parsed > _MAX_CLI_PURGE_ROWS:
        raise argparse.ArgumentTypeError(f"must be between 1 and {_MAX_CLI_PURGE_ROWS}")
    return parsed


def _bounded_positive_int(value: str, *, maximum: int, label: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < 1 or parsed > maximum:
        raise argparse.ArgumentTypeError(f"{label} must be between 1 and {maximum}")
    return parsed


def _consent_duration(value: str) -> int:
    return _bounded_positive_int(
        value,
        maximum=MAX_CONSENT_DURATION_SECONDS,
        label="duration-seconds",
    )


def _snapshot_quota(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < 0 or parsed > MAX_SNAPSHOT_QUOTA:
        raise argparse.ArgumentTypeError(
            f"snapshot-quota must be between 0 and {MAX_SNAPSHOT_QUOTA}"
        )
    return parsed


def _bounded_cli_int(value: str, *, minimum: int, maximum: int, label: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < minimum or parsed > maximum:
        raise argparse.ArgumentTypeError(f"{label} must be between {minimum} and {maximum}")
    return parsed


def _camera_index(value: str) -> int:
    return _bounded_cli_int(value, minimum=0, maximum=1_024, label="device-index")


def _camera_dimension(value: str) -> int:
    return _bounded_cli_int(
        value,
        minimum=1,
        maximum=MAX_CAPTURE_DIMENSION,
        label="camera dimension",
    )


def _camera_fps(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a number") from error
    if not math.isfinite(parsed) or not 0.0 < parsed <= MAX_CAPTURE_FPS:
        raise argparse.ArgumentTypeError(
            f"fps must be finite, greater than zero, and at most {MAX_CAPTURE_FPS:g}"
        )
    return parsed


def _agent_snapshot_bytes(value: str) -> int:
    return _bounded_cli_int(
        value,
        minimum=64 * 1_024,
        maximum=MAX_SNAPSHOT_BYTES,
        label="max-snapshot-bytes",
    )


def _agent_snapshot_edge(value: str) -> int:
    return _bounded_cli_int(
        value,
        minimum=1,
        maximum=MAX_SNAPSHOT_EDGE_PX,
        label="max-snapshot-edge-px",
    )


def _agent_wait_ms(value: str) -> int:
    return _bounded_cli_int(
        value,
        minimum=1,
        maximum=15_000,
        label="max-wait-ms",
    )


def _agent_wire_bytes(value: str) -> int:
    return _bounded_cli_int(
        value,
        minimum=64 * 1_024,
        maximum=1 * 1_024 * 1_024,
        label="max-wire-bytes",
    )


def _agent_inflight_requests(value: str) -> int:
    return _bounded_cli_int(
        value,
        minimum=1,
        maximum=32,
        label="max-inflight-requests",
    )


def _path_argument(value: str) -> Path:
    if not value.strip():
        raise argparse.ArgumentTypeError("path must not be empty")
    return Path(value)


def _add_path_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--data-root",
        type=_path_argument,
        default=argparse.SUPPRESS,
        help="override the Viskium data root for this invocation",
    )
    parser.add_argument(
        "--config",
        dest="config_file",
        type=_path_argument,
        default=argparse.SUPPRESS,
        help="read configuration from this TOML file",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="viskium", description="Viskium engineering runtime")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    _add_path_options(parser)
    commands = parser.add_subparsers(dest="command", required=True)

    doctor = commands.add_parser("doctor", help="inspect the host without changing it")
    _add_path_options(doctor)
    doctor.add_argument("--json", action="store_true", dest="as_json")
    doctor.set_defaults(handler=_handle_doctor)

    config = commands.add_parser("config", help="inspect Viskium configuration")
    config_commands = config.add_subparsers(dest="config_command", required=True)
    show = config_commands.add_parser("show", help="show resolved configuration")
    _add_path_options(show)
    show.add_argument("--effective", action="store_true", help="show effective values")
    show.add_argument("--json", action="store_true", dest="as_json")
    show.set_defaults(handler=_handle_config_show)

    replay = commands.add_parser("replay", help="run the deterministic synthetic replay")
    _add_path_options(replay)
    replay.add_argument("--mode", choices=("exhaustive", "faithful"), required=True)
    replay.add_argument("--frames", type=_replay_frame_count, required=True)
    replay.add_argument("--json", action="store_true", dest="as_json")
    replay.set_defaults(handler=_handle_replay)

    storage = commands.add_parser("storage", help="manage bounded local observation storage")
    storage_commands = storage.add_subparsers(dest="storage_command", required=True)

    storage_init = storage_commands.add_parser(
        "init", help="explicitly initialize the data root and observation database"
    )
    _add_path_options(storage_init)
    storage_init.add_argument("--json", action="store_true", dest="as_json")
    storage_init.set_defaults(handler=_handle_storage_init)

    storage_status = storage_commands.add_parser(
        "status", help="inspect storage without creating or changing it"
    )
    _add_path_options(storage_status)
    storage_status.add_argument("--json", action="store_true", dest="as_json")
    storage_status.set_defaults(handler=_handle_storage_status)

    storage_purge = storage_commands.add_parser(
        "purge-expired", help="explicitly delete a bounded batch of expired observations"
    )
    _add_path_options(storage_purge)
    storage_purge.add_argument("--limit", type=_purge_limit, default=_MAX_CLI_PURGE_ROWS)
    storage_purge.add_argument("--json", action="store_true", dest="as_json")
    storage_purge.set_defaults(handler=_handle_storage_purge)

    consent = commands.add_parser(
        "consent",
        help="manage explicit, local agent-read consent",
    )
    consent_commands = consent.add_subparsers(dest="consent_command", required=True)

    consent_grant = consent_commands.add_parser(
        "grant",
        help="create or replace a bounded local consent grant",
    )
    _add_path_options(consent_grant)
    consent_grant.add_argument(
        "--scope",
        action="append",
        choices=_AGENT_SCOPES,
        required=True,
        dest="scopes",
        help="grant one scope; repeat to grant both",
    )
    consent_grant.add_argument(
        "--duration-seconds",
        type=_consent_duration,
        required=True,
    )
    consent_grant.add_argument(
        "--snapshot-quota",
        type=_snapshot_quota,
        default=0,
        help="maximum snapshot attempts for this grant (default: 0)",
    )
    consent_grant.add_argument(
        "--sensitivity-ceiling",
        choices=_CONSENT_SENSITIVITIES,
        default="public",
    )
    consent_grant.add_argument("--json", action="store_true", dest="as_json")
    consent_grant.set_defaults(handler=_handle_consent_grant)

    consent_status = consent_commands.add_parser(
        "status",
        help="inspect consent without creating or changing it",
    )
    _add_path_options(consent_status)
    consent_status.add_argument("--json", action="store_true", dest="as_json")
    consent_status.set_defaults(handler=_handle_consent_status)

    consent_revoke = consent_commands.add_parser(
        "revoke",
        help="remove only the current consent grant",
    )
    _add_path_options(consent_revoke)
    consent_revoke.add_argument("--json", action="store_true", dest="as_json")
    consent_revoke.set_defaults(handler=_handle_consent_revoke)

    agent = commands.add_parser("agent", help="serve bounded local agent-read tools")
    agent_commands = agent.add_subparsers(dest="agent_command", required=True)
    agent_serve = agent_commands.add_parser(
        "serve",
        help="run the optional MCP server over stdio without opening hardware",
    )
    _add_path_options(agent_serve)
    agent_serve.add_argument("--device-index", type=_camera_index, default=0)
    agent_serve.add_argument("--width", type=_camera_dimension, default=640)
    agent_serve.add_argument("--height", type=_camera_dimension, default=480)
    agent_serve.add_argument("--fps", type=_camera_fps, default=15.0)
    agent_serve.add_argument(
        "--max-snapshot-bytes",
        type=_agent_snapshot_bytes,
        default=4 * 1_024 * 1_024,
    )
    agent_serve.add_argument(
        "--max-snapshot-edge-px",
        type=_agent_snapshot_edge,
        default=1_280,
    )
    agent_serve.add_argument("--max-wait-ms", type=_agent_wait_ms, default=10_000)
    agent_serve.add_argument("--max-wire-bytes", type=_agent_wire_bytes, default=256 * 1_024)
    agent_serve.add_argument(
        "--max-inflight-requests",
        type=_agent_inflight_requests,
        default=4,
    )
    agent_serve.set_defaults(handler=_handle_agent_serve)
    return parser


def _load_from_args(args: argparse.Namespace) -> EffectiveConfig:
    return load_effective_config(
        cli_data_root=getattr(args, "data_root", None),
        config_file=getattr(args, "config_file", None),
    )


def _print_payload(payload: Any, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return
    if isinstance(payload, dict):
        for key, value in payload.items():
            print(f"{key}: {value}")
        return
    print(payload)


def _handle_doctor(args: argparse.Namespace) -> int:
    report = build_doctor_report(_load_from_args(args))
    _print_payload(report, as_json=args.as_json)
    return 0 if report["status"] != "error" else 1


def _handle_config_show(args: argparse.Namespace) -> int:
    # The only representation implemented in F1 is the effective configuration;
    # --effective is accepted explicitly so future raw/config-source views can be added.
    payload = _load_from_args(args).to_dict()
    _print_payload(payload, as_json=args.as_json)
    return 0


def _handle_replay(args: argparse.Namespace) -> int:
    # Importing only on execution keeps doctor/config free of runtime side effects.
    from viskium.runtime.replay import run_synthetic_replay

    result = run_synthetic_replay(args.mode, args.frames)
    _print_payload(result.to_dict(), as_json=args.as_json)
    return 0


def _database_path(config: EffectiveConfig) -> Path:
    return config.storage.root / "observations" / _STORAGE_DATABASE_NAME


def _storage_error(
    args: argparse.Namespace,
    *,
    command: str,
    reason: str,
    message: str,
) -> int:
    _print_payload(
        {
            "schema_version": 1,
            "command": command,
            "status": "error",
            "reason": reason,
            "message": message,
        },
        as_json=args.as_json,
    )
    return 1


def _consent_error(args: argparse.Namespace, *, command: str, message: str) -> int:
    _print_payload(
        {
            "schema_version": 1,
            "command": command,
            "status": "error",
            "reason": "consent_unavailable",
            "message": message,
        },
        as_json=args.as_json,
    )
    return 1


def _regular_file_size(path: Path) -> int:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return 0
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise StorageLayoutError(f"storage-owned path is not a regular file: {path}")
    return metadata.st_size


def _read_only_database_status(path: Path) -> dict[str, Any]:
    if _regular_file_size(path) == 0:
        raise SQLiteStoreError(f"observation database does not exist: {path}")
    uri_path = quote(path.as_posix(), safe="/:")
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"file:{uri_path}?mode=ro",
            uri=True,
            timeout=0.25,
            autocommit=True,
            check_same_thread=True,
        )
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        connection.execute("PRAGMA busy_timeout=250")
        meta = connection.execute(
            "SELECT store_kind, schema_version FROM store_meta WHERE singleton=1"
        ).fetchone()
        if meta != ("viskium.observations", 1):
            raise SQLiteStoreError("observation database metadata is not supported")
        state = connection.execute(
            "SELECT next_sequence, row_count, logical_bytes FROM store_state WHERE singleton=1"
        ).fetchone()
        if state is None:
            raise SQLiteStoreError("observation database state is missing")
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).upper()
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        next_sequence, row_count, logical_bytes = map(int, state)
    except (sqlite3.Error, TypeError, ValueError) as error:
        raise SQLiteStoreError("observation database could not be inspected read-only") from error
    finally:
        if connection is not None:
            connection.close()
    journal_path = Path(f"{path}-journal")
    wal_path = Path(f"{path}-wal")
    shm_path = Path(f"{path}-shm")
    database_bytes = _regular_file_size(path)
    journal_bytes = _regular_file_size(journal_path)
    wal_bytes = _regular_file_size(wal_path)
    shm_bytes = _regular_file_size(shm_path)
    return {
        "path": str(path),
        "exists": True,
        "inspection_mode": "read_only",
        "journal_mode": journal_mode,
        "desired_journal_mode": "DELETE",
        "next_sequence": next_sequence,
        "row_count": row_count,
        "logical_bytes": logical_bytes,
        "page_size": page_size,
        "page_count": page_count,
        "footprint": {
            "database_bytes": database_bytes,
            "journal_bytes": journal_bytes,
            "wal_bytes": wal_bytes,
            "shm_bytes": shm_bytes,
            "total_bytes": database_bytes + journal_bytes + wal_bytes + shm_bytes,
        },
    }


def _handle_storage_init(args: argparse.Namespace) -> int:
    config = _load_from_args(args)
    root = config.storage.root
    database = _database_path(config)
    root_existed = root.exists()
    database_existed = database.exists()
    try:
        layout = initialize_data_root(root)
        database = layout.observations / _STORAGE_DATABASE_NAME
        with SQLiteStore(database) as store:
            health = store.health()
            footprint = store.footprint()
    except (OSError, SQLiteStoreError, StorageLayoutError) as error:
        return _storage_error(
            args,
            command="storage.init",
            reason="storage_initialization_failed",
            message=str(error),
        )
    _print_payload(
        {
            "schema_version": 1,
            "command": "storage.init",
            "status": "ok",
            "reason": None,
            "data_root": {
                "path": str(layout.root),
                "source": config.storage.root_source,
                "root_id": layout.root_id,
                "created": not root_existed,
            },
            "database": {
                "path": str(database),
                "created": not database_existed,
                "state": health.state,
                "journal_mode": health.journal_mode.upper(),
                "row_count": health.row_count,
                "logical_bytes": health.logical_bytes,
                "footprint_bytes": footprint.total_bytes,
            },
        },
        as_json=args.as_json,
    )
    return 0


def _handle_storage_status(args: argparse.Namespace) -> int:
    config = _load_from_args(args)
    root = config.storage.root
    if not root.exists():
        return _storage_error(
            args,
            command="storage.status",
            reason="data_root_missing",
            message=f"data root does not exist: {root}",
        )
    try:
        layout = verify_data_root(root)
        database_status = _read_only_database_status(layout.observations / _STORAGE_DATABASE_NAME)
    except (OSError, SQLiteStoreError, StorageLayoutError) as error:
        return _storage_error(
            args,
            command="storage.status",
            reason="storage_unavailable",
            message=str(error),
        )
    _print_payload(
        {
            "schema_version": 1,
            "command": "storage.status",
            "status": "ok",
            "reason": None,
            "read_only": True,
            "data_root": {
                "path": str(layout.root),
                "source": config.storage.root_source,
                "root_id": layout.root_id,
            },
            "database": database_status,
        },
        as_json=args.as_json,
    )
    return 0


def _handle_storage_purge(args: argparse.Namespace) -> int:
    config = _load_from_args(args)
    try:
        layout = verify_data_root(config.storage.root)
        database = layout.observations / _STORAGE_DATABASE_NAME
        if _regular_file_size(database) == 0:
            raise SQLiteStoreError(f"observation database does not exist: {database}")
        with SQLiteStore(database) as store:
            purge = store.purge_expired(limit=args.limit)
            health = store.health()
            footprint = store.footprint()
    except (OSError, SQLiteStoreError, StorageLayoutError) as error:
        return _storage_error(
            args,
            command="storage.purge-expired",
            reason="storage_purge_failed",
            message=str(error),
        )
    _print_payload(
        {
            "schema_version": 1,
            "command": "storage.purge-expired",
            "status": "ok",
            "reason": None,
            "limit": args.limit,
            "rows_deleted": purge.rows_deleted,
            "logical_bytes_deleted": purge.logical_bytes_deleted,
            "database": {
                "path": str(database),
                "state": health.state,
                "row_count": health.row_count,
                "logical_bytes": health.logical_bytes,
                "footprint_bytes": footprint.total_bytes,
            },
        },
        as_json=args.as_json,
    )
    return 0


def _consent_ledger(args: argparse.Namespace) -> tuple[ConsentLedger, EffectiveConfig]:
    config = _load_from_args(args)
    return ConsentLedger(verify_data_root(config.storage.root)), config


def _handle_consent_grant(args: argparse.Namespace) -> int:
    try:
        ledger, config = _consent_ledger(args)
        state = ledger.grant(
            scopes=frozenset(args.scopes),
            duration_seconds=args.duration_seconds,
            snapshot_quota=args.snapshot_quota,
            sensitivity_ceiling=args.sensitivity_ceiling,
        )
    except (ConsentLedgerError, OSError, StorageLayoutError, TypeError, ValueError) as error:
        return _consent_error(args, command="consent.grant", message=str(error))
    _print_payload(
        {
            "schema_version": 1,
            "command": "consent.grant",
            "status": "ok",
            "reason": None,
            "data_root": str(config.storage.root),
            "consent": state.to_public_dict(),
        },
        as_json=args.as_json,
    )
    return 0


def _handle_consent_status(args: argparse.Namespace) -> int:
    try:
        ledger, config = _consent_ledger(args)
        state = ledger.load()
    except (ConsentLedgerError, OSError, StorageLayoutError) as error:
        return _consent_error(args, command="consent.status", message=str(error))
    now_unix_ns = time.time_ns()
    _print_payload(
        {
            "schema_version": 1,
            "command": "consent.status",
            "status": "ok",
            "reason": None,
            "read_only": True,
            "data_root": str(config.storage.root),
            "active": state is not None and now_unix_ns < state.grant.expires_unix_ns,
            "consent": None if state is None else state.to_public_dict(),
        },
        as_json=args.as_json,
    )
    return 0


def _handle_consent_revoke(args: argparse.Namespace) -> int:
    try:
        ledger, config = _consent_ledger(args)
        revoked = ledger.revoke()
    except (ConsentLedgerError, OSError, StorageLayoutError) as error:
        return _consent_error(args, command="consent.revoke", message=str(error))
    _print_payload(
        {
            "schema_version": 1,
            "command": "consent.revoke",
            "status": "ok",
            "reason": None,
            "data_root": str(config.storage.root),
            "revoked": revoked,
        },
        as_json=args.as_json,
    )
    return 0


def _handle_agent_serve(args: argparse.Namespace) -> int:
    """Build the local service, then dedicate stdout to MCP stdio."""

    from viskium.agent.mcp_server import MCPDependencyError, run_mcp_server
    from viskium.app import build_agent_application

    config = _load_from_args(args)
    try:
        frame_bytes = args.width * args.height * 3
        if frame_bytes > MAX_CAPTURE_FRAME_BYTES:
            raise ValueError(f"requested BGR frame exceeds {MAX_CAPTURE_FRAME_BYTES} bytes")
        request = CaptureRequest(
            device_index=args.device_index,
            requested_width=args.width,
            requested_height=args.height,
            requested_fps=args.fps,
            max_frame_bytes=frame_bytes,
        )
        limits = AgentLimits(
            max_wire_bytes=args.max_wire_bytes,
            max_inflight_requests=args.max_inflight_requests,
            max_snapshot_bytes=args.max_snapshot_bytes,
            max_snapshot_edge_px=args.max_snapshot_edge_px,
            max_wait_ms=args.max_wait_ms,
        )
        application = build_agent_application(
            config.storage.root,
            capture_request=request,
            limits=limits,
        )
        run_mcp_server(application.service)
    except (
        ConsentLedgerError,
        MCPDependencyError,
        OSError,
        RuntimeError,
        StorageLayoutError,
        TypeError,
        ValueError,
    ) as error:
        print(f"viskium agent serve: {error}", file=sys.stderr)
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except ConfigError as error:
        parser.error(str(error))


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
