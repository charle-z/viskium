"""Dependency-free command-line interface for Viskium."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from viskium import __version__
from viskium.config import ConfigError, EffectiveConfig, load_effective_config
from viskium.limits import MAX_SYNTHETIC_REPLAY_FRAMES
from viskium.resources import build_doctor_report


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
