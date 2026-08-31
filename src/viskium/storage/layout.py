"""Explicit, marker-backed layout for data owned by Viskium."""

from __future__ import annotations

import json
import os
import stat
import uuid
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DATA_ROOT_MARKER = ".viskium-root.json"
DATA_ROOT_SCHEMA_VERSION = 1
DATA_CATEGORIES = (
    "state",
    "observations",
    "models",
    "runs",
    "logs",
    "cache",
    "tmp",
    "locks",
    "quarantine",
)
_MARKER_KIND = "viskium.data-root"
_MAX_MARKER_BYTES = 16_384


class StorageLayoutError(ValueError):
    """Raised when a path cannot safely be treated as a Viskium data root."""


@dataclass(frozen=True, slots=True)
class DataRootLayout:
    """A verified data root and its immutable identity."""

    root: Path
    root_id: str
    schema_version: int = DATA_ROOT_SCHEMA_VERSION

    def category(self, name: str) -> Path:
        if name not in DATA_CATEGORIES:
            raise StorageLayoutError(f"unsupported data category: {name}")
        return self.root / name

    @property
    def observations(self) -> Path:
        return self.category("observations")


def _absolute_root(value: str | os.PathLike[str]) -> Path:
    raw = os.fspath(value).strip()
    if not raw:
        raise StorageLayoutError("data root must not be empty")
    windows_path = raw.replace("/", "\\")
    if windows_path.startswith("\\\\"):
        raise StorageLayoutError("remote and device namespace paths are not supported")
    root = Path(os.path.abspath(Path(raw).expanduser()))
    if root == Path(root.anchor):
        raise StorageLayoutError("a filesystem root cannot be a Viskium data root")
    try:
        if root == Path.home().resolve(strict=False):
            raise StorageLayoutError("the home directory cannot be a Viskium data root")
    except OSError as error:
        raise StorageLayoutError("the home directory could not be resolved safely") from error
    return root


def _is_reparse_or_symlink(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise StorageLayoutError(f"cannot inspect layout path: {path}") from error
    attributes = getattr(metadata, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return stat.S_ISLNK(metadata.st_mode) or bool(attributes & reparse_flag)


def _require_plain_directory(path: Path) -> None:
    if _is_reparse_or_symlink(path):
        raise StorageLayoutError(f"layout path must not be a link or reparse point: {path}")
    try:
        if not path.is_dir():
            raise StorageLayoutError(f"layout path is not a directory: {path}")
    except OSError as error:
        raise StorageLayoutError(f"cannot inspect layout directory: {path}") from error


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise StorageLayoutError(f"duplicate marker key: {key}")
        result[key] = value
    return result


def _marker_bytes(root_id: str) -> bytes:
    document = {
        "categories": list(DATA_CATEGORIES),
        "kind": _MARKER_KIND,
        "root_id": root_id,
        "schema_version": DATA_ROOT_SCHEMA_VERSION,
    }
    return (
        json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode("utf-8")


def _write_marker_atomically(root: Path, root_id: str) -> None:
    marker = root / DATA_ROOT_MARKER
    temporary = root / f".{DATA_ROOT_MARKER}.{uuid.uuid4().hex}.tmp"
    payload = _marker_bytes(root_id)
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if marker.exists():
            raise StorageLayoutError(f"data root marker already exists: {marker}")
        os.replace(temporary, marker)
    finally:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)


def initialize_data_root(
    value: str | os.PathLike[str],
    *,
    root_id: str | None = None,
) -> DataRootLayout:
    """Explicitly initialize a root, publishing its marker only after all categories exist."""

    root = _absolute_root(value)
    marker = root / DATA_ROOT_MARKER
    if marker.exists():
        return verify_data_root(root)

    if root.exists():
        _require_plain_directory(root)
        try:
            existing = tuple(root.iterdir())
        except OSError as error:
            raise StorageLayoutError(f"cannot inspect candidate data root: {root}") from error
        if existing:
            raise StorageLayoutError("an unmarked data root must be empty")
    else:
        try:
            root.mkdir(parents=True, exist_ok=False)
        except OSError as error:
            raise StorageLayoutError(f"cannot create data root: {root}") from error

    if (root / ".git").exists() or (root / ".hg").exists():
        raise StorageLayoutError("a repository root cannot be used as the data root")

    try:
        selected_id = uuid.uuid4() if root_id is None else uuid.UUID(root_id)
    except (AttributeError, TypeError, ValueError) as error:
        raise StorageLayoutError("root_id must be a canonical UUID") from error
    canonical_id = str(selected_id)
    try:
        for category in DATA_CATEGORIES:
            (root / category).mkdir(exist_ok=False)
        _write_marker_atomically(root, canonical_id)
    except (OSError, StorageLayoutError) as error:
        raise StorageLayoutError(f"data root initialization did not complete: {root}") from error
    return verify_data_root(root)


def verify_data_root(value: str | os.PathLike[str]) -> DataRootLayout:
    """Verify a root without creating, repairing, or following owned links."""

    root = _absolute_root(value)
    _require_plain_directory(root)
    marker = root / DATA_ROOT_MARKER
    if not marker.exists() or _is_reparse_or_symlink(marker):
        raise StorageLayoutError(f"valid data root marker not found: {marker}")
    try:
        metadata = marker.stat()
        if not stat.S_ISREG(metadata.st_mode):
            raise StorageLayoutError("data root marker is not a regular file")
        if metadata.st_size > _MAX_MARKER_BYTES:
            raise StorageLayoutError("data root marker exceeds its byte limit")
        raw = marker.read_bytes()
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys)
    except StorageLayoutError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StorageLayoutError("data root marker cannot be decoded") from error
    if not isinstance(document, dict):
        raise StorageLayoutError("data root marker must be a JSON object")
    if set(document) != {"categories", "kind", "root_id", "schema_version"}:
        raise StorageLayoutError("data root marker has unsupported fields")
    if document["kind"] != _MARKER_KIND:
        raise StorageLayoutError("data root marker kind is not recognized")
    if document["schema_version"] != DATA_ROOT_SCHEMA_VERSION:
        raise StorageLayoutError("data root marker schema version is not supported")
    if document["categories"] != list(DATA_CATEGORIES):
        raise StorageLayoutError("data root categories do not match this version")
    try:
        canonical_id = str(uuid.UUID(document["root_id"]))
    except (AttributeError, TypeError, ValueError) as error:
        raise StorageLayoutError("data root marker has an invalid root_id") from error
    if document["root_id"] != canonical_id:
        raise StorageLayoutError("data root root_id is not canonical")
    for category in DATA_CATEGORIES:
        _require_plain_directory(root / category)
    return DataRootLayout(root=root, root_id=canonical_id)


__all__ = [
    "DATA_CATEGORIES",
    "DATA_ROOT_MARKER",
    "DATA_ROOT_SCHEMA_VERSION",
    "DataRootLayout",
    "StorageLayoutError",
    "initialize_data_root",
    "verify_data_root",
]
