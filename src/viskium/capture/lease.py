"""Camera-device wrapper around the hardened path-based file lease."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Protocol, runtime_checkable

from viskium.core.file_lock import FileLockLease

_MAX_DEVICE_INDEX = 1_024
_LEASE_DIRECTORY_NAME = "viskium-camera-leases"
_LEASE_FILE_PREFIX = "device-"
_LEASE_FILE_SUFFIX = ".lock"


@runtime_checkable
class CameraLease(Protocol):
    """Injectable non-blocking lease contract used by camera owners."""

    def acquire(self) -> bool: ...

    def release(self) -> None: ...


def _validate_device_index(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("device_index must be an integer")
    if not 0 <= value <= _MAX_DEVICE_INDEX:
        raise ValueError(f"device_index must be between 0 and {_MAX_DEVICE_INDEX}")
    return value


def _lock_directory(directory: str | os.PathLike[str] | None) -> Path:
    if directory is None:
        return Path(tempfile.gettempdir()) / _LEASE_DIRECTORY_NAME
    if not isinstance(directory, (str, os.PathLike)):
        raise TypeError("directory must be a path-like value")
    path = Path(directory)
    if not path.is_absolute():
        raise ValueError("directory must be an absolute path")
    return path


class FileCameraLease:
    """A bounded, non-blocking camera lease shared by processes.

    ``directory`` is intended for tests and must be absolute. The default is
    the OS temporary directory, intentionally outside Viskium's data root.
    Lock files remain one-byte rendezvous files; ownership is represented
    solely by the OS lock and therefore is reclaimed when a process exits.
    """

    __slots__ = ("_device_index", "_directory", "_lease")

    def __init__(
        self,
        device_index: int,
        *,
        directory: str | os.PathLike[str] | None = None,
    ) -> None:
        self._device_index = _validate_device_index(device_index)
        self._directory = _lock_directory(directory)
        self._lease = FileLockLease(self.path)

    @property
    def device_index(self) -> int:
        return self._device_index

    @property
    def path(self) -> Path:
        """Return the lock rendezvous path without exposing any owner data."""

        return self._directory / f"{_LEASE_FILE_PREFIX}{self._device_index}{_LEASE_FILE_SUFFIX}"

    @property
    def held(self) -> bool:
        return self._lease.held

    def acquire(self) -> bool:
        """Attempt acquisition immediately; never waits for another owner."""

        try:
            # The camera wrapper owns creation; FileLockLease only validates
            # an already-existing parent before touching the sentinel.
            self._directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError:
            return False
        return self._lease.acquire()

    def release(self) -> None:
        """Release the OS lock and close its handle; idempotent."""

        self._lease.release()


__all__ = ["CameraLease", "FileCameraLease"]
