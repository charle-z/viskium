"""Hardened, non-blocking advisory file leases.

The lease uses an operating-system lock rather than lock-file contents. The
one-byte sentinel is only a stable rendezvous point; no frame data, source
identifiers, or process metadata are written to disk. Callers own directory
creation and must provide an absolute path whose parent already exists.
"""

from __future__ import annotations

import errno
import importlib
import os
import stat
from pathlib import Path
from threading import Lock
from typing import Any

_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
_REGISTRY_LOCK = Lock()
_HELD_LEASES: dict[Path, FileLockLease] = {}


def _is_reparse_point(file_stat: os.stat_result) -> bool:
    return bool(getattr(file_stat, "st_file_attributes", 0) & _REPARSE_POINT)


def _validate_parent(path: Path) -> None:
    try:
        file_stat = os.lstat(path)
    except OSError:
        raise
    if not stat.S_ISDIR(file_stat.st_mode) or stat.S_ISLNK(file_stat.st_mode):
        raise OSError(errno.ENOTDIR, "file lease parent is not a real directory")
    if _is_reparse_point(file_stat):
        raise OSError(errno.ELOOP, "file lease parent is a reparse point")
    if os.name != "nt" and file_stat.st_mode & 0o077:
        raise OSError(errno.EACCES, "file lease parent is not private")


def _open_lock_file(path: Path) -> int:
    flags = os.O_RDWR
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    created = False
    try:
        # O_EXCL makes first sentinel creation atomic and prevents an
        # existing symlink/reparse point from being followed. Existing paths
        # are never resized or otherwise mutated.
        fd = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
        created = True
    except FileExistsError:
        file_stat = os.lstat(path)
        if (
            not stat.S_ISREG(file_stat.st_mode)
            or stat.S_ISLNK(file_stat.st_mode)
            or _is_reparse_point(file_stat)
        ):
            raise OSError(errno.ELOOP, "file lease path is not a regular file") from None
        if os.name != "nt" and file_stat.st_mode & 0o077:
            raise OSError(errno.EACCES, "file lease file is not private") from None
        if file_stat.st_size != 1:
            raise OSError(errno.EINVAL, "file lease file is not a one-byte sentinel") from None
        fd = os.open(path, flags)
    try:
        if created:
            written = os.write(fd, b"\0")
            if written != 1:
                raise OSError(errno.EIO, "file lease sentinel write was incomplete")
        opened_stat = os.fstat(fd)
        if (
            not stat.S_ISREG(opened_stat.st_mode)
            or _is_reparse_point(opened_stat)
            or opened_stat.st_size != 1
        ):
            raise OSError(errno.ELOOP, "file lease handle is not a regular one-byte file")
        if os.name != "nt" and opened_stat.st_mode & 0o077:
            raise OSError(errno.EACCES, "file lease handle is not private")
    except BaseException:
        os.close(fd)
        raise
    return fd


def _try_os_lock(fd: int) -> bool:
    """Acquire the one-byte sentinel without waiting."""

    if os.fstat(fd).st_size != 1:
        raise OSError(errno.EINVAL, "file lease file is not a one-byte sentinel")

    if os.name == "nt":
        msvcrt: Any = importlib.import_module("msvcrt")

        # msvcrt.locking uses the current file position as the start of the
        # lock range. The sentinel is created exactly once above.
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK, errno.EPERM}:
                return False
            raise
        return True

    fcntl: Any = importlib.import_module("fcntl")

    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        if error.errno in {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK, errno.EDEADLK}:
            return False
        raise
    return True


class FileLockLease:
    """A path-based, non-blocking advisory lease.

    The parent directory must already exist and is validated as a private,
    non-reparse directory. The lock path is never created by this class's
    caller-facing API except for its one-byte sentinel.
    """

    __slots__ = ("_fd", "_path", "_state_lock")

    def __init__(self, path: str | os.PathLike[str]) -> None:
        if not isinstance(path, (str, os.PathLike)):
            raise TypeError("path must be a path-like value")
        normalized_path = Path(path)
        if not normalized_path.is_absolute():
            raise ValueError("path must be an absolute path")
        self._path = normalized_path
        self._fd: int | None = None
        self._state_lock = Lock()

    @property
    def path(self) -> Path:
        """Return the lock rendezvous path without exposing owner data."""

        return self._path

    @property
    def held(self) -> bool:
        with self._state_lock:
            return self._fd is not None

    def acquire(self) -> bool:
        """Attempt acquisition immediately; never waits for another owner."""

        with self._state_lock:
            if self._fd is not None:
                return True
            try:
                _validate_parent(self._path.parent)
                fd = _open_lock_file(self._path)
                try:
                    acquired = _try_os_lock(fd)
                except BaseException:
                    os.close(fd)
                    raise
                if not acquired:
                    os.close(fd)
                    return False
                with _REGISTRY_LOCK:
                    if self._path in _HELD_LEASES:
                        os.close(fd)
                        return False
                    _HELD_LEASES[self._path] = self
            except OSError:
                # A lock that cannot be established is fail-closed and is
                # indistinguishable from contention at the resource boundary.
                return False
            self._fd = fd
            return True

    def release(self) -> None:
        """Release by closing the handle; idempotent after confirmation."""

        with self._state_lock:
            fd = self._fd
            if fd is None:
                return
            # Closing the descriptor is the confirmed release for both flock
            # and msvcrt locks. If close fails, retain ownership state.
            os.close(fd)
            self._fd = None
            with _REGISTRY_LOCK:
                if _HELD_LEASES.get(self._path) is self:
                    _HELD_LEASES.pop(self._path, None)


__all__ = ["FileLockLease"]
