from __future__ import annotations

import errno
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

import viskium.capture.lease as lease_module
from viskium.capture import FileCameraLease
from viskium.core.file_lock import FileLockLease


def _acquire_eventually(
    lease: FileCameraLease | FileLockLease,
    *,
    timeout_seconds: float = 1.0,
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if lease.acquire():
            return True
        time.sleep(0.01)
    return lease.acquire()


def test_file_lease_is_nonblocking_per_device_and_releases_after_close(tmp_path: Path) -> None:
    first = FileCameraLease(0, directory=tmp_path)
    second = FileCameraLease(0, directory=tmp_path)
    other_device = FileCameraLease(1, directory=tmp_path)

    assert first.acquire()
    assert not second.acquire()
    assert other_device.acquire()
    assert first.path == tmp_path / "device-0.lock"
    assert first.path.stat().st_size == 1

    first.release()
    assert second.acquire()
    second.release()
    other_device.release()


def test_file_lease_is_exclusive_across_processes_and_reclaimed_on_exit(tmp_path: Path) -> None:
    child_code = (
        "from viskium.capture import FileCameraLease; "
        "import sys, time; "
        "lease = FileCameraLease(0, directory=sys.argv[1]); "
        "print(int(lease.acquire()), flush=True); time.sleep(30)"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", child_code, str(tmp_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "1"
        contender = FileCameraLease(0, directory=tmp_path)
        assert not contender.acquire()
        child.terminate()
        child.wait(timeout=5)
        assert _acquire_eventually(contender)
        contender.release()
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=5)


def test_file_lock_is_exclusive_across_processes_and_reclaimed_on_exit(tmp_path: Path) -> None:
    lock_path = tmp_path / "generic.lock"
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
        contender = FileLockLease(lock_path)
        assert not contender.acquire()
        child.terminate()
        child.wait(timeout=5)
        assert _acquire_eventually(contender)
        contender.release()
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=5)


def test_file_lock_requires_an_existing_parent(tmp_path: Path) -> None:
    lock_path = tmp_path / "missing" / "generic.lock"

    assert not FileLockLease(lock_path).acquire()
    assert not lock_path.parent.exists()


def test_file_lease_does_not_clear_ownership_when_close_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = FileCameraLease(0, directory=tmp_path)
    contender = FileCameraLease(0, directory=tmp_path)
    assert owner.acquire()

    real_close = lease_module.os.close
    failure_pending = True

    def close_with_one_failure(fd: int) -> None:
        nonlocal failure_pending
        if failure_pending:
            failure_pending = False
            raise OSError(errno.EIO, "injected close failure")
        real_close(fd)

    monkeypatch.setattr(lease_module.os, "close", close_with_one_failure)
    with pytest.raises(OSError, match="injected close failure"):
        owner.release()

    assert owner.held
    assert not contender.acquire()

    monkeypatch.setattr(lease_module.os, "close", real_close)
    owner.release()
    assert not owner.held
    assert contender.acquire()
    contender.release()


def test_file_lease_accepts_an_existing_one_byte_sentinel_without_mutating(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / "device-0.lock"
    lock_path.write_bytes(b"x")
    if os.name != "nt":
        lock_path.chmod(0o600)

    lease = FileLockLease(lock_path)
    assert lease.acquire()
    lease.release()
    assert lock_path.read_bytes() == b"x"


@pytest.mark.parametrize("contents", [b"", b"too-long"])
def test_file_lease_rejects_existing_non_one_byte_sentinel_without_mutating(
    tmp_path: Path, contents: bytes
) -> None:
    lock_path = tmp_path / "device-0.lock"
    lock_path.write_bytes(contents)

    assert not FileLockLease(lock_path).acquire()
    assert lock_path.read_bytes() == contents


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode bits are unavailable")
def test_file_lease_rejects_existing_non_private_sentinel(
    tmp_path: Path,
) -> None:
    lock_path = tmp_path / "device-0.lock"
    lock_path.write_bytes(b"\0")
    lock_path.chmod(0o644)

    assert not FileLockLease(lock_path).acquire()
    assert lock_path.read_bytes() == b"\0"


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlinks are unavailable")
def test_file_lease_fails_closed_for_a_symlinked_lock_path(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.write_bytes(b"sentinel")
    lock_path = tmp_path / "device-0.lock"
    try:
        lock_path.symlink_to(target)
    except OSError as error:
        pytest.skip(f"symlink creation unavailable: {error}")

    lease = FileLockLease(lock_path)
    assert not lease.acquire()
    assert target.read_bytes() == b"sentinel"


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlinks are unavailable")
def test_file_lock_fails_closed_for_a_symlinked_parent(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    parent = tmp_path / "parent"
    try:
        parent.symlink_to(target, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"symlink creation unavailable: {error}")

    assert not FileLockLease(parent / "generic.lock").acquire()
    assert not (target / "generic.lock").exists()


def test_file_lease_fails_closed_for_a_non_regular_lock_path(tmp_path: Path) -> None:
    lock_path = tmp_path / "device-0.lock"
    lock_path.mkdir()
    assert not FileLockLease(lock_path).acquire()
