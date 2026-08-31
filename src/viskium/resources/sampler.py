"""Read-only, failure-tolerant host resource sampling."""

from __future__ import annotations

import ctypes
import os
import shutil
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .budget import ResourceSnapshot

type MemoryReader = Callable[[], tuple[int | None, int | None]]
type DiskReader = Callable[[Path], tuple[int, int, int]]


class _MemoryStatusEx(ctypes.Structure):
    _fields_ = [
        ("length", ctypes.c_uint32),
        ("memory_load", ctypes.c_uint32),
        ("total_physical", ctypes.c_uint64),
        ("available_physical", ctypes.c_uint64),
        ("total_page_file", ctypes.c_uint64),
        ("available_page_file", ctypes.c_uint64),
        ("total_virtual", ctypes.c_uint64),
        ("available_virtual", ctypes.c_uint64),
        ("available_extended_virtual", ctypes.c_uint64),
    ]


class _ProcessMemoryCountersEx(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_uint32),
        ("page_fault_count", ctypes.c_uint32),
        ("peak_working_set_size", ctypes.c_size_t),
        ("working_set_size", ctypes.c_size_t),
        ("quota_peak_paged_pool_usage", ctypes.c_size_t),
        ("quota_paged_pool_usage", ctypes.c_size_t),
        ("quota_peak_non_paged_pool_usage", ctypes.c_size_t),
        ("quota_non_paged_pool_usage", ctypes.c_size_t),
        ("pagefile_usage", ctypes.c_size_t),
        ("peak_pagefile_usage", ctypes.c_size_t),
        ("private_usage", ctypes.c_size_t),
    ]


def _load_windows_library(name: str) -> Any:  # pragma: no cover
    ctypes_module: Any = ctypes
    return ctypes_module.WinDLL(name, use_last_error=True)


def _windows_memory_snapshot(
    *,
    library_loader: Callable[[str], Any] = _load_windows_library,
) -> tuple[int | None, int | None]:  # pragma: no cover
    """Return current RSS and available physical memory on Windows."""

    try:
        kernel32 = library_loader("kernel32.dll")
        psapi = library_loader("psapi.dll")

        global_memory_status: Any = kernel32.GlobalMemoryStatusEx
        global_memory_status.argtypes = [ctypes.POINTER(_MemoryStatusEx)]
        global_memory_status.restype = ctypes.c_int32

        get_current_process: Any = kernel32.GetCurrentProcess
        get_current_process.argtypes = []
        get_current_process.restype = ctypes.c_void_p

        get_process_memory_info: Any = psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_ProcessMemoryCountersEx),
            ctypes.c_uint32,
        ]
        get_process_memory_info.restype = ctypes.c_int32

        memory = _MemoryStatusEx()
        memory.length = ctypes.sizeof(_MemoryStatusEx)
        if not global_memory_status(ctypes.byref(memory)):
            return None, None
        available_memory = int(memory.available_physical)

        counters = _ProcessMemoryCountersEx()
        counters.cb = ctypes.sizeof(_ProcessMemoryCountersEx)
        process = get_current_process()
        if not process or not get_process_memory_info(
            process,
            ctypes.byref(counters),
            counters.cb,
        ):
            return None, available_memory
        process_rss = int(counters.working_set_size)
        return (process_rss if process_rss > 0 else None), available_memory
    except Exception:
        return None, None


def _proc_memory_snapshot() -> tuple[int | None, int | None]:  # pragma: no cover
    """Return current RSS and available memory from bounded Linux procfs reads."""

    rss_bytes: int | None = None
    available_bytes: int | None = None
    try:
        with Path("/proc/self/statm").open("r", encoding="ascii") as stream:
            document = stream.read(4_097)
        if len(document) <= 4_096:
            fields = document.split()
            if len(fields) >= 2:
                sysconf: Any = vars(os)["sysconf"]
                page_size = int(sysconf("SC_PAGE_SIZE"))
                rss_bytes = int(fields[1]) * page_size
    except (OSError, ValueError):
        pass

    try:
        with Path("/proc/meminfo").open("r", encoding="ascii") as stream:
            document = stream.read(65_537)
        if len(document) <= 65_536:
            for line in document.splitlines():
                if line.startswith("MemAvailable:"):
                    available_bytes = int(line.split()[1]) * 1_024
                    break
    except (OSError, ValueError, IndexError):
        pass
    return rss_bytes, available_bytes


def default_memory_snapshot() -> tuple[int | None, int | None]:
    """Read memory without importing a runtime dependency."""

    if sys.platform == "win32":
        return _windows_memory_snapshot()
    if sys.platform.startswith("linux"):
        return _proc_memory_snapshot()
    return None, None


def _nearest_existing_directory(path: Path) -> Path | None:
    candidate = path
    while True:
        try:
            if candidate.is_dir():
                return candidate
        except OSError:
            return None
        if candidate.parent == candidate:
            return None
        candidate = candidate.parent


@dataclass(frozen=True, slots=True)
class ResourceSample:
    """One sample plus bounded reason codes for unavailable measurements."""

    snapshot: ResourceSnapshot
    errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if isinstance(self.errors, (str, bytes)):
            raise TypeError("errors must be an iterable of strings")
        normalized = tuple(self.errors)
        if len(normalized) > 4:
            raise ValueError("resource sample has too many errors")
        if any(not isinstance(error, str) or not error for error in normalized):
            raise ValueError("resource errors must be non-empty strings")
        object.__setattr__(self, "errors", normalized)


class ResourceSampler:
    """Sample process memory and the selected local volume without writing."""

    def __init__(
        self,
        data_root: Path,
        *,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        memory_reader: MemoryReader = default_memory_snapshot,
        disk_reader: DiskReader = shutil.disk_usage,
    ) -> None:
        if not isinstance(data_root, Path):
            raise TypeError("data_root must be a Path")
        self._data_root = data_root
        self._monotonic_ns = monotonic_ns
        self._memory_reader = memory_reader
        self._disk_reader = disk_reader

    def sample(self, *, queue_bytes: int = 0, queue_count: int = 0) -> ResourceSample:
        errors: list[str] = []
        try:
            process_rss, available_memory = self._memory_reader()
        except (OSError, RuntimeError, ValueError):
            process_rss, available_memory = None, None
            errors.append("memory_probe_failed")

        disk_free: int | None = None
        probe_path = _nearest_existing_directory(self._data_root)
        if probe_path is None:
            errors.append("disk_probe_path_unavailable")
        else:
            try:
                _total, _used, disk_free = self._disk_reader(probe_path)
            except (OSError, RuntimeError, ValueError):
                errors.append("disk_probe_failed")

        snapshot = ResourceSnapshot(
            monotonic_ns=self._monotonic_ns(),
            process_rss_bytes=process_rss,
            available_memory_bytes=available_memory,
            disk_free_bytes=disk_free,
            queue_bytes=queue_bytes,
            queue_count=queue_count,
        )
        return ResourceSample(snapshot=snapshot, errors=tuple(errors))


__all__ = [
    "ResourceSample",
    "ResourceSampler",
    "default_memory_snapshot",
]
