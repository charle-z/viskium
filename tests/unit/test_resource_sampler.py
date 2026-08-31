from __future__ import annotations

import ctypes
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from viskium.resources.sampler import ResourceSample, ResourceSampler, default_memory_snapshot


class _FakeNativeFunction:
    def __init__(self, implementation: Any) -> None:
        self._implementation = implementation
        self.argtypes: list[object] | None = None
        self.restype: object | None = None

    def __call__(self, *args: object) -> object:
        return self._implementation(*args)


def test_sampler_reports_memory_disk_and_queue_without_writing(tmp_path: Path) -> None:
    sampler = ResourceSampler(
        tmp_path / "future-root",
        monotonic_ns=lambda: 42,
        memory_reader=lambda: (100, 200),
        disk_reader=lambda path: (1_000, 250, 750),
    )

    result = sampler.sample(queue_bytes=12, queue_count=1)

    assert result.errors == ()
    assert result.snapshot.monotonic_ns == 42
    assert result.snapshot.process_rss_bytes == 100
    assert result.snapshot.available_memory_bytes == 200
    assert result.snapshot.disk_free_bytes == 750
    assert result.snapshot.queue_bytes == 12
    assert not (tmp_path / "future-root").exists()


def test_sampler_turns_probe_failures_into_bounded_reason_codes(tmp_path: Path) -> None:
    def fail_memory() -> tuple[int | None, int | None]:
        raise OSError("injected")

    def fail_disk(_path: Path) -> tuple[int, int, int]:
        raise OSError("injected")

    result = ResourceSampler(
        tmp_path,
        memory_reader=fail_memory,
        disk_reader=fail_disk,
    ).sample()

    assert result.snapshot.process_rss_bytes is None
    assert result.snapshot.available_memory_bytes is None
    assert result.snapshot.disk_free_bytes is None
    assert result.errors == ("memory_probe_failed", "disk_probe_failed")


def test_sampler_reports_missing_probe_ancestor(monkeypatch: pytest.MonkeyPatch) -> None:
    from viskium.resources import sampler as module

    monkeypatch.setattr(module, "_nearest_existing_directory", lambda _path: None)
    result = ResourceSampler(Path("missing"), memory_reader=lambda: (1, 2)).sample()

    assert result.errors == ("disk_probe_path_unavailable",)


def test_default_memory_probe_is_safe_on_the_current_platform() -> None:
    rss, available = default_memory_snapshot()

    if sys.platform == "win32":
        assert rss is not None
    if rss is not None:
        assert rss > 0
    assert available is None or available > 0


def test_windows_memory_probe_configures_abi_and_reads_positive_values() -> None:
    from viskium.resources import sampler as module

    def global_memory_status(pointer: object) -> int:
        memory = ctypes.cast(pointer, ctypes.POINTER(module._MemoryStatusEx)).contents
        assert memory.length == ctypes.sizeof(module._MemoryStatusEx)
        memory.available_physical = 8_000
        return 1

    def get_process_memory_info(handle: object, pointer: object, size: object) -> int:
        assert handle == 123
        assert size == ctypes.sizeof(module._ProcessMemoryCountersEx)
        counters = ctypes.cast(
            pointer,
            ctypes.POINTER(module._ProcessMemoryCountersEx),
        ).contents
        assert counters.cb == ctypes.sizeof(module._ProcessMemoryCountersEx)
        counters.working_set_size = 4_000
        return 1

    memory_call = _FakeNativeFunction(global_memory_status)
    handle_call = _FakeNativeFunction(lambda: 123)
    process_call = _FakeNativeFunction(get_process_memory_info)
    libraries = {
        "kernel32.dll": SimpleNamespace(
            GlobalMemoryStatusEx=memory_call,
            GetCurrentProcess=handle_call,
        ),
        "psapi.dll": SimpleNamespace(GetProcessMemoryInfo=process_call),
    }

    result = module._windows_memory_snapshot(library_loader=libraries.__getitem__)

    assert result == (4_000, 8_000)
    assert memory_call.argtypes == [ctypes.POINTER(module._MemoryStatusEx)]
    assert memory_call.restype is ctypes.c_int32
    assert handle_call.argtypes == []
    assert handle_call.restype is ctypes.c_void_p
    assert process_call.argtypes == [
        ctypes.c_void_p,
        ctypes.POINTER(module._ProcessMemoryCountersEx),
        ctypes.c_uint32,
    ]
    assert process_call.restype is ctypes.c_int32


def test_windows_memory_probe_preserves_available_memory_when_rss_fails() -> None:
    from viskium.resources import sampler as module

    def global_memory_status(pointer: object) -> int:
        memory = ctypes.cast(pointer, ctypes.POINTER(module._MemoryStatusEx)).contents
        memory.available_physical = 8_000
        return 1

    libraries = {
        "kernel32.dll": SimpleNamespace(
            GlobalMemoryStatusEx=_FakeNativeFunction(global_memory_status),
            GetCurrentProcess=_FakeNativeFunction(lambda: 123),
        ),
        "psapi.dll": SimpleNamespace(GetProcessMemoryInfo=_FakeNativeFunction(lambda *_args: 0)),
    }

    assert module._windows_memory_snapshot(library_loader=libraries.__getitem__) == (None, 8_000)


def test_windows_memory_probe_fails_closed_on_loader_error() -> None:
    from viskium.resources import sampler as module

    def fail_loader(_name: str) -> object:
        raise RuntimeError("injected loader failure")

    assert module._windows_memory_snapshot(library_loader=fail_loader) == (None, None)


@pytest.mark.parametrize(
    "errors",
    [
        "one",
        ("",),
        tuple(str(index) for index in range(5)),
    ],
)
def test_resource_sample_rejects_invalid_error_metadata(errors: Any) -> None:
    from viskium.resources.budget import ResourceSnapshot

    snapshot = ResourceSnapshot(0, None, None, None, 0, 0)
    with pytest.raises((TypeError, ValueError)):
        ResourceSample(snapshot=snapshot, errors=errors)


def test_sampler_rejects_non_path_root() -> None:
    with pytest.raises(TypeError, match="Path"):
        ResourceSampler("data")  # type: ignore[arg-type]
