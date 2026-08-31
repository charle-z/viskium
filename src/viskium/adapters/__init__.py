"""Dependency-free adapters for Viskium's deterministic foundation."""

from .deterministic_processor import DeterministicProcessor
from .fake_camera import FakeCameraBackend
from .memory_store import MemoryStore
from .opencv_process_camera import OpenCVProcessCameraBackend, OpenCVWorkerState
from .synthetic import SyntheticSource

__all__ = [
    "DeterministicProcessor",
    "FakeCameraBackend",
    "MemoryStore",
    "OpenCVProcessCameraBackend",
    "OpenCVWorkerState",
    "SyntheticSource",
]
