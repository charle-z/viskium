"""Bounded contracts for camera capture without a hardware dependency."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

MAX_CAPTURE_FRAME_BYTES = 33_554_432
MAX_CAPTURE_DIMENSION = 8_192
MAX_CAPTURE_FPS = 240.0
MAX_CAPTURE_TIMEOUT_NS = 60_000_000_000
MAX_CAPTURE_COOLDOWN_NS = 300_000_000_000
MAX_REOPEN_ATTEMPTS = 16
MAX_WARMUP_FRAMES = 300
MAX_REASON_CODE_CHARS = 128


class CameraState(StrEnum):
    CLOSED = "closed"
    OPENING = "opening"
    WARMING = "warming"
    STREAMING = "streaming"
    DEGRADED = "degraded"
    COOLDOWN = "cooldown"
    STOPPING = "stopping"
    FAILED = "failed"
    STUCK = "stuck"


class DeadlineCapability(StrEnum):
    ENFORCED = "enforced"
    BEST_EFFORT = "best_effort"
    NONE = "none"


class ReadStatus(StrEnum):
    FRAME = "frame"
    TIMEOUT = "timeout"
    DISCONNECTED = "disconnected"
    RECOVERABLE_ERROR = "recoverable_error"
    FATAL_ERROR = "fatal_error"


class CaptureError(RuntimeError):
    """Base class for capture-boundary failures."""


class CaptureDeadlineExceeded(CaptureError):
    """Raised before an operation whose monotonic deadline has expired."""


class CaptureOwnershipError(CaptureError):
    """Raised when a non-owner thread accesses a backend instance."""


class CaptureStateError(CaptureError):
    """Raised when an operation is invalid for the backend lifecycle state."""


class CaptureOpenError(CaptureError):
    """Raised for a scripted or backend-specific failure to open a device."""


def _require_integer(
    value: object,
    field_name: str,
    *,
    minimum: int,
    maximum: int | None = None,
) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field_name} must not exceed {maximum}")


def _require_finite_float(
    value: object,
    field_name: str,
    *,
    minimum_exclusive: float,
    maximum: float,
) -> None:
    if isinstance(value, bool) or not isinstance(value, float):
        raise TypeError(f"{field_name} must be a float")
    if not math.isfinite(value) or value <= minimum_exclusive:
        raise ValueError(f"{field_name} must be finite and greater than {minimum_exclusive}")
    if value > maximum:
        raise ValueError(f"{field_name} must not exceed {maximum}")


def _require_reason_code(value: object, field_name: str = "reason_code") -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    if len(value) > MAX_REASON_CODE_CHARS:
        raise ValueError(f"{field_name} exceeds {MAX_REASON_CODE_CHARS} characters")


@dataclass(frozen=True, slots=True)
class CaptureCapabilities:
    open_deadline: DeadlineCapability
    read_deadline: DeadlineCapability
    cooperative_close: bool

    def __post_init__(self) -> None:
        if not isinstance(self.open_deadline, DeadlineCapability):
            raise TypeError("open_deadline must be a DeadlineCapability")
        if not isinstance(self.read_deadline, DeadlineCapability):
            raise TypeError("read_deadline must be a DeadlineCapability")
        if not isinstance(self.cooperative_close, bool):
            raise TypeError("cooperative_close must be a bool")

    @property
    def safe_in_process(self) -> bool:
        return (
            self.open_deadline is DeadlineCapability.ENFORCED
            and self.read_deadline is DeadlineCapability.ENFORCED
            and self.cooperative_close
        )


@dataclass(frozen=True, slots=True)
class CameraPolicy:
    max_frame_bytes: int
    open_timeout_ns: int
    read_timeout_ns: int
    stale_after_ns: int
    shutdown_timeout_ns: int
    initial_cooldown_ns: int
    maximum_cooldown_ns: int
    minimum_reopen_interval_ns: int
    stable_reset_after_ns: int
    max_reopen_attempts: int
    warmup_frames: int

    def __post_init__(self) -> None:
        _require_integer(
            self.max_frame_bytes,
            "max_frame_bytes",
            minimum=1,
            maximum=MAX_CAPTURE_FRAME_BYTES,
        )
        for field_name in (
            "open_timeout_ns",
            "read_timeout_ns",
            "stale_after_ns",
            "shutdown_timeout_ns",
        ):
            _require_integer(
                getattr(self, field_name),
                field_name,
                minimum=1,
                maximum=MAX_CAPTURE_TIMEOUT_NS,
            )
        for field_name in (
            "initial_cooldown_ns",
            "maximum_cooldown_ns",
            "minimum_reopen_interval_ns",
            "stable_reset_after_ns",
        ):
            _require_integer(
                getattr(self, field_name),
                field_name,
                minimum=1,
                maximum=MAX_CAPTURE_COOLDOWN_NS,
            )
        _require_integer(
            self.max_reopen_attempts,
            "max_reopen_attempts",
            minimum=0,
            maximum=MAX_REOPEN_ATTEMPTS,
        )
        _require_integer(
            self.warmup_frames,
            "warmup_frames",
            minimum=0,
            maximum=MAX_WARMUP_FRAMES,
        )
        if self.stale_after_ns < self.read_timeout_ns:
            raise ValueError("stale_after_ns cannot be shorter than read_timeout_ns")
        if self.shutdown_timeout_ns < self.read_timeout_ns:
            raise ValueError("shutdown_timeout_ns cannot be shorter than read_timeout_ns")
        if self.shutdown_timeout_ns < self.open_timeout_ns:
            raise ValueError("shutdown_timeout_ns cannot be shorter than open_timeout_ns")
        if self.initial_cooldown_ns > self.maximum_cooldown_ns:
            raise ValueError("initial_cooldown_ns cannot exceed maximum_cooldown_ns")
        if self.minimum_reopen_interval_ns > self.maximum_cooldown_ns:
            raise ValueError("minimum_reopen_interval_ns cannot exceed maximum_cooldown_ns")

    def cooldown_ns(self, failed_attempts: int) -> int:
        """Return a deterministic, capped exponential retry delay."""

        _require_integer(
            failed_attempts,
            "failed_attempts",
            minimum=1,
            maximum=MAX_REOPEN_ATTEMPTS + 1,
        )
        exponent = min(failed_attempts - 1, MAX_REOPEN_ATTEMPTS)
        multiplier = 1 << exponent
        return min(self.initial_cooldown_ns * multiplier, self.maximum_cooldown_ns)


@dataclass(frozen=True, slots=True)
class CaptureRequest:
    device_index: int
    requested_width: int
    requested_height: int
    requested_fps: float
    max_frame_bytes: int

    def __post_init__(self) -> None:
        _require_integer(self.device_index, "device_index", minimum=0, maximum=1_024)
        _require_integer(
            self.requested_width,
            "requested_width",
            minimum=1,
            maximum=MAX_CAPTURE_DIMENSION,
        )
        _require_integer(
            self.requested_height,
            "requested_height",
            minimum=1,
            maximum=MAX_CAPTURE_DIMENSION,
        )
        _require_finite_float(
            self.requested_fps,
            "requested_fps",
            minimum_exclusive=0.0,
            maximum=MAX_CAPTURE_FPS,
        )
        _require_integer(
            self.max_frame_bytes,
            "max_frame_bytes",
            minimum=1,
            maximum=MAX_CAPTURE_FRAME_BYTES,
        )
        if self.requested_width * self.requested_height > self.max_frame_bytes:
            raise ValueError("requested mode cannot fit inside max_frame_bytes")


def default_capture_request(device_index: int = 0) -> CaptureRequest:
    """Return a low-resource 640x480 at 15 FPS request with a 1 MiB frame budget."""

    return CaptureRequest(
        device_index=device_index,
        requested_width=640,
        requested_height=480,
        requested_fps=15.0,
        max_frame_bytes=1_048_576,
    )


def default_camera_policy() -> CameraPolicy:
    """Return conservative live-camera defaults; every value remains replaceable."""

    return CameraPolicy(
        max_frame_bytes=1_048_576,
        open_timeout_ns=5_000_000_000,
        read_timeout_ns=250_000_000,
        stale_after_ns=2_000_000_000,
        shutdown_timeout_ns=5_000_000_000,
        initial_cooldown_ns=250_000_000,
        maximum_cooldown_ns=30_000_000_000,
        minimum_reopen_interval_ns=1_000_000_000,
        stable_reset_after_ns=30_000_000_000,
        max_reopen_attempts=5,
        warmup_frames=3,
    )


@dataclass(frozen=True, slots=True)
class NegotiatedStream:
    backend_id: str
    width: int
    height: int
    fps: float | None
    pixel_format: str
    stride: int

    def __post_init__(self) -> None:
        _require_reason_code(self.backend_id, "backend_id")
        _require_integer(self.width, "width", minimum=1, maximum=MAX_CAPTURE_DIMENSION)
        _require_integer(self.height, "height", minimum=1, maximum=MAX_CAPTURE_DIMENSION)
        if self.fps is not None:
            _require_finite_float(
                self.fps,
                "fps",
                minimum_exclusive=0.0,
                maximum=MAX_CAPTURE_FPS,
            )
        _require_reason_code(self.pixel_format, "pixel_format")
        _require_integer(self.stride, "stride", minimum=self.width)
        if self.stride * self.height > MAX_CAPTURE_FRAME_BYTES:
            raise ValueError("negotiated frame exceeds the global capture ceiling")


@dataclass(frozen=True, slots=True)
class BackendFrame:
    payload: bytes
    received_monotonic_ns: int
    width: int
    height: int
    stride: int
    pixel_format: str

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes):
            raise TypeError("payload must be immutable bytes")
        _require_integer(
            self.received_monotonic_ns,
            "received_monotonic_ns",
            minimum=0,
        )
        _require_integer(self.width, "width", minimum=1, maximum=MAX_CAPTURE_DIMENSION)
        _require_integer(self.height, "height", minimum=1, maximum=MAX_CAPTURE_DIMENSION)
        _require_integer(self.stride, "stride", minimum=self.width)
        _require_reason_code(self.pixel_format, "pixel_format")
        expected_bytes = self.stride * self.height
        if expected_bytes > MAX_CAPTURE_FRAME_BYTES:
            raise ValueError("frame exceeds the global capture ceiling")
        if len(self.payload) != expected_bytes:
            raise ValueError("payload length must equal stride multiplied by height")


@dataclass(frozen=True, slots=True)
class CaptureRead:
    status: ReadStatus
    frame: BackendFrame | None = None
    reason_code: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, ReadStatus):
            raise TypeError("status must be a ReadStatus")
        if self.status is ReadStatus.FRAME:
            if self.frame is None:
                raise ValueError("a frame result requires a frame")
            if not isinstance(self.frame, BackendFrame):
                raise TypeError("frame must be a BackendFrame")
            if self.reason_code is not None:
                raise ValueError("a frame result cannot include a reason_code")
            return
        if self.frame is not None:
            raise ValueError("non-frame results cannot include a frame")
        if self.reason_code is None:
            raise ValueError("non-frame results require a reason_code")
        _require_reason_code(self.reason_code)


__all__ = [
    "MAX_CAPTURE_COOLDOWN_NS",
    "MAX_CAPTURE_DIMENSION",
    "MAX_CAPTURE_FPS",
    "MAX_CAPTURE_FRAME_BYTES",
    "MAX_CAPTURE_TIMEOUT_NS",
    "MAX_REOPEN_ATTEMPTS",
    "MAX_WARMUP_FRAMES",
    "BackendFrame",
    "CameraPolicy",
    "CameraState",
    "CaptureCapabilities",
    "CaptureDeadlineExceeded",
    "CaptureError",
    "CaptureOpenError",
    "CaptureOwnershipError",
    "CaptureRead",
    "CaptureRequest",
    "CaptureStateError",
    "DeadlineCapability",
    "NegotiatedStream",
    "ReadStatus",
    "default_camera_policy",
    "default_capture_request",
]
