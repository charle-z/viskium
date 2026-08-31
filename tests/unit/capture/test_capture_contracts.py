from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from viskium.capture import (
    BackendFrame,
    CameraPolicy,
    CameraState,
    CaptureCapabilities,
    CaptureRead,
    CaptureRequest,
    DeadlineCapability,
    NegotiatedStream,
    ReadStatus,
    default_camera_policy,
    default_capture_request,
)
from viskium.capture.contracts import MAX_CAPTURE_FRAME_BYTES


def _policy(**overrides: Any) -> CameraPolicy:
    values: dict[str, Any] = {
        "max_frame_bytes": 1_048_576,
        "open_timeout_ns": 1_000_000_000,
        "read_timeout_ns": 500_000_000,
        "stale_after_ns": 2_000_000_000,
        "shutdown_timeout_ns": 3_000_000_000,
        "initial_cooldown_ns": 1_000_000_000,
        "maximum_cooldown_ns": 30_000_000_000,
        "minimum_reopen_interval_ns": 2_000_000_000,
        "stable_reset_after_ns": 30_000_000_000,
        "max_reopen_attempts": 3,
        "warmup_frames": 2,
    }
    values.update(overrides)
    return CameraPolicy(**values)


def _frame() -> BackendFrame:
    return BackendFrame(
        payload=b"\x01\x02\x03\x04",
        received_monotonic_ns=10,
        width=2,
        height=2,
        stride=2,
        pixel_format="GRAY8",
    )


def test_capture_enums_are_stable_strings() -> None:
    assert str(CameraState.STREAMING) == "streaming"
    assert str(DeadlineCapability.ENFORCED) == "enforced"
    assert str(ReadStatus.DISCONNECTED) == "disconnected"


def test_capabilities_identify_safe_in_process_backends() -> None:
    safe = CaptureCapabilities(
        DeadlineCapability.ENFORCED,
        DeadlineCapability.ENFORCED,
        True,
    )
    unsafe = replace(safe, read_deadline=DeadlineCapability.BEST_EFFORT)

    assert safe.safe_in_process
    assert not unsafe.safe_in_process
    with pytest.raises(TypeError, match="DeadlineCapability"):
        replace(safe, open_deadline="enforced")
    with pytest.raises(TypeError, match="DeadlineCapability"):
        replace(safe, read_deadline="enforced")
    with pytest.raises(TypeError, match="bool"):
        replace(safe, cooperative_close=1)


def test_camera_policy_is_frozen_and_cooldown_is_capped() -> None:
    policy = _policy(
        initial_cooldown_ns=2,
        maximum_cooldown_ns=5,
        minimum_reopen_interval_ns=3,
    )

    assert policy.cooldown_ns(1) == 2
    assert policy.cooldown_ns(2) == 4
    assert policy.cooldown_ns(3) == 5
    with pytest.raises(FrozenInstanceError):
        policy.warmup_frames = 9  # type: ignore[misc]


@pytest.mark.parametrize(
    "overrides",
    [
        {"max_frame_bytes": 0},
        {"max_frame_bytes": 33_554_433},
        {"open_timeout_ns": True},
        {"read_timeout_ns": 0},
        {"stale_after_ns": 400_000_000, "read_timeout_ns": 500_000_000},
        {"shutdown_timeout_ns": 400_000_000, "read_timeout_ns": 500_000_000},
        {
            "read_timeout_ns": 300_000_000,
            "shutdown_timeout_ns": 400_000_000,
            "open_timeout_ns": 500_000_000,
        },
        {"initial_cooldown_ns": 31, "maximum_cooldown_ns": 30},
        {
            "initial_cooldown_ns": 1,
            "minimum_reopen_interval_ns": 31,
            "maximum_cooldown_ns": 30,
        },
        {"stable_reset_after_ns": 300_000_000_001},
        {"max_reopen_attempts": 17},
        {"warmup_frames": 301},
    ],
)
def test_camera_policy_rejects_invalid_or_unbounded_values(overrides: dict[str, Any]) -> None:
    with pytest.raises((TypeError, ValueError)):
        _policy(**overrides)


@pytest.mark.parametrize("failed_attempts", [0, True, 18])
def test_camera_policy_rejects_invalid_retry_attempts(failed_attempts: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        _policy().cooldown_ns(failed_attempts)  # type: ignore[arg-type]


def test_capture_request_and_negotiated_stream_are_bounded() -> None:
    request = CaptureRequest(0, 640, 480, 15.0, 1_048_576)
    stream = NegotiatedStream("fake", 640, 480, 15.0, "GRAY8", 640)

    assert request.requested_width == stream.width
    assert stream.stride * stream.height < request.max_frame_bytes


def test_public_defaults_stay_lightweight_while_4k_raw_is_explicitly_available() -> None:
    request = default_capture_request()
    policy = default_camera_policy()

    assert request == CaptureRequest(0, 640, 480, 15.0, 1_048_576)
    assert policy.max_frame_bytes == 1_048_576
    assert policy.open_timeout_ns == policy.shutdown_timeout_ns == 5_000_000_000
    assert policy.read_timeout_ns == 250_000_000
    assert policy.stale_after_ns == 2_000_000_000
    assert policy.warmup_frames == 3
    assert policy.initial_cooldown_ns == 250_000_000
    assert policy.maximum_cooldown_ns == 30_000_000_000
    assert MAX_CAPTURE_FRAME_BYTES == 33_554_432

    four_k_rgba = NegotiatedStream("camera", 3_840, 2_160, 30.0, "RGBA8", 15_360)
    assert four_k_rgba.stride * four_k_rgba.height == 33_177_600
    with pytest.raises(TypeError, match="device_index"):
        default_capture_request(True)


@pytest.mark.parametrize(
    "overrides",
    [
        {"device_index": -1},
        {"requested_width": 0},
        {"requested_height": 8_193},
        {"requested_fps": 15},
        {"requested_fps": 0.0},
        {"requested_fps": float("nan")},
        {"requested_fps": 241.0},
        {"max_frame_bytes": 10, "requested_width": 4, "requested_height": 4},
    ],
)
def test_capture_request_rejects_invalid_modes(overrides: dict[str, Any]) -> None:
    values: dict[str, Any] = {
        "device_index": 0,
        "requested_width": 2,
        "requested_height": 2,
        "requested_fps": 15.0,
        "max_frame_bytes": 16,
    }
    values.update(overrides)
    with pytest.raises((TypeError, ValueError)):
        CaptureRequest(**values)


def test_backend_frame_and_read_result_enforce_tagged_union() -> None:
    frame = _frame()
    result = CaptureRead(ReadStatus.FRAME, frame=frame)

    assert result.frame is frame
    with pytest.raises(ValueError, match="requires a frame"):
        CaptureRead(ReadStatus.FRAME)
    with pytest.raises(TypeError, match="BackendFrame"):
        CaptureRead(ReadStatus.FRAME, frame=object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cannot include a reason"):
        CaptureRead(ReadStatus.FRAME, frame=frame, reason_code="unexpected")
    with pytest.raises(ValueError, match="cannot include a frame"):
        CaptureRead(ReadStatus.TIMEOUT, frame=frame, reason_code="timeout")
    with pytest.raises(ValueError, match="require a reason"):
        CaptureRead(ReadStatus.TIMEOUT)
    with pytest.raises(TypeError, match="string"):
        CaptureRead(ReadStatus.TIMEOUT, reason_code=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="exceeds"):
        CaptureRead(ReadStatus.TIMEOUT, reason_code="x" * 129)
    with pytest.raises(TypeError, match="ReadStatus"):
        CaptureRead("frame", frame=frame)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "overrides",
    [
        {"payload": bytearray(b"1234")},
        {"received_monotonic_ns": -1},
        {"width": 0},
        {"height": 8_193},
        {"stride": 1},
        {"payload": b"123"},
        {"pixel_format": ""},
    ],
)
def test_backend_frame_rejects_invalid_dense_payload(overrides: dict[str, Any]) -> None:
    values: dict[str, Any] = {
        "payload": b"1234",
        "received_monotonic_ns": 0,
        "width": 2,
        "height": 2,
        "stride": 2,
        "pixel_format": "GRAY8",
    }
    values.update(overrides)
    with pytest.raises((TypeError, ValueError)):
        BackendFrame(**values)


def test_negotiated_stream_allows_unknown_fps_but_enforces_global_frame_ceiling() -> None:
    stream = NegotiatedStream("fake", 2, 2, None, "GRAY8", 2)

    assert stream.fps is None
    with pytest.raises(ValueError, match="global capture ceiling"):
        NegotiatedStream("fake", 1, 2, None, "GRAY8", 16_777_217)
    with pytest.raises(ValueError, match="global capture ceiling"):
        BackendFrame(b"", 0, 1, 2, 16_777_217, "GRAY8")
