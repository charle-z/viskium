"""Safe, hardware-neutral capture contracts and latest-frame handoff."""

from .contracts import (
    BackendFrame,
    CameraPolicy,
    CameraState,
    CaptureCapabilities,
    CaptureDeadlineExceeded,
    CaptureError,
    CaptureOpenError,
    CaptureOwnershipError,
    CaptureRead,
    CaptureRequest,
    CaptureStateError,
    DeadlineCapability,
    NegotiatedStream,
    ReadStatus,
    default_camera_policy,
    default_capture_request,
)
from .controller import (
    DEFAULT_ESTIMATED_BACKEND_BYTES,
    MAX_ESTIMATED_BACKEND_BYTES,
    BackendFactory,
    CameraController,
    CameraControllerMetrics,
)
from .latest import FrameOffer, LatestFrameSlot, OfferStatus
from .lease import CameraLease, FileCameraLease
from .ports import CaptureBackend, ResourceAdmission

__all__ = [
    "DEFAULT_ESTIMATED_BACKEND_BYTES",
    "MAX_ESTIMATED_BACKEND_BYTES",
    "BackendFactory",
    "BackendFrame",
    "CameraController",
    "CameraControllerMetrics",
    "CameraLease",
    "CameraPolicy",
    "CameraState",
    "CaptureBackend",
    "CaptureCapabilities",
    "CaptureDeadlineExceeded",
    "CaptureError",
    "CaptureOpenError",
    "CaptureOwnershipError",
    "CaptureRead",
    "CaptureRequest",
    "CaptureStateError",
    "DeadlineCapability",
    "FileCameraLease",
    "FrameOffer",
    "LatestFrameSlot",
    "NegotiatedStream",
    "OfferStatus",
    "ReadStatus",
    "ResourceAdmission",
    "default_camera_policy",
    "default_capture_request",
]
