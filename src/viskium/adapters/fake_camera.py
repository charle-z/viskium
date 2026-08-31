"""Scriptable camera backend for lifecycle and fault tests without hardware."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Iterable
from itertools import islice
from threading import get_ident
from typing import TYPE_CHECKING

from viskium.capture import (
    CaptureBackend,
    CaptureCapabilities,
    CaptureDeadlineExceeded,
    CaptureOpenError,
    CaptureOwnershipError,
    CaptureRead,
    CaptureRequest,
    CaptureStateError,
    DeadlineCapability,
    NegotiatedStream,
    ReadStatus,
)
from viskium.capture.contracts import MAX_CAPTURE_FRAME_BYTES, MAX_REASON_CODE_CHARS

_MAX_SCRIPTED_READS = 10_000
_MAX_SCRIPTED_OPEN_FAILURES = 32
_MAX_SCRIPTED_PAYLOAD_BYTES = MAX_CAPTURE_FRAME_BYTES


def _bounded_tuple[T](values: Iterable[T], *, maximum: int, field_name: str) -> tuple[T, ...]:
    bounded = tuple(islice(values, maximum + 1))
    if len(bounded) > maximum:
        raise ValueError(f"{field_name} exceeds {maximum} entries")
    return bounded


class FakeCameraBackend:
    """A deterministic backend that enforces deadlines and exclusive ownership."""

    def __init__(
        self,
        *,
        reads: Iterable[CaptureRead] = (),
        open_failure_reasons: Iterable[str] = (),
        negotiated_stream: NegotiatedStream | None = None,
        now_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        scripted_reads = _bounded_tuple(
            reads,
            maximum=_MAX_SCRIPTED_READS,
            field_name="reads",
        )
        if any(not isinstance(item, CaptureRead) for item in scripted_reads):
            raise TypeError("reads entries must be CaptureRead instances")
        retained_payload_bytes = sum(
            len(item.frame.payload) for item in scripted_reads if item.frame is not None
        )
        if retained_payload_bytes > _MAX_SCRIPTED_PAYLOAD_BYTES:
            raise ValueError(f"scripted frame payloads exceed {_MAX_SCRIPTED_PAYLOAD_BYTES} bytes")
        failure_reasons = _bounded_tuple(
            open_failure_reasons,
            maximum=_MAX_SCRIPTED_OPEN_FAILURES,
            field_name="open_failure_reasons",
        )
        for reason in failure_reasons:
            if not isinstance(reason, str):
                raise TypeError("open failure reasons must be strings")
            if not reason or not reason.strip():
                raise ValueError("open failure reasons must not be empty")
            if len(reason) > MAX_REASON_CODE_CHARS:
                raise ValueError(
                    f"open failure reasons must not exceed {MAX_REASON_CODE_CHARS} characters"
                )
        if negotiated_stream is not None and not isinstance(negotiated_stream, NegotiatedStream):
            raise TypeError("negotiated_stream must be a NegotiatedStream")
        if not callable(now_ns):
            raise TypeError("now_ns must be callable")

        self._reads = deque(scripted_reads)
        self._open_failures = deque(failure_reasons)
        self._negotiated_stream = negotiated_stream
        self._now_ns = now_ns
        self._owner_thread_id: int | None = None
        self._is_open = False
        self._open_calls = 0
        self._read_calls = 0
        self._close_calls = 0

    @property
    def capabilities(self) -> CaptureCapabilities:
        return CaptureCapabilities(
            open_deadline=DeadlineCapability.ENFORCED,
            read_deadline=DeadlineCapability.ENFORCED,
            cooperative_close=True,
        )

    @property
    def owner_thread_id(self) -> int | None:
        return self._owner_thread_id

    @property
    def is_open(self) -> bool:
        return self._is_open

    @property
    def open_calls(self) -> int:
        return self._open_calls

    @property
    def read_calls(self) -> int:
        return self._read_calls

    @property
    def close_calls(self) -> int:
        return self._close_calls

    def _claim_or_check_owner(self) -> None:
        current_thread_id = get_ident()
        if self._owner_thread_id is None:
            self._owner_thread_id = current_thread_id
        elif self._owner_thread_id != current_thread_id:
            raise CaptureOwnershipError("fake backend accessed outside its owner thread")

    def _check_deadline(self, deadline_monotonic_ns: int) -> None:
        if isinstance(deadline_monotonic_ns, bool) or not isinstance(deadline_monotonic_ns, int):
            raise TypeError("deadline_monotonic_ns must be an integer")
        if deadline_monotonic_ns < 0:
            raise ValueError("deadline_monotonic_ns must be non-negative")
        now = self._now_ns()
        if isinstance(now, bool) or not isinstance(now, int) or now < 0:
            raise ValueError("now_ns must return a non-negative integer")
        if now >= deadline_monotonic_ns:
            raise CaptureDeadlineExceeded("capture deadline has expired")

    def open(
        self,
        request: CaptureRequest,
        *,
        deadline_monotonic_ns: int,
    ) -> NegotiatedStream:
        if not isinstance(request, CaptureRequest):
            raise TypeError("request must be a CaptureRequest")
        self._claim_or_check_owner()
        self._check_deadline(deadline_monotonic_ns)
        if self._is_open:
            raise CaptureStateError("fake backend is already open")
        self._open_calls += 1
        if self._open_failures:
            raise CaptureOpenError(self._open_failures.popleft())

        negotiated = self._negotiated_stream or NegotiatedStream(
            backend_id="fake-camera",
            width=request.requested_width,
            height=request.requested_height,
            fps=request.requested_fps,
            pixel_format="GRAY8",
            stride=request.requested_width,
        )
        if negotiated.stride * negotiated.height > request.max_frame_bytes:
            raise CaptureOpenError("negotiated_frame_exceeds_request_limit")
        self._is_open = True
        return negotiated

    def read(self, *, deadline_monotonic_ns: int) -> CaptureRead:
        self._claim_or_check_owner()
        self._check_deadline(deadline_monotonic_ns)
        if not self._is_open:
            raise CaptureStateError("fake backend is not open")
        self._read_calls += 1
        if self._reads:
            return self._reads.popleft()
        return CaptureRead(ReadStatus.TIMEOUT, reason_code="fake_script_exhausted")

    def close(self) -> None:
        if self._owner_thread_id is None:
            return
        self._claim_or_check_owner()
        if not self._is_open:
            return
        self._is_open = False
        self._close_calls += 1


if TYPE_CHECKING:
    _backend_contract: CaptureBackend = FakeCameraBackend()


__all__ = ["FakeCameraBackend"]
