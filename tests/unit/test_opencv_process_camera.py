from __future__ import annotations

import json
from pathlib import Path
from threading import Thread
from typing import Any

import anyio
import pytest
from mcp import Client
from mcp.types import TextContent

from viskium.adapters.opencv_process_camera import (
    OpenCVProcessCameraBackend,
    OpenCVWorkerState,
    _run_worker,
)
from viskium.agent import AgentReadService, CameraSnapshotProvider, ConsentLedger
from viskium.agent.mcp_server import SNAPSHOT_TOOL_V1, create_mcp_server
from viskium.capture import (
    CaptureDeadlineExceeded,
    CaptureOpenError,
    CaptureOwnershipError,
    CaptureRequest,
    CaptureStateError,
    DeadlineCapability,
    ReadStatus,
)
from viskium.observations import LatestObservationSlot
from viskium.storage import initialize_data_root


class _FakeConnection:
    def __init__(
        self,
        replies: list[object] | None = None,
        *,
        poll_results: list[bool] | None = None,
        incoming: list[object] | None = None,
    ) -> None:
        self.replies = [] if replies is None else list(replies)
        self.poll_results = [] if poll_results is None else list(poll_results)
        self.incoming = [] if incoming is None else list(incoming)
        self.sent: list[object] = []
        self.poll_timeouts: list[float] = []
        self.closed = False
        self.send_error: Exception | None = None

    def poll(self, timeout: float = 0.0) -> bool:
        self.poll_timeouts.append(timeout)
        if self.poll_results:
            return self.poll_results.pop(0)
        return bool(self.replies)

    def recv(self) -> object:
        if self.incoming:
            return self.incoming.pop(0)
        if not self.replies:
            raise EOFError
        return self.replies.pop(0)

    def send(self, obj: object) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(obj)

    def close(self) -> None:
        self.closed = True


class _FakeProcess:
    def __init__(
        self,
        parent: _FakeConnection,
        *,
        cooperative: bool = True,
        stubborn: bool = False,
        start_error: Exception | None = None,
    ) -> None:
        self.daemon = False
        self.parent = parent
        self.cooperative = cooperative
        self.stubborn = stubborn
        self.start_error = start_error
        self.started = False
        self.alive = False
        self.terminate_calls = 0
        self.kill_calls = 0
        self.join_timeouts: list[float | None] = []

    def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.started = True
        self.alive = True

    def is_alive(self) -> bool:
        return self.alive

    def join(self, timeout: float | None = None) -> None:
        self.join_timeouts.append(timeout)
        if self.cooperative and ("close",) in self.parent.sent:
            self.alive = False

    def terminate(self) -> None:
        self.terminate_calls += 1
        if not self.stubborn:
            self.alive = False

    def kill(self) -> None:
        self.kill_calls += 1
        if not self.stubborn:
            self.alive = False


class _FakeContext:
    def __init__(
        self,
        replies: list[object] | None = None,
        *,
        poll_results: list[bool] | None = None,
        cooperative: bool = True,
        stubborn: bool = False,
        start_error: Exception | None = None,
    ) -> None:
        self.parent = _FakeConnection(replies, poll_results=poll_results)
        self.child = _FakeConnection()
        self.process = _FakeProcess(
            self.parent,
            cooperative=cooperative,
            stubborn=stubborn,
            start_error=start_error,
        )
        self.process_kwargs: dict[str, object] | None = None
        self.process_calls = 0

    def Pipe(self, duplex: bool = True) -> tuple[_FakeConnection, _FakeConnection]:
        assert duplex
        return self.parent, self.child

    def Process(self, **kwargs: object) -> _FakeProcess:
        self.process_calls += 1
        self.process_kwargs = kwargs
        return self.process


class _ExitedProcess(_FakeProcess):
    def is_alive(self) -> bool:
        return False


class _ExitedContext(_FakeContext):
    def __init__(self, replies: list[object]) -> None:
        super().__init__(replies)
        self.process = _ExitedProcess(self.parent)


class _RecordingLease:
    def __init__(self) -> None:
        self.acquire_calls = 0
        self.release_calls = 0
        self.held = False

    def acquire(self) -> bool:
        self.acquire_calls += 1
        self.held = True
        return True

    def release(self) -> None:
        self.release_calls += 1
        self.held = False


def _request(*, max_frame_bytes: int = 12) -> CaptureRequest:
    return CaptureRequest(0, 2, 2, 30.0, max_frame_bytes)


def _opened() -> tuple[object, ...]:
    return ("opened", 2, 2, 30.0, 6)


def _backend(context: _FakeContext) -> OpenCVProcessCameraBackend:
    return OpenCVProcessCameraBackend(context=context, monotonic_ns=lambda: 0)


def _worker_state(backend: OpenCVProcessCameraBackend) -> OpenCVWorkerState:
    return backend.worker_state


@pytest.mark.parametrize(
    ("scenario", "expected_outcome", "expected_reason"),
    [
        ("device_open_failed", "unavailable", "device_open_failed"),
        ("capture_read_failed", "unavailable", "capture_read_failed"),
        ("camera_open_timeout", "timeout", "camera_open_timeout"),
        ("camera_worker_exited", "unavailable", "camera_worker_exited"),
    ],
)
def test_fake_opencv_provider_service_mcp_chain_is_bounded_and_releases_resources(
    tmp_path: Path,
    scenario: str,
    expected_outcome: str,
    expected_reason: str,
) -> None:
    if scenario == "device_open_failed":
        context = _FakeContext([("open_error", "device_open_failed")])
    elif scenario == "capture_read_failed":
        context = _FakeContext([_opened(), ("disconnected", "capture_read_failed")])
    elif scenario == "camera_open_timeout":
        context = _FakeContext([], poll_results=[False], cooperative=False)
    else:
        context = _ExitedContext([_opened()])

    request = CaptureRequest(
        device_index=7,
        requested_width=2,
        requested_height=2,
        requested_fps=30.0,
        max_frame_bytes=12,
    )
    backend = OpenCVProcessCameraBackend(context=context, monotonic_ns=lambda: 0)
    lease = _RecordingLease()
    provider = CameraSnapshotProvider(
        backend_factory=lambda: backend,
        capture_request=request,
        warmup_frames=0,
        max_attempts=1,
        minimum_open_interval_seconds=0.0,
        monotonic_ns=lambda: 0,
        lease=lease,
    )
    ledger = ConsentLedger(initialize_data_root(tmp_path / "data"))
    ledger.grant(
        scopes=frozenset({"snapshot.read"}),
        duration_seconds=60,
        snapshot_quota=1,
        sensitivity_ceiling="identifiable",
        now_unix_ns=1_000_000_000,
    )
    service = AgentReadService(
        observations=LatestObservationSlot(),
        consent=ledger,
        snapshot_provider=provider,
        status_provider=lambda: {"state": "ready"},
        unix_time_ns=lambda: 2_000_000_000,
        monotonic_ns=lambda: 0,
    )
    server = create_mcp_server(service)

    async def scenario_call() -> Any:
        async with Client(server) as client:
            return await client.call_tool(
                SNAPSHOT_TOOL_V1,
                {"max_edge_px": 64, "wait_ms": 10},
            )

    result = anyio.run(scenario_call)
    assert not result.is_error
    assert len(result.content) == 1
    assert isinstance(result.content[0], TextContent)
    assert json.loads(result.content[0].text) == {
        "agent_contract": "urn:viskium:agent-read:1",
        "contract": "urn:viskium:mcp:snapshot:1",
        "outcome": expected_outcome,
        "reason_code": expected_reason,
    }
    assert lease.acquire_calls == 1
    assert lease.release_calls == 1
    assert not lease.held
    assert backend.worker_state is OpenCVWorkerState.ABSENT
    assert context.process_kwargs is not None
    process_args = context.process_kwargs["args"]
    assert isinstance(process_args, tuple)
    assert len(process_args) == 2
    passed_request = process_args[1]
    assert isinstance(passed_request, CaptureRequest)
    assert passed_request.device_index == 7


def test_backend_open_read_and_cooperative_close_are_memory_only() -> None:
    context = _FakeContext(
        [
            _opened(),
            ("frame", bytes(range(12)), 2, 2, 6, "bgr24", 1),
        ]
    )
    backend = _backend(context)

    stream = backend.open(_request(), deadline_monotonic_ns=10)
    result = backend.read(deadline_monotonic_ns=10)
    backend.close()

    assert stream.backend_id == "opencv-process"
    assert result.status is ReadStatus.FRAME
    assert result.frame is not None
    assert result.frame.payload == bytes(range(12))
    assert context.parent.sent == [("read",), ("close",)]
    assert context.child.closed
    assert context.process.started
    assert context.process_kwargs is not None
    assert context.process_kwargs["daemon"] is True
    assert context.process.terminate_calls == 0
    assert context.process.kill_calls == 0
    assert context.parent.closed
    assert not backend.is_open
    assert backend.worker_state is OpenCVWorkerState.ABSENT
    assert backend.capabilities.open_deadline is DeadlineCapability.ENFORCED
    assert backend.capabilities.safe_in_process


def test_open_timeout_terminates_worker_and_releases_connections() -> None:
    context = _FakeContext([], poll_results=[False], cooperative=False)
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match="camera_open_timeout"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.process.terminate_calls == 1
    assert context.parent.closed
    assert context.child.closed
    assert not backend.is_open
    assert backend.worker_state is OpenCVWorkerState.ABSENT


@pytest.mark.parametrize(
    ("reply", "reason"),
    [
        ((), "invalid_open_response"),
        ({"opened": True}, "invalid_open_response"),
        (("open_error", "busy"), "busy"),
        (("opened", 3, 3, 30.0, 9), "camera_worker_start_failed"),
        (("opened", True, 2, 30.0, 6), "camera_worker_start_failed"),
    ],
)
def test_open_rejects_failed_or_malformed_worker_responses(reply: object, reason: str) -> None:
    context = _FakeContext([reply], cooperative=False)
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match=reason):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.process.terminate_calls == 1
    assert not backend.is_open


def test_worker_start_failure_is_sanitized_and_child_is_closed() -> None:
    context = _FakeContext(start_error=OSError("private driver detail"))
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match="camera_worker_start_failed"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.child.closed
    assert not backend.is_open


@pytest.mark.parametrize(
    ("reply", "expected", "remains_open"),
    [
        (("disconnected", "unplugged"), ReadStatus.DISCONNECTED, True),
        (("recoverable", "temporary"), ReadStatus.RECOVERABLE_ERROR, True),
        (("fatal", "driver_fault"), ReadStatus.FATAL_ERROR, False),
        ((), ReadStatus.FATAL_ERROR, False),
        ({"frame": True}, ReadStatus.FATAL_ERROR, False),
    ],
)
def test_read_maps_bounded_worker_statuses(
    reply: object,
    expected: ReadStatus,
    remains_open: bool,
) -> None:
    context = _FakeContext([_opened(), reply])
    backend = _backend(context)
    backend.open(_request(), deadline_monotonic_ns=10)

    result = backend.read(deadline_monotonic_ns=10)

    assert result.status is expected
    assert backend.is_open is remains_open
    backend.close()


@pytest.mark.parametrize(
    "frame_reply",
    [
        ("frame", bytearray(12), 2, 2, 6, "bgr24", 1),
        ("frame", bytes(13), 2, 2, 6, "bgr24", 1),
        ("frame", bytes(18), 3, 2, 9, "bgr24", 1),
        ("frame", bytes(12), 2, 2, 6, "rgb24", 1),
        ("frame", bytes(12), 2, 2, 6, "bgr24", -1),
    ],
)
def test_read_fails_closed_for_invalid_or_changed_frames(frame_reply: tuple[object, ...]) -> None:
    context = _FakeContext([_opened(), frame_reply], cooperative=False)
    backend = _backend(context)
    backend.open(_request(max_frame_bytes=24), deadline_monotonic_ns=10)

    result = backend.read(deadline_monotonic_ns=10)

    assert result.status is ReadStatus.FATAL_ERROR
    assert result.reason_code == "invalid_backend_frame"
    assert not backend.is_open


def test_read_timeout_and_dead_worker_fail_closed() -> None:
    timeout_context = _FakeContext([_opened()], poll_results=[True, False], cooperative=False)
    timeout_backend = _backend(timeout_context)
    timeout_backend.open(_request(), deadline_monotonic_ns=10)
    assert timeout_backend.read(deadline_monotonic_ns=10).status is ReadStatus.TIMEOUT
    assert timeout_context.process.terminate_calls == 1

    dead_context = _FakeContext([_opened()])
    dead_backend = _backend(dead_context)
    dead_backend.open(_request(), deadline_monotonic_ns=10)
    dead_context.process.alive = False
    assert dead_backend.read(deadline_monotonic_ns=10).status is ReadStatus.DISCONNECTED
    assert not dead_backend.is_open


def test_expired_and_invalid_deadlines_are_rejected_before_hardware_work() -> None:
    context = _FakeContext([_opened()])
    backend = _backend(context)

    with pytest.raises(CaptureDeadlineExceeded):
        backend.open(_request(), deadline_monotonic_ns=0)
    assert not context.process.started
    with pytest.raises(TypeError, match="integer"):
        backend.open(_request(), deadline_monotonic_ns=True)
    with pytest.raises(ValueError, match="signed int64"):
        backend.open(_request(), deadline_monotonic_ns=-1)


def test_backend_rejects_cross_thread_access() -> None:
    context = _FakeContext([_opened()])
    backend = _backend(context)
    backend.open(_request(), deadline_monotonic_ns=10)
    failures: list[Exception] = []

    def intrude() -> None:
        try:
            backend.read(deadline_monotonic_ns=10)
        except Exception as error:  # test captures ownership failure
            failures.append(error)

    thread = Thread(target=intrude)
    thread.start()
    thread.join(timeout=1)

    assert len(failures) == 1
    assert isinstance(failures[0], CaptureOwnershipError)
    backend.close()


def test_stubborn_worker_reports_stuck_only_after_terminate_and_kill() -> None:
    context = _FakeContext([_opened()], cooperative=False, stubborn=True)
    backend = _backend(context)
    backend.open(_request(), deadline_monotonic_ns=10)

    with pytest.raises(CaptureStateError, match="could not be terminated"):
        backend.close()

    assert context.process.terminate_calls == 1
    assert context.process.kill_calls == 1
    assert context.parent.closed
    assert not backend.is_open
    assert context.process.alive
    assert backend.worker_state is OpenCVWorkerState.STUCK
    assert context.process_calls == 1

    with pytest.raises(CaptureStateError, match="cleanup is incomplete"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.process_calls == 1
    assert backend.worker_state is OpenCVWorkerState.STUCK

    context.process.stubborn = False
    backend.close()

    assert context.process.terminate_calls == 2
    assert context.process.kill_calls == 1
    assert _worker_state(backend) is OpenCVWorkerState.ABSENT
    assert not context.process.alive


@pytest.mark.parametrize("cleanup", [True, 0.0, 2.1, float("inf")])
def test_backend_validates_constructor(cleanup: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        OpenCVProcessCameraBackend(
            context=_FakeContext(),
            cleanup_timeout_seconds=cleanup,  # type: ignore[arg-type]
        )

    with pytest.raises(TypeError, match="Pipe and Process"):
        OpenCVProcessCameraBackend(context=object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="callable"):
        OpenCVProcessCameraBackend(context=_FakeContext(), monotonic_ns=0)  # type: ignore[arg-type]


class _FakeFrame:
    shape = (2, 2, 3)
    dtype = "uint8"

    def __init__(self, payload: bytes = bytes(12)) -> None:
        self.payload = payload

    def tobytes(self, *, order: str) -> bytes:
        assert order == "C"
        return self.payload


class _FakeCapture:
    def __init__(
        self,
        *,
        opened: bool = True,
        reads: list[object] | None = None,
    ) -> None:
        self.opened = opened
        self.reads = [] if reads is None else list(reads)
        self.released = False
        self.set_calls: list[tuple[int, float]] = []

    def isOpened(self) -> bool:
        return self.opened

    def set(self, property_id: int, value: float) -> bool:
        self.set_calls.append((property_id, value))
        return True

    def get(self, property_id: int) -> float:
        return {1: 2.0, 2: 2.0, 3: 30.0}[property_id]

    def read(self) -> object:
        value = self.reads.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    def release(self) -> None:
        self.released = True


class _FakeCV2:
    CAP_PROP_FRAME_WIDTH = 1
    CAP_PROP_FRAME_HEIGHT = 2
    CAP_PROP_FPS = 3

    def __init__(self, capture: _FakeCapture) -> None:
        self.capture = capture
        self.device_indices: list[int] = []

    def VideoCapture(self, device_index: int) -> _FakeCapture:
        self.device_indices.append(device_index)
        return self.capture


def test_worker_negotiates_reads_and_releases_capture() -> None:
    connection = _FakeConnection(incoming=[("read",), ("close",)])
    capture = _FakeCapture(reads=[(True, _FakeFrame(bytes(range(12))))])
    cv2 = _FakeCV2(capture)

    _run_worker(connection, _request(), cv2)

    assert connection.sent[0] == _opened()
    frame_message = connection.sent[1]
    assert isinstance(frame_message, tuple)
    assert frame_message[:6] == (
        "frame",
        bytes(range(12)),
        2,
        2,
        6,
        "bgr24",
    )
    assert isinstance(frame_message[6], int)
    assert capture.released
    assert connection.closed
    assert cv2.device_indices == [0]


@pytest.mark.parametrize(
    ("capture", "incoming", "terminal"),
    [
        (_FakeCapture(opened=False), [], ("open_error", "device_open_failed")),
        (
            _FakeCapture(reads=[RuntimeError("driver detail")]),
            [("read",), ("close",)],
            ("recoverable", "capture_read_error"),
        ),
        (
            _FakeCapture(reads=[(False, None)]),
            [("read",), ("close",)],
            ("disconnected", "capture_read_failed"),
        ),
        (
            _FakeCapture(reads=[(True, object())]),
            [("read",)],
            ("fatal", "invalid_backend_frame"),
        ),
        (
            _FakeCapture(),
            [("unexpected",)],
            ("fatal", "invalid_worker_command"),
        ),
    ],
)
def test_worker_sanitizes_backend_failures(
    capture: _FakeCapture,
    incoming: list[object],
    terminal: tuple[str, str],
) -> None:
    connection = _FakeConnection(incoming=incoming)

    _run_worker(connection, _request(), _FakeCV2(capture))

    assert terminal in connection.sent
    assert capture.released
    assert connection.closed
