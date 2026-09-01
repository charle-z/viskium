from __future__ import annotations

import builtins
import json
import os
import sys
import time
from pathlib import Path
from threading import Thread
from typing import Any

import anyio
import pytest
from mcp import Client
from mcp.types import TextContent

import viskium.adapters.opencv_process_camera as camera_module
from viskium._camera_worker_bootstrap import (
    _camera_worker_bootstrap,
)
from viskium._worker_transport import launch_socket_subprocess
from viskium.adapters.opencv_process_camera import (
    OpenCVProcessCameraBackend,
    OpenCVWorkerState,
    _opencv_worker,
    _process_has_exited,
    _receive_worker_message,
    _run_worker,
    _select_videoio_api,
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
    VideoIOPreference,
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
        if self.start_error is not None and not self.started:
            raise AssertionError("is_alive before start")
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
        process_error: Exception | None = None,
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
        self.process_error = process_error

    def Pipe(self, duplex: bool = True) -> tuple[_FakeConnection, _FakeConnection]:
        assert duplex
        return self.parent, self.child

    def Process(self, **kwargs: object) -> _FakeProcess:
        self.process_calls += 1
        self.process_kwargs = kwargs
        if self.process_error is not None:
            raise self.process_error
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
    return ("opened",)


def _worker_started() -> tuple[object, ...]:
    return ("worker_started",)


def _backend_ready(api: int | None = None) -> tuple[object, ...]:
    selected_api = (700 if os.name == "nt" else 0) if api is None else api
    return ("backend_ready", selected_api)


def _configured() -> tuple[object, ...]:
    return ("configured", 2, 2, 30.0, 6)


def _backend(context: _FakeContext) -> OpenCVProcessCameraBackend:
    return OpenCVProcessCameraBackend(context=context, monotonic_ns=lambda: 0)


def _worker_state(backend: OpenCVProcessCameraBackend) -> OpenCVWorkerState:
    return backend.worker_state


def test_videoio_selection_is_directshow_on_windows_and_auto_elsewhere() -> None:
    cv2 = _FakeCV2(_FakeCapture(), available_backends={1400, 700})

    assert _select_videoio_api(cv2, VideoIOPreference.AUTO, platform_name="nt") == 1400
    assert _select_videoio_api(cv2, VideoIOPreference.MEDIA_FOUNDATION, platform_name="nt") == 1400
    assert _select_videoio_api(cv2, VideoIOPreference.DIRECTSHOW, platform_name="nt") == 700
    assert _select_videoio_api(cv2, VideoIOPreference.AUTO, platform_name="posix") == 0

    with pytest.raises(RuntimeError, match="directshow_unavailable"):
        _select_videoio_api(cv2, VideoIOPreference.DIRECTSHOW, platform_name="posix")


def test_videoio_selection_does_not_fallback_when_directshow_is_unavailable() -> None:
    class NoDirectShow:
        CAP_ANY = 0

    with pytest.raises(RuntimeError, match="directshow_unavailable"):
        _select_videoio_api(NoDirectShow(), VideoIOPreference.AUTO, platform_name="nt")


def test_videoio_auto_prefers_media_foundation_without_opening_a_device() -> None:
    cv2 = _FakeCV2(_FakeCapture(), available_backends={1400, 700})

    assert _select_videoio_api(cv2, VideoIOPreference.AUTO, platform_name="nt") == 1400
    assert cv2.videoio_registry.calls == [1400]
    assert cv2.video_capture_calls == []


def test_videoio_explicit_media_foundation_reports_its_own_unavailability() -> None:
    cv2 = _FakeCV2(_FakeCapture(), available_backends={700})

    with pytest.raises(RuntimeError, match="mediafoundation_unavailable"):
        _select_videoio_api(cv2, VideoIOPreference.MEDIA_FOUNDATION, platform_name="nt")


def test_videoio_explicit_windows_backends_are_unavailable_on_posix() -> None:
    cv2 = _FakeCV2(_FakeCapture())

    with pytest.raises(RuntimeError, match="mediafoundation_unavailable"):
        _select_videoio_api(cv2, VideoIOPreference.MEDIA_FOUNDATION, platform_name="posix")


def test_videoio_selection_rejects_invalid_preference_without_opening() -> None:
    with pytest.raises(TypeError, match="VideoIOPreference"):
        _select_videoio_api(_FakeCV2(_FakeCapture()), "auto")  # type: ignore[arg-type]


def test_videoio_selection_rejects_invalid_posix_cap_any() -> None:
    class _InvalidCapAny:
        CAP_ANY = True

    with pytest.raises(RuntimeError, match="videoio_backend_unavailable"):
        _select_videoio_api(_InvalidCapAny(), VideoIOPreference.AUTO, platform_name="posix")


def test_videoio_selection_rejects_missing_windows_registry_api() -> None:
    class _MissingRegistry:
        CAP_MSMF = 1400

    with pytest.raises(RuntimeError, match="mediafoundation_unavailable"):
        _select_videoio_api(
            _MissingRegistry(),
            VideoIOPreference.MEDIA_FOUNDATION,
            platform_name="nt",
        )


def test_videoio_selection_sanitizes_windows_registry_failure() -> None:
    class _RaisingRegistry:
        def hasBackend(self, api: int) -> bool:
            raise RuntimeError("private registry detail")

    class _BrokenRegistryCV2:
        CAP_MSMF = 1400
        videoio_registry = _RaisingRegistry()

    with pytest.raises(RuntimeError, match="mediafoundation_unavailable"):
        _select_videoio_api(
            _BrokenRegistryCV2(),
            VideoIOPreference.MEDIA_FOUNDATION,
            platform_name="nt",
        )


@pytest.mark.parametrize(
    ("scenario", "expected_outcome", "expected_reason"),
    [
        ("device_open_failed", "unavailable", "device_open_failed"),
        ("capture_read_failed", "unavailable", "capture_read_failed"),
        ("camera_worker_start_timeout", "timeout", "camera_worker_start_timeout"),
        ("camera_backend_init_timeout", "timeout", "camera_backend_init_timeout"),
        ("camera_device_open_timeout", "timeout", "camera_device_open_timeout"),
        ("camera_configure_timeout", "timeout", "camera_configure_timeout"),
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
        context = _FakeContext(
            [_worker_started(), _backend_ready(), ("open_error", "device_open_failed")]
        )
    elif scenario == "capture_read_failed":
        context = _FakeContext(
            [
                _worker_started(),
                _backend_ready(),
                _opened(),
                _configured(),
                ("disconnected", "capture_read_failed"),
            ]
        )
    elif scenario == "camera_worker_start_timeout":
        context = _FakeContext([], poll_results=[False], cooperative=False)
    elif scenario == "camera_backend_init_timeout":
        context = _FakeContext([_worker_started()], poll_results=[True, False], cooperative=False)
    elif scenario == "camera_device_open_timeout":
        context = _FakeContext(
            [_worker_started(), _backend_ready()],
            poll_results=[True, True, False],
            cooperative=False,
        )
    elif scenario == "camera_configure_timeout":
        context = _FakeContext(
            [_worker_started(), _backend_ready(), _opened()],
            poll_results=[True, True, True, False],
            cooperative=False,
        )
    else:
        context = _ExitedContext([_worker_started(), _backend_ready(), _opened(), _configured()])

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
            _worker_started(),
            _backend_ready(),
            _opened(),
            _configured(),
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
    assert context.process_kwargs["target"] is _camera_worker_bootstrap
    assert context.process.terminate_calls == 0
    assert context.process.kill_calls == 0
    assert context.parent.closed
    assert not backend.is_open
    assert backend.worker_state is OpenCVWorkerState.ABSENT
    assert backend.capabilities.open_deadline is DeadlineCapability.ENFORCED
    assert backend.capabilities.safe_in_process


def test_camera_worker_subprocess_rejects_malformed_request_before_camera_import() -> None:
    deadline = time.monotonic() + 5.0
    connection, process = launch_socket_subprocess(
        "viskium._camera_worker_subprocess",
        windows=os.name == "nt",
        deadline=deadline,
        monotonic=time.monotonic,
    )
    try:
        assert connection.poll(max(0.0, deadline - time.monotonic()))
        assert connection.recv() == _worker_started()
        connection.send(("request",))
        assert connection.poll(max(0.0, deadline - time.monotonic()))
        assert connection.recv() == ("open_error", "invalid_worker_command")
        process.join(timeout=max(0.0, deadline - time.monotonic()))
        assert not process.is_alive()
        assert process.exitcode == 0
    finally:
        connection.close()
        if process.is_alive():
            process.terminate()
            process.join(timeout=2.0)


def test_camera_worker_subprocess_exits_on_request_channel_eof() -> None:
    deadline = time.monotonic() + 5.0
    connection, process = launch_socket_subprocess(
        "viskium._camera_worker_subprocess",
        windows=os.name == "nt",
        deadline=deadline,
        monotonic=time.monotonic,
    )
    assert connection.poll(max(0.0, deadline - time.monotonic()))
    assert connection.recv() == _worker_started()
    connection.close()
    process.join(timeout=max(0.0, deadline - time.monotonic()))
    assert not process.is_alive()
    assert process.exitcode == 0


def test_bootstrap_sanitizes_cv2_import_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def fail_cv2_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "cv2":
            raise ImportError("private import detail")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fail_cv2_import)
    connection = _FakeConnection()

    _camera_worker_bootstrap(connection, _request())

    assert connection.sent == [_worker_started(), ("open_error", "opencv_unavailable")]
    assert connection.closed


def test_bootstrap_sanitizes_unexpected_import_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def fail_cv2_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "cv2":
            raise RuntimeError("private import detail")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fail_cv2_import)
    connection = _FakeConnection()

    _camera_worker_bootstrap(connection, _request())

    assert connection.sent == [_worker_started(), ("open_error", "opencv_worker_error")]
    assert connection.closed


def test_bootstrap_sanitizes_worker_dispatch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "cv2", object())

    def fail_worker(connection: object, request: object, cv2: object) -> None:
        raise RuntimeError("private dispatch detail")

    monkeypatch.setattr(camera_module, "_run_worker", fail_worker)
    connection = _FakeConnection()

    _camera_worker_bootstrap(connection, _request())

    assert connection.sent == [_worker_started(), ("open_error", "opencv_worker_error")]
    assert connection.closed


def test_worker_liveness_classification_is_bounded_and_content_free() -> None:
    assert not _process_has_exited(None)

    alive_process = _FakeProcess(_FakeConnection())
    alive_process.alive = True
    assert not _process_has_exited(alive_process)
    assert _process_has_exited(_ExitedProcess(_FakeConnection()))

    class _ExitedWithUnknownLiveness:
        def is_alive(self) -> bool:
            raise AssertionError("not attached")

        exitcode = 3

    assert _process_has_exited(_ExitedWithUnknownLiveness())

    class _UnreadableExitcode:
        def is_alive(self) -> bool:
            raise AssertionError("not attached")

        @property
        def exitcode(self) -> None:
            raise AssertionError("not attached")

    assert not _process_has_exited(_UnreadableExitcode())


def test_receive_worker_message_checks_exit_after_deadline() -> None:
    with pytest.raises(CaptureOpenError, match="camera_worker_exited"):
        _receive_worker_message(
            _FakeConnection(),
            process=_ExitedProcess(_FakeConnection()),
            deadline_monotonic_ns=0,
            monotonic_ns=lambda: 0,
            timeout_reason="camera_worker_start_timeout",
            invalid_reason="invalid_open_response",
        )

    with pytest.raises(CaptureOpenError, match="camera_worker_start_timeout"):
        _receive_worker_message(
            _FakeConnection(),
            deadline_monotonic_ns=0,
            monotonic_ns=lambda: 0,
            timeout_reason="camera_worker_start_timeout",
            invalid_reason="invalid_open_response",
        )


def test_exited_worker_before_start_ack_is_reported_without_private_details() -> None:
    context = _ExitedContext([])
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match="camera_worker_exited"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.parent.closed
    assert context.child.closed
    assert not backend.is_open


def test_eof_from_exited_worker_before_start_ack_is_reported_safely() -> None:
    context = _ExitedContext([])
    context.parent.poll_results.append(True)
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match="camera_worker_exited"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.parent.closed
    assert context.child.closed
    assert not backend.is_open


def test_open_timeout_terminates_worker_and_releases_connections() -> None:
    context = _FakeContext([], poll_results=[False], cooperative=False)
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match="camera_worker_start_timeout"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.process.terminate_calls == 1
    assert context.parent.closed
    assert context.child.closed
    assert not backend.is_open
    assert backend.worker_state is OpenCVWorkerState.ABSENT


def test_windows_request_send_failure_is_bounded_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _FakeContext([_worker_started()], cooperative=False)
    context.process.alive = True
    context.parent.send_error = BrokenPipeError("private transport detail")
    monkeypatch.setattr(camera_module.os, "name", "nt")
    monkeypatch.setattr(
        camera_module,
        "_launch_socket_subprocess",
        lambda module, **kwargs: (context.parent, context.process),
    )
    backend = OpenCVProcessCameraBackend(monotonic_ns=lambda: 0)

    with pytest.raises(CaptureOpenError, match="camera_worker_start_failed"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.parent.sent == []
    assert context.process.terminate_calls == 1
    assert context.parent.closed
    assert not backend.is_open


@pytest.mark.parametrize(
    ("replies", "poll_results", "expected_reason"),
    [
        ([], [False], "camera_worker_start_timeout"),
        ([_worker_started()], [True, False], "camera_backend_init_timeout"),
        ([_worker_started(), _backend_ready()], [True, True, False], "camera_device_open_timeout"),
        (
            [_worker_started(), _backend_ready(), _opened()],
            [True, True, True, False],
            "camera_configure_timeout",
        ),
    ],
)
def test_open_timeout_reports_the_last_completed_phase(
    replies: list[object], poll_results: list[bool], expected_reason: str
) -> None:
    context = _FakeContext(replies, poll_results=poll_results, cooperative=False)
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match=expected_reason):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.process.terminate_calls == 1
    assert context.parent.closed
    assert context.child.closed
    assert not backend.is_open


@pytest.mark.parametrize(
    ("replies", "poll_results", "expected_reason"),
    [
        ([], [True], "invalid_open_response"),
        ([_worker_started()], [True, True], "invalid_open_response"),
        ([_worker_started(), _backend_ready()], [True, True, True], "invalid_open_response"),
        (
            [_worker_started(), _backend_ready(), _opened()],
            [True, True, True, True],
            "invalid_configure_response",
        ),
    ],
)
def test_open_eof_is_sanitized_for_each_phase(
    replies: list[object], poll_results: list[bool], expected_reason: str
) -> None:
    context = _FakeContext(replies, poll_results=poll_results, cooperative=False)
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match=expected_reason):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.process.terminate_calls == 1
    assert not backend.is_open


@pytest.mark.parametrize(
    "reply",
    [
        ("backend_ready",),
        ("backend_ready", True),
        ("backend_ready", 700, "extra"),
        ("opened",),
        ("configured", 2, 2, 30.0, 6),
    ],
)
def test_open_rejects_malformed_or_out_of_order_phase_ack(reply: tuple[object, ...]) -> None:
    context = _FakeContext([_worker_started(), reply], cooperative=False)
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match="invalid_open_response"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.process.terminate_calls == 1
    assert not backend.is_open


def test_opencv_worker_acknowledges_start_before_importing_cv2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    fake_cv2 = object()
    calls: list[object] = []

    def fake_run(connection: object, request: object, cv2: object) -> None:
        calls.append(cv2)

    monkeypatch.setitem(sys.modules, "cv2", fake_cv2)
    monkeypatch.setattr(camera_module, "_run_worker", fake_run)

    _opencv_worker(connection, _request())

    assert connection.sent == [_worker_started()]
    assert calls == [fake_cv2]


def test_receive_worker_message_rechecks_deadline_after_recv() -> None:
    connection = _FakeConnection([_worker_started()])
    clock_values = iter([0, 11])

    with pytest.raises(CaptureOpenError, match="camera_worker_start_timeout"):
        _receive_worker_message(
            connection,
            deadline_monotonic_ns=10,
            monotonic_ns=lambda: next(clock_values),
            timeout_reason="camera_worker_start_timeout",
            invalid_reason="invalid_open_response",
        )


@pytest.mark.parametrize(
    ("reply", "reason"),
    [
        ((), "invalid_open_response"),
        ({"opened": True}, "invalid_open_response"),
        (("open_error", "busy"), "busy"),
        (("open_error", "busy", "extra"), "invalid_open_response"),
        (("open_error",), "invalid_open_response"),
        (("unknown", "busy"), "invalid_open_response"),
        (("opened", 3, 3, 30.0, 9), "invalid_open_response"),
        (("opened", True, 2, 30.0, 6), "invalid_open_response"),
        (("configured", 2, 2, 30.0, 6), "invalid_open_response"),
    ],
)
def test_open_rejects_failed_or_malformed_worker_responses(reply: object, reason: str) -> None:
    if isinstance(reply, tuple) and reply and reply[0] == "open_error":
        replies = [_worker_started(), _backend_ready(), reply]
    else:
        replies = [_worker_started(), reply]
    context = _FakeContext(replies, cooperative=False)
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
    assert context.process.terminate_calls == 0
    assert context.process.kill_calls == 0
    assert backend.worker_state is OpenCVWorkerState.ABSENT


def test_process_factory_failure_closes_both_pipe_ends() -> None:
    context = _FakeContext(process_error=RuntimeError("private process detail"))
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match="camera_worker_start_failed"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.parent.closed
    assert context.child.closed
    assert not backend.is_open
    assert backend.worker_state is OpenCVWorkerState.ABSENT


def test_configure_timeout_after_open_ack_uses_one_deadline_and_cleans_up() -> None:
    context = _FakeContext(
        [_worker_started(), _backend_ready(), _opened()],
        poll_results=[True, True, True, False],
        cooperative=False,
    )
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match="camera_configure_timeout"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert len(context.parent.poll_timeouts) == 4
    assert len(set(context.parent.poll_timeouts)) == 1
    assert context.process.terminate_calls == 1
    assert context.parent.closed
    assert context.child.closed
    assert not backend.is_open


def test_eof_between_open_and_configure_is_sanitized_and_cleans_up() -> None:
    context = _FakeContext(
        [_worker_started(), _backend_ready(), _opened()],
        poll_results=[True, True, True, True],
        cooperative=False,
    )
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match="invalid_configure_response"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.process.terminate_calls == 1
    assert not backend.is_open


@pytest.mark.parametrize(
    ("reply", "expected"),
    [
        (("opened",), "invalid_configure_response"),
        (("frame", b"private bytes"), "invalid_configure_response"),
        (("configured", 2, 2, 30.0), "invalid_configure_response"),
        (("configured", 2, 2, 30.0, 5), "camera_configure_failed"),
        (("configured", 3, 2, 30.0, 9), "camera_configure_failed"),
        (("configured", 0, 2, 30.0, 0), "camera_configure_failed"),
        (("configured", 2, 2, float("nan"), 6), "camera_configure_failed"),
        (("configure_error", "driver detail"), "invalid_configure_response"),
        (("configure_error", "camera_configure_failed", "extra"), "invalid_configure_response"),
        (("configure_error",), "invalid_configure_response"),
        (("unknown", "camera_configure_failed"), "invalid_configure_response"),
        ({"configured": True}, "invalid_configure_response"),
        (("configure_error", "camera_configure_failed"), "camera_configure_failed"),
        (
            ("configure_error", "negotiated_mode_exceeds_limit"),
            "negotiated_mode_exceeds_limit",
        ),
    ],
)
def test_invalid_or_out_of_order_configure_messages_fail_closed(
    reply: object, expected: str
) -> None:
    context = _FakeContext(
        [_worker_started(), _backend_ready(), _opened(), reply], cooperative=False
    )
    backend = _backend(context)

    with pytest.raises(CaptureOpenError, match=expected):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.process.terminate_calls == 1
    assert context.parent.closed
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
    context = _FakeContext([_worker_started(), _backend_ready(), _opened(), _configured(), reply])
    backend = _backend(context)
    backend.open(_request(), deadline_monotonic_ns=10)

    result = backend.read(deadline_monotonic_ns=10)

    assert result.status is expected
    assert backend.is_open is remains_open
    backend.close()


@pytest.mark.parametrize(
    "reply",
    [
        ("disconnected", "unplugged", "extra"),
        ("recoverable", "temporary", "extra"),
        ("fatal", "driver_fault", "extra"),
        ("disconnected",),
        ("recoverable",),
        ("fatal",),
        ("unknown", "driver_fault"),
    ],
)
def test_read_rejects_malformed_error_messages_and_cleans_up(reply: object) -> None:
    context = _FakeContext(
        [_worker_started(), _backend_ready(), _opened(), _configured(), reply], cooperative=False
    )
    backend = _backend(context)
    backend.open(_request(), deadline_monotonic_ns=10)

    result = backend.read(deadline_monotonic_ns=10)

    assert result.status is ReadStatus.FATAL_ERROR
    assert result.reason_code == "invalid_worker_response"
    assert not backend.is_open
    assert context.process.terminate_calls == 1


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
    context = _FakeContext(
        [_worker_started(), _backend_ready(), _opened(), _configured(), frame_reply],
        cooperative=False,
    )
    backend = _backend(context)
    backend.open(_request(max_frame_bytes=24), deadline_monotonic_ns=10)

    result = backend.read(deadline_monotonic_ns=10)

    assert result.status is ReadStatus.FATAL_ERROR
    assert result.reason_code == "invalid_backend_frame"
    assert not backend.is_open


def test_read_timeout_and_dead_worker_fail_closed() -> None:
    timeout_context = _FakeContext(
        [_worker_started(), _backend_ready(), _opened(), _configured()],
        poll_results=[True, True, True, True, False],
        cooperative=False,
    )
    timeout_backend = _backend(timeout_context)
    timeout_backend.open(_request(), deadline_monotonic_ns=10)
    assert timeout_backend.read(deadline_monotonic_ns=10).status is ReadStatus.TIMEOUT
    assert timeout_context.process.terminate_calls == 1

    dead_context = _FakeContext([_worker_started(), _backend_ready(), _opened(), _configured()])
    dead_backend = _backend(dead_context)
    dead_backend.open(_request(), deadline_monotonic_ns=10)
    dead_context.process.alive = False
    assert dead_backend.read(deadline_monotonic_ns=10).status is ReadStatus.DISCONNECTED
    assert not dead_backend.is_open


def test_configured_message_cannot_return_after_constructor_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _FakeContext(
        [_worker_started(), _backend_ready(), _opened(), _configured()], cooperative=False
    )
    now = [0]

    def monotonic_ns() -> int:
        return now[0]

    real_stream = camera_module.NegotiatedStream

    def advancing_stream(*args: Any, **kwargs: Any) -> Any:
        stream = real_stream(*args, **kwargs)
        now[0] = 11
        return stream

    monkeypatch.setattr(camera_module, "NegotiatedStream", advancing_stream)
    backend = OpenCVProcessCameraBackend(context=context, monotonic_ns=monotonic_ns)

    with pytest.raises(CaptureOpenError, match="camera_configure_timeout"):
        backend.open(_request(), deadline_monotonic_ns=10)

    assert context.process.terminate_calls == 1
    assert context.parent.closed
    assert context.child.closed
    assert not backend.is_open
    assert backend.worker_state is OpenCVWorkerState.ABSENT


def test_expired_and_invalid_deadlines_are_rejected_before_hardware_work() -> None:
    context = _FakeContext([_worker_started(), _backend_ready(), _opened(), _configured()])
    backend = _backend(context)

    with pytest.raises(CaptureDeadlineExceeded):
        backend.open(_request(), deadline_monotonic_ns=0)
    assert not context.process.started
    with pytest.raises(TypeError, match="integer"):
        backend.open(_request(), deadline_monotonic_ns=True)
    with pytest.raises(ValueError, match="signed int64"):
        backend.open(_request(), deadline_monotonic_ns=-1)


def test_backend_rejects_cross_thread_access() -> None:
    context = _FakeContext([_worker_started(), _backend_ready(), _opened(), _configured()])
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
    context = _FakeContext(
        [_worker_started(), _backend_ready(), _opened(), _configured()],
        cooperative=False,
        stubborn=True,
    )
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
    dtype = "uint8"

    def __init__(self, payload: bytes = bytes(12), *, width: int = 2, height: int = 2) -> None:
        self.payload = payload
        self.shape = (height, width, 3)

    def tobytes(self, *, order: str) -> bytes:
        assert order == "C"
        return self.payload


class _FakeCapture:
    def __init__(
        self,
        *,
        opened: bool = True,
        reads: list[object] | None = None,
        set_results: list[bool] | None = None,
        negotiated: dict[int, object] | None = None,
    ) -> None:
        self.opened = opened
        self.reads = [] if reads is None else list(reads)
        self.set_results = [] if set_results is None else list(set_results)
        self.negotiated = {1: 2.0, 2: 2.0, 3: 30.0} if negotiated is None else dict(negotiated)
        self.released = False
        self.set_calls: list[tuple[int, float]] = []

    def isOpened(self) -> bool:
        return self.opened

    def set(self, property_id: int, value: float) -> bool:
        self.set_calls.append((property_id, value))
        return self.set_results.pop(0) if self.set_results else True

    def get(self, property_id: int) -> object:
        return self.negotiated[property_id]

    def read(self) -> object:
        value = self.reads.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    def release(self) -> None:
        self.released = True


class _FakeCV2:
    CAP_ANY = 0
    CAP_DSHOW = 700
    CAP_MSMF = 1400
    CAP_PROP_FRAME_WIDTH = 1
    CAP_PROP_FRAME_HEIGHT = 2
    CAP_PROP_FPS = 3

    def __init__(
        self, capture: _FakeCapture, *, available_backends: set[int] | None = None
    ) -> None:
        self.capture = capture
        self.video_capture_calls: list[tuple[int, int]] = []
        self.videoio_registry = _FakeVideoIORegistry(
            {700} if available_backends is None else available_backends
        )

    def VideoCapture(self, device_index: int, api_preference: int) -> _FakeCapture:
        self.video_capture_calls.append((device_index, api_preference))
        return self.capture


class _FakeVideoIORegistry:
    def __init__(self, available_backends: set[int]) -> None:
        self.available_backends = available_backends
        self.calls: list[int] = []

    def hasBackend(self, api: int) -> bool:
        self.calls.append(api)
        return api in self.available_backends


def test_worker_negotiates_reads_and_releases_capture() -> None:
    connection = _FakeConnection(incoming=[("read",), ("close",)])
    capture = _FakeCapture(reads=[(True, _FakeFrame(bytes(range(12))))])
    cv2 = _FakeCV2(capture)

    _run_worker(connection, _request(), cv2)

    assert connection.sent[0] == _backend_ready()
    assert connection.sent[1] == _opened()
    assert connection.sent[2] == _configured()
    frame_message = connection.sent[3]
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
    assert cv2.video_capture_calls == [(0, 700 if os.name == "nt" else 0)]


def test_worker_treats_false_fps_set_and_zero_fps_as_optional() -> None:
    connection = _FakeConnection(incoming=[("read",), ("close",)])
    capture = _FakeCapture(
        reads=[(True, _FakeFrame(bytes(range(12))))],
        set_results=[True, True, False],
        negotiated={1: 2.0, 2: 2.0, 3: 0.0},
    )

    _run_worker(connection, _request(), _FakeCV2(capture))

    assert connection.sent[0] == _backend_ready()
    assert connection.sent[1] == _opened()
    assert connection.sent[2] == ("configured", 2, 2, None, 6)
    assert connection.sent[3][:6] == ("frame", bytes(range(12)), 2, 2, 6, "bgr24")
    assert capture.released
    assert connection.closed


def test_worker_uses_real_dimensions_when_dimension_set_is_rejected() -> None:
    connection = _FakeConnection(incoming=[("read",), ("close",)])
    capture = _FakeCapture(
        reads=[(True, _FakeFrame(bytes(range(36)), width=4, height=3))],
        set_results=[False, False, False],
        negotiated={1: 4.0, 2: 3.0, 3: 30.0},
    )

    _run_worker(connection, _request(max_frame_bytes=36), _FakeCV2(capture))

    assert connection.sent[2] == ("configured", 4, 3, 30.0, 12)
    assert connection.sent[3][:6] == ("frame", bytes(range(36)), 4, 3, 12, "bgr24")
    assert capture.released
    assert connection.closed


@pytest.mark.parametrize(
    "capture",
    [
        _FakeCapture(negotiated={1: 0.0, 2: 2.0, 3: 30.0}),
        _FakeCapture(negotiated={1: float("nan"), 2: 2.0, 3: 30.0}),
        _FakeCapture(negotiated={1: float("inf"), 2: 2.0, 3: 30.0}),
        _FakeCapture(negotiated={1: 2.0, 2: -1.0, 3: 30.0}),
        _FakeCapture(negotiated={1: 2.0, 2: 2.0, 3: float("nan")}),
        _FakeCapture(negotiated={1: 2.0, 2: 2.0, 3: float("inf")}),
        _FakeCapture(negotiated={1: 2.0, 2: 2.0, 3: -1.0}),
    ],
)
def test_worker_rejects_configuration_failures_without_requested_fallback(
    capture: _FakeCapture,
) -> None:
    connection = _FakeConnection()

    _run_worker(connection, _request(), _FakeCV2(capture))

    assert connection.sent == [
        _backend_ready(),
        _opened(),
        ("configure_error", "camera_configure_failed"),
    ]
    assert capture.released
    assert connection.closed


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
