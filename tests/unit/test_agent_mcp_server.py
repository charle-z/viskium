from __future__ import annotations

import base64
import json
import subprocess
import sys
from collections.abc import Callable
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import pytest
from mcp import Client
from mcp.client.stdio import StdioServerParameters
from mcp.types import ImageContent, TextContent

from viskium.agent import (
    AgentLimits,
    AgentReadService,
    ConsentLedger,
    SnapshotCaptureResult,
)
from viskium.agent import mcp_server as mcp_module
from viskium.agent.mcp_server import (
    LATEST_OBSERVATION_TOOL_V1,
    SNAPSHOT_TOOL_V1,
    STATUS_TOOL_V1,
    MCPDependencyError,
    create_mcp_server,
)
from viskium.core import ObservationEnvelope
from viskium.observations import LatestObservationSlot
from viskium.snapshots import SnapshotEnvelope
from viskium.snapshots.contracts import PNG_SIGNATURE
from viskium.storage import initialize_data_root


class RecordingSnapshotProvider:
    def __init__(self, *results: SnapshotCaptureResult) -> None:
        self._results = list(results) or [SnapshotCaptureResult("unavailable")]
        self.sensitivity_class = "public"
        self.calls: list[tuple[int, int, float]] = []

    def capture(
        self,
        *,
        max_edge_px: int,
        max_bytes: int,
        timeout_seconds: float,
    ) -> SnapshotCaptureResult:
        self.calls.append((max_edge_px, max_bytes, timeout_seconds))
        if len(self._results) > 1:
            return self._results.pop(0)
        return self._results[0]


def _snapshot() -> SnapshotEnvelope:
    return SnapshotEnvelope(
        source_id="fixture",
        stream_epoch=1,
        source_sequence=2,
        received_monotonic_ns=5_000_000_000,
        width=16,
        height=12,
        sensitivity_class="public",
        png_bytes=PNG_SIGNATURE + b"bounded-png",
    )


def _observation() -> ObservationEnvelope:
    return ObservationEnvelope(
        session_id="session",
        source_id="fixture",
        stream_epoch=1,
        source_sequence=2,
        observed_monotonic_ns=5_000_000_000,
        producer_id="test",
        producer_version="1",
        schema_id="viskium.test",
        schema_version=1,
        payload={"value": 7},
        idempotency_key="fixture:1:2:test",
        trace_id="trace",
        sensitivity_class="public",
    )


def _service(
    tmp_path: Path,
    *,
    scopes: frozenset[str] | None = None,
    quota: int = 4,
    slot: LatestObservationSlot | None = None,
    provider: RecordingSnapshotProvider | None = None,
    status_provider: Callable[[], dict[str, object]] = lambda: {"state": "ready"},
    limits: AgentLimits | None = None,
) -> tuple[AgentReadService, ConsentLedger, RecordingSnapshotProvider]:
    ledger = ConsentLedger(initialize_data_root(tmp_path / "data"))
    if scopes is not None:
        ledger.grant(
            scopes=scopes,  # type: ignore[arg-type]
            duration_seconds=60,
            snapshot_quota=quota,
            sensitivity_ceiling="identifiable",
            now_unix_ns=1_000_000_000,
        )
    selected_provider = provider or RecordingSnapshotProvider()
    service = AgentReadService(
        observations=LatestObservationSlot() if slot is None else slot,
        consent=ledger,
        snapshot_provider=selected_provider,
        status_provider=status_provider,
        limits=limits,
        unix_time_ns=lambda: 2_000_000_000,
        monotonic_ns=lambda: 5_000_000_000,
    )
    return service, ledger, selected_provider


def _run(scenario: Callable[[], Any]) -> Any:
    return anyio.run(scenario)


def _tool_by_name(listing: Any, name: str) -> Any:
    return next(tool for tool in listing.tools if tool.name == name)


def _array_schema(property_schema: dict[str, Any]) -> dict[str, Any]:
    if property_schema.get("type") == "array":
        return property_schema
    return next(item for item in property_schema["anyOf"] if item.get("type") == "array")


def test_lists_exact_versioned_tools_with_safe_annotations_and_effective_limits(
    tmp_path: Path,
) -> None:
    limits = AgentLimits(
        max_age_ms=321,
        max_wait_ms=177,
        max_schema_ids=3,
        max_snapshot_edge_px=96,
    )
    service, _, _ = _service(tmp_path, limits=limits)
    server = create_mcp_server(service)

    async def scenario() -> Any:
        async with Client(server) as client:
            return await client.list_tools()

    listing = _run(scenario)
    assert {tool.name for tool in listing.tools} == {
        STATUS_TOOL_V1,
        LATEST_OBSERVATION_TOOL_V1,
        SNAPSHOT_TOOL_V1,
    }

    status = _tool_by_name(listing, STATUS_TOOL_V1)
    latest = _tool_by_name(listing, LATEST_OBSERVATION_TOOL_V1)
    snapshot = _tool_by_name(listing, SNAPSHOT_TOOL_V1)
    assert status.annotations.model_dump(by_alias=True) == {
        "title": None,
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
    assert latest.annotations.model_dump(by_alias=True) == status.annotations.model_dump(
        by_alias=True
    )
    assert snapshot.annotations.model_dump(by_alias=True) == {
        "title": None,
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False,
    }

    latest_properties = latest.input_schema["properties"]
    snapshot_properties = snapshot.input_schema["properties"]
    assert latest_properties["max_age_ms"]["maximum"] == limits.max_age_ms
    assert latest_properties["wait_ms"]["maximum"] == limits.max_wait_ms
    schema_ids = _array_schema(latest_properties["schema_ids"])
    assert schema_ids["maxItems"] == limits.max_schema_ids
    assert schema_ids["items"]["maxLength"] == 256
    assert snapshot_properties["max_edge_px"]["maximum"] == limits.max_snapshot_edge_px
    assert snapshot_properties["wait_ms"]["maximum"] == limits.max_wait_ms
    assert snapshot_properties["wait_ms"]["default"] == min(10_000, limits.max_wait_ms)
    assert status.output_schema is not None
    assert latest.output_schema is not None
    assert snapshot.output_schema is None


def test_status_is_in_memory_public_and_never_touches_consent_or_camera(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    service, ledger, provider = _service(
        tmp_path,
        status_provider=lambda: {"state": "ready", "load": 0.25},
        limits=AgentLimits(max_request_bytes=1),
    )

    def forbidden(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("status crossed the consent or hardware boundary")

    monkeypatch.setattr(ledger, "load", forbidden)
    monkeypatch.setattr(ledger, "reserve_snapshot", forbidden)
    monkeypatch.setattr(provider, "capture", forbidden)
    server = create_mcp_server(service)

    async def scenario() -> Any:
        async with Client(server) as client:
            return await client.call_tool(STATUS_TOOL_V1)

    result = _run(scenario)
    assert not result.is_error
    assert result.structured_content == {
        "contract": "urn:viskium:mcp:status:1",
        "agent_contract": "urn:viskium:agent-read:1",
        "outcome": "ok",
        "metadata": {
            "contract": "urn:viskium:agent-read:1",
            "limits": service.limits.to_dict(),
            "status": {"load": 0.25, "state": "ready"},
        },
    }
    assert not ledger.path.exists()
    assert capsys.readouterr().out == ""


def test_latest_observation_is_structured_bounded_and_consent_gated(tmp_path: Path) -> None:
    slot = LatestObservationSlot()
    slot.offer(_observation())
    service, _, provider = _service(
        tmp_path,
        scopes=frozenset({"observation.read"}),
        slot=slot,
    )
    server = create_mcp_server(service)

    async def scenario() -> Any:
        async with Client(server) as client:
            return await client.call_tool(
                LATEST_OBSERVATION_TOOL_V1,
                {"max_age_ms": 10_000, "wait_ms": 2_000, "schema_ids": ["viskium.test"]},
            )

    result = _run(scenario)
    assert not result.is_error
    assert result.structured_content["outcome"] == "ok"
    assert result.structured_content["age_ns"] == 0
    assert result.structured_content["observation"]["schema_id"] == "viskium.test"
    assert result.structured_content["observation"]["payload"] == {"value": 7}
    assert provider.calls == []


def test_latest_inputs_are_strict_and_rejected_before_service(tmp_path: Path) -> None:
    limits = AgentLimits(max_age_ms=37, max_wait_ms=41, max_schema_ids=2)
    service, _, _ = _service(
        tmp_path,
        scopes=frozenset({"observation.read"}),
        limits=limits,
    )
    server = create_mcp_server(service)

    invalid_requests = [
        {"max_age_ms": True},
        {"max_age_ms": 38},
        {"max_age_ms": 1, "wait_ms": 42},
        {"max_age_ms": 1, "schema_ids": ["a", "b", "c"]},
        {"max_age_ms": 1, "schema_ids": ["x" * 257]},
        {"max_age_ms": 1, "unexpected": 1},
    ]

    async def scenario() -> list[Any]:
        async with Client(server) as client:
            results = [
                await client.call_tool(LATEST_OBSERVATION_TOOL_V1, request)
                for request in invalid_requests
            ]
            results.append(
                await client.call_tool(
                    LATEST_OBSERVATION_TOOL_V1,
                    {"max_age_ms": 37, "wait_ms": 41, "schema_ids": ["a", "b"]},
                )
            )
            return results

    *invalid, boundary = _run(scenario)
    assert all(result.is_error for result in invalid)
    assert not boundary.is_error
    assert boundary.structured_content["outcome"] == "timeout"
    assert service.metrics.observation_requests == 1


def test_effective_request_byte_limit_is_enforced_before_service(tmp_path: Path) -> None:
    service, _, _ = _service(
        tmp_path,
        scopes=frozenset({"observation.read"}),
        limits=AgentLimits(max_request_bytes=32),
    )
    server = create_mcp_server(service)

    async def scenario() -> Any:
        async with Client(server) as client:
            return await client.call_tool(
                LATEST_OBSERVATION_TOOL_V1,
                {"max_age_ms": 1, "wait_ms": 0, "schema_ids": None},
            )

    result = _run(scenario)
    assert result.is_error
    assert service.metrics.observation_requests == 0


def test_snapshot_uses_full_effective_limits_returns_png_and_allows_retry(
    tmp_path: Path,
) -> None:
    provider = RecordingSnapshotProvider(
        SnapshotCaptureResult("busy"),
        SnapshotCaptureResult("ok", _snapshot()),
    )
    service, _, _ = _service(
        tmp_path,
        scopes=frozenset({"snapshot.read"}),
        quota=2,
        provider=provider,
    )
    server = create_mcp_server(service)

    async def scenario() -> tuple[Any, Any]:
        async with Client(server) as client:
            arguments = {
                "max_edge_px": service.limits.max_snapshot_edge_px,
                "wait_ms": service.limits.max_wait_ms,
            }
            first = await client.call_tool(SNAPSHOT_TOOL_V1, arguments)
            second = await client.call_tool(SNAPSHOT_TOOL_V1, arguments)
            return first, second

    first, second = _run(scenario)
    assert not first.is_error
    assert len(first.content) == 1
    assert isinstance(first.content[0], TextContent)
    assert json.loads(first.content[0].text)["outcome"] == "busy"
    assert not second.is_error
    assert len(second.content) == 1
    assert isinstance(second.content[0], ImageContent)
    assert second.content[0].mime_type == "image/png"
    assert base64.b64decode(second.content[0].data) == _snapshot().png_bytes
    assert provider.calls == [
        (
            service.limits.max_snapshot_edge_px,
            service.limits.max_snapshot_bytes,
            service.limits.max_wait_ms / 1_000,
        ),
        (
            service.limits.max_snapshot_edge_px,
            service.limits.max_snapshot_bytes,
            service.limits.max_wait_ms / 1_000,
        ),
    ]


def test_snapshot_denial_is_bounded_machine_readable_and_never_calls_provider(
    tmp_path: Path,
) -> None:
    service, _, provider = _service(tmp_path)
    server = create_mcp_server(service)

    async def scenario() -> Any:
        async with Client(server) as client:
            return await client.call_tool(
                SNAPSHOT_TOOL_V1,
                {"max_edge_px": 64, "wait_ms": 0},
            )

    result = _run(scenario)
    assert not result.is_error
    assert len(result.content) == 1
    assert isinstance(result.content[0], TextContent)
    assert json.loads(result.content[0].text) == {
        "agent_contract": "urn:viskium:agent-read:1",
        "contract": "urn:viskium:mcp:snapshot:1",
        "outcome": "grant_missing",
    }
    assert provider.calls == []


@pytest.mark.parametrize(
    ("provider_reason", "expected_reason"),
    [
        ("device_open_failed", "device_open_failed"),
        (r"driver failed at C:\\private\\camera.sys", "generic"),
    ],
)
def test_snapshot_failure_payload_propagates_only_safe_reason_code(
    tmp_path: Path,
    provider_reason: str,
    expected_reason: str,
) -> None:
    provider = RecordingSnapshotProvider(
        SnapshotCaptureResult("unavailable", reason_code=provider_reason)  # type: ignore[arg-type]
    )
    service, _, _ = _service(
        tmp_path,
        scopes=frozenset({"snapshot.read"}),
        provider=provider,
        quota=1,
    )
    server = create_mcp_server(service)

    async def scenario() -> Any:
        async with Client(server) as client:
            return await client.call_tool(
                SNAPSHOT_TOOL_V1,
                {"max_edge_px": 64, "wait_ms": 1},
            )

    result = _run(scenario)
    assert not result.is_error
    assert len(result.content) == 1
    assert isinstance(result.content[0], TextContent)
    payload = json.loads(result.content[0].text)
    assert payload == {
        "agent_contract": "urn:viskium:agent-read:1",
        "contract": "urn:viskium:mcp:snapshot:1",
        "outcome": "unavailable",
        "reason_code": expected_reason,
    }
    assert "driver failed" not in result.content[0].text
    assert "private" not in result.content[0].text


def test_snapshot_failure_payload_propagates_capture_read_error_without_payload_bytes(
    tmp_path: Path,
) -> None:
    provider = RecordingSnapshotProvider(
        SnapshotCaptureResult("failed", reason_code="capture_read_error")
    )
    service, _, _ = _service(
        tmp_path,
        scopes=frozenset({"snapshot.read"}),
        provider=provider,
        quota=1,
    )
    server = create_mcp_server(service)

    async def scenario() -> Any:
        async with Client(server) as client:
            return await client.call_tool(
                SNAPSHOT_TOOL_V1,
                {"max_edge_px": 64, "wait_ms": 1},
            )

    result = _run(scenario)
    assert not result.is_error
    assert len(result.content) == 1
    assert isinstance(result.content[0], TextContent)
    payload = json.loads(result.content[0].text)
    assert payload["outcome"] == "failed"
    assert payload["reason_code"] == "capture_read_error"
    assert "payload" not in payload
    assert "bytes" not in result.content[0].text


def test_optional_sdk_failure_is_clear_and_chained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _, _ = _service(tmp_path)
    real_import = mcp_module.import_module

    def missing_mcp(name: str) -> Any:
        if name.startswith("mcp"):
            raise ModuleNotFoundError("private import detail")
        return real_import(name)

    monkeypatch.setattr(mcp_module, "import_module", missing_mcp)
    with pytest.raises(
        MCPDependencyError,
        match=r"optional MCP extra with mcp==2\.1\.1",
    ) as raised:
        create_mcp_server(service)

    assert isinstance(raised.value.__cause__, ModuleNotFoundError)
    assert "private import detail" not in str(raised.value)


@pytest.mark.parametrize(
    ("module_name", "replacement"),
    [
        ("mcp.server", SimpleNamespace(MCPServer=object())),
        (
            "mcp.types",
            SimpleNamespace(
                ToolAnnotations=object(),
                CallToolResult=TextContent,
                TextContent=TextContent,
            ),
        ),
        (
            "mcp.types",
            SimpleNamespace(
                ToolAnnotations=TextContent,
                CallToolResult=object(),
                TextContent=TextContent,
            ),
        ),
        ("pydantic", SimpleNamespace(Field=None)),
    ],
)
def test_optional_sdk_failure_rejects_incompatible_module_shapes(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    replacement: object,
) -> None:
    real_import = mcp_module.import_module

    def malformed(name: str) -> Any:
        return replacement if name == module_name else real_import(name)

    monkeypatch.setattr(mcp_module, "import_module", malformed)
    with pytest.raises(MCPDependencyError, match=r"mcp==2\.1\.1"):
        mcp_module._load_mcp_bindings()


@pytest.mark.parametrize(
    ("encoded", "maximum_bytes", "error"),
    [(b"{}", 1, ValueError), (b"[]", 2, TypeError)],
)
def test_json_result_decoder_rejects_oversize_and_non_objects(
    encoded: bytes,
    maximum_bytes: int,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        mcp_module._decode_json_object(encoded, maximum_bytes=maximum_bytes)


def test_status_dependency_failure_and_unknown_tool_are_bounded(tmp_path: Path) -> None:
    def fail_status() -> dict[str, object]:
        raise RuntimeError("private status failure")

    service, _, _ = _service(tmp_path, status_provider=fail_status)
    server = create_mcp_server(service)

    async def scenario() -> tuple[Any, Any]:
        async with Client(server) as client:
            status = await client.call_tool(STATUS_TOOL_V1)
            unknown = await client.call_tool("viskium_unknown_v1", {})
            return status, unknown

    status, unknown = _run(scenario)
    assert not status.is_error
    assert status.structured_content["outcome"] == "status_unavailable"
    assert status.structured_content["metadata"] is None
    assert unknown.is_error
    assert "private status failure" not in repr(unknown)


def test_stdio_runner_delegates_to_bounded_runner_without_application_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    service, _, _ = _service(tmp_path)
    invocations: list[tuple[object, ...]] = []

    class FakeServer:
        pass

    class FakeAnyio:
        @staticmethod
        def run(*args: object) -> None:
            invocations.append(args)

    stdin = BytesIO()
    bindings = SimpleNamespace(anyio_module=FakeAnyio())
    monkeypatch.setattr(mcp_module, "_load_mcp_bindings", lambda: bindings)
    monkeypatch.setattr(mcp_module, "create_mcp_server", lambda _service: FakeServer())
    monkeypatch.setattr(mcp_module.sys, "stdin", SimpleNamespace(buffer=stdin))
    mcp_module.run_mcp_server(service)

    assert len(invocations) == 1
    runner, server, passed_bindings, passed_service, passed_stdin = invocations[0]
    assert runner is mcp_module._run_bounded_stdio
    assert isinstance(server, FakeServer)
    assert passed_bindings is bindings
    assert passed_service is service
    assert passed_stdin is stdin
    assert capsys.readouterr().out == ""


def test_bounded_binary_input_rejects_before_decoding_or_allocating_an_unbounded_line() -> None:
    async def run_sync(function: Callable[[], bytes]) -> bytes:
        return function()

    accepted = mcp_module._BoundedBinaryLineInput(
        BytesIO(b"1234567\n"),
        maximum_bytes=8,
        run_sync=run_sync,
    )
    oversized = mcp_module._BoundedBinaryLineInput(
        BytesIO(b"12345678\n"),
        maximum_bytes=8,
        run_sync=run_sync,
    )

    async def scenario() -> str:
        line = await accepted.__anext__()
        with pytest.raises(StopAsyncIteration):
            await accepted.__anext__()
        with pytest.raises(mcp_module.MCPTransportError, match="wire limit"):
            await oversized.__anext__()
        return line

    assert _run(scenario) == "1234567\n"


def test_request_admission_backpressures_and_normalizes_duplicate_ids() -> None:
    class Response:
        def __init__(self, request_id: object) -> None:
            self.id = request_id

    class Envelope:
        def __init__(self, message: object) -> None:
            self.message = message

    class Writes:
        def __init__(self) -> None:
            self.items: list[object] = []

        async def send(self, item: object) -> None:
            self.items.append(item)

    async def scenario() -> None:
        admission = mcp_module._InboundRequestAdmission(anyio.Semaphore(1))
        await admission.admit(1)
        with anyio.move_on_after(0.01) as blocked:
            await admission.admit(2)
        assert blocked.cancel_called

        writes = Writes()
        stream = mcp_module._AdmissionWriteStream(writes, admission, (Response,))
        await stream.send(Envelope(Response("1")))
        await admission.admit(2)
        admission.complete(2)
        assert len(writes.items) == 1

        duplicates = mcp_module._InboundRequestAdmission(anyio.Semaphore(2))
        await duplicates.admit(7)
        with pytest.raises(mcp_module.MCPTransportError, match="duplicate"):
            await duplicates.admit("7")
        duplicates.complete(7)

    _run(scenario)


def test_bounded_runner_rejects_an_incompatible_private_sdk_seam() -> None:
    with pytest.raises(MCPDependencyError, match=r"mcp==2\.1\.1"):
        mcp_module._validated_lowlevel_server(SimpleNamespace())


def test_real_stdio_runner_serves_status_without_camera_or_consent(tmp_path: Path) -> None:
    root = initialize_data_root(tmp_path / "stdio-data").root
    parameters = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "viskium",
            "agent",
            "serve",
            "--data-root",
            str(root),
            "--max-wire-bytes",
            str(64 * 1_024),
            "--max-inflight-requests",
            "2",
        ],
        cwd=Path(__file__).parents[2],
    )

    async def scenario() -> Any:
        async with Client(parameters) as client:
            return await client.call_tool(STATUS_TOOL_V1)

    result = _run(scenario)
    assert not result.is_error
    assert result.structured_content["outcome"] == "ok"
    camera_status = result.structured_content["metadata"]["status"]["camera"]
    assert camera_status == {
        "backend_retained": False,
        "close_stuck": False,
        "mode": "one_shot_on_demand",
    }
    assert not (root / "state" / "agent-consent.json").exists()


def test_real_stdio_runner_rejects_oversize_before_json_with_sanitized_stderr(
    tmp_path: Path,
) -> None:
    root = initialize_data_root(tmp_path / "oversize-data").root
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "viskium",
            "agent",
            "serve",
            "--data-root",
            str(root),
            "--max-wire-bytes",
            str(64 * 1_024),
        ],
        input=(b"x" * (64 * 1_024 + 1)) + b"\n",
        capture_output=True,
        check=False,
        cwd=Path(__file__).parents[2],
        timeout=10,
    )

    assert result.returncode == 1
    assert result.stdout == b""
    stderr = result.stderr.decode(errors="replace")
    assert stderr.strip() == (
        "viskium agent serve: MCP stdio rejected an invalid or oversized request"
    )
    assert str(root) not in stderr
    assert "Traceback" not in stderr


def test_constructor_rejects_non_service_before_loading_optional_sdk() -> None:
    with pytest.raises(TypeError, match="AgentReadService"):
        create_mcp_server(object())  # type: ignore[arg-type]
