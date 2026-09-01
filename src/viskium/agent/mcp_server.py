"""Optional MCP v2 transport for Viskium's bounded agent-read service.

The transport deliberately exposes only three versioned tools. Consent is
managed out of band by :class:`~viskium.agent.service.AgentReadService`; this
module cannot create grants, stream frames, or control camera lifecycle.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING, Annotated, Any, BinaryIO, Literal, NotRequired, TypedDict, cast

from viskium.agent.service import AgentReadService, SnapshotReasonCode
from viskium.core.serialization import bounded_canonical_json_bytes

if TYPE_CHECKING:
    from mcp.server import MCPServer

MCP_SERVER_NAME: Literal["viskium-agent-read"] = "viskium-agent-read"
MCP_SERVER_VERSION: Literal["1"] = "1"

STATUS_TOOL_V1: Literal["viskium_status_v1"] = "viskium_status_v1"
LATEST_OBSERVATION_TOOL_V1: Literal["viskium_latest_observation_v1"] = (
    "viskium_latest_observation_v1"
)
SNAPSHOT_TOOL_V1: Literal["viskium_snapshot_v1"] = "viskium_snapshot_v1"

STATUS_RESULT_CONTRACT_V1: Literal["urn:viskium:mcp:status:1"] = "urn:viskium:mcp:status:1"
OBSERVATION_RESULT_CONTRACT_V1: Literal["urn:viskium:mcp:latest-observation:1"] = (
    "urn:viskium:mcp:latest-observation:1"
)
SNAPSHOT_RESULT_CONTRACT_V1: Literal["urn:viskium:mcp:snapshot:1"] = "urn:viskium:mcp:snapshot:1"

_MAX_SCHEMA_ID_CHARS = 256
_MCP_DEPENDENCY_MESSAGE = (
    "Viskium MCP support requires the validated optional MCP extra with mcp==2.1.1"
)


class MCPDependencyError(RuntimeError):
    """Raised when the optional MCP SDK is not available."""


class MCPTransportError(RuntimeError):
    """Raised when bounded stdio cannot preserve its transport invariants."""


class StatusToolResult(TypedDict):
    """Stable structured result for :data:`STATUS_TOOL_V1`."""

    contract: Literal["urn:viskium:mcp:status:1"]
    agent_contract: Literal["urn:viskium:agent-read:1"]
    outcome: str
    metadata: dict[str, Any] | None


class ObservationToolResult(TypedDict):
    """Stable structured result for :data:`LATEST_OBSERVATION_TOOL_V1`."""

    contract: Literal["urn:viskium:mcp:latest-observation:1"]
    agent_contract: Literal["urn:viskium:agent-read:1"]
    outcome: str
    age_ns: int | None
    observation: dict[str, Any] | None


class SnapshotFailureResult(TypedDict):
    """Small machine-readable result used when no snapshot image is available."""

    contract: Literal["urn:viskium:mcp:snapshot:1"]
    agent_contract: Literal["urn:viskium:agent-read:1"]
    outcome: str
    reason_code: NotRequired[SnapshotReasonCode]


@dataclass(frozen=True, slots=True)
class _MCPBindings:
    server_type: type[Any]
    image_type: type[Any]
    annotations_type: type[Any]
    call_tool_result_type: type[Any]
    text_content_type: type[Any]
    tool_error_type: type[Exception]
    field: Any
    anyio_module: Any
    stdio_server: Callable[..., Any]
    jsonrpc_request_type: type[Any]
    jsonrpc_response_type: type[Any]
    jsonrpc_error_type: type[Any]


def _load_mcp_bindings() -> _MCPBindings:
    """Load the optional SDK only when an MCP server is requested."""

    try:
        server_module = import_module("mcp.server")
        types_module = import_module("mcp.types")
        utilities_module = import_module("mcp.server.mcpserver.utilities.types")
        exceptions_module = import_module("mcp.server.mcpserver.exceptions")
        stdio_module = import_module("mcp.server.stdio")
        anyio_module = import_module("anyio")
        pydantic_module = import_module("pydantic")
        server_type = server_module.MCPServer
        image_type = utilities_module.Image
        annotations_type = types_module.ToolAnnotations
        call_tool_result_type = types_module.CallToolResult
        text_content_type = types_module.TextContent
        tool_error_type = exceptions_module.ToolError
        field = pydantic_module.Field
        stdio_server = stdio_module.stdio_server
        jsonrpc_request_type = types_module.JSONRPCRequest
        jsonrpc_response_type = types_module.JSONRPCResponse
        jsonrpc_error_type = types_module.JSONRPCError
    except (AttributeError, ImportError) as error:
        raise MCPDependencyError(_MCP_DEPENDENCY_MESSAGE) from error

    if not isinstance(server_type, type) or not isinstance(image_type, type):
        raise MCPDependencyError(_MCP_DEPENDENCY_MESSAGE)
    if not isinstance(annotations_type, type) or not isinstance(tool_error_type, type):
        raise MCPDependencyError(_MCP_DEPENDENCY_MESSAGE)
    if not isinstance(call_tool_result_type, type) or not isinstance(text_content_type, type):
        raise MCPDependencyError(_MCP_DEPENDENCY_MESSAGE)
    if not callable(field):
        raise MCPDependencyError(_MCP_DEPENDENCY_MESSAGE)
    if not callable(stdio_server) or not callable(getattr(anyio_module, "run", None)):
        raise MCPDependencyError(_MCP_DEPENDENCY_MESSAGE)
    if not callable(getattr(getattr(anyio_module, "to_thread", None), "run_sync", None)):
        raise MCPDependencyError(_MCP_DEPENDENCY_MESSAGE)
    if not all(
        isinstance(message_type, type)
        for message_type in (jsonrpc_request_type, jsonrpc_response_type, jsonrpc_error_type)
    ):
        raise MCPDependencyError(_MCP_DEPENDENCY_MESSAGE)
    return _MCPBindings(
        server_type=server_type,
        image_type=image_type,
        annotations_type=annotations_type,
        call_tool_result_type=call_tool_result_type,
        text_content_type=text_content_type,
        tool_error_type=tool_error_type,
        field=field,
        anyio_module=anyio_module,
        stdio_server=stdio_server,
        jsonrpc_request_type=jsonrpc_request_type,
        jsonrpc_response_type=jsonrpc_response_type,
        jsonrpc_error_type=jsonrpc_error_type,
    )


class _BoundedBinaryLineInput:
    """Read one MCP frame without ever materializing an oversized line."""

    __slots__ = ("_maximum_bytes", "_run_sync", "_stream")

    def __init__(
        self,
        stream: BinaryIO,
        *,
        maximum_bytes: int,
        run_sync: Callable[[Callable[[], bytes]], Awaitable[bytes]],
    ) -> None:
        if not callable(getattr(stream, "readline", None)):
            raise TypeError("MCP stdin must be a readable binary stream")
        if isinstance(maximum_bytes, bool) or not isinstance(maximum_bytes, int):
            raise TypeError("maximum_bytes must be an integer")
        if maximum_bytes <= 0:
            raise ValueError("maximum_bytes must be positive")
        if not callable(run_sync):
            raise TypeError("run_sync must be callable")
        self._stream = stream
        self._maximum_bytes = maximum_bytes
        self._run_sync = run_sync

    def __aiter__(self) -> _BoundedBinaryLineInput:
        return self

    async def __anext__(self) -> str:
        encoded = await self._run_sync(self._readline)
        if not encoded:
            raise StopAsyncIteration
        return encoded.decode("utf-8", errors="replace")

    def _readline(self) -> bytes:
        encoded = self._stream.readline(self._maximum_bytes + 1)
        if not isinstance(encoded, bytes):
            raise MCPTransportError("MCP stdin returned a non-binary frame")
        if len(encoded) > self._maximum_bytes:
            raise MCPTransportError("MCP request frame exceeds the configured wire limit")
        return encoded


def _request_key(request_id: object) -> str | int:
    if isinstance(request_id, bool) or not isinstance(request_id, (str, int)):
        raise MCPTransportError("MCP request id is invalid")
    if isinstance(request_id, str):
        try:
            return int(request_id)
        except ValueError:
            return request_id
    return request_id


class _InboundRequestAdmission:
    """Bound requests that the SDK may fan out before their responses are handed off."""

    __slots__ = ("_pending", "_semaphore")

    def __init__(self, semaphore: Any) -> None:
        if not callable(getattr(semaphore, "acquire", None)) or not callable(
            getattr(semaphore, "release", None)
        ):
            raise TypeError("semaphore must provide acquire and release")
        self._semaphore = semaphore
        self._pending: set[str | int] = set()

    async def admit(self, request_id: object) -> None:
        await self._semaphore.acquire()
        try:
            key = _request_key(request_id)
            if key in self._pending:
                raise MCPTransportError("duplicate MCP request id is already in flight")
            self._pending.add(key)
        except BaseException:
            self._semaphore.release()
            raise

    def complete(self, request_id: object) -> None:
        try:
            key = _request_key(request_id)
        except MCPTransportError:
            return
        if key in self._pending:
            self._pending.remove(key)
            self._semaphore.release()


class _AdmissionReadStream:
    __slots__ = ("_admission", "_inner", "_request_type", "last_context")

    def __init__(
        self, inner: Any, admission: _InboundRequestAdmission, request_type: type[Any]
    ) -> None:
        self._inner = inner
        self._admission = admission
        self._request_type = request_type
        self.last_context: Any = None

    async def _admit(self, item: Any) -> Any:
        self.last_context = getattr(self._inner, "last_context", None)
        message = getattr(item, "message", None)
        if isinstance(message, self._request_type):
            await self._admission.admit(message.id)
        return item

    async def receive(self) -> Any:
        return await self._admit(await self._inner.receive())

    def __aiter__(self) -> _AdmissionReadStream:
        return self

    async def __anext__(self) -> Any:
        return await self._admit(await self._inner.__anext__())

    def close(self) -> None:
        self._inner.close()

    async def aclose(self) -> None:
        await self._inner.aclose()

    async def __aenter__(self) -> _AdmissionReadStream:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()


class _AdmissionWriteStream:
    __slots__ = ("_admission", "_inner", "_response_types")

    def __init__(
        self,
        inner: Any,
        admission: _InboundRequestAdmission,
        response_types: tuple[type[Any], ...],
    ) -> None:
        self._inner = inner
        self._admission = admission
        self._response_types = response_types

    async def send(self, item: Any) -> None:
        await self._inner.send(item)
        message = getattr(item, "message", None)
        if isinstance(message, self._response_types):
            self._admission.complete(message.id)

    def close(self) -> None:
        self._inner.close()

    async def aclose(self) -> None:
        await self._inner.aclose()

    async def __aenter__(self) -> _AdmissionWriteStream:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()


def _decode_json_object(encoded: bytes, *, maximum_bytes: int) -> dict[str, Any]:
    if len(encoded) > maximum_bytes:
        raise ValueError("service result exceeds its declared byte limit")
    decoded = json.loads(encoded)
    if not isinstance(decoded, dict):
        raise TypeError("service result must contain a JSON object")
    return cast(dict[str, Any], decoded)


def _request_within_limit(document: dict[str, Any], *, maximum_bytes: int) -> bool:
    return bounded_canonical_json_bytes(document, max_bytes=maximum_bytes) is not None


def create_mcp_server(service: AgentReadService) -> MCPServer[Any]:
    """Create the optional in-process MCP v2 server for one read service.

    The effective service limits are copied into each generated input schema;
    the transport does not impose smaller time, size, or count ceilings.
    """

    if not isinstance(service, AgentReadService):
        raise TypeError("service must be an AgentReadService")
    sdk = _load_mcp_bindings()
    limits = service.limits
    snapshot_default_wait_ms = min(10_000, limits.max_wait_ms)
    allowed_arguments = {
        STATUS_TOOL_V1: frozenset(),
        LATEST_OBSERVATION_TOOL_V1: frozenset({"max_age_ms", "wait_ms", "schema_ids"}),
        SNAPSHOT_TOOL_V1: frozenset({"max_edge_px", "wait_ms"}),
    }

    def rejected_request(reason: str) -> Any:
        return sdk.call_tool_result_type(
            content=[sdk.text_content_type(type="text", text=reason)],
            is_error=True,
        )

    async def strict_tool_inputs(
        context: Any,
        call_next: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        """Bound raw arguments before the SDK can discard unknown fields."""

        if context.method != "tools/call" or not isinstance(context.params, Mapping):
            return await call_next(context)
        tool_name = context.params.get("name")
        if tool_name not in allowed_arguments:
            return await call_next(context)
        arguments = context.params.get("arguments")
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, Mapping):
            return rejected_request("tool arguments must be an object")
        if any(not isinstance(key, str) for key in arguments):
            return rejected_request("tool argument names must be strings")
        if set(arguments) - allowed_arguments[tool_name]:
            return rejected_request("tool request contains unknown arguments")
        if tool_name == STATUS_TOOL_V1:
            return await call_next(context)
        try:
            encoded = bounded_canonical_json_bytes(
                dict(arguments),
                max_bytes=limits.max_request_bytes,
            )
        except (TypeError, ValueError):
            return rejected_request("tool request is not bounded JSON")
        if encoded is None:
            return rejected_request("tool request exceeds the effective service byte limit")
        return await call_next(context)

    server = sdk.server_type(
        name=MCP_SERVER_NAME,
        title="Viskium bounded agent reads",
        description="Bounded status, latest-observation, and one-shot PNG access.",
        instructions=(
            "Use only the three versioned tools. Consent is out of band; no tool grants "
            "access, streams frames, or controls camera lifecycle."
        ),
        version=MCP_SERVER_VERSION,
        debug=False,
        log_level="ERROR",
        middleware=[strict_tool_inputs],
    )

    read_annotations = sdk.annotations_type(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
    snapshot_annotations = sdk.annotations_type(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=False,
    )

    def status_v1() -> StatusToolResult:
        """Return bounded public runtime status without consent or hardware access."""

        result = service.status()
        metadata: dict[str, Any] | None = None
        if result.metadata_json is not None:
            try:
                metadata = _decode_json_object(
                    result.metadata_json,
                    maximum_bytes=limits.max_metadata_bytes,
                )
            except (TypeError, ValueError, json.JSONDecodeError) as error:
                raise sdk.tool_error_type("invalid bounded status result") from error
        return {
            "contract": STATUS_RESULT_CONTRACT_V1,
            "agent_contract": result.contract,
            "outcome": result.outcome,
            "metadata": metadata,
        }

    def latest_observation_v1(
        max_age_ms: int,
        wait_ms: int = 0,
        schema_ids: list[str] | None = None,
    ) -> ObservationToolResult:
        """Return at most one consent-gated latest observation."""

        request = {
            "max_age_ms": max_age_ms,
            "wait_ms": wait_ms,
            "schema_ids": schema_ids,
        }
        if not _request_within_limit(request, maximum_bytes=limits.max_request_bytes):
            raise sdk.tool_error_type("request exceeds the effective service byte limit")
        result = service.latest_observation(
            max_age_ms=max_age_ms,
            wait_ms=wait_ms,
            schema_ids=schema_ids,
        )
        observation: dict[str, Any] | None = None
        if result.observation_json is not None:
            try:
                observation = _decode_json_object(
                    result.observation_json,
                    maximum_bytes=limits.max_observation_bytes,
                )
            except (TypeError, ValueError, json.JSONDecodeError) as error:
                raise sdk.tool_error_type("invalid bounded observation result") from error
        return {
            "contract": OBSERVATION_RESULT_CONTRACT_V1,
            "agent_contract": result.contract,
            "outcome": result.outcome,
            "age_ns": result.age_ns,
            "observation": observation,
        }

    def snapshot_v1(max_edge_px: int, wait_ms: int = 0) -> Any:
        """Return one consent-gated PNG image; never expose camera lifecycle controls."""

        request = {"max_edge_px": max_edge_px, "wait_ms": wait_ms}
        if not _request_within_limit(request, maximum_bytes=limits.max_request_bytes):
            raise sdk.tool_error_type("request exceeds the effective service byte limit")
        result = service.snapshot(max_edge_px=max_edge_px, wait_ms=wait_ms)
        snapshot = result.snapshot
        if snapshot is None:
            failure: SnapshotFailureResult = {
                "contract": SNAPSHOT_RESULT_CONTRACT_V1,
                "agent_contract": result.contract,
                "outcome": result.outcome,
            }
            if result.explicit_reason_code is not None:
                failure["reason_code"] = result.explicit_reason_code
            return failure
        if snapshot.encoded_bytes > limits.max_snapshot_bytes:
            raise sdk.tool_error_type("invalid bounded snapshot result")
        return sdk.image_type(data=snapshot.png_bytes, format="png")

    field = sdk.field
    schema_id = Annotated[
        str,
        field(strict=True, min_length=1, max_length=_MAX_SCHEMA_ID_CHARS),
    ]
    latest_observation_v1.__annotations__.update(
        {
            "max_age_ms": Annotated[
                int,
                field(strict=True, ge=0, le=limits.max_age_ms),
            ],
            "wait_ms": Annotated[
                int,
                field(strict=True, ge=0, le=limits.max_wait_ms),
            ],
            "schema_ids": Annotated[
                list[schema_id] | None,
                field(max_length=limits.max_schema_ids),
            ],
            "return": ObservationToolResult,
        }
    )
    snapshot_v1.__annotations__.update(
        {
            "max_edge_px": Annotated[
                int,
                field(strict=True, ge=1, le=limits.max_snapshot_edge_px),
            ],
            "wait_ms": Annotated[
                int,
                field(strict=True, ge=0, le=limits.max_wait_ms),
            ],
            "return": Any,
        }
    )
    snapshot_v1.__defaults__ = (snapshot_default_wait_ms,)

    server.add_tool(
        status_v1,
        name=STATUS_TOOL_V1,
        title="Viskium status v1",
        description=(
            "Read bounded public status. This tool does not inspect consent or touch hardware."
        ),
        annotations=read_annotations,
        structured_output=True,
    )
    server.add_tool(
        latest_observation_v1,
        name=LATEST_OBSERVATION_TOOL_V1,
        title="Viskium latest observation v1",
        description=(
            "Read at most one bounded latest observation under the existing out-of-band grant."
        ),
        annotations=read_annotations,
        structured_output=True,
    )
    server.add_tool(
        snapshot_v1,
        name=SNAPSHOT_TOOL_V1,
        title="Viskium one-shot snapshot v1",
        description=(
            "Request at most one bounded PNG under the existing out-of-band grant. "
            "This is not a stream and exposes no camera lifecycle control; the injected "
            "provider may perform its own bounded one-shot open/capture/close cycle."
        ),
        annotations=snapshot_annotations,
        structured_output=False,
    )
    return cast("MCPServer[Any]", server)


def _validated_lowlevel_server(server: Any) -> Any:
    """Validate the single private MCP 2.1 seam used by bounded stdio."""

    lowlevel = getattr(server, "_lowlevel_server", None)
    if not callable(getattr(lowlevel, "run", None)) or not callable(
        getattr(lowlevel, "create_initialization_options", None)
    ):
        raise MCPDependencyError(_MCP_DEPENDENCY_MESSAGE)
    return lowlevel


def _only_transport_errors(error: BaseExceptionGroup[BaseException]) -> bool:
    return all(
        _only_transport_errors(item)
        if isinstance(item, BaseExceptionGroup)
        else isinstance(item, MCPTransportError)
        for item in error.exceptions
    )


async def _run_bounded_stdio(
    server: Any,
    sdk: _MCPBindings,
    service: AgentReadService,
    binary_stdin: BinaryIO,
) -> None:
    limits = service.limits
    bounded_input = _BoundedBinaryLineInput(
        binary_stdin,
        maximum_bytes=limits.max_wire_bytes,
        run_sync=cast(
            "Callable[[Callable[[], bytes]], Awaitable[bytes]]",
            sdk.anyio_module.to_thread.run_sync,
        ),
    )
    lowlevel = _validated_lowlevel_server(server)
    async with sdk.stdio_server(stdin=cast(Any, bounded_input)) as (read_stream, write_stream):
        admission = _InboundRequestAdmission(
            sdk.anyio_module.Semaphore(limits.max_inflight_requests)
        )
        admitted_reads = _AdmissionReadStream(
            read_stream,
            admission,
            sdk.jsonrpc_request_type,
        )
        admitted_writes = _AdmissionWriteStream(
            write_stream,
            admission,
            (sdk.jsonrpc_response_type, sdk.jsonrpc_error_type),
        )
        await lowlevel.run(
            admitted_reads,
            admitted_writes,
            lowlevel.create_initialization_options(),
        )


def run_mcp_server(service: AgentReadService) -> None:
    """Run size- and concurrency-bounded stdio without writing app data to stdout."""

    if not isinstance(service, AgentReadService):
        raise TypeError("service must be an AgentReadService")
    binary_stdin = getattr(sys.stdin, "buffer", None)
    if binary_stdin is None or not callable(getattr(binary_stdin, "readline", None)):
        raise MCPTransportError("MCP stdio requires a binary stdin buffer")
    sdk = _load_mcp_bindings()
    server = create_mcp_server(service)
    try:
        sdk.anyio_module.run(_run_bounded_stdio, server, sdk, service, binary_stdin)
    except BaseExceptionGroup as error:
        if _only_transport_errors(error):
            raise MCPTransportError("MCP stdio rejected an invalid or oversized request") from None
        raise


__all__ = [
    "LATEST_OBSERVATION_TOOL_V1",
    "MCP_SERVER_NAME",
    "MCP_SERVER_VERSION",
    "OBSERVATION_RESULT_CONTRACT_V1",
    "SNAPSHOT_RESULT_CONTRACT_V1",
    "SNAPSHOT_TOOL_V1",
    "STATUS_RESULT_CONTRACT_V1",
    "STATUS_TOOL_V1",
    "MCPDependencyError",
    "MCPTransportError",
    "SnapshotFailureResult",
    "create_mcp_server",
    "run_mcp_server",
]
