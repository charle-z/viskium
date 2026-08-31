"""Explicitly opted-in physical camera smoke test.

The test requests one bounded still image, keeps it in memory, and relies on
the provider's same-call close guarantee. It never changes optical controls,
starts a stream, retries, displays the image, or writes image bytes to disk.
"""

from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Any

import anyio
import pytest
from mcp import Client
from mcp.types import ImageContent

from viskium.adapters.opencv_process_camera import (
    OpenCVProcessCameraBackend,
    OpenCVWorkerState,
)
from viskium.agent import ConsentLedger
from viskium.agent.mcp_server import SNAPSHOT_TOOL_V1, create_mcp_server
from viskium.app import build_agent_application
from viskium.capture import CaptureRequest
from viskium.snapshots.contracts import PNG_SIGNATURE
from viskium.storage import initialize_data_root

pytestmark = pytest.mark.hardware


def test_default_camera_can_deliver_one_ephemeral_png_through_mcp(tmp_path: Path) -> None:
    if os.environ.get("VISKIUM_RUN_CAMERA_TESTS") != "1":
        pytest.skip("set VISKIUM_RUN_CAMERA_TESTS=1 for one physical camera capture")

    device_index = int(os.environ.get("VISKIUM_CAMERA_DEVICE_INDEX", "0"))
    layout = initialize_data_root(tmp_path / "data")
    consent = ConsentLedger(layout)
    consent.grant(
        scopes=frozenset({"snapshot.read"}),
        duration_seconds=60,
        snapshot_quota=1,
        sensitivity_ceiling="identifiable",
    )
    backends: list[OpenCVProcessCameraBackend] = []

    def backend_factory() -> OpenCVProcessCameraBackend:
        backend = OpenCVProcessCameraBackend()
        backends.append(backend)
        return backend

    application = build_agent_application(
        layout,
        capture_request=CaptureRequest(
            device_index=device_index,
            requested_width=640,
            requested_height=480,
            requested_fps=15.0,
            max_frame_bytes=1_048_576,
        ),
        snapshot_backend_factory=backend_factory,
    )
    server = create_mcp_server(application.service)

    async def scenario() -> Any:
        async with Client(server) as client:
            # Omit wait_ms deliberately: the public tool default is 10 seconds.
            return await client.call_tool(SNAPSHOT_TOOL_V1, {"max_edge_px": 640})

    result = anyio.run(scenario)

    assert not result.is_error
    assert len(result.content) == 1
    image = result.content[0]
    assert isinstance(image, ImageContent)
    assert image.mime_type == "image/png"
    png_bytes = base64.b64decode(image.data, validate=True)
    assert png_bytes.startswith(PNG_SIGNATURE)
    assert len(png_bytes) <= application.service.limits.max_snapshot_bytes

    state = consent.load()
    assert state is not None
    assert state.snapshot_attempts == 1
    assert state.grant.snapshot_quota == 1
    assert len(backends) == 1
    assert not backends[0].is_open
    assert backends[0].worker_state is OpenCVWorkerState.ABSENT
    metrics = application.snapshots.metrics
    assert metrics.delivered == 1
    assert metrics.close_failures == 0
    assert not metrics.close_stuck
    assert not metrics.backend_retained
    assert metrics.worker_state == "absent"
    assert not metrics.active
    for owned_file in layout.root.rglob("*"):
        if owned_file.is_file():
            assert PNG_SIGNATURE not in owned_file.read_bytes()
