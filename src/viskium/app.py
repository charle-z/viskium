"""Manual composition roots for executable Viskium surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from os import PathLike
from typing import Any

from viskium import __version__
from viskium.agent import AgentLimits, AgentReadService, CameraSnapshotProvider, ConsentLedger
from viskium.capture import BackendFactory, CaptureRequest, default_capture_request
from viskium.observations import LatestObservationSlot
from viskium.resources.admission import ResourceAdmissionGate
from viskium.resources.budget import BudgetPolicy
from viskium.resources.sampler import ResourceSampler
from viskium.storage import DataRootLayout, verify_data_root

DEFAULT_MEMORY_RESERVE_BYTES = 256 * 1_024 * 1_024
DEFAULT_DISK_RESERVE_BYTES = 512 * 1_024 * 1_024
DEFAULT_MAX_PROCESS_RSS_BYTES = 2 * 1_024 * 1_024 * 1_024
DEFAULT_MAX_QUEUE_BYTES = 1 * 1_024 * 1_024
DEFAULT_MAX_QUEUE_COUNT = 256


def default_budget_policy() -> BudgetPolicy:
    """Return modest-host defaults that callers may replace after profiling."""

    return BudgetPolicy(
        memory_reserve_bytes=DEFAULT_MEMORY_RESERVE_BYTES,
        disk_reserve_bytes=DEFAULT_DISK_RESERVE_BYTES,
        max_process_rss_bytes=DEFAULT_MAX_PROCESS_RSS_BYTES,
        max_queue_bytes=DEFAULT_MAX_QUEUE_BYTES,
        max_queue_count=DEFAULT_MAX_QUEUE_COUNT,
    )


@dataclass(frozen=True, slots=True)
class AgentApplication:
    """Live objects owned by one local agent-server process."""

    service: AgentReadService
    observations: LatestObservationSlot
    snapshots: CameraSnapshotProvider


def build_agent_application(
    data_root: DataRootLayout | str | PathLike[str],
    *,
    capture_request: CaptureRequest | None = None,
    snapshot_backend_factory: BackendFactory | None = None,
    limits: AgentLimits | None = None,
    budget_policy: BudgetPolicy | None = None,
) -> AgentApplication:
    """Wire the bounded local MCP application without opening the camera."""

    layout = (
        verify_data_root(data_root)
        if not isinstance(data_root, DataRootLayout)
        else verify_data_root(data_root.root)
    )
    request = default_capture_request() if capture_request is None else capture_request
    selected_limits = AgentLimits() if limits is None else limits
    selected_budget = default_budget_policy() if budget_policy is None else budget_policy
    if not isinstance(request, CaptureRequest):
        raise TypeError("capture_request must be a CaptureRequest")
    if snapshot_backend_factory is not None and not callable(snapshot_backend_factory):
        raise TypeError("snapshot_backend_factory must be callable")
    if not isinstance(selected_limits, AgentLimits):
        raise TypeError("limits must be AgentLimits")
    if not isinstance(selected_budget, BudgetPolicy):
        raise TypeError("budget_policy must be BudgetPolicy")

    observations = LatestObservationSlot()
    admission = ResourceAdmissionGate(
        sampler=ResourceSampler(layout.root),
        policy=selected_budget,
    )
    snapshots = CameraSnapshotProvider(
        backend_factory=snapshot_backend_factory,
        capture_request=request,
        resource_gate=admission,
        source_id=f"camera-{request.device_index}",
    )
    consent = ConsentLedger(layout)

    def public_status() -> dict[str, Any]:
        snapshot_metrics = snapshots.metrics
        return {
            "schema_version": 1,
            "component": "viskium-agent-read",
            "version": __version__,
            "camera": {
                "mode": "one_shot_on_demand",
                "close_stuck": snapshot_metrics.close_stuck,
                "backend_retained": snapshot_metrics.backend_retained,
            },
            "observations": {
                "producer": "not_configured",
                "capacity": observations.capacity,
            },
            "data_policy": {
                "raw_frames": "ephemeral",
                "visual_persistence": "declared_visual_rejected",
                "classification": "producer_declared",
                "consent": "out_of_band",
            },
        }

    service = AgentReadService(
        observations=observations,
        consent=consent,
        snapshot_provider=snapshots,
        status_provider=public_status,
        limits=selected_limits,
    )
    return AgentApplication(
        service=service,
        observations=observations,
        snapshots=snapshots,
    )


__all__ = [
    "DEFAULT_DISK_RESERVE_BYTES",
    "DEFAULT_MAX_PROCESS_RSS_BYTES",
    "DEFAULT_MAX_QUEUE_BYTES",
    "DEFAULT_MAX_QUEUE_COUNT",
    "DEFAULT_MEMORY_RESERVE_BYTES",
    "AgentApplication",
    "build_agent_application",
    "default_budget_policy",
]
