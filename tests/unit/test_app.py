from __future__ import annotations

import json
from pathlib import Path
from typing import Never

import pytest

from viskium.agent import AgentLimits
from viskium.app import AgentApplication, build_agent_application, default_budget_policy
from viskium.capture import CaptureRequest
from viskium.storage import StorageLayoutError, initialize_data_root


def test_agent_application_builds_without_importing_or_opening_opencv(tmp_path: Path) -> None:
    layout = initialize_data_root(tmp_path / "data")
    backend_factory_calls = 0

    def forbidden_backend_factory() -> Never:
        nonlocal backend_factory_calls
        backend_factory_calls += 1
        raise AssertionError("status must not create a camera backend")

    application = build_agent_application(
        layout,
        snapshot_backend_factory=forbidden_backend_factory,
    )
    status = application.service.status()

    assert isinstance(application, AgentApplication)
    assert application.snapshots.metrics.requests == 0
    assert application.snapshots.metrics.backend_instances == 0
    assert backend_factory_calls == 0
    assert status.outcome == "ok"
    assert status.metadata_json is not None
    document = json.loads(status.metadata_json)
    assert document["status"]["camera"] == {
        "backend_retained": False,
        "close_stuck": False,
        "mode": "one_shot_on_demand",
    }
    assert document["status"]["observations"] == {
        "capacity": 1,
        "producer": "not_configured",
    }
    assert document["status"]["data_policy"]["raw_frames"] == "ephemeral"
    assert document["status"]["data_policy"]["visual_persistence"] == ("declared_visual_rejected")
    assert document["status"]["data_policy"]["classification"] == "producer_declared"
    assert document["limits"]["max_wait_ms"] == 10_000


def test_agent_application_accepts_tuned_limits_and_capture_request(tmp_path: Path) -> None:
    layout = initialize_data_root(tmp_path / "data")
    request = CaptureRequest(2, 1_280, 720, 24.0, 1_280 * 720 * 3)
    limits = AgentLimits(
        max_snapshot_bytes=8 * 1_024 * 1_024,
        max_snapshot_edge_px=1_920,
        max_wait_ms=15_000,
    )

    application = build_agent_application(
        layout.root,
        capture_request=request,
        limits=limits,
        budget_policy=default_budget_policy(),
    )

    assert application.service.limits is limits
    assert application.snapshots.metrics.requests == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"capture_request": object()},
        {"snapshot_backend_factory": object()},
        {"limits": object()},
        {"budget_policy": object()},
    ],
)
def test_agent_application_rejects_invalid_composition_inputs(
    tmp_path: Path,
    kwargs: dict[str, object],
) -> None:
    layout = initialize_data_root(tmp_path / "data")

    with pytest.raises(TypeError):
        build_agent_application(layout, **kwargs)  # type: ignore[arg-type]


def test_agent_application_requires_an_explicitly_initialized_data_root(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing"

    with pytest.raises(StorageLayoutError):
        build_agent_application(missing)

    assert not missing.exists()
