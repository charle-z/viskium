from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from viskium.resources.budget import BudgetDecision, BudgetPolicy, ResourceSnapshot


def _policy() -> BudgetPolicy:
    return BudgetPolicy(
        memory_reserve_bytes=1_000,
        disk_reserve_bytes=2_000,
        max_process_rss_bytes=10_000,
        max_queue_bytes=1_000,
        max_queue_count=4,
    )


def _snapshot(**overrides: Any) -> ResourceSnapshot:
    values: dict[str, Any] = {
        "monotonic_ns": 1,
        "process_rss_bytes": 2_000,
        "available_memory_bytes": 8_000,
        "disk_free_bytes": 9_000,
        "queue_bytes": 0,
        "queue_count": 0,
    }
    values.update(overrides)
    return ResourceSnapshot(**values)


def test_healthy_budget_admits_all_components() -> None:
    assert _policy().evaluate(_snapshot()) == BudgetDecision(
        allow_capture=True,
        allow_processing=True,
        allow_persistence=True,
        severity="normal",
    )


def test_memory_reserve_degrades_then_stops_capture_at_critical_pressure() -> None:
    constrained = _policy().evaluate(
        _snapshot(available_memory_bytes=1_500),
        estimated_working_bytes=600,
    )
    critical = _policy().evaluate(_snapshot(available_memory_bytes=999))

    assert constrained.allow_capture is True
    assert constrained.allow_processing is False
    assert constrained.severity == "constrained"
    assert constrained.reasons == ("memory_reserve",)
    assert critical.allow_capture is False
    assert critical.severity == "critical"


def test_rss_and_queue_limits_block_new_processing() -> None:
    decision = _policy().evaluate(
        _snapshot(process_rss_bytes=9_500, queue_bytes=1_000),
        estimated_working_bytes=501,
    )

    assert decision.allow_processing is False
    assert decision.severity == "critical"
    assert decision.reasons == ("process_rss_limit", "queue_limit")


def test_disk_reserve_only_disables_persistence() -> None:
    decision = _policy().evaluate(
        _snapshot(disk_free_bytes=2_100),
        estimated_write_bytes=101,
    )

    assert decision.allow_capture is True
    assert decision.allow_processing is True
    assert decision.allow_persistence is False
    assert decision.reasons == ("disk_reserve",)


def test_unknown_measurements_fail_closed_for_the_affected_action() -> None:
    decision = _policy().evaluate(
        _snapshot(
            process_rss_bytes=None,
            available_memory_bytes=None,
            disk_free_bytes=None,
        )
    )

    assert decision.allow_capture is True
    assert decision.allow_processing is False
    assert decision.allow_persistence is False
    assert decision.severity == "constrained"


@pytest.mark.parametrize(
    "factory",
    [
        lambda: _policy().evaluate(_snapshot(), estimated_working_bytes=-1),
        lambda: _policy().evaluate(_snapshot(), estimated_write_bytes=True),
        lambda: replace(_snapshot(), queue_count=-1),
        lambda: BudgetPolicy(
            memory_reserve_bytes=0,
            disk_reserve_bytes=1,
            max_process_rss_bytes=1,
            max_queue_bytes=1,
            max_queue_count=1,
        ),
    ],
)
def test_budget_inputs_reject_invalid_numbers(factory: Any) -> None:
    with pytest.raises((TypeError, ValueError)):
        factory()


def test_budget_decision_rejects_inconsistent_or_unbounded_metadata() -> None:
    with pytest.raises(ValueError, match="normal"):
        BudgetDecision(True, True, True, "normal", ("reason",))
    with pytest.raises(ValueError, match="too many"):
        BudgetDecision(False, False, False, "critical", tuple(str(i) for i in range(9)))
    with pytest.raises(TypeError, match="boolean"):
        BudgetDecision(1, True, True, "critical")  # type: ignore[arg-type]
