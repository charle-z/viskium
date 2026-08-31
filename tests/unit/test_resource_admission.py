from __future__ import annotations

from dataclasses import FrozenInstanceError
from threading import Barrier, Lock, Thread
from typing import Any

import pytest

from viskium.resources import ResourceAdmissionGate, ResourceAdmissionMetrics
from viskium.resources.budget import BudgetPolicy, ResourceSnapshot
from viskium.resources.sampler import ResourceSample
from viskium.runtime.live import AdmissionGate


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


class _Sampler:
    def __init__(self, *samples: ResourceSample) -> None:
        self._samples = samples
        self.calls = 0

    def sample(self) -> ResourceSample:
        selected = self._samples[min(self.calls, len(self._samples) - 1)]
        self.calls += 1
        return selected


class _FailingSampler:
    def __init__(self) -> None:
        self.calls = 0

    def sample(self) -> ResourceSample:
        self.calls += 1
        raise OSError("injected sampler failure")


class _Clock:
    def __init__(self, *values: int) -> None:
        self._values = iter(values)

    def __call__(self) -> int:
        return next(self._values)


def test_gate_structurally_satisfies_live_admission_protocol() -> None:
    gate = ResourceAdmissionGate(
        sampler=_Sampler(ResourceSample(_snapshot())),
        policy=_policy(),
        monotonic_ns=lambda: 0,
    )

    assert isinstance(gate, AdmissionGate)


def test_cached_snapshot_reapplies_each_processing_estimate() -> None:
    sampler = _Sampler(ResourceSample(_snapshot(available_memory_bytes=1_500)))
    gate = ResourceAdmissionGate(sampler=sampler, policy=_policy(), monotonic_ns=lambda: 1)

    admitted = gate.evaluate(stage="processing", estimated_bytes=500)
    rejected = gate.evaluate(stage="processing", estimated_bytes=501)

    assert admitted.allow_processing is True
    assert rejected.allow_processing is False
    assert rejected.reasons == ("memory_reserve",)
    assert sampler.calls == 1
    assert gate.metrics == ResourceAdmissionMetrics(samples=1, cache_hits=1, failures=0)


def test_cached_snapshot_reapplies_each_persistence_estimate() -> None:
    sampler = _Sampler(ResourceSample(_snapshot(disk_free_bytes=2_100)))
    gate = ResourceAdmissionGate(sampler=sampler, policy=_policy(), monotonic_ns=lambda: 1)

    admitted = gate.evaluate(stage="persistence", estimated_bytes=100)
    rejected = gate.evaluate(stage="persistence", estimated_bytes=101)

    assert admitted.allow_persistence is True
    assert rejected.allow_persistence is False
    assert rejected.reasons == ("disk_reserve",)
    assert sampler.calls == 1


def test_cached_host_snapshot_reapplies_current_queue_pressure() -> None:
    sampler = _Sampler(ResourceSample(_snapshot()))
    gate = ResourceAdmissionGate(sampler=sampler, policy=_policy(), monotonic_ns=lambda: 1)

    admitted = gate.evaluate(
        stage="processing",
        estimated_bytes=0,
        queue_bytes=999,
        queue_count=3,
    )
    rejected = gate.evaluate(
        stage="processing",
        estimated_bytes=0,
        queue_bytes=1_000,
        queue_count=3,
    )

    assert admitted.allow_processing is True
    assert rejected.allow_processing is False
    assert rejected.reasons == ("queue_limit",)
    assert sampler.calls == 1
    assert gate.metrics == ResourceAdmissionMetrics(samples=1, cache_hits=1, failures=0)


def test_cache_refreshes_at_interval_boundary_and_after_clock_regression() -> None:
    sampler = _Sampler(ResourceSample(_snapshot()), ResourceSample(_snapshot(monotonic_ns=2)))
    clock = _Clock(0, 249_999_999, 250_000_000, 249_000_000)
    gate = ResourceAdmissionGate(sampler=sampler, policy=_policy(), monotonic_ns=clock)

    for _ in range(4):
        assert gate.evaluate(stage="processing", estimated_bytes=0).allow_processing is True

    assert sampler.calls == 3
    assert gate.metrics == ResourceAdmissionMetrics(samples=3, cache_hits=1, failures=0)


def test_zero_cache_interval_samples_every_decision() -> None:
    sampler = _Sampler(ResourceSample(_snapshot()))
    gate = ResourceAdmissionGate(
        sampler=sampler,
        policy=_policy(),
        cache_interval_ns=0,
        monotonic_ns=lambda: 1,
    )

    gate.evaluate(stage="processing", estimated_bytes=0)
    gate.evaluate(stage="persistence", estimated_bytes=0)

    assert sampler.calls == 2
    assert gate.metrics.samples == 2
    assert gate.metrics.cache_hits == 0


def test_sampler_exception_fails_closed_and_caches_only_unknown_snapshot() -> None:
    sampler = _FailingSampler()
    gate = ResourceAdmissionGate(sampler=sampler, policy=_policy(), monotonic_ns=lambda: 10)

    processing = gate.evaluate(stage="processing", estimated_bytes=0)
    persistence = gate.evaluate(stage="persistence", estimated_bytes=0)

    assert processing.allow_processing is False
    assert persistence.allow_persistence is False
    assert processing.severity == persistence.severity == "constrained"
    assert "memory_available_unknown" in processing.reasons
    assert "disk_free_unknown" in persistence.reasons
    assert sampler.calls == 1
    assert gate.metrics == ResourceAdmissionMetrics(samples=1, cache_hits=1, failures=1)


def test_sampler_reported_errors_fail_only_the_affected_boundary_closed() -> None:
    sampler = _Sampler(
        ResourceSample(
            _snapshot(disk_free_bytes=None),
            errors=("disk_probe_failed",),
        )
    )
    gate = ResourceAdmissionGate(sampler=sampler, policy=_policy(), monotonic_ns=lambda: 10)

    decision = gate.evaluate(stage="processing", estimated_bytes=0)

    assert decision.allow_processing is True
    assert decision.allow_persistence is False
    assert decision.severity == "constrained"
    assert gate.metrics.failures == 1


def test_concurrent_decisions_share_one_sample_safely() -> None:
    sampler = _Sampler(ResourceSample(_snapshot()))
    gate = ResourceAdmissionGate(sampler=sampler, policy=_policy(), monotonic_ns=lambda: 1)
    start = Barrier(9)
    results: list[bool] = []
    results_lock = Lock()

    def evaluate_once() -> None:
        start.wait(timeout=2.0)
        decision = gate.evaluate(stage="processing", estimated_bytes=1)
        with results_lock:
            results.append(decision.allow_processing)

    workers = [Thread(target=evaluate_once) for _ in range(8)]
    for worker in workers:
        worker.start()
    start.wait(timeout=2.0)
    for worker in workers:
        worker.join(timeout=2.0)

    assert all(not worker.is_alive() for worker in workers)
    assert results == [True] * 8
    assert sampler.calls == 1
    assert gate.metrics == ResourceAdmissionMetrics(samples=1, cache_hits=7, failures=0)


@pytest.mark.parametrize(
    ("kwargs", "error_type"),
    [
        ({"stage": "capture"}, ValueError),
        ({"estimated_bytes": -1}, ValueError),
        ({"estimated_bytes": True}, TypeError),
        ({"estimated_bytes": 2**63}, ValueError),
        ({"queue_bytes": -1}, ValueError),
        ({"queue_bytes": True}, TypeError),
        ({"queue_count": 2**63}, ValueError),
    ],
)
def test_evaluate_rejects_invalid_control_inputs(
    kwargs: dict[str, Any], error_type: type[Exception]
) -> None:
    values: dict[str, Any] = {"stage": "processing", "estimated_bytes": 0}
    values.update(kwargs)
    gate = ResourceAdmissionGate(
        sampler=_Sampler(ResourceSample(_snapshot())),
        policy=_policy(),
    )

    with pytest.raises(error_type):
        gate.evaluate(**values)


@pytest.mark.parametrize(
    ("kwargs", "error_type"),
    [
        ({"sampler": object()}, TypeError),
        ({"policy": object()}, TypeError),
        ({"cache_interval_ns": -1}, ValueError),
        ({"cache_interval_ns": True}, TypeError),
        ({"cache_interval_ns": 2**63}, ValueError),
        ({"monotonic_ns": 1}, TypeError),
    ],
)
def test_constructor_rejects_invalid_dependencies_and_limits(
    kwargs: dict[str, Any], error_type: type[Exception]
) -> None:
    values: dict[str, Any] = {
        "sampler": _Sampler(ResourceSample(_snapshot())),
        "policy": _policy(),
    }
    values.update(kwargs)
    with pytest.raises(error_type):
        ResourceAdmissionGate(**values)


def test_invalid_clock_and_sampler_results_fail_safely() -> None:
    invalid_sample = _Sampler()  # No result can be selected.
    invalid_snapshot = ResourceSample(snapshot=object())  # type: ignore[arg-type]
    bad_clock_gate = ResourceAdmissionGate(
        sampler=_Sampler(ResourceSample(_snapshot())),
        policy=_policy(),
        monotonic_ns=lambda: -1,
    )

    with pytest.raises(ValueError, match="monotonic"):
        bad_clock_gate.evaluate(stage="processing", estimated_bytes=0)

    invalid_gate = ResourceAdmissionGate(
        sampler=invalid_sample,
        policy=_policy(),
        monotonic_ns=lambda: 1,
    )
    decision = invalid_gate.evaluate(stage="processing", estimated_bytes=0)
    assert decision.allow_processing is False
    assert invalid_gate.metrics.failures == 1

    invalid_snapshot_gate = ResourceAdmissionGate(
        sampler=_Sampler(invalid_snapshot),
        policy=_policy(),
        monotonic_ns=lambda: 1,
    )
    decision = invalid_snapshot_gate.evaluate(stage="persistence", estimated_bytes=0)
    assert decision.allow_persistence is False
    assert invalid_snapshot_gate.metrics.failures == 1


def test_metrics_are_frozen_slotted_and_validate_int64() -> None:
    metrics = ResourceAdmissionMetrics(samples=1, cache_hits=2, failures=3)

    assert not hasattr(metrics, "__dict__")
    with pytest.raises(FrozenInstanceError):
        metrics.samples = 4  # type: ignore[misc]
    with pytest.raises(ValueError, match="int64"):
        ResourceAdmissionMetrics(samples=2**63, cache_hits=0, failures=0)
    with pytest.raises(TypeError):
        ResourceAdmissionMetrics(samples=True, cache_hits=0, failures=0)
