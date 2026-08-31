"""Thread-safe cached resource admission for the live runtime."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from threading import Lock
from typing import Protocol, runtime_checkable

from .budget import BudgetDecision, BudgetPolicy, ResourceSnapshot
from .sampler import ResourceSample

_MAX_INT64 = 2**63 - 1
DEFAULT_RESOURCE_CACHE_INTERVAL_NS = 250_000_000


def _bounded_integer(value: object, field_name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not minimum <= value <= _MAX_INT64:
        raise ValueError(f"{field_name} must be between {minimum} and signed int64 max")
    return value


@runtime_checkable
class _ResourceSamplerPort(Protocol):
    def sample(self) -> ResourceSample: ...


@dataclass(frozen=True, slots=True)
class ResourceAdmissionMetrics:
    """Immutable counters that retain no frames or observation data."""

    samples: int
    cache_hits: int
    failures: int

    def __post_init__(self) -> None:
        _bounded_integer(self.samples, "samples")
        _bounded_integer(self.cache_hits, "cache_hits")
        _bounded_integer(self.failures, "failures")


class ResourceAdmissionGate:
    """Apply a pure budget policy to a short-lived resource snapshot cache.

    Only ``ResourceSnapshot`` is cached.  Estimated bytes are never cached and
    are reapplied to the policy on every decision.
    """

    __slots__ = (
        "_cache_hits",
        "_cache_interval_ns",
        "_cached_at_ns",
        "_cached_snapshot",
        "_failures",
        "_lock",
        "_monotonic_ns",
        "_policy",
        "_sampler",
        "_samples",
    )

    def __init__(
        self,
        *,
        sampler: _ResourceSamplerPort,
        policy: BudgetPolicy,
        cache_interval_ns: int = DEFAULT_RESOURCE_CACHE_INTERVAL_NS,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if not isinstance(sampler, _ResourceSamplerPort):
            raise TypeError("sampler must provide sample()")
        if not isinstance(policy, BudgetPolicy):
            raise TypeError("policy must be a BudgetPolicy")
        self._cache_interval_ns = _bounded_integer(cache_interval_ns, "cache_interval_ns")
        if not callable(monotonic_ns):
            raise TypeError("monotonic_ns must be callable")

        self._sampler = sampler
        self._policy = policy
        self._monotonic_ns = monotonic_ns
        self._lock = Lock()
        self._cached_snapshot: ResourceSnapshot | None = None
        self._cached_at_ns: int | None = None
        self._samples = 0
        self._cache_hits = 0
        self._failures = 0

    def evaluate(
        self,
        *,
        stage: str,
        estimated_bytes: int,
        queue_bytes: int = 0,
        queue_count: int = 0,
    ) -> BudgetDecision:
        """Evaluate processing memory or persistence bytes against current pressure."""

        if stage not in {"processing", "persistence"}:
            raise ValueError("unsupported admission stage")
        estimate = _bounded_integer(estimated_bytes, "estimated_bytes")
        current_queue_bytes = _bounded_integer(queue_bytes, "queue_bytes")
        current_queue_count = _bounded_integer(queue_count, "queue_count")
        snapshot = replace(
            self._snapshot(),
            queue_bytes=current_queue_bytes,
            queue_count=current_queue_count,
        )
        if stage == "processing":
            return self._policy.evaluate(snapshot, estimated_working_bytes=estimate)
        return self._policy.evaluate(snapshot, estimated_write_bytes=estimate)

    @property
    def metrics(self) -> ResourceAdmissionMetrics:
        with self._lock:
            return ResourceAdmissionMetrics(
                samples=self._samples,
                cache_hits=self._cache_hits,
                failures=self._failures,
            )

    def _snapshot(self) -> ResourceSnapshot:
        with self._lock:
            now_ns = _bounded_integer(self._monotonic_ns(), "monotonic clock value")
            if self._cache_is_fresh(now_ns):
                self._increment("_cache_hits")
                cached = self._cached_snapshot
                if cached is None:  # Defensive: freshness requires a snapshot.
                    raise RuntimeError("resource cache invariant violated")
                return cached

            self._increment("_samples")
            try:
                sample = self._sampler.sample()
                if not isinstance(sample, ResourceSample):
                    raise TypeError("sampler returned an invalid resource sample")
                if not isinstance(sample.snapshot, ResourceSnapshot):
                    raise TypeError("sampler returned an invalid resource snapshot")
            except Exception:
                snapshot = self._unknown_snapshot(now_ns)
                self._increment("_failures")
            else:
                snapshot = sample.snapshot
                if sample.errors:
                    # Missing fields remain ``None`` in the sampled snapshot, so
                    # BudgetPolicy fails only the affected resource boundary
                    # closed (for example, disk loss does not stop processing).
                    self._increment("_failures")

            self._cached_snapshot = snapshot
            self._cached_at_ns = now_ns
            return snapshot

    def _cache_is_fresh(self, now_ns: int) -> bool:
        return (
            self._cache_interval_ns > 0
            and self._cached_snapshot is not None
            and self._cached_at_ns is not None
            and now_ns >= self._cached_at_ns
            and now_ns - self._cached_at_ns < self._cache_interval_ns
        )

    def _increment(self, field_name: str) -> None:
        current = getattr(self, field_name)
        if current < _MAX_INT64:
            setattr(self, field_name, current + 1)

    @staticmethod
    def _unknown_snapshot(monotonic_ns: int) -> ResourceSnapshot:
        return ResourceSnapshot(
            monotonic_ns=monotonic_ns,
            process_rss_bytes=None,
            available_memory_bytes=None,
            disk_free_bytes=None,
            queue_bytes=0,
            queue_count=0,
        )


__all__ = [
    "DEFAULT_RESOURCE_CACHE_INTERVAL_NS",
    "ResourceAdmissionGate",
    "ResourceAdmissionMetrics",
]
