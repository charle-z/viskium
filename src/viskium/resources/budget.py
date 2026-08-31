"""Pure resource admission decisions for bounded Viskium runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

type BudgetSeverity = Literal["normal", "constrained", "critical"]

_MAX_INT64 = 2**63 - 1


def _non_negative(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if not 0 <= value <= _MAX_INT64:
        raise ValueError(f"{field_name} must be between zero and signed int64 max")
    return value


def _positive(value: object, field_name: str) -> int:
    parsed = _non_negative(value, field_name)
    if parsed == 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


@dataclass(frozen=True, slots=True)
class ResourceSnapshot:
    """One bounded, already-aggregated view of process and host pressure."""

    monotonic_ns: int
    process_rss_bytes: int | None
    available_memory_bytes: int | None
    disk_free_bytes: int | None
    queue_bytes: int
    queue_count: int

    def __post_init__(self) -> None:
        _non_negative(self.monotonic_ns, "monotonic_ns")
        _non_negative(self.queue_bytes, "queue_bytes")
        _non_negative(self.queue_count, "queue_count")
        for field_name in ("process_rss_bytes", "available_memory_bytes", "disk_free_bytes"):
            value = getattr(self, field_name)
            if value is not None:
                _non_negative(value, field_name)


@dataclass(frozen=True, slots=True)
class BudgetPolicy:
    """Conservative ceilings used before admitting work or a disk write."""

    memory_reserve_bytes: int
    disk_reserve_bytes: int
    max_process_rss_bytes: int
    max_queue_bytes: int
    max_queue_count: int

    def __post_init__(self) -> None:
        for field_name in (
            "memory_reserve_bytes",
            "disk_reserve_bytes",
            "max_process_rss_bytes",
            "max_queue_bytes",
            "max_queue_count",
        ):
            _positive(getattr(self, field_name), field_name)

    def evaluate(
        self,
        snapshot: ResourceSnapshot,
        *,
        estimated_working_bytes: int = 0,
        estimated_write_bytes: int = 0,
    ) -> BudgetDecision:
        """Return a deterministic decision without mutating any component."""

        work_bytes = _non_negative(estimated_working_bytes, "estimated_working_bytes")
        write_bytes = _non_negative(estimated_write_bytes, "estimated_write_bytes")
        reasons: list[str] = []
        allow_capture = True
        allow_processing = True
        allow_persistence = True
        severity: BudgetSeverity = "normal"

        if snapshot.available_memory_bytes is None:
            reasons.append("memory_available_unknown")
            allow_processing = False
            severity = "constrained"
        elif snapshot.available_memory_bytes < self.memory_reserve_bytes + work_bytes:
            reasons.append("memory_reserve")
            allow_processing = False
            severity = "constrained"
            if snapshot.available_memory_bytes < self.memory_reserve_bytes:
                allow_capture = False
                severity = "critical"

        if snapshot.process_rss_bytes is None:
            reasons.append("process_rss_unknown")
            allow_processing = False
            severity = "constrained" if severity == "normal" else severity
        elif snapshot.process_rss_bytes + work_bytes > self.max_process_rss_bytes:
            reasons.append("process_rss_limit")
            allow_processing = False
            severity = "critical"

        if (
            snapshot.queue_bytes >= self.max_queue_bytes
            or snapshot.queue_count >= self.max_queue_count
        ):
            reasons.append("queue_limit")
            allow_processing = False
            severity = "critical"

        if snapshot.disk_free_bytes is None:
            reasons.append("disk_free_unknown")
            allow_persistence = False
            severity = "constrained" if severity == "normal" else severity
        elif snapshot.disk_free_bytes < self.disk_reserve_bytes + write_bytes:
            reasons.append("disk_reserve")
            allow_persistence = False
            severity = "critical"

        return BudgetDecision(
            allow_capture=allow_capture,
            allow_processing=allow_processing,
            allow_persistence=allow_persistence,
            severity=severity,
            reasons=tuple(reasons),
        )


@dataclass(frozen=True, slots=True)
class BudgetDecision:
    """Explicit component-local actions produced by a budget evaluation."""

    allow_capture: bool
    allow_processing: bool
    allow_persistence: bool
    severity: BudgetSeverity
    reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field_name in ("allow_capture", "allow_processing", "allow_persistence"):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a boolean")
        if self.severity not in {"normal", "constrained", "critical"}:
            raise ValueError("unsupported budget severity")
        if isinstance(self.reasons, (str, bytes)):
            raise TypeError("reasons must be an iterable of strings")
        normalized = tuple(self.reasons)
        if any(not isinstance(reason, str) or not reason for reason in normalized):
            raise ValueError("budget reasons must be non-empty strings")
        if len(normalized) > 8:
            raise ValueError("budget decision has too many reasons")
        if self.severity == "normal" and normalized:
            raise ValueError("a normal decision cannot contain pressure reasons")
        object.__setattr__(self, "reasons", normalized)


__all__ = [
    "BudgetDecision",
    "BudgetPolicy",
    "BudgetSeverity",
    "ResourceSnapshot",
]
