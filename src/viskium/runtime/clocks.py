"""Real and virtual monotonic clocks."""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(slots=True)
class VirtualClock:
    """A manually advanced monotonic clock for deterministic tests and replay."""

    current_ns: int = 0

    def __post_init__(self) -> None:
        if isinstance(self.current_ns, bool) or not isinstance(self.current_ns, int):
            raise TypeError("current_ns must be an integer")
        if self.current_ns < 0:
            raise ValueError("current_ns must be non-negative")

    def monotonic_ns(self) -> int:
        return self.current_ns

    def advance_ns(self, delta_ns: int) -> int:
        if isinstance(delta_ns, bool) or not isinstance(delta_ns, int):
            raise TypeError("delta_ns must be an integer")
        if delta_ns < 0:
            raise ValueError("a monotonic clock cannot advance backwards")
        self.current_ns += delta_ns
        return self.current_ns

    def advance_to_ns(self, target_ns: int) -> int:
        if isinstance(target_ns, bool) or not isinstance(target_ns, int):
            raise TypeError("target_ns must be an integer")
        if target_ns < self.current_ns:
            raise ValueError("a monotonic clock cannot move backwards")
        self.current_ns = target_ns
        return self.current_ns


@dataclass(frozen=True, slots=True)
class SystemClock:
    """Thin injectable wrapper around the operating system monotonic clock."""

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()
