"""Hardware-neutral ports for capture backends."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from viskium.resources.budget import BudgetDecision

from .contracts import (
    CaptureCapabilities,
    CaptureRead,
    CaptureRequest,
    NegotiatedStream,
)


@runtime_checkable
class CaptureBackend(Protocol):
    @property
    def capabilities(self) -> CaptureCapabilities: ...

    def open(
        self,
        request: CaptureRequest,
        *,
        deadline_monotonic_ns: int,
    ) -> NegotiatedStream: ...

    def read(self, *, deadline_monotonic_ns: int) -> CaptureRead: ...

    def close(self) -> None: ...


@runtime_checkable
class ResourceAdmission(Protocol):
    """Small structural port for admission before continuous capture starts."""

    def evaluate(self, *, stage: str, estimated_bytes: int) -> BudgetDecision: ...


__all__ = ["CaptureBackend", "ResourceAdmission"]
