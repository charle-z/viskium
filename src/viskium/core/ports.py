"""Small runtime-checkable ports for the neutral Viskium slice."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .contracts import FrameEnvelope, ObservationEnvelope, PersistenceReceipt


@runtime_checkable
class FrameSource(Protocol):
    @property
    def source_id(self) -> str: ...

    def next_frame(self) -> FrameEnvelope | None: ...

    def close(self) -> None: ...


@runtime_checkable
class Processor(Protocol):
    @property
    def producer_id(self) -> str: ...

    @property
    def producer_version(self) -> str: ...

    def process(self, frame: FrameEnvelope, *, session_id: str) -> ObservationEnvelope: ...


@runtime_checkable
class ObservationStore(Protocol):
    def put(self, observation: ObservationEnvelope) -> PersistenceReceipt: ...

    def close(self) -> None: ...


@runtime_checkable
class Clock(Protocol):
    def monotonic_ns(self) -> int: ...
