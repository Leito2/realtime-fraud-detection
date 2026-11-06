"""What the use cases need from the outside world, written from the use case's point of view (ADR-0002).

Critical-path ports are batch-shaped on purpose: one Redis round trip and one inference per micro-batch.
"""
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import NamedTuple, Protocol

import numpy as np

from fraudcore.contracts import DecisionEvent
from fraudcore.thresholds import Thresholds


class ProfileUnavailable(Exception):
    """Any ProfileStore raises this when it cannot answer in time; the core never sees Redis errors."""


class ProfileStore(Protocol):
    def get_many(self, user_ids: Sequence[str]) -> list[dict]: ...


class Prediction(NamedTuple):
    """Scores plus the identity of the model that produced them, so a hot swap can never mislabel a batch."""

    scores: np.ndarray
    model: str
    version: str
    thresholds: Thresholds | None = None      # published with the model; None = use the configured ones
    reference: dict | None = None             # score distribution at training time, for drift (PSI)


class ModelRuntime(Protocol):
    def predict(self, X: np.ndarray) -> Prediction: ...


class DecisionSink(Protocol):
    def publish_batch(self, decisions: Sequence[DecisionEvent]) -> None: ...


class Clock(Protocol):
    def now_ns(self) -> int: ...


@dataclass(frozen=True)
class BatchStats:
    """What one micro-batch tells the outside world; the use case builds it, an adapter exports it."""

    size: int
    redis_s: float
    inference_s: float
    publish_s: float
    degraded: bool
    model: str
    version: str
    decisions: dict[str, int] = field(default_factory=dict)
    scores: list[float] = field(default_factory=list)
    e2e_s: list[float] = field(default_factory=list)       # decision published - payment scheduled, per event
    score_reference: dict | None = None                   # the serving model's training-time score bins


class Metrics(Protocol):
    def observe_batch(self, stats: BatchStats) -> None: ...


@dataclass(frozen=True)
class SpanData:
    name: str
    start_ns: int
    end_ns: int


class Tracer(Protocol):
    def record(self, traceparent: str, root: SpanData, children: Sequence[SpanData]) -> None: ...


class NullTracer:
    def record(self, traceparent: str, root: SpanData, children: Sequence[SpanData]) -> None:
        pass


class NullMetrics:
    def observe_batch(self, stats: BatchStats) -> None:
        pass
