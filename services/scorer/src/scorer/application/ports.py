"""What the use cases need from the outside world, written from the use case's point of view (ADR-0002).

Critical-path ports are batch-shaped on purpose: one Redis round trip and one inference per micro-batch.
"""
from collections.abc import Sequence
from typing import Protocol

import numpy as np

from fraudcore.contracts import DecisionEvent


class ProfileUnavailable(Exception):
    """Any ProfileStore raises this when it cannot answer in time; the core never sees Redis errors."""


class ProfileStore(Protocol):
    def get_many(self, user_ids: Sequence[str]) -> list[dict]: ...


class ModelRuntime(Protocol):
    name: str
    version: str

    def predict(self, X: np.ndarray) -> np.ndarray: ...


class DecisionSink(Protocol):
    def publish_batch(self, decisions: Sequence[DecisionEvent]) -> None: ...


class Clock(Protocol):
    def now_ns(self) -> int: ...
