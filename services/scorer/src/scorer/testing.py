"""In-memory fakes of the ports, for use-case unit tests (ADR-0002 rule 4)."""
from collections.abc import Sequence

import numpy as np

from fraudcore.contracts import DecisionEvent
from scorer.application.ports import ProfileUnavailable


class FakeProfileStore:
    def __init__(self, profiles: dict[str, dict] | None = None, fail: bool = False) -> None:
        self.profiles, self.fail, self.calls = profiles or {}, fail, 0

    def get_many(self, user_ids: Sequence[str]) -> list[dict]:
        self.calls += 1
        if self.fail:
            raise ProfileUnavailable("redis down")
        return [self.profiles.get(u, {}) for u in user_ids]


class FakeModel:
    """Returns a fixed score per row, or the value in ``column`` scaled by ``scale``."""

    name, version = "fake", "1"

    def __init__(self, scores: Sequence[float]) -> None:
        self.scores, self.batches = list(scores), []

    def predict(self, X: np.ndarray) -> np.ndarray:
        self.batches.append(X.shape)
        return np.array(self.scores[: len(X)], dtype=float)


class ListSink:
    def __init__(self) -> None:
        self.published: list[DecisionEvent] = []
        self.batches = 0

    def publish_batch(self, decisions: Sequence[DecisionEvent]) -> None:
        self.batches += 1
        self.published.extend(decisions)


class FixedClock:
    def __init__(self, ns: int = 5_000) -> None:
        self.ns = ns

    def now_ns(self) -> int:
        return self.ns
