"""Load shedding and failure isolation for the slow, optional LLM call."""
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class BackpressurePolicy:
    """The LLM is slower than the flow of cases. Shedding load beats an ever-growing lag (PLAN.md 3.10)."""

    max_inflight: int = 2
    max_age_s: float = 30.0

    def should_skip(self, age_s: float, inflight: int) -> bool:
        return inflight >= self.max_inflight or age_s > self.max_age_s


class CircuitBreaker:
    """Closed -> open after N consecutive failures -> one probe after the cooldown -> closed on success."""

    def __init__(self, failures: int, cooldown_s: float, clock: Callable[[], float]) -> None:
        self._limit, self._cooldown, self._clock = failures, cooldown_s, clock
        self._failures, self._opened_at, self._probing = 0, None, False

    def allow(self) -> bool:
        if self._opened_at is None:
            return True
        if self._probing or self._clock() - self._opened_at < self._cooldown:
            return False
        self._probing = True
        return True

    def record(self, ok: bool) -> None:
        if ok:
            self._failures, self._opened_at, self._probing = 0, None, False
            return
        self._failures += 1
        if self._probing or self._failures >= self._limit:
            self._opened_at, self._probing = self._clock(), False
