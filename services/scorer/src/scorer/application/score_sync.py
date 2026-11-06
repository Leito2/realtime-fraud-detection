"""Synchronous scoring use case (PLAN.md 14): a payment gateway asks, a decision comes back inside a deadline.

It shares everything with the Kafka path (features from ``fraudcore``, the model runtime, thresholds, the
degraded policy) by delegating to ``ScoreBatch``. The one difference is velocity: there is no Flink window
here, so it is rebuilt from the Feature Writer's last state plus the current payment (``sync_velocity``).
"""
from collections.abc import Sequence
from dataclasses import dataclass

from fraudcore.contracts import DecisionEvent, EnrichedPayment, Payment
from fraudcore.features import sync_velocity
from scorer.application.ports import ProfileUnavailable, VelocityStore
from scorer.application.score_batch import ScoreBatch

MIN_LOOKUP_BUDGET_S = 0.003          # below this a Redis round trip cannot finish: answer degraded instead


@dataclass
class ScoreSync:
    velocity: VelocityStore
    batch: ScoreBatch

    def __call__(self, payments: Sequence[Payment], budget_s: float | None = None) -> list[DecisionEvent]:
        """One decision per payment, in order. ``budget_s`` is the time left before the caller's deadline."""
        if not payments:
            return []
        now = self.batch.clock.now_ns()
        degraded = budget_s is not None and budget_s < MIN_LOOKUP_BUDGET_S
        snapshots: list[dict | None] = [None] * len(payments)
        if not degraded:
            try:
                snapshots = self.velocity.get_many([p.user_id for p in payments])
            except ProfileUnavailable:
                degraded = True
        events = [
            EnrichedPayment(**{**p.model_dump(), "t_scheduled_ns": now, "t_sent_ns": now},
                            **sync_velocity(s, p.amount, p.event_time.timestamp()))
            for p, s in zip(payments, snapshots, strict=True)]
        return self.batch(events, force_degraded=degraded)
