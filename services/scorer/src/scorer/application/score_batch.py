"""Kafka-path use case: enriched events → decisions (PLAN.md §2.6 step 4)."""
from dataclasses import dataclass

import numpy as np

from fraudcore.contracts import DecisionEvent, EnrichedPayment
from fraudcore.features import PROFILE_DEFAULTS, build_row, vectorize
from fraudcore.thresholds import Thresholds
from scorer.application.ports import Clock, DecisionSink, ModelRuntime, ProfileStore, ProfileUnavailable
from scorer.domain.degraded import DegradedPolicy


@dataclass
class ScoreBatch:
    profiles: ProfileStore
    model: ModelRuntime
    sink: DecisionSink
    thresholds: Thresholds
    policy: DegradedPolicy
    clock: Clock

    def __call__(self, events: list[EnrichedPayment]) -> list[DecisionEvent]:
        if not events:
            return []
        try:
            profiles, degraded = self.profiles.get_many([e.user_id for e in events]), False
        except ProfileUnavailable:
            profiles, degraded = [dict(PROFILE_DEFAULTS) for _ in events], True

        X = np.array([vectorize(build_row(e, p)) for e, p in zip(events, profiles, strict=True)])
        scores = self.model.predict(X)
        now = self.clock.now_ns()
        applied = self.policy.thresholds if degraded else self.thresholds

        decisions = []
        for e, s in zip(events, scores, strict=True):
            score = float(s)
            decision = self.policy.decide(score, e.amount) if degraded else self.thresholds.decide(score)
            decisions.append(DecisionEvent(
                payment_id=e.payment_id, user_id=e.user_id, decision=decision, score=score,
                model=self.model.name, model_version=self.model.version, threshold_set=applied.name,
                degraded=degraded, t_scheduled_ns=e.t_scheduled_ns, t_decided_ns=now))
        self.sink.publish_batch(decisions)
        return decisions
