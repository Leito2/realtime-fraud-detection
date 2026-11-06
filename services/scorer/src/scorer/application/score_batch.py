"""Kafka-path use case: enriched events → decisions (PLAN.md §2.6 step 4)."""
from collections import Counter
from dataclasses import dataclass, field

import numpy as np

from fraudcore.contracts import DecisionEvent, EnrichedPayment
from fraudcore.features import FEATURE_ORDER, PROFILE_DEFAULTS, build_row, vectorize
from fraudcore.thresholds import Thresholds
from scorer.application.ports import (
    BatchStats,
    Clock,
    DecisionSink,
    Metrics,
    ModelRuntime,
    NullMetrics,
    NullTracer,
    ProfileStore,
    ProfileUnavailable,
    SpanData,
    Tracer,
)
from scorer.domain.degraded import DegradedPolicy


@dataclass
class ScoreBatch:
    profiles: ProfileStore
    model: ModelRuntime
    sink: DecisionSink
    thresholds: Thresholds
    policy: DegradedPolicy
    clock: Clock
    metrics: Metrics = field(default_factory=NullMetrics)
    tracer: Tracer = field(default_factory=NullTracer)
    log_features: bool | str = False         # False | True/"all" | "flagged" (only REVIEW and BLOCK)

    def __call__(self, events: list[EnrichedPayment]) -> list[DecisionEvent]:
        if not events:
            return []
        t0 = self.clock.now_ns()
        try:
            profiles, degraded = self.profiles.get_many([e.user_id for e in events]), False
        except ProfileUnavailable:
            profiles, degraded = [dict(PROFILE_DEFAULTS) for _ in events], True

        t1 = self.clock.now_ns()
        X = np.array([vectorize(build_row(e, p)) for e, p in zip(events, profiles, strict=True)])
        served = None
        if self.log_features:                              # "flagged" filters per decision below
            served = [dict(zip(FEATURE_ORDER, map(float, x), strict=True)) for x in X]
        prediction = self.model.predict(X)
        now = self.clock.now_ns()                 # t_decided: the model has spoken
        t2 = now
        own = prediction.thresholds or self.thresholds          # a model carries the cutoffs it was tuned for
        applied = self.policy.thresholds if degraded else own

        decisions = []
        for i, (e, s) in enumerate(zip(events, prediction.scores, strict=True)):
            score = float(s)
            decision = self.policy.decide(score, e.amount) if degraded else own.decide(score)
            decisions.append(DecisionEvent(
                payment_id=e.payment_id, user_id=e.user_id, decision=decision, score=score,
                model=prediction.model, model_version=prediction.version, threshold_set=applied.name,
                degraded=degraded, t_scheduled_ns=e.t_scheduled_ns, t_decided_ns=now,
                features=served[i] if served and (self.log_features != "flagged" or decision != "APPROVE")
                else None))
        self.sink.publish_batch(decisions)
        t3 = self.clock.now_ns()
        self.metrics.observe_batch(BatchStats(
            size=len(events), redis_s=(t1 - t0) / 1e9, inference_s=(t2 - t1) / 1e9, publish_s=(t3 - t2) / 1e9,
            degraded=degraded, model=prediction.model, version=prediction.version,
            decisions=dict(Counter(d.decision for d in decisions)), scores=[d.score for d in decisions],
            e2e_s=[(t3 - d.t_scheduled_ns) / 1e9 for d in decisions],
            score_reference=prediction.reference))
        for e in events:
            if e.traceparent:
                self.tracer.record(e.traceparent, SpanData("payment.decision", e.t_scheduled_ns, t3), [
                    SpanData("kafka+flink transit", e.t_scheduled_ns, t0), SpanData("profile lookup", t0, t1),
                    SpanData("model inference", t1, t2), SpanData("publish decision", t2, t3)])
        return decisions
