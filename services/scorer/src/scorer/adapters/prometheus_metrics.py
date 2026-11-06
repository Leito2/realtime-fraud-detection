"""Metrics adapter: the catalog of PLAN.md section 6.3 (serving + business + model layers).

Never put ``user_id`` or ``payment_id`` in a label: cardinality would take Prometheus down.
"""
from collections import deque

import numpy as np
from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram, start_http_server

from fraudcore.drift import psi
from scorer.application.ports import BatchStats

E2E_BUCKETS = [.005, .01, .02, .03, .04, .05, .06, .07, .08, .09, .1, .15, .25, .5, 1, 2.5]
STAGE_BUCKETS = [.0002, .0005, .001, .002, .005, .01, .02, .05, .1, .25]
SCORE_BUCKETS = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0]
BATCH_BUCKETS = [1, 5, 10, 25, 50, 100, 200, 300, 400, 500]


class PrometheusMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY, psi_window: int = 2000,
                 psi_min_scores: int = 500) -> None:
        self._e2e = Histogram("fraud_e2e_latency_seconds", "Payment scheduled -> decision published",
                              buckets=E2E_BUCKETS, registry=registry)
        self._stage = Histogram("fraud_stage_latency_seconds", "Per micro-batch stage duration", ["stage"],
                                buckets=STAGE_BUCKETS, registry=registry)
        self._batch = Histogram("fraud_batch_size", "Events per micro-batch", buckets=BATCH_BUCKETS,
                                registry=registry)
        self._decisions = Counter("fraud_decisions_total", "Decisions by outcome", ["decision"],
                                  registry=registry)
        self._score = Histogram("fraud_score", "Model score distribution", buckets=SCORE_BUCKETS,
                                registry=registry)
        self._degraded = Counter("fraud_degraded_batches_total", "Batches scored without profiles",
                                 registry=registry)
        self._info = Gauge("fraud_model_info", "1 for the model version currently serving",
                           ["model", "version"], registry=registry)
        self._psi = Gauge("fraud_score_psi", "PSI of recent scores vs the serving model's training scores",
                          registry=registry)
        self._recent: deque[float] = deque(maxlen=psi_window)
        self._psi_min = psi_min_scores
        self._current: tuple[str, str] | None = None

    def observe_batch(self, s: BatchStats) -> None:
        for x in s.e2e_s:
            self._e2e.observe(x)
        self._stage.labels("redis").observe(s.redis_s)
        self._stage.labels("inference").observe(s.inference_s)
        self._stage.labels("publish").observe(s.publish_s)
        self._batch.observe(s.size)
        for decision, n in s.decisions.items():
            self._decisions.labels(decision).inc(n)
        for x in s.scores:
            self._score.observe(x)
        if s.degraded:
            self._degraded.inc()
        if self._current != (s.model, s.version):
            if self._current:
                self._info.labels(*self._current).set(0)
            self._info.labels(s.model, s.version).set(1)
            self._current = (s.model, s.version)
            self._recent.clear()                          # a new model's scores are not comparable to the old
        if s.score_reference:
            self._recent.extend(s.scores)
            if len(self._recent) >= self._psi_min:
                self._psi.set(psi(s.score_reference, np.fromiter(self._recent, dtype=np.float64)))


def serve(port: int = 9100) -> PrometheusMetrics:
    metrics = PrometheusMetrics()
    start_http_server(port)
    return metrics
