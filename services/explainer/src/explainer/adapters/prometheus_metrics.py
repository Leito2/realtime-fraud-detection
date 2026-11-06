"""Explainer metrics for dashboard D5. No payment or user ids in labels."""
from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram, start_http_server

BUCKETS = [.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30]


class PrometheusMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._total = Counter("explanations_total", "Explanations by outcome", ["status"], registry=registry)
        self._rejected = Counter("explanations_rejected_total", "LLM texts replaced for unsupported claims",
                                 registry=registry)
        self._seconds = Histogram("explainer_seconds", "Time to explain one decision", buckets=BUCKETS,
                                  registry=registry)
        self._llm = Histogram("explainer_llm_latency_seconds", "Time spent in the LLM call", buckets=BUCKETS,
                              registry=registry)
        self.inflight = Gauge("explainer_inflight", "Explanations being produced now", registry=registry)

    def observe(self, status: str, seconds: float, llm_seconds: float | None = None,
                rejected: bool = False) -> None:
        self._total.labels(status).inc()
        self._seconds.observe(seconds)
        if llm_seconds is not None:
            self._llm.observe(llm_seconds)
        if rejected:
            self._rejected.inc()


def serve(port: int = 9102) -> PrometheusMetrics:
    metrics = PrometheusMetrics()
    start_http_server(port)
    return metrics
