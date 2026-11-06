"""Pure latency maths: HdrHistogram in microseconds (PLAN.md §3.12)."""
from hdrh.histogram import HdrHistogram

_MAX_US = 60_000_000      # 60 s


def e2e_ms(broker_ts_ms: int, t_scheduled_ns: int) -> float:
    return broker_ts_ms - t_scheduled_ns / 1e6


class LatencyStats:
    """Two views of the same decisions: broker-timestamped end to end, and scorer-side (t_decided)."""

    def __init__(self) -> None:
        self._e2e = HdrHistogram(1, _MAX_US, 3)
        self._scorer = HdrHistogram(1, _MAX_US, 3)
        self.count = 0

    @staticmethod
    def _us(ms: float) -> int:
        return min(_MAX_US, max(1, int(max(ms, 0.0) * 1000)))

    def record(self, e2e: float, scorer: float) -> None:
        self._e2e.record_value(self._us(e2e))
        self._scorer.record_value(self._us(scorer))
        self.count += 1

    @staticmethod
    def _pcts(h: HdrHistogram) -> dict:
        def p(q: float) -> float:
            v = h.get_value_at_percentile(q) / 1000
            return 0.0 if v <= 0.001 else round(v, 3)
        return {"p50": p(50), "p95": p(95), "p99": p(99), "max": round(h.get_max_value() / 1000, 3)}

    def summary(self) -> dict:
        if not self.count:
            return {"count": 0}
        return {"count": self.count, "e2e_ms": self._pcts(self._e2e), "scorer_ms": self._pcts(self._scorer)}
