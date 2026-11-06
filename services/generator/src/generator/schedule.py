"""Open-loop schedule (PLAN.md §5.3): event i is due at t0 + i / rate, whatever happened before."""


def scheduled_ns(t0_ns: int, i: int, rate: float) -> int:
    return t0_ns + int(i * 1_000_000_000 / rate)


def gen_lag_ms(scheduled: int, sent: int) -> float:
    """How late the generator itself was. If its p99 exceeds 5 ms the run is invalid (PLAN.md §5.3)."""
    return max(0, sent - scheduled) / 1e6
