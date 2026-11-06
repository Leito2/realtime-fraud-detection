"""Pure decisions of the load protocol (PLAN.md sections 5.3, 5.4): what counts as 'sustained'."""
from statistics import median

P95_BUDGET_MS = 80.0
MAX_LAG_SLOPE_FRACTION = 0.02       # lag may grow by at most 2% of the offered rate per second
MAX_STANDING_LAG_S = 2.0            # and the backlog at the end must be under 2 s of traffic


def median_range(values: list[float]) -> tuple[float, float, float]:
    """(median, min, max) of the repetitions of one step, as reported in the results table."""
    return float(median(values)), float(min(values)), float(max(values))


def lag_slope(samples: list[tuple[float, float]]) -> float:
    """Least-squares slope of (time_s, lag_messages): messages per second the backlog grows."""
    if len(samples) < 2:
        return 0.0
    n = len(samples)
    mean_t = sum(t for t, _ in samples) / n
    mean_l = sum(lag for _, lag in samples) / n
    den = sum((t - mean_t) ** 2 for t, _ in samples)
    return sum((t - mean_t) * (lag - mean_l) for t, lag in samples) / den if den else 0.0


def lag_stable(samples: list[tuple[float, float]], rate: float) -> bool:
    """Condition 2: consumer lag is not growing and is not a standing backlog."""
    if not samples:
        return False
    final = samples[-1][1]
    return lag_slope(samples) <= MAX_LAG_SLOPE_FRACTION * rate and final <= MAX_STANDING_LAG_S * rate


def step_conditions(step: dict) -> dict[str, bool]:
    """The four conditions of a sustained step (section 5.4); ``step`` is one repetition's measurements."""
    return {
        "p95_within_budget": step["p95"] <= P95_BUDGET_MS,
        "lag_stable": bool(step["lag_stable"]),
        "gen_lag_valid": bool(step["gen_valid"]),
        "no_errors": step["delivery_errors"] == 0 and step["dlq_growth"] == 0 and step["lost"] == 0,
    }


def sustained_rate(by_rate: dict[int, list[dict]]) -> int | None:
    """Highest rate with median p95 within budget and every repetition meeting the other conditions."""
    best = None
    for rate, reps in sorted(by_rate.items()):
        p95_median = median_range([r["p95"] for r in reps])[0]
        others_ok = all(
            ok for r in reps for name, ok in step_conditions(r).items() if name != "p95_within_budget"
        )
        if p95_median <= P95_BUDGET_MS and others_ok:
            best = rate
    return best
