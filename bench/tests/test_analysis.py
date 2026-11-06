import pytest
from fraud_bench.analysis import (
    P95_BUDGET_MS,
    lag_slope,
    lag_stable,
    median_range,
    step_conditions,
    sustained_rate,
)


def test_median_range_of_repetitions():
    assert median_range([30.0, 50.0, 40.0]) == (40.0, 30.0, 50.0)
    assert median_range([7.0]) == (7.0, 7.0, 7.0)


def test_lag_slope_is_messages_per_second():
    assert lag_slope([(0, 0), (10, 100), (20, 200)]) == pytest.approx(10.0)
    assert lag_slope([(0, 50), (10, 50), (20, 50)]) == pytest.approx(0.0)
    assert lag_slope([(0, 5)]) == 0.0


def test_lag_is_stable_when_flat_or_small_and_not_when_it_grows():
    flat = [(t, 120) for t in range(0, 60, 2)]
    growing = [(t, 100 * t) for t in range(0, 60, 2)]
    assert lag_stable(flat, rate=2000) is True
    assert lag_stable(growing, rate=2000) is False
    big_but_flat = [(t, 50_000) for t in range(0, 60, 2)]       # a standing backlog is not stable either
    assert lag_stable(big_but_flat, rate=2000) is False


def step(p95=40.0, valid=True, errors=0, lag_ok=True, dlq=0, lost=0):
    return {"p95": p95, "gen_valid": valid, "delivery_errors": errors, "lag_stable": lag_ok,
            "dlq_growth": dlq, "lost": lost}


def test_four_conditions_of_a_sustained_step():
    assert all(step_conditions(step()).values())
    assert step_conditions(step(p95=P95_BUDGET_MS + 1))["p95_within_budget"] is False
    assert step_conditions(step(lag_ok=False))["lag_stable"] is False
    assert step_conditions(step(valid=False))["gen_lag_valid"] is False
    assert step_conditions(step(errors=1))["no_errors"] is False
    assert step_conditions(step(dlq=3))["no_errors"] is False
    assert step_conditions(step(lost=2))["no_errors"] is False


def test_sustained_rate_is_the_highest_rate_whose_repetitions_all_pass():
    by_rate = {
        1000: [step(20), step(22), step(25)],
        2000: [step(60), step(70), step(75)],
        5000: [step(95), step(90), step(100)],           # median p95 over budget
        8000: [step(30), step(30), step(lag_ok=False)],  # one repetition lost its lag stability
    }
    assert sustained_rate(by_rate) == 2000


def test_median_p95_decides_the_budget_not_the_worst_run():
    by_rate = {2000: [step(60), step(70), step(95)]}      # median 70 <= 80
    assert sustained_rate(by_rate) == 2000


def test_no_sustained_rate_returns_none():
    assert sustained_rate({1000: [step(500)]}) is None
