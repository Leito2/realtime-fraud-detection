from datetime import UTC, datetime

import polars as pl
from generator.logic import Population
from generator.offline import generate_history

from fraudcore.contracts import Payment

START = datetime(2026, 8, 1, tzinfo=UTC)


def test_history_has_the_requested_volume_span_and_order():
    df = generate_history(Population(n_users=50, seed=1), seed=1, start=START, days=3, payments_per_day=200)
    assert df.height == 600
    assert df["event_time"].is_sorted()
    assert df["event_time"].min() >= START and df["event_time"].max() < START.replace(day=4)


def test_rows_validate_against_the_payment_contract():
    df = generate_history(Population(n_users=20, seed=2), seed=2, start=START, days=1, payments_per_day=30)
    row = df.row(0, named=True)
    row["event_time"] = row["event_time"].isoformat()
    Payment.model_validate(row)


def test_same_seed_same_history():
    a = generate_history(Population(n_users=20, seed=3), seed=3, start=START, days=1, payments_per_day=50)
    b = generate_history(Population(n_users=20, seed=3), seed=3, start=START, days=1, payments_per_day=50)
    assert a.equals(b)
    assert isinstance(a, pl.DataFrame)
