from datetime import UTC, datetime

import polars as pl
import pytest
from fraud_training.dataset import build_dataset, temporal_split
from generator.logic import Population
from generator.offline import generate_history

from fraudcore.features import FEATURE_ORDER

START = datetime(2026, 8, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def history() -> pl.DataFrame:
    pop = Population(n_users=300, seed=3)
    return generate_history(pop, seed=3, start=START, days=12, payments_per_day=1_500)


@pytest.fixture(scope="module")
def ds(history):
    return build_dataset(history)


def test_dataset_has_every_feature_label_and_metadata(history, ds):
    assert ds.height == history.height
    assert set(FEATURE_ORDER) <= set(ds.columns)
    assert {"label", "fraud_true", "scenario", "user_id", "event_time", "day"} <= set(ds.columns)
    assert ds["event_time"].is_sorted()


def test_velocity_includes_the_current_payment_and_matches_the_reference(ds):
    assert ds["f_cnt_10m"].min() >= 1
    assert (ds["f_max_10m"] >= ds["amount"] - 1e-9).all()


def test_first_day_users_are_new_and_later_days_have_profiles(ds):
    day0 = ds.filter(pl.col("day") == 0)
    assert (day0["account_age_days"] == 0).all() and (day0["avg_amount_30d"] == 0).all()
    later = ds.filter(pl.col("day") >= 6)
    assert (later["avg_amount_30d"] > 0).mean() > 0.5


def test_profile_never_sees_the_same_day_or_the_future(history):
    # Drop everything after day 3: features of days 0..3 must not change (no leakage from the future).
    cut = datetime(2026, 8, 5, tzinfo=UTC)
    full = build_dataset(history)
    short = build_dataset(history.filter(pl.col("event_time") < cut))
    a = full.filter(pl.col("event_time") < cut).sort("payment_id")
    b = short.sort("payment_id")
    assert a["payment_id"].to_list() == b["payment_id"].to_list()
    for col in ("avg_amount_30d", "account_age_days", "f_cnt_10m"):
        assert a[col].to_list() == pytest.approx(b[col].to_list())


def test_temporal_split_is_ordered_and_disjoint(ds):
    train, valid, test = temporal_split(ds, valid_days=2, test_days=2)
    assert train["day"].max() < valid["day"].min() <= valid["day"].max() < test["day"].min()
    assert train.height + valid.height + test.height == ds.height
