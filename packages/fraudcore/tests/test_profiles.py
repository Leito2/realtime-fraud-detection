from datetime import UTC, datetime, timedelta

import polars as pl

from fraudcore.profiles import build_profiles

AS_OF = datetime(2026, 10, 31, tzinfo=UTC)


def frame(rows):
    schema = {"user_id": pl.String, "amount": pl.Float64, "country": pl.String, "device_id": pl.String,
              "event_time": pl.Datetime("us", "UTC")}
    return pl.DataFrame(rows, orient="row", schema=schema)


def day(n):
    return AS_OF - timedelta(days=n)


def test_profile_statistics_for_one_user():
    df = frame([("u1", 10.0, "CO", "d1", day(1)), ("u1", 30.0, "CO", "d2", day(2)),
                ("u1", 20.0, "US", "d1", day(3))])
    p = build_profiles(df, AS_OF).to_dicts()[0]
    assert p["user_id"] == "u1"
    assert p["avg_amount_30d"] == 20.0
    assert p["std_amount_30d"] == 10.0                    # sample std of 10, 20, 30
    assert p["n_known_devices"] == 2
    assert p["home_country"] == "CO"                      # most frequent
    assert p["account_age_days"] == 3.0


def test_only_the_last_30_days_before_as_of_count_but_age_uses_all_history():
    df = frame([("u1", 100.0, "CO", "d1", day(60)), ("u1", 10.0, "CO", "d1", day(5)),
                ("u1", 999.0, "CO", "d1", AS_OF + timedelta(days=1))])        # future: must be ignored
    p = build_profiles(df, AS_OF).to_dicts()[0]
    assert p["avg_amount_30d"] == 10.0
    assert p["account_age_days"] == 60.0


def test_single_payment_has_zero_std_not_null():
    p = build_profiles(frame([("u1", 10.0, "CO", "d1", day(1))]), AS_OF).to_dicts()[0]
    assert p["std_amount_30d"] == 0.0


def test_user_with_only_old_history_still_gets_a_profile_with_defaults():
    p = build_profiles(frame([("u1", 10.0, "CO", "d1", day(90))]), AS_OF).to_dicts()[0]
    assert p["avg_amount_30d"] == 0.0 and p["n_known_devices"] == 0 and p["account_age_days"] == 90.0
