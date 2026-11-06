"""Per-user 30-day profile (PLAN.md §3.4). One function feeds the Profile Builder (Redis) and training."""
from datetime import datetime, timedelta

import polars as pl

PROFILE_WINDOW_DAYS = 30


def build_profiles(history: pl.DataFrame, as_of: datetime) -> pl.DataFrame:
    """Profile of every user seen before ``as_of``.

    ``history`` needs ``user_id, amount, country, device_id, event_time``. Payments at or after ``as_of``
    are ignored (no leakage from the future). ``account_age_days`` uses the whole history; the
    statistics use only the last 30 days, with the same defaults as ``PROFILE_DEFAULTS`` when empty.
    """
    past = history.filter(pl.col("event_time") < as_of)
    recent = pl.col("event_time") >= as_of - timedelta(days=PROFILE_WINDOW_DAYS)
    return (
        past.group_by("user_id")
        .agg(
            pl.col("amount").filter(recent).mean().fill_null(0.0).alias("avg_amount_30d"),
            pl.col("amount").filter(recent).std().fill_null(0.0).alias("std_amount_30d"),
            pl.col("device_id").filter(recent).n_unique().cast(pl.Int64).alias("n_known_devices"),
            pl.col("country").filter(recent).mode().sort().first().alias("home_country"),
            ((as_of - pl.col("event_time").min()).dt.total_seconds() / 86400).alias("account_age_days"),
        )
        .with_columns(pl.col("n_known_devices").cast(pl.Float64))
        .sort("user_id")
    )
