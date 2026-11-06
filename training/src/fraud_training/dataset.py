"""History -> training rows, built with the same fraudcore code the scorer uses (PLAN.md section 4.3).

Velocity comes from ``velocity_reference`` (the offline twin of the Flink window, pinned by the parity test),
profiles from ``build_profiles`` as of the start of each day (no leakage; the online Profile Builder is also a
batch job), and derived features from ``build_row``. Nothing is re-implemented here, so there is no skew.
"""
from datetime import timedelta

import polars as pl

from fraudcore.features import FEATURE_ORDER, PROFILE, PROFILE_DEFAULTS, build_row, velocity_reference
from fraudcore.profiles import build_profiles

META = ["payment_id", "user_id", "event_time", "day", "label", "fraud_true", "scenario"]


def _velocity(df: pl.DataFrame) -> pl.DataFrame:
    events = [
        {"user_id": u, "amount": a, "ts": t}
        for u, a, t in zip(df["user_id"], df["amount"], df["event_time"].dt.epoch("us") / 1e6, strict=True)
    ]
    return pl.DataFrame(velocity_reference(events))


def _with_profiles(df: pl.DataFrame, history: pl.DataFrame, day0) -> pl.DataFrame:
    parts = []
    for day, chunk in df.group_by("day", maintain_order=True):
        as_of = day0 + timedelta(days=day[0])
        prof = build_profiles(history, as_of)
        parts.append(chunk.join(prof, on="user_id", how="left"))
    return pl.concat(parts)


def build_dataset(history: pl.DataFrame) -> pl.DataFrame:
    """One row per payment: metadata, the label the bank would know, and every feature in FEATURE_ORDER."""
    df = history.sort("event_time", maintain_order=True)
    day0 = df["event_time"].min().replace(hour=0, minute=0, second=0, microsecond=0)
    df = df.with_columns(
        ((pl.col("event_time") - day0).dt.total_days()).cast(pl.Int64).alias("day"),
        pl.col("is_fraud").cast(pl.Int8).alias("label"),
    )
    df = pl.concat([df, _velocity(df)], how="horizontal")
    df = _with_profiles(df, history, day0)

    rows = []
    for r in df.iter_rows(named=True):
        profile = {k: r[k] for k in (*PROFILE, "home_country") if r[k] is not None}
        profile = {**PROFILE_DEFAULTS, **profile}
        rows.append(build_row(r, profile))
    feats = pl.DataFrame(rows).select(FEATURE_ORDER)
    return pl.concat([df.select(META), feats], how="horizontal")


def temporal_split(ds: pl.DataFrame, valid_days: int, test_days: int):
    """Train on the past, validate on the next days, test on the last (random splits leak the future)."""
    last = ds["day"].max()
    test = ds.filter(pl.col("day") > last - test_days)
    valid = ds.filter((pl.col("day") > last - test_days - valid_days) & (pl.col("day") <= last - test_days))
    train = ds.filter(pl.col("day") <= last - test_days - valid_days)
    return train, valid, test
