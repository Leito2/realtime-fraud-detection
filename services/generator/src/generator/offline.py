"""Offline mode: simulated multi-day history as Parquet, for training and the Profile Builder (PLAN §3.1).

Columns: the Payment fields, plus ``scenario``, ``fraud_true`` (ground truth, for per-scenario recall) and
``is_fraud`` (the noisy label the bank would actually know; training uses this one).
"""
import random
from datetime import datetime, timedelta

import polars as pl

from generator.logic import Population, build_payment, next_payment
from generator.scenarios import (
    AVG_FRAUD_EPISODE_PAYMENTS,
    AVG_LEGIT_EPISODE_PAYMENTS,
    FRAUD_KINDS,
    LEGIT_HARD_KINDS,
    apply_label_noise,
    make_episode,
)


def _stochastic_round(x: float, rng: random.Random) -> int:
    return int(x) + (1 if rng.random() < x - int(x) else 0)


def _episode_rows(pop: Population, rng: random.Random, kind: str, day_start: datetime) -> list[dict]:
    uid = pop.pick_user(rng) if rng.random() < 0.5 else rng.choice(pop.user_ids)
    habits = pop.habits(uid)
    ep = make_episode(kind, habits, rng)
    span = ep.steps[-1].offset_s
    start_s = rng.uniform(0, max(1.0, 86_400 - span - 1))
    new_device = f"d_{uid[2:]}_new{rng.randint(0, 99_999)}"
    rows = []
    for s in ep.steps:
        device = new_device if s.device == "NEW" else s.device
        e = build_payment(habits, uid, rng, amount=s.amount, country=s.country, device=device,
                          ip_country=s.ip_country)
        e["event_time"] = day_start + timedelta(seconds=start_s + s.offset_s)
        e["scenario"], e["fraud_true"] = kind, ep.is_fraud
        rows.append(e)
    return rows


def generate_history(
    pop: Population, seed: int, start: datetime, days: int, payments_per_day: int,
    fraud_rate: float = 0.008, hard_legit_rate: float = 0.01,
    miss_rate: float = 0.05, false_alarm_rate: float = 0.0005,
) -> pl.DataFrame:
    rng = random.Random(seed)
    frames = []
    for day in range(days):                 # one frame per day: columnar and compact, not millions of dicts
        day_start = start + timedelta(days=day)
        rows: list[dict] = []
        n_fraud = _stochastic_round(fraud_rate * payments_per_day / AVG_FRAUD_EPISODE_PAYMENTS, rng)
        n_legit = _stochastic_round(hard_legit_rate * payments_per_day / AVG_LEGIT_EPISODE_PAYMENTS, rng)
        for kind in [rng.choice(FRAUD_KINDS) for _ in range(n_fraud)] + \
                    [rng.choice(LEGIT_HARD_KINDS) for _ in range(n_legit)]:
            rows.extend(_episode_rows(pop, rng, kind, day_start))
        for _ in range(max(0, payments_per_day - len(rows))):
            event = next_payment(pop, rng, t_scheduled_ns=0, t_sent_ns=0)
            event["event_time"] = day_start + timedelta(seconds=rng.random() * 86_400 - 1e-3)
            event["scenario"], event["fraud_true"] = "normal", False
            rows.append(event)
        frames.append(pl.DataFrame(rows))
    df = pl.concat(frames).sort("event_time")
    noisy = apply_label_noise(df["fraud_true"].to_list(), rng, miss_rate, false_alarm_rate)
    return df.with_columns(pl.Series("is_fraud", noisy))
