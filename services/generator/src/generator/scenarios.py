"""Fraud and hard-legit scenarios (PLAN.md §3.1, §12.2). Pure: an episode is a list of steps.

The history must be hard: legitimate behaviour that looks like fraud (travel, bill bursts, new phones, big
purchases), fraud that looks legitimate (``slow_fraud``) and noisy labels. If rules already score PR-AUC > 0.9
on it, the generator is too easy and gets tuned before the final model is trained (risk R6).
"""
import math
import random
from dataclasses import dataclass

from generator.logic import COUNTRIES

FRAUD_KINDS = ["burst", "geo_jump", "card_testing", "atypical_amount", "account_takeover", "slow_fraud"]
LEGIT_HARD_KINDS = ["traveler", "legit_burst", "new_device", "big_purchase"]
AVG_FRAUD_EPISODE_PAYMENTS = 4.5
AVG_LEGIT_EPISODE_PAYMENTS = 3.0


@dataclass(frozen=True)
class Step:
    offset_s: float
    amount: float
    country: str | None = None       # None: the user's home country
    device: str | None = None        # None: a known device; "NEW": one device the user never used
    ip_country: str | None = None    # None: same as country


@dataclass(frozen=True)
class Episode:
    scenario: str
    is_fraud: bool
    steps: list[Step]


def _far(home: str, rng: random.Random) -> str:
    return rng.choice([c for c in COUNTRIES if c != home])


def _gaps(rng: random.Random, n: int, lo: float, hi: float) -> list[float]:
    t, out = 0.0, []
    for _ in range(n):
        out.append(round(t, 2))
        t += rng.uniform(lo, hi)
    return out


def make_episode(kind: str, habits: dict, rng: random.Random) -> Episode:
    typical = math.exp(habits["mu"])
    home = habits["country"]
    amount = lambda lo, hi: round(max(0.5, typical * rng.uniform(lo, hi)), 2)  # noqa: E731

    if kind == "burst":
        offs = _gaps(rng, rng.randint(6, 12), 5, 25)
        return Episode(kind, True, [Step(o, amount(0.5, 2.5)) for o in offs])
    if kind == "geo_jump":
        far = _far(home, rng)
        ip = far if rng.random() < 0.5 else None
        return Episode(kind, True, [Step(0, amount(0.7, 1.5)),
                                    Step(rng.uniform(300, 1800), amount(1, 4), country=far, ip_country=ip)])
    if kind == "card_testing":
        offs = _gaps(rng, rng.randint(5, 15), 3, 12)
        steps = [Step(o, round(rng.uniform(0.5, 5), 2)) for o in offs]
        if rng.random() < 0.5:
            steps.append(Step(offs[-1] + rng.uniform(5, 30), amount(2, 6)))
        return Episode(kind, True, steps)
    if kind == "atypical_amount":
        return Episode(kind, True, [Step(0, amount(8, 15))])
    if kind == "account_takeover":
        far = _far(home, rng)
        offs = _gaps(rng, rng.randint(1, 3), 30, 300)
        return Episode(kind, True, [Step(o, amount(3, 8), country=far, device="NEW") for o in offs])
    if kind == "slow_fraud":
        offs = [0.0, rng.uniform(900, 1200), rng.uniform(2300, 2700)]
        return Episode(kind, True, [Step(o, amount(0.9, 1.5)) for o in offs])
    if kind == "traveler":
        far = _far(home, rng)
        offs = sorted(rng.uniform(0, 6 * 3600) for _ in range(rng.randint(2, 5)))
        return Episode(kind, False, [Step(o, amount(0.5, 2), country=far) for o in offs])
    if kind == "legit_burst":
        offs = _gaps(rng, rng.randint(4, 8), 20, 90)
        return Episode(kind, False, [Step(o, amount(0.5, 2)) for o in offs])
    if kind == "new_device":
        offs = sorted(rng.uniform(0, 3600) for _ in range(rng.randint(1, 3)))
        return Episode(kind, False, [Step(o, amount(0.7, 1.8), device="NEW") for o in offs])
    if kind == "big_purchase":
        return Episode(kind, False, [Step(0, amount(5, 10))])
    raise ValueError(f"unknown scenario {kind!r}")


def apply_label_noise(true_labels: list[bool], rng: random.Random, miss_rate: float,
                      false_alarm_rate: float) -> list[bool]:
    """What the bank *knows*: some fraud is never reported, a few legitimate payments get disputed."""
    return [(rng.random() >= miss_rate) if t else (rng.random() < false_alarm_rate) for t in true_labels]
