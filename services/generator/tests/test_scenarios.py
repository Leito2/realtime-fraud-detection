import random
from datetime import UTC, datetime

import pytest
from generator.logic import Population
from generator.offline import generate_history
from generator.scenarios import FRAUD_KINDS, LEGIT_HARD_KINDS, apply_label_noise, make_episode

START = datetime(2026, 8, 1, tzinfo=UTC)
HABITS = {"country": "CO", "devices": ["d_1", "d_2"], "mu": 3.5, "card": "c_1"}


@pytest.mark.parametrize("kind", FRAUD_KINDS)
def test_fraud_episodes_are_labelled_fraud_and_time_ordered(kind):
    ep = make_episode(kind, HABITS, random.Random(1))
    assert ep.is_fraud and ep.scenario == kind and ep.steps
    offsets = [s.offset_s for s in ep.steps]
    assert offsets == sorted(offsets) and offsets[0] >= 0


@pytest.mark.parametrize("kind", LEGIT_HARD_KINDS)
def test_hard_legit_episodes_look_odd_but_are_not_fraud(kind):
    ep = make_episode(kind, HABITS, random.Random(2))
    assert not ep.is_fraud and ep.scenario == kind and ep.steps


def test_scenarios_have_their_signature():
    rng = random.Random(3)
    burst = make_episode("burst", HABITS, rng)
    assert len(burst.steps) >= 6 and burst.steps[-1].offset_s - burst.steps[0].offset_s <= 300
    testing = make_episode("card_testing", HABITS, rng)
    assert sum(1 for s in testing.steps if s.amount < 10) >= 4
    geo = make_episode("geo_jump", HABITS, rng)
    assert {s.country or "CO" for s in geo.steps} != {"CO"}
    ato = make_episode("account_takeover", HABITS, rng)
    assert all(s.device == "NEW" for s in ato.steps)


def test_label_noise_flips_a_small_share_and_keeps_the_true_label():
    rng = random.Random(4)
    true = [True] * 2_000 + [False] * 200_000
    noisy = apply_label_noise(true, rng, miss_rate=0.05, false_alarm_rate=0.0005)
    missed = sum(1 for t, n in zip(true[:2_000], noisy[:2_000], strict=True) if t and not n)
    false_alarms = sum(1 for n in noisy[2_000:] if n)
    assert 60 <= missed <= 140           # ~5% of 2 000
    assert 40 <= false_alarms <= 170     # ~0.05% of 200 000


def test_history_contains_every_scenario_and_a_realistic_fraud_rate():
    pop = Population(n_users=500, seed=1)
    df = generate_history(pop, seed=1, start=START, days=10, payments_per_day=2_000)
    scenarios = set(df["scenario"].unique().to_list())
    assert {"normal", *FRAUD_KINDS, *LEGIT_HARD_KINDS} <= scenarios
    rate = df["fraud_true"].mean()
    assert 0.004 <= rate <= 0.016        # target 0.8%
    assert {"is_fraud", "fraud_true", "scenario"} <= set(df.columns)
    assert df["event_time"].is_sorted()
