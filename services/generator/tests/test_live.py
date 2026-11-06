import random
from collections import Counter

from generator.live import LiveTraffic
from generator.logic import Population

from fraudcore.contracts import LabelEvent, Payment

SECOND = 1_000_000_000


def run(n: int = 6_000, rate: float = 100, **kw):
    pop = Population(n_users=300, seed=3)
    live = LiveTraffic(pop, random.Random(5), **kw)
    out = []
    for i in range(n):
        out.append(live.next(due_ns=int(i * SECOND / rate), sent_ns=int(i * SECOND / rate)))
    return out


def test_every_payment_and_label_match_their_contracts():
    for payment, label in run(500, fraud_rate=0.05):
        Payment.model_validate(payment)
        assert LabelEvent.model_validate(label).payment_id == payment["payment_id"]


def test_the_total_rate_is_not_inflated_by_episodes():
    out = run(2_000, fraud_rate=0.05)
    assert len(out) == 2_000
    assert len({p["payment_id"] for p, _ in out}) == 2_000


def test_scenarios_appear_with_labels_and_normal_traffic_dominates():
    counts = Counter(label["scenario"] for _, label in run(fraud_rate=0.05, hard_legit_rate=0.03))
    assert counts["normal"] > 0.7 * sum(counts.values())
    assert (set(counts) - {"normal"}) & {"burst", "geo_jump", "card_testing", "atypical_amount",
                                         "account_takeover"}


def test_fraud_labels_follow_the_scenario_unless_noise_hides_them():
    out = run(fraud_rate=0.05, hard_legit_rate=0.03, miss_rate=0.0, false_alarm_rate=0.0)
    for _, label in out:
        fraud_kinds = {"burst", "geo_jump", "card_testing", "atypical_amount", "account_takeover"}
        assert label["is_fraud"] == (label["scenario"] in fraud_kinds)
    hidden = run(fraud_rate=0.05, miss_rate=1.0, false_alarm_rate=0.0)
    assert not any(label["is_fraud"] for _, label in hidden)


def test_episode_steps_are_spread_over_time_for_the_same_user():
    out = run(8_000, fraud_rate=0.05)
    bursts = [p for p, label in out if label["scenario"] == "burst"]
    users = Counter(p["user_id"] for p in bursts)
    assert users and max(users.values()) >= 6                   # a burst is >= 6 payments of one user


def test_no_fraud_traffic_when_rates_are_zero():
    labels = {label["scenario"] for _, label in run(1_000, fraud_rate=0.0, hard_legit_rate=0.0)}
    assert labels == {"normal"}


def test_same_seed_same_stream():
    a = [p["payment_id"] for p, _ in run(300, fraud_rate=0.05)]
    b = [p["payment_id"] for p, _ in run(300, fraud_rate=0.05)]
    assert a == b


def test_slow_episodes_longer_than_the_max_span_are_not_started():
    kinds = {label["scenario"] for _, label in run(6_000, fraud_rate=0.1, max_span_s=60)}
    assert "slow_fraud" not in kinds and "geo_jump" not in kinds


def test_an_injected_pattern_appears_only_after_injection():
    pop = Population(n_users=300, seed=3)
    live = LiveTraffic(pop, random.Random(5), fraud_rate=0.0, hard_legit_rate=0.0)
    before = Counter()
    for i in range(3_000):
        before[live.next(i * SECOND // 100, i * SECOND // 100)[1]["scenario"]] += 1
    live.inject("new_pattern", rate=0.05)
    after = Counter()
    for i in range(3_000, 9_000):
        after[live.next(i * SECOND // 100, i * SECOND // 100)[1]["scenario"]] += 1
    assert before["new_pattern"] == 0 and after["new_pattern"] > 50
