import random

from generator.logic import Population, next_payment
from generator.schedule import gen_lag_ms, scheduled_ns

from fraudcore.contracts import Payment


def test_generated_payment_matches_the_contract():
    pop = Population(n_users=50, seed=1)
    rng = random.Random(1)
    d = next_payment(pop, rng, t_scheduled_ns=1_000, t_sent_ns=2_000)
    p = Payment.model_validate(d)               # raises if the generator drifts from fraudcore.contracts
    assert p.t_scheduled_ns == 1_000 and p.t_sent_ns == 2_000


def test_same_seed_gives_same_population_and_stream():
    a = Population(n_users=20, seed=7)
    b = Population(n_users=20, seed=7)
    ra, rb = random.Random(3), random.Random(3)
    sa = [next_payment(a, ra, 0, 0)["user_id"] for _ in range(30)]
    sb = [next_payment(b, rb, 0, 0)["user_id"] for _ in range(30)]
    assert sa == sb


def test_user_activity_is_skewed():
    pop = Population(n_users=100, seed=2)
    rng = random.Random(2)
    counts: dict[str, int] = {}
    for _ in range(5_000):
        u = next_payment(pop, rng, 0, 0)["user_id"]
        counts[u] = counts.get(u, 0) + 1
    ranked = sorted(counts.values(), reverse=True)
    assert ranked[0] > 5 * ranked[len(ranked) // 2]        # Zipf: a few users dominate


def test_workers_get_disjoint_user_subsets():
    a = Population(n_users=100, seed=1, worker=0, workers=2)
    b = Population(n_users=100, seed=1, worker=1, workers=2)
    assert set(a.user_ids).isdisjoint(b.user_ids)
    assert len(a.user_ids) + len(b.user_ids) == 100


def test_schedule_is_fixed_rate_and_independent_of_send_time():
    # open loop: event i is due at t0 + i/rate no matter how late previous sends were
    assert scheduled_ns(t0_ns=1_000_000_000, i=0, rate=1000) == 1_000_000_000
    assert scheduled_ns(t0_ns=1_000_000_000, i=500, rate=1000) == 1_500_000_000


def test_gen_lag_is_sent_minus_scheduled_in_ms():
    assert gen_lag_ms(scheduled=1_000_000, sent=3_500_000) == 2.5
    assert gen_lag_ms(scheduled=5_000_000, sent=4_000_000) == 0.0     # early sends do not count as lag


def test_generation_is_fast_enough_for_open_loop_at_realistic_scale():
    # regression: rebuilding Zipf weights on every call cost ~ms per event with 100k users
    import time

    pop = Population(n_users=100_000, seed=1)
    rng = random.Random(1)
    start = time.perf_counter()
    for _ in range(2_000):
        next_payment(pop, rng, 0, 0)
    per_event_us = (time.perf_counter() - start) / 2_000 * 1e6
    assert per_event_us < 200, f"{per_event_us:.0f} µs per event"
