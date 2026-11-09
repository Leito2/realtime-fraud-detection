"""Option B (PLAN.md 3.3, E7): the per-user state a PyFlink KeyedProcessFunction would keep.

The operator logic is a plain class, testable without a Flink cluster: for the 10-minute window it must
match ``velocity_reference`` exactly (the same function the Flink SQL job is tested against), and it adds the
1 h and 24 h windows the SQL job cannot have next to it.
"""
import collections
import importlib.util
import math
import random
from pathlib import Path

import pytest

from fraudcore.features import velocity_reference

spec = importlib.util.spec_from_file_location(
    "user_state", Path(__file__).parents[1] / "flink" / "pyflink" / "user_state.py")
user_state = importlib.util.module_from_spec(spec)
spec.loader.exec_module(user_state)


def events(n=3000, users=20, seed=3):
    rnd = random.Random(seed)
    t, out = 0.0, []
    for _ in range(n):
        t += rnd.expovariate(1 / 90)                       # about one payment per 90 s across the stream
        out.append({"user_id": f"u{rnd.randrange(users)}", "amount": round(rnd.uniform(1, 200), 2), "ts": t})
    return out


def test_ten_minute_window_matches_the_reference():
    evs = events()
    ref = velocity_reference(evs)
    states = {}
    for e, want in zip(evs, ref, strict=True):
        got = states.setdefault(e["user_id"], user_state.UserWindows()).add(e["ts"], e["amount"])
        exact = [k for k in want if k != "f_sum_10m"]
        assert {k: got[k] for k in exact} == {k: want[k] for k in exact}
        assert got["f_sum_10m"] == pytest.approx(want["f_sum_10m"])


def test_long_windows_include_the_current_payment_and_the_exact_boundary():
    w = user_state.UserWindows()
    seen = [w.add(t * 60.0, 10.0) for t in (0, 30, 60, 90, 1500)]
    assert [r["f_cnt_1h"] for r in seen] == [1, 2, 3, 3, 1]
    assert [r["f_cnt_24h"] for r in seen] == [1, 2, 3, 4, 3]
    assert [r["f_sum_1h"] for r in seen] == [10.0, 20.0, 30.0, 30.0, 10.0]


def test_state_is_bounded_by_the_longest_window():
    w = user_state.UserWindows()
    for t in range(0, 3 * 86400, 60):
        w.add(float(t), 1.0)
    assert w.size() <= 86400 / 60 + 1


def test_out_of_order_timestamp_is_rejected_not_silently_miscounted():
    w = user_state.UserWindows()
    w.add(100.0, 5.0)
    with pytest.raises(ValueError, match="out of order"):
        w.add(99.0, 5.0)
    assert w.add(100.0, 1.0)["f_cnt_10m"] == 2                 # equal timestamps are peers, still fine


def test_running_sums_are_exact_after_many_expiries():
    rnd = random.Random(7)
    w, live = user_state.UserWindows(), collections.deque()
    for i in range(20_000):
        ts, amount = i * 37.0, round(rnd.uniform(0.01, 999.99), 2)
        live.append((ts, amount))
        while live[0][0] < ts - 3600:                            # keep only the 1 h window for the reference
            live.popleft()
        got = w.add(ts, amount)
        assert got["f_sum_1h"] == round(math.fsum(a for _, a in live), 2)   # cents-exact, no drift


def test_max_in_every_window_matches_brute_force():
    rnd = random.Random(11)
    w, seen, ts = user_state.UserWindows(), [], 0.0
    for _ in range(4000):
        ts += rnd.uniform(5, 400)
        amount = round(rnd.uniform(0.01, 500), 2)
        seen.append((ts, amount))
        got = w.add(ts, amount)
        for name, span in user_state.WINDOWS_S.items():
            assert got[f"f_max_{name}"] == max(a for t, a in seen if t >= ts - span)
