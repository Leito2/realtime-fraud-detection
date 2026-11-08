"""Option B (PLAN.md 3.3, E7): the per-user state a PyFlink KeyedProcessFunction would keep.

The operator logic is a plain class, testable without a Flink cluster: for the 10-minute window it must
match ``velocity_reference`` exactly (the same function the Flink SQL job is tested against), and it adds the
1 h and 24 h windows the SQL job cannot have next to it.
"""
import importlib.util
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
        assert got["f_sum_10m"] == pytest.approx(want["f_sum_10m"])   # running sum: float drift, not exact


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
