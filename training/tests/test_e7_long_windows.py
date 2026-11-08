from datetime import datetime, timedelta

import polars as pl
from fraud_training.e7_long_windows import add_long_windows


def frame(rows):
    t0 = datetime(2026, 1, 1)
    return pl.DataFrame(
        {
            "user_id": [r[0] for r in rows],
            "event_time": [t0 + timedelta(minutes=r[1]) for r in rows],
            "amount": [float(r[2]) for r in rows],
        }
    )


def test_windows_include_the_current_payment_and_the_boundary_like_flink():
    # same user at minute 0, 30, 60, 90 and 1500 (25 h): 1 h window is inclusive of exactly 60 minutes
    rows = [("u", 0, 10), ("u", 30, 20), ("u", 60, 30), ("u", 90, 40), ("u", 1500, 50)]
    out = add_long_windows(frame(rows))
    assert out["f_cnt_1h"].to_list() == [1, 2, 3, 3, 1]
    assert out["f_sum_1h"].to_list() == [10.0, 30.0, 60.0, 90.0, 50.0]
    assert out["f_cnt_24h"].to_list() == [1, 2, 3, 4, 3]  # minute 1500 sees 60 (exactly 24 h), 90, itself


def test_windows_are_per_user():
    out = add_long_windows(frame([("a", 0, 1), ("b", 1, 1), ("a", 2, 1)])).sort("event_time")
    assert out["f_cnt_1h"].to_list() == [1, 1, 2]
