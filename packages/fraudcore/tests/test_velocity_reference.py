from pathlib import Path

import pytest

from fraudcore.features import SMALL_TX_AMOUNT, WINDOW_S, velocity_reference


def ev(user, amount, ts):
    return {"user_id": user, "amount": amount, "ts": ts}


def test_current_payment_is_always_included():
    out = velocity_reference([ev("u", 25.0, 0.0)])
    assert out == [{"f_cnt_10m": 1, "f_sum_10m": 25.0, "f_max_10m": 25.0, "f_small_tx_cnt_10m": 0}]


def test_window_accumulates_per_user_and_counts_small_payments():
    out = velocity_reference([ev("a", 5.0, 0), ev("a", 30.0, 10), ev("b", 7.0, 11), ev("a", 8.0, 20)])
    assert out[2] == {"f_cnt_10m": 1, "f_sum_10m": 7.0, "f_max_10m": 7.0, "f_small_tx_cnt_10m": 1}   # user b
    assert out[3] == {"f_cnt_10m": 3, "f_sum_10m": 43.0, "f_max_10m": 30.0, "f_small_tx_cnt_10m": 2}


def test_old_payments_leave_the_window_with_an_inclusive_lower_bound():
    out = velocity_reference([ev("u", 1.0, 0), ev("u", 2.0, WINDOW_S), ev("u", 4.0, WINDOW_S + 1)])
    assert out[1]["f_cnt_10m"] == 2          # exactly 10 min old is still inside (RANGE ... PRECEDING)
    assert out[2]["f_cnt_10m"] == 2          # the t=0 payment has expired
    assert out[2]["f_sum_10m"] == 6.0


@pytest.mark.parametrize("job", ["m1_passthrough.sql", "m8_validated.sql"])
def test_constants_match_the_flink_sql_job(job):
    sql = (Path(__file__).resolve().parents[3] / "flink" / "sql" / job).read_text()
    assert f"amount < {int(SMALL_TX_AMOUNT)}" in sql
    assert f"INTERVAL '{WINDOW_S // 60}' MINUTE PRECEDING" in sql
