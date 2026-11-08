from fraud_bench.sync_vs_flink import residuals, summarize


def ev(user, ts, amount):
    return {"user_id": user, "ts": float(ts), "amount": float(amount)}


def test_spaced_payments_inside_the_window_match_flink_exactly():
    events = [ev("u", 0, 50), ev("u", 100, 20), ev("u", 200, 5)]
    rows = residuals(events, writer_delay_s=0.0)
    assert [r["cnt_error"] for r in rows] == [0, 0, 0]
    assert all(r["sum_error"] == 0 for r in rows)


def test_events_that_expired_since_the_last_payment_make_sync_overcount():
    # at t=650 the payment from t=0 is outside the 600 s window, but it is still in the state stored at t=100
    events = [ev("u", 0, 10), ev("u", 100, 10), ev("u", 650, 10)]
    rows = residuals(events, writer_delay_s=0.0)
    assert rows[2]["cnt_error"] == 1            # sync counts 3 (stored 2 + current), Flink counts 2
    assert rows[2]["sum_error"] == 10


def test_a_slow_feature_writer_makes_sync_miss_the_previous_payment():
    events = [ev("u", 0, 10), ev("u", 1, 10)]
    fast = residuals(events, writer_delay_s=0.0)
    slow = residuals(events, writer_delay_s=2.0)          # the first payment is not in Redis yet at t=1
    assert fast[1]["cnt_error"] == 0 and slow[1]["cnt_error"] == -1


def test_summary_reports_the_share_of_exact_matches_and_error_percentiles():
    events = [ev("u", 0, 10), ev("u", 100, 10), ev("u", 650, 10)]
    s = summarize(residuals(events, writer_delay_s=0.0))
    assert s["events"] == 3 and s["exact_share"] == 2 / 3
    assert s["cnt_error_max"] == 1 and s["cnt_error_min"] == 0
