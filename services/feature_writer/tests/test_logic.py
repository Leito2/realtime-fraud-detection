from feature_writer.logic import latest_per_user

from fraudcore.contracts import EnrichedPayment
from fraudcore.keys import velocity_key


def event(pid, user, cnt, total):
    return EnrichedPayment(
        payment_id=pid, user_id=user, card_id="c", amount=5.0, currency="USD", merchant_id="m", mcc="1",
        country="CO", device_id="d", ip_country="CO", channel="web", event_time="2026-10-05T14:03:22.123Z",
        t_scheduled_ns=0, t_sent_ns=0, f_cnt_10m=cnt, f_sum_10m=total, f_max_10m=5.0, f_small_tx_cnt_10m=1)


def test_last_event_per_user_wins_inside_a_batch():
    out = latest_per_user([event("p1", "a", 1, 5.0), event("p2", "b", 1, 5.0), event("p3", "a", 2, 10.0)])
    assert set(out) == {velocity_key("a"), velocity_key("b")}
    assert out[velocity_key("a")]["f_cnt_10m"] == "2"
    assert out[velocity_key("a")]["payment_id"] == "p3"


def test_hash_has_every_velocity_feature_as_strings():
    out = latest_per_user([event("p1", "a", 3, 15.0)])[velocity_key("a")]
    assert out == {"f_cnt_10m": "3", "f_sum_10m": "15.0", "f_max_10m": "5.0", "f_small_tx_cnt_10m": "1",
                   "payment_id": "p1"}
