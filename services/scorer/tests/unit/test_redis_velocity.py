import pytest
import redis
from scorer.adapters.redis_velocity import RedisVelocityStore, parse_velocity
from scorer.application.ports import ProfileUnavailable


def test_a_stored_hash_becomes_a_typed_snapshot():
    raw = {b"f_cnt_10m": b"3", b"f_sum_10m": b"120.5", b"f_max_10m": b"80.0", b"f_small_tx_cnt_10m": b"1",
           b"payment_id": b"p9", b"ts": b"1791209340.123"}
    assert parse_velocity(raw) == {"f_cnt_10m": 3, "f_sum_10m": 120.5, "f_max_10m": 80.0,
                                   "f_small_tx_cnt_10m": 1, "ts": 1791209340.123}


def test_no_state_or_state_without_a_timestamp_is_unknown():
    assert parse_velocity({}) is None
    assert parse_velocity({b"f_cnt_10m": b"3"}) is None       # written before the writer stored ``ts``


class BrokenClient:
    def pipeline(self, transaction=False):
        return self

    def hgetall(self, key):
        return self

    def execute(self):
        raise redis.TimeoutError("slow")


def test_redis_errors_surface_as_profile_unavailable():
    with pytest.raises(ProfileUnavailable):
        RedisVelocityStore(BrokenClient()).get_many(["u1"])
