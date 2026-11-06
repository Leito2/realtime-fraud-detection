import pytest
import redis
from scorer.adapters.redis_profiles import RedisProfileStore, parse_profile
from scorer.application.ports import ProfileUnavailable

from fraudcore.keys import profile_key


def test_parse_converts_numbers_and_keeps_the_home_country_as_text():
    raw = {b"avg_amount_30d": b"12.5", b"n_known_devices": b"2.0", b"home_country": b"CO"}
    assert parse_profile(raw) == {"avg_amount_30d": 12.5, "n_known_devices": 2.0, "home_country": "CO"}


def test_parse_of_an_unknown_user_is_empty_so_defaults_apply():
    assert parse_profile({}) == {}


class BrokenPipeline:
    def hgetall(self, key):
        return self

    def execute(self):
        raise redis.TimeoutError("slow")


class BrokenClient:
    def pipeline(self, transaction=False):
        return BrokenPipeline()


def test_redis_errors_become_profile_unavailable():
    with pytest.raises(ProfileUnavailable):
        RedisProfileStore(BrokenClient()).get_many(["u_1"])


def _live_redis():
    client = redis.Redis(host="localhost", port=6379, socket_connect_timeout=0.3, socket_timeout=0.3)
    try:
        client.ping()
    except redis.RedisError:
        return None
    return client


@pytest.mark.integration
def test_get_many_reads_profiles_in_one_pipeline_against_real_redis():
    client = _live_redis()
    if client is None:
        pytest.skip("Redis not reachable on localhost:6379 (run: make up)")
    client.hset(profile_key("it_user"), mapping={"avg_amount_30d": "42.0", "home_country": "CO"})
    try:
        out = RedisProfileStore(client).get_many(["it_user", "it_missing"])
    finally:
        client.delete(profile_key("it_user"))
    assert out == [{"avg_amount_30d": 42.0, "home_country": "CO"}, {}]
