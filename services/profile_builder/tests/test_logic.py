from profile_builder.logic import batched, to_redis

from fraudcore.keys import profile_key


def test_row_becomes_a_string_hash_without_nulls():
    key, mapping = to_redis({"user_id": "u_1", "avg_amount_30d": 12.5, "std_amount_30d": 0.0,
                             "n_known_devices": 2.0, "account_age_days": 40.25, "home_country": None})
    assert key == profile_key("u_1")
    assert mapping == {"avg_amount_30d": "12.5", "std_amount_30d": "0.0", "n_known_devices": "2.0",
                       "account_age_days": "40.25"}                    # null home_country is simply absent


def test_batched_splits_into_fixed_chunks():
    assert [len(b) for b in batched(range(2500), 1000)] == [1000, 1000, 500]
    assert list(batched([], 10)) == []
