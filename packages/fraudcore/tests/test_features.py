import pytest

from fraudcore.features import FEATURE_ORDER, derived, vectorize


def test_feature_order_has_no_duplicates():
    assert len(FEATURE_ORDER) == len(set(FEATURE_ORDER))


def test_derived_new_user_defaults_are_finite():
    d = derived(100.0, "CO", "CO", {})
    assert d == {"amount_to_avg_ratio": 0.0, "amount_zscore": 0.0,
                 "is_foreign": 0.0, "ip_country_mismatch": 0.0}


def test_vectorize_rejects_missing_feature():
    with pytest.raises(KeyError):
        vectorize({"amount": 1.0})


def test_build_row_covers_feature_order_for_a_new_user():
    from fraudcore.features import build_row

    event = {"amount": 100.0, "country": "CO", "ip_country": "US", "f_cnt_10m": 2, "f_sum_10m": 150.0,
             "f_max_10m": 100.0, "f_small_tx_cnt_10m": 0}
    row = build_row(event, {})
    assert set(FEATURE_ORDER) <= set(row)
    assert row["ip_country_mismatch"] == 1.0
    assert row["is_foreign"] == 0.0            # no home country known yet
    assert len(vectorize(row)) == len(FEATURE_ORDER)


def test_build_row_uses_profile_values():
    from fraudcore.features import build_row

    event = {"amount": 300.0, "country": "RO", "ip_country": "RO", "f_cnt_10m": 1, "f_sum_10m": 300.0,
             "f_max_10m": 300.0, "f_small_tx_cnt_10m": 0}
    profile = {"avg_amount_30d": 100.0, "std_amount_30d": 50.0, "n_known_devices": 2.0,
               "account_age_days": 400.0, "home_country": "CO"}
    row = build_row(event, profile)
    assert row["amount_to_avg_ratio"] == 3.0
    assert row["amount_zscore"] == 4.0
    assert row["is_foreign"] == 1.0


def test_sync_velocity_adds_the_current_payment_to_the_stored_window_like_an_over_window():
    from fraudcore.features import sync_velocity

    snap = {"f_cnt_10m": 3, "f_sum_10m": 120.0, "f_max_10m": 80.0, "f_small_tx_cnt_10m": 1, "ts": 1000.0}
    v = sync_velocity(snap, amount=5.0, now_ts=1100.0)
    assert v == {"f_cnt_10m": 4, "f_sum_10m": 125.0, "f_max_10m": 80.0, "f_small_tx_cnt_10m": 2}
    assert sync_velocity(snap, amount=200.0, now_ts=1100.0)["f_max_10m"] == 200.0


def test_sync_velocity_forgets_a_window_that_has_aged_out_and_handles_unknown_users():
    from fraudcore.features import WINDOW_S, sync_velocity

    snap = {"f_cnt_10m": 9, "f_sum_10m": 900.0, "f_max_10m": 500.0, "f_small_tx_cnt_10m": 0, "ts": 1000.0}
    expected = {"f_cnt_10m": 1, "f_sum_10m": 50.0, "f_max_10m": 50.0, "f_small_tx_cnt_10m": 0}
    assert sync_velocity(snap, 50.0, now_ts=1000.0 + WINDOW_S + 1) == expected
    assert sync_velocity(None, 50.0, now_ts=1.0) == expected
    assert sync_velocity(snap, 50.0, now_ts=1000.0 + WINDOW_S)["f_cnt_10m"] == 10   # exactly 10 min: in
