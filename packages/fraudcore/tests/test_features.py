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
