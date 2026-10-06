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
