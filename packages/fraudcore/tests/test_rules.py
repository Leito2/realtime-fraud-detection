import pytest

from fraudcore.features import FEATURE_ORDER
from fraudcore.rules import RULES, rules_score


def row(**over) -> dict:
    base = dict.fromkeys(FEATURE_ORDER, 0.0)
    base.update(f_cnt_10m=1.0, f_max_10m=20.0, amount=20.0)
    base.update(over)
    return base


def test_quiet_payment_scores_zero():
    assert rules_score(row()) == 0.0


@pytest.mark.parametrize(
    "over",
    [
        {"f_cnt_10m": 8.0},
        {"is_foreign": 1.0, "amount_to_avg_ratio": 4.0},
        {"amount_to_avg_ratio": 9.0},
        {"f_small_tx_cnt_10m": 5.0},
    ],
)
def test_each_rule_fires_alone(over):
    assert 0.0 < rules_score(row(**over)) < 1.0


def test_hits_add_up_and_cap_at_one():
    r = row(f_cnt_10m=9.0, is_foreign=1.0, amount_to_avg_ratio=9.0, f_small_tx_cnt_10m=6.0,
            ip_country_mismatch=1.0)
    assert rules_score(r) == 1.0


def test_rules_only_read_known_features():
    for rule in RULES:
        assert set(rule.reads) <= set(FEATURE_ORDER)
