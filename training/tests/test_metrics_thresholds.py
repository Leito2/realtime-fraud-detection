import numpy as np
import pytest
from fraud_training.costs import Costs, choose_thresholds, expected_cost
from fraud_training.metrics import pr_auc, recall_at_fpr, recall_by_scenario


def test_pr_auc_perfect_and_random():
    y = np.array([0] * 90 + [1] * 10)
    assert pr_auc(y, np.r_[np.zeros(90), np.ones(10)]) == pytest.approx(1.0)
    rng = np.random.default_rng(0)
    big = np.array([0] * 9_000 + [1] * 1_000)
    assert pr_auc(big, rng.random(10_000)) == pytest.approx(0.1, abs=0.03)   # random ~ the base rate


def test_recall_at_fpr_counts_frauds_above_the_threshold_that_flags_1pct_of_legit():
    y = np.array([0] * 1000 + [1] * 10)
    scores = np.r_[np.linspace(0, 0.9, 1000), [0.99] * 6 + [0.5] * 4]
    # 1% FPR -> top 10 legit flagged -> threshold ~0.89; only the six 0.99 frauds are above it
    assert recall_at_fpr(y, scores, 0.01) == pytest.approx(0.6)


def test_recall_by_scenario_uses_ground_truth_and_one_threshold():
    scen = np.array(["normal"] * 5 + ["burst"] * 2 + ["geo_jump"] * 2)
    truth = np.array([False] * 5 + [True] * 4)
    scores = np.array([0.1, 0.1, 0.1, 0.1, 0.1, 0.9, 0.9, 0.9, 0.2])
    out = recall_by_scenario(scen, truth, scores, threshold=0.5)
    assert out == {"burst": 1.0, "geo_jump": 0.5}


COSTS = Costs(review_cost=5.0, block_friction_cost=20.0)


def test_expected_cost_charges_missed_fraud_by_amount():
    scores = np.array([0.1, 0.1, 0.6, 0.9])
    y = np.array([1, 0, 0, 1])
    amount = np.array([100.0, 10.0, 10.0, 50.0])
    # t1=0.5 t2=0.8: payment0 missed (-100), payment2 reviewed (5), payment3 blocked fraud (0)
    assert expected_cost(scores, y, amount, 0.5, 0.8, COSTS) == pytest.approx(105.0)
    # a legit blocked costs friction
    assert expected_cost(np.array([0.9]), np.array([0]), np.array([10.0]), 0.5, 0.8, COSTS) == 20.0


def test_choose_thresholds_beats_do_nothing_and_respects_review_capacity():
    rng = np.random.default_rng(1)
    n = 5000
    y = (rng.random(n) < 0.02).astype(int)
    scores = np.clip(rng.normal(0.15 + 0.6 * y, 0.12), 0, 1)
    amount = rng.lognormal(4, 1, n)
    t = choose_thresholds(scores, y, amount, COSTS, max_review_rate=0.05)
    assert 0 < t.review <= t.block <= 1
    chosen = expected_cost(scores, y, amount, t.review, t.block, COSTS)
    assert chosen < expected_cost(scores, y, amount, 1.1, 1.1, COSTS)   # never flag anything
    review_rate = float(((scores >= t.review) & (scores < t.block)).mean())
    assert review_rate <= 0.05


def test_flag_rate_matching_puts_the_review_threshold_at_the_target_share():
    from fraud_training.costs import match_flag_rate

    from fraudcore.thresholds import Thresholds

    scores = np.linspace(0, 1, 1001)
    t = match_flag_rate(scores, 0.10, Thresholds("cost", 0.5, 0.9))
    assert abs((scores >= t.review).mean() - 0.10) < 0.005
    assert t.block > t.review


def test_flag_rate_matching_never_puts_block_below_review():
    from fraud_training.costs import match_flag_rate

    from fraudcore.thresholds import Thresholds

    scores = np.linspace(0, 1, 1001)
    t = match_flag_rate(scores, 0.02, Thresholds("cost", 0.5, 0.6))     # 2% flagged -> review ~0.98
    assert t.block > t.review >= 0.97
