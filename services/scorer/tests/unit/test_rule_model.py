import numpy as np
from scorer.adapters.rule_model import RuleModel

from fraudcore.features import FEATURE_ORDER


def test_score_grows_with_ten_minute_count_and_is_capped_at_one():
    X = np.zeros((3, len(FEATURE_ORDER)))
    col = FEATURE_ORDER.index("f_cnt_10m")
    X[:, col] = [1, 5, 50]
    s = RuleModel().predict(X).scores
    assert list(s) == [0.1, 0.5, 1.0]
