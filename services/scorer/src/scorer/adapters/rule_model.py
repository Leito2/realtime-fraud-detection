"""M1 placeholder model: score from the 10-minute payment count. Replaced by OnnxModel in M3."""
import numpy as np

from fraudcore.features import FEATURE_ORDER

_CNT = FEATURE_ORDER.index("f_cnt_10m")


class RuleModel:
    name = "rule-dummy"
    version = "0"

    def predict(self, X: np.ndarray) -> np.ndarray:
        return np.minimum(1.0, X[:, _CNT] / 10.0)
