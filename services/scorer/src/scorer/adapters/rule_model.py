"""Fallback model: the 10-minute payment count as a score. Used until a champion exists in the registry."""
import numpy as np

from fraudcore.features import FEATURE_ORDER
from scorer.application.ports import Prediction

_CNT = FEATURE_ORDER.index("f_cnt_10m")


class RuleModel:
    name = "rule-dummy"
    version = "0"

    def predict(self, X: np.ndarray) -> Prediction:
        return Prediction(np.minimum(1.0, X[:, _CNT] / 10.0), self.name, self.version)
