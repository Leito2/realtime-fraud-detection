"""Offline metrics (PLAN.md section 4.9). Pure numpy."""
import math

import numpy as np
from sklearn.metrics import average_precision_score


def pr_auc(y: np.ndarray, scores: np.ndarray) -> float:
    """Average precision: the honest metric for a 1%-positive problem (ROC-AUC looks good for bad models)."""
    return float(average_precision_score(y, scores))


def fpr_threshold(y: np.ndarray, scores: np.ndarray, fpr: float) -> float:
    """Smallest cutoff such that ``score > cutoff`` flags at most ``fpr`` of the legitimate payments."""
    legit = np.sort(scores[y == 0])[::-1]
    allowed = math.floor(fpr * len(legit))
    return float(legit[allowed]) if allowed < len(legit) else float("-inf")


def recall_at_fpr(y: np.ndarray, scores: np.ndarray, fpr: float = 0.01) -> float:
    """The business question: how much fraud do we catch while bothering 1 in 100 legitimate customers?"""
    cutoff = fpr_threshold(y, scores, fpr)
    return float((scores[y == 1] > cutoff).mean())


def recall_by_scenario(scenario: np.ndarray, fraud_true: np.ndarray, scores: np.ndarray,
                       threshold: float) -> dict[str, float]:
    """Which fraud types the model catches and which slip through (ground truth, not the noisy label)."""
    out = {}
    for name in np.unique(scenario[fraud_true]):
        mask = fraud_true & (scenario == name)
        out[str(name)] = float((scores[mask] >= threshold).mean())
    return out
