"""One evaluation routine for every model, so the rows of the results table are comparable (PLAN.md 4.9)."""
import numpy as np
import polars as pl

from fraud_training.costs import Costs, expected_cost
from fraud_training.metrics import fpr_threshold, pr_auc, recall_at_fpr, recall_by_scenario

# LightGBM replaces the XGBoost default only with a clear margin (PLAN.md section 4.5).
PR_AUC_MARGIN = 0.01
RECALL_MARGIN = 0.02


def evaluate_scores(df: pl.DataFrame, scores: np.ndarray, review: float, block: float, costs: Costs,
                    unseen_users: set[str]) -> dict:
    """``df`` carries label (noisy), fraud_true, scenario, amount, user_id. All metrics use the noisy label,
    except recall by scenario which uses the ground truth to show what each model really misses."""
    y = df["label"].to_numpy()
    truth = df["fraud_true"].to_numpy()
    scenario = df["scenario"].to_numpy()
    amount = df["amount"].to_numpy()
    cutoff = fpr_threshold(y, scores, 0.01)
    unseen = df["user_id"].is_in(list(unseen_users)).to_numpy()
    has_unseen = bool(unseen.sum() and y[unseen].any())
    return {
        "pr_auc": pr_auc(y, scores),
        "recall_at_fpr_1pct": recall_at_fpr(y, scores, 0.01),
        "recall_by_scenario": recall_by_scenario(scenario, truth, scores, cutoff),
        "pr_auc_unseen_users": pr_auc(y[unseen], scores[unseen]) if has_unseen else None,
        "cost": expected_cost(scores, y, amount, review, block, costs),
        "review_rate": float(((scores >= review) & (scores < block)).mean()),
        "block_rate": float((scores >= block).mean()),
    }


def pick_champion(xgboost: dict, lightgbm: dict, eligible: tuple[str, ...] = ("xgboost", "lightgbm")) -> str:
    """XGBoost unless LightGBM is better by a clear margin on PR-AUC or recall at 1% FPR.

    Only models whose ONNX twin passed the parity check are eligible: one that fails is never registered.
    """
    if not eligible:
        raise ValueError("no model passed the ONNX parity check")
    if eligible == ("xgboost",) or eligible == ("lightgbm",):
        return eligible[0]
    dp = lightgbm["pr_auc"] - xgboost["pr_auc"]
    dr = lightgbm["recall_at_fpr_1pct"] - xgboost["recall_at_fpr_1pct"]
    return "lightgbm" if dp >= PR_AUC_MARGIN or dr >= RECALL_MARGIN else "xgboost"
