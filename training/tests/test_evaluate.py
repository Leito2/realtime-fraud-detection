import numpy as np
import polars as pl
import pytest
from fraud_training.costs import Costs
from fraud_training.evaluate import evaluate_scores, pick_champion


def frame(n=6000, seed=0):
    rng = np.random.default_rng(seed)
    truth = rng.random(n) < 0.02
    scen = np.where(truth, rng.choice(["burst", "geo_jump"], n), "normal")
    users = rng.choice(["a", "b", "c", "d"], n)
    df = pl.DataFrame({"label": truth.astype(int), "fraud_true": truth, "scenario": scen,
                       "amount": rng.lognormal(4, 1, n), "user_id": users})
    return df, truth


def test_evaluate_scores_reports_every_axis_of_the_results_table():
    df, truth = frame()
    scores = np.clip(np.random.default_rng(1).normal(0.2 + 0.6 * truth, 0.15), 0, 1)
    out = evaluate_scores(df, scores, review=0.4, block=0.7, costs=Costs(), unseen_users={"d"})
    assert {"pr_auc", "recall_at_fpr_1pct", "cost", "recall_by_scenario", "pr_auc_unseen_users",
            "review_rate"} <= set(out)
    assert out["pr_auc"] > 0.5 and set(out["recall_by_scenario"]) == {"burst", "geo_jump"}


def results(pr, rec):
    return {"pr_auc": pr, "recall_at_fpr_1pct": rec}


def test_xgboost_stays_champion_unless_lightgbm_wins_by_a_clear_margin():
    assert pick_champion(results(0.80, 0.50), results(0.805, 0.51)) == "xgboost"
    assert pick_champion(results(0.80, 0.50), results(0.82, 0.50)) == "lightgbm"
    assert pick_champion(results(0.80, 0.50), results(0.80, 0.53)) == "lightgbm"
    assert pick_champion(results(0.80, 0.50), results(0.70, 0.40)) == "xgboost"


def test_pick_champion_requires_both_candidates():
    with pytest.raises(KeyError):
        pick_champion(results(0.8, 0.5), {})


def test_a_model_whose_onnx_twin_fails_parity_cannot_be_champion():
    lgbm_wins = results(0.90, 0.80)
    assert pick_champion(results(0.80, 0.50), lgbm_wins, eligible=("xgboost",)) == "xgboost"
    assert pick_champion(results(0.80, 0.50), lgbm_wins, eligible=("lightgbm",)) == "lightgbm"
    with pytest.raises(ValueError, match="parity"):
        pick_champion(results(0.80, 0.50), lgbm_wins, eligible=())
