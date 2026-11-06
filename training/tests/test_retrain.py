from datetime import UTC, datetime, timedelta

import mlflow
import numpy as np
import pytest
from fraud_training.dataset import build_dataset
from fraud_training.retrain import NotEnoughFeedback, feedback_arrays, retrain
from fraud_training.train import Settings
from generator.logic import Population
from generator.offline import generate_history

from fraudcore.features import FEATURE_ORDER


@pytest.fixture(scope="module")
def history():
    return generate_history(Population(n_users=400, seed=5), seed=5, start=datetime(2026, 8, 1, tzinfo=UTC),
                            days=14, payments_per_day=1_500)


def fake_feedback(history, n=3000):
    ds = build_dataset(history).tail(n)
    t0 = datetime(2026, 11, 1, tzinfo=UTC)
    return [({f: float(r[f]) for f in FEATURE_ORDER}, bool(r["label"]), t0 + timedelta(seconds=i))
            for i, r in enumerate(ds.iter_rows(named=True))]


def test_feedback_arrays_follow_the_feature_order_and_time(history):
    rows = fake_feedback(history, 50)
    shuffled = list(reversed(rows))
    X, y = feedback_arrays(shuffled)
    assert X.shape == (50, len(FEATURE_ORDER)) and X.dtype == np.float32
    X2, y2 = feedback_arrays(rows)
    assert np.array_equal(X, X2) and np.array_equal(y, y2)           # ordered by decided_at, not arrival


def test_a_feedback_vector_with_missing_features_is_rejected(history):
    features, label, at = fake_feedback(history, 1)[0]
    features.pop("amount")
    with pytest.raises(KeyError):
        feedback_arrays([(features, label, at)])


def test_too_little_feedback_is_refused_instead_of_training_noise(history):
    rows = [r for r in fake_feedback(history, 300) if not r[1]][:100]          # no fraud at all
    with pytest.raises(NotEnoughFeedback):
        retrain(Settings(register=False), rows)


def test_retrain_registers_a_verified_challenger_from_history_plus_feedback(history, tmp_path):
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    path = tmp_path / "history.parquet"
    history.write_parquet(path)
    s = Settings(history=path, tracking_uri=uri, artifact_root=(tmp_path / "a").as_uri(), valid_days=3,
                 test_days=3, results_out=tmp_path / "r.json", max_review_rate=0.05)
    result = retrain(s, fake_feedback(history), min_fraud=10, use_history=True)
    mv = mlflow.MlflowClient(tracking_uri=uri).get_model_version_by_alias("fraud-champion", "challenger")
    assert str(mv.version) == result["registry"]["version"]
    assert mv.tags["source"] == "feedback+history" and int(mv.tags["feedback_rows"]) == 3000
    assert result["onnx_max_abs_diff"] < 1e-5


def test_matching_the_flag_rate_keeps_a_valid_threshold_pair(history, tmp_path):
    path = tmp_path / "history.parquet"
    history.write_parquet(path)
    s = Settings(history=path, register=False, valid_days=3, test_days=3, results_out=tmp_path / "r.json",
                 max_review_rate=0.05)
    result = retrain(s, fake_feedback(history), min_fraud=10, use_history=True, match_flag_rate_to=0.04)
    th = result["thresholds"]
    assert 0 <= th["review"] < th["block"] <= 1 and result["matched_flag_rate"] == 0.04
