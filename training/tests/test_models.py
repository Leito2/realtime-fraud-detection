import numpy as np
import pytest
from fraud_training.models import (
    PARITY_TOL,
    OnnxScorer,
    benchmark,
    check_parity,
    fit_model,
    native_scores,
    to_onnx,
)


@pytest.fixture(scope="module")
def data():
    rng = np.random.default_rng(0)
    X = rng.normal(size=(4000, 12)).astype(np.float32)
    y = ((X[:, 0] + X[:, 1] ** 2) > 1.6).astype(int)
    return X[:3000], y[:3000], X[3000:], y[3000:]


@pytest.mark.parametrize("kind", ["xgboost", "lightgbm"])
def test_onnx_matches_native_to_1e5_and_beats_chance(kind, data, tmp_path):
    Xtr, ytr, Xva, yva = data
    model = fit_model(kind, Xtr, ytr, Xva, yva, params={"n_estimators": 60, "max_depth": 4})
    native = native_scores(model, Xva)
    onnx_bytes = to_onnx(model, kind, n_features=Xva.shape[1])
    scorer = OnnxScorer(onnx_bytes)
    diff = check_parity(native, scorer.predict(Xva))
    assert diff < PARITY_TOL
    assert ((native > 0.5) == yva).mean() > 0.9


def test_parity_check_fails_loudly():
    with pytest.raises(AssertionError, match="parity"):
        check_parity(np.array([0.1, 0.2]), np.array([0.1, 0.25]))


def test_benchmark_reports_percentiles_per_batch_size():
    out = benchmark(lambda X: X.sum(axis=1), np.ones((600, 4), dtype=np.float32), batch_sizes=(1, 32, 500),
                    repeats=20)
    assert set(out) == {1, 32, 500}
    for stats in out.values():
        assert stats["p50_ms"] <= stats["p99_ms"] and stats["per_event_p50_us"] > 0
