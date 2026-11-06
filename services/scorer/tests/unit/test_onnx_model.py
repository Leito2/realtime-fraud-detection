import numpy as np
from scorer.adapters.onnx_model import OnnxModel

from fraudcore.features import FEATURE_ORDER


def test_predict_returns_fraud_probability_and_model_identity(make_onnx):
    model = OnnxModel(make_onnx(0.0), name="fraud-champion", version="7")
    X = np.ones((4, len(FEATURE_ORDER)), dtype=np.float64)       # float64 in, float32 inside
    pred = model.predict(X)
    assert pred.scores.shape == (4,) and np.allclose(pred.scores, 0.5)
    assert (pred.model, pred.version) == ("fraud-champion", "7")


def test_larger_weight_gives_higher_score(make_onnx):
    X = np.ones((1, len(FEATURE_ORDER)))
    low = OnnxModel(make_onnx(-0.5), "m", "1").predict(X).scores[0]
    high = OnnxModel(make_onnx(0.5), "m", "1").predict(X).scores[0]
    assert low < 0.5 < high
