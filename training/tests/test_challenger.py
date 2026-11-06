import numpy as np
import pytest

torch = pytest.importorskip("torch")

from fraud_training.challenger import MLP, fit_mlp, mlp_scores, mlp_to_onnx  # noqa: E402
from fraud_training.metrics import pr_auc  # noqa: E402
from fraud_training.models import PARITY_TOL, OnnxScorer, check_parity  # noqa: E402


@pytest.fixture(scope="module")
def data():
    rng = np.random.default_rng(0)
    scale = np.array([1000.0, 1.0, 50.0, 0.01, 7.0, 300.0], dtype=np.float32)    # wildly different units
    X = (rng.normal(size=(6000, 6)) * scale + scale).astype(np.float32)
    z = (X[:, 0] / scale[0] - 1) + (X[:, 1] / scale[1] - 1) ** 2 * 0.5
    y = (z > 1.4).astype(int)
    return X[:4500], y[:4500], X[4500:], y[4500:]


@pytest.fixture(scope="module")
def trained(data):
    Xtr, ytr, Xva, yva = data
    return fit_mlp(Xtr, ytr, Xva, yva, epochs=25, seed=1)


def test_the_mlp_learns_the_signal_despite_unscaled_inputs(trained, data):
    _, _, Xva, yva = data
    assert pr_auc(yva, mlp_scores(trained, Xva)) > 0.8


def test_normalisation_lives_inside_the_model_so_raw_features_go_in(trained):
    assert isinstance(trained, MLP)
    state = trained.state_dict()
    assert "mean" in state and "std" in state                 # travels with the weights: no skew


def test_onnx_twin_matches_torch_within_the_registry_gate(trained, data):
    _, _, Xva, _ = data
    blob = mlp_to_onnx(trained, n_features=Xva.shape[1])
    diff = check_parity(mlp_scores(trained, Xva), OnnxScorer(blob).predict(Xva))
    assert diff < PARITY_TOL


def test_onnx_output_has_the_label_then_probabilities_layout_the_scorer_reads(trained, data):
    import onnxruntime as ort

    sess = ort.InferenceSession(mlp_to_onnx(trained, n_features=data[2].shape[1]),
                                providers=["CPUExecutionProvider"])
    out = sess.run(None, {sess.get_inputs()[0].name: data[2][:5]})
    assert out[1].shape == (5, 2) and np.allclose(out[1].sum(axis=1), 1.0, atol=1e-6)


def test_onnx_accepts_any_batch_size(trained, data):
    scorer = OnnxScorer(mlp_to_onnx(trained, n_features=data[2].shape[1]))
    assert scorer.predict(data[2][:1]).shape == (1,) and scorer.predict(data[2][:333]).shape == (333,)


def test_training_is_deterministic_for_a_seed(data):
    Xtr, ytr, Xva, yva = data
    a = mlp_scores(fit_mlp(Xtr, ytr, Xva, yva, epochs=3, seed=7), Xva)
    b = mlp_scores(fit_mlp(Xtr, ytr, Xva, yva, epochs=3, seed=7), Xva)
    assert np.allclose(a, b, atol=1e-6)
