import numpy as np
import pytest
from scorer.adapters.hot_swap import HotSwapModel, RegisteredModel
from scorer.adapters.rule_model import RuleModel
from scorer.adapters.triton_model import TritonModel, TritonRuntimeBuilder, TritonUnavailable
from scorer.adapters.triton_repository import TritonRepository

from fraudcore.features import FEATURE_ORDER

X = np.ones((3, len(FEATURE_ORDER)))


class FakeResult:
    def __init__(self, probabilities):
        self._p = probabilities

    def as_numpy(self, name):
        assert name == "probabilities"
        return self._p


class FakeTriton:
    """Stands in for tritonclient.grpc.InferenceServerClient; answers P(fraud) = 0.9 for every row."""

    def __init__(self):
        self.calls, self.ready, self.fail = [], True, False

    def infer(self, model_name, inputs, outputs, model_version, client_timeout):
        if self.fail:
            raise ConnectionError("triton down")
        data = inputs[0].as_numpy() if hasattr(inputs[0], "as_numpy") else None
        self.calls.append((model_name, model_version, client_timeout, inputs[0].shape(), data))
        n = inputs[0].shape()[0]
        p1 = np.full(n, 0.9, dtype=np.float32)
        return FakeResult(np.stack([1 - p1, p1], axis=1))

    def is_model_ready(self, model_name, model_version):
        return self.ready


def test_predict_sends_one_batched_request_pinned_to_the_version_and_returns_the_fraud_column():
    client = FakeTriton()
    model = TritonModel(client, "fraud_xgb", "5", name="fraud-champion", timeout_s=0.05)
    pred = model.predict(X)
    assert np.allclose(pred.scores, 0.9) and pred.scores.shape == (3,)
    assert (pred.model, pred.version) == ("fraud-champion", "5")
    (call,) = client.calls
    assert call[:3] == ("fraud_xgb", "5", 0.05) and call[3] == [3, len(FEATURE_ORDER)]


def test_a_transport_error_becomes_triton_unavailable():
    client = FakeTriton()
    client.fail = True
    with pytest.raises(TritonUnavailable):
        TritonModel(client, "fraud_xgb", "5", name="m", timeout_s=0.05).predict(X)


def test_repository_publishes_the_onnx_under_a_numeric_version_with_a_config(tmp_path, make_onnx):
    repo = TritonRepository(tmp_path)
    repo.publish("fraud_xgb", "7", make_onnx(0.1))
    assert (tmp_path / "fraud_xgb" / "7" / "model.onnx").read_bytes() == make_onnx(0.1)
    config = (tmp_path / "fraud_xgb" / "config.pbtxt").read_text()
    assert 'platform: "onnxruntime_onnx"' in config and "max_batch_size: 0" in config
    assert f"dims: [ -1, {len(FEATURE_ORDER)} ]" in config
    assert 'name: "probabilities"' in config and 'name: "input"' in config
    assert not list((tmp_path / "fraud_xgb").glob("*.partial"))


def test_the_builder_publishes_waits_until_ready_and_serves_through_triton(tmp_path, make_onnx):
    client = FakeTriton()
    builder = TritonRuntimeBuilder(client, TritonRepository(tmp_path), "fraud_xgb", timeout_s=0.05,
                                   ready_timeout_s=1.0, poll_s=0.0)
    reg = RegisteredModel(make_onnx(0.1), list(FEATURE_ORDER))
    runtime = builder(reg, "fraud-champion", "3", None)
    assert runtime.predict(X).version == "3" and (tmp_path / "fraud_xgb" / "3" / "model.onnx").exists()


def test_a_version_triton_never_loads_is_refused_so_the_old_model_keeps_serving(tmp_path, make_onnx):
    client = FakeTriton()
    client.ready = False
    builder = TritonRuntimeBuilder(client, TritonRepository(tmp_path), "fraud_xgb", timeout_s=0.05,
                                   ready_timeout_s=0.05, poll_s=0.0)

    class Registry:
        def current_version(self):
            return "9"

        def fetch(self, version):
            return RegisteredModel(make_onnx(0.1), list(FEATURE_ORDER))

    swap = HotSwapModel(Registry(), "fraud-champion", fallback=RuleModel(), build=builder)
    assert swap.refresh() is False
    assert swap.predict(X).model == "rule-dummy"
