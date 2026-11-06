import numpy as np
from scorer.adapters.hot_swap import HotSwapModel, RegisteredModel
from scorer.adapters.rule_model import RuleModel

from fraudcore.features import FEATURE_ORDER

X = np.ones((2, len(FEATURE_ORDER)))


class FakeRegistry:
    def __init__(self, make_onnx):
        self.make_onnx, self.alias, self.fail = make_onnx, None, False
        self.versions: dict[str, RegisteredModel] = {}

    def publish(self, version: str, weight: float, features=None) -> None:
        self.versions[version] = RegisteredModel(self.make_onnx(weight), features or list(FEATURE_ORDER))
        self.alias = version

    def current_version(self):
        if self.fail:
            raise ConnectionError("registry down")
        return self.alias

    def fetch(self, version):
        return self.versions[version]


def model_with(registry):
    return HotSwapModel(registry, name="fraud-champion", fallback=RuleModel())


def test_serves_the_fallback_until_a_champion_exists(make_onnx):
    reg = FakeRegistry(make_onnx)
    m = model_with(reg)
    assert m.refresh() is False
    assert m.predict(X).model == "rule-dummy"


def test_swaps_to_the_new_champion_and_labels_batches_with_its_version(make_onnx):
    reg = FakeRegistry(make_onnx)
    m = model_with(reg)
    reg.publish("1", -0.5)
    assert m.refresh() is True
    first = m.predict(X)
    assert (first.model, first.version) == ("fraud-champion", "1") and first.scores[0] < 0.5

    reg.publish("2", 0.5)                       # promotion = the alias moves
    assert m.refresh() is True
    second = m.predict(X)
    assert second.version == "2" and second.scores[0] > 0.5
    assert m.refresh() is False                 # nothing new: no reload


def test_a_registry_outage_keeps_the_current_model(make_onnx):
    reg = FakeRegistry(make_onnx)
    m = model_with(reg)
    reg.publish("1", 0.1)
    m.refresh()
    reg.fail = True
    assert m.refresh() is False
    assert m.predict(X).version == "1"


def test_a_model_with_a_different_feature_order_is_refused(make_onnx):
    reg = FakeRegistry(make_onnx)
    m = model_with(reg)
    reg.publish("1", 0.1)
    m.refresh()
    reg.publish("2", 0.9, features=list(reversed(FEATURE_ORDER)))
    assert m.refresh() is False
    assert m.predict(X).version == "1"
    assert m.refresh() is False                 # and it does not retry the same bad version forever


def test_background_thread_swaps_without_blocking_predictions(make_onnx):
    reg = FakeRegistry(make_onnx)
    m = HotSwapModel(reg, name="fraud-champion", fallback=RuleModel(), poll_s=0.02)
    m.start()
    try:
        reg.publish("3", 0.2)
        for _ in range(100):
            if m.predict(X).version == "3":
                break
            import time
            time.sleep(0.02)
        assert m.predict(X).version == "3"
    finally:
        m.stop()


def test_thresholds_published_with_the_model_reach_the_prediction(make_onnx):
    reg = FakeRegistry(make_onnx)
    m = model_with(reg)
    reg.versions["1"] = RegisteredModel(make_onnx(0.1), list(FEATURE_ORDER), {"review": 0.4, "block": 0.9})
    reg.alias = "1"
    m.refresh()
    th = m.predict(X).thresholds
    assert (th.name, th.review, th.block) == ("fraud-champion-v1", 0.4, 0.9)


def test_models_without_thresholds_leave_the_configured_ones_in_charge(make_onnx):
    reg = FakeRegistry(make_onnx)
    m = model_with(reg)
    reg.publish("1", 0.1)
    m.refresh()
    assert m.predict(X).thresholds is None
