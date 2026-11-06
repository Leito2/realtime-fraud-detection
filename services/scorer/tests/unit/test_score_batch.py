from scorer.application.score_batch import ScoreBatch
from scorer.domain.degraded import DegradedPolicy
from scorer.testing import FakeModel, FakeProfileStore, FixedClock, ListSink

from fraudcore.contracts import EnrichedPayment
from fraudcore.thresholds import Thresholds


def event(pid: str, user: str = "u_1", amount: float = 50.0) -> EnrichedPayment:
    return EnrichedPayment(
        payment_id=pid, user_id=user, card_id="c", amount=amount, currency="USD", merchant_id="m",
        mcc="5411", country="CO", device_id="d", ip_country="CO", channel="web",
        event_time="2026-10-05T14:03:22.123Z", t_scheduled_ns=1_000, t_sent_ns=1_100,
        f_cnt_10m=1, f_sum_10m=amount, f_max_10m=amount, f_small_tx_cnt_10m=0)


def build(scores, profiles=None, fail=False):
    sink = ListSink()
    store = FakeProfileStore(profiles, fail)
    model = FakeModel(scores)
    uc = ScoreBatch(
        profiles=store, model=model, sink=sink, thresholds=Thresholds("v1", 0.3, 0.8),
        policy=DegradedPolicy(Thresholds("v1-degraded", 0.2, 0.7), max_auto_approve_amount=500),
        clock=FixedClock(9_000))
    return uc, store, model, sink


def test_one_profile_lookup_and_one_inference_per_batch():
    uc, store, model, sink = build([0.1, 0.5, 0.9])
    out = uc([event("a"), event("b", "u_2"), event("c", "u_3")])
    assert store.calls == 1 and len(model.batches) == 1 and sink.batches == 1
    assert [d.decision for d in out] == ["APPROVE", "REVIEW", "BLOCK"]


def test_decision_carries_measurement_fields_and_model_identity():
    uc, *_ = build([0.1])
    d = uc([event("a")])[0]
    assert (d.payment_id, d.user_id, d.model, d.model_version) == ("a", "u_1", "fake", "1")
    assert (d.t_scheduled_ns, d.t_decided_ns, d.threshold_set, d.degraded) == (1_000, 9_000, "v1", False)


def test_profile_outage_marks_degraded_and_applies_the_conservative_policy():
    uc, _, _, sink = build([0.25, 0.05], fail=True)
    out = uc([event("a"), event("b", amount=900.0)])
    assert all(d.degraded for d in out)
    assert [d.decision for d in out] == ["REVIEW", "REVIEW"]      # 0.25 >= 0.2; 900 > limit
    assert out[0].threshold_set == "v1-degraded"
    assert sink.published == out


def test_empty_batch_does_nothing():
    uc, store, model, sink = build([])
    assert uc([]) == [] and store.calls == 0 and not model.batches and sink.batches == 0


def test_thresholds_travel_with_the_model_and_win_over_the_configured_ones():
    import numpy as np
    from scorer.application.ports import Prediction

    class ModelWithOwnThresholds(FakeModel):
        def predict(self, X):
            return Prediction(np.array(self.scores[: len(X)]), "m", "7", Thresholds("m-v7", 0.05, 0.5))

    uc, *_ = build([0.1, 0.6])
    uc.model = ModelWithOwnThresholds([0.1, 0.6])
    out = uc([event("a"), event("b", "u_2")])
    assert [d.decision for d in out] == ["REVIEW", "BLOCK"]  # configured 0.3/0.8 would differ
    assert {d.threshold_set for d in out} == {"m-v7"}


def test_served_features_are_logged_only_when_asked():
    from fraudcore.features import FEATURE_ORDER

    uc, *_ = build([0.1])
    assert uc([event("a")])[0].features is None                 # critical path stays lean by default
    uc, *_ = build([0.1])
    uc.log_features = True
    served = uc([event("a", amount=77.0)])[0].features
    assert list(served) == list(FEATURE_ORDER) and served["amount"] == 77.0


def test_the_models_score_reference_is_forwarded_to_the_metrics():
    import numpy as np
    from scorer.application.ports import Prediction
    from scorer.testing import RecordingMetrics

    ref = {"edges": [0.0, 1.0], "fractions": [1.0]}

    class WithReference(FakeModel):
        def predict(self, X):
            return Prediction(np.array(self.scores[: len(X)]), "m", "7", None, ref)

    uc, *_ = build([0.1])
    uc.model, uc.metrics = WithReference([0.1]), RecordingMetrics()
    uc([event("a")])
    assert uc.metrics.batches[0].score_reference == ref
