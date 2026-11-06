import numpy as np
from scorer.application.ports import Prediction, ProfileUnavailable
from scorer.application.score_batch import ScoreBatch
from scorer.application.score_sync import ScoreSync
from scorer.domain.degraded import DegradedPolicy
from scorer.testing import FakeProfileStore, FixedClock, ListSink

from fraudcore.contracts import Payment
from fraudcore.features import FEATURE_ORDER
from fraudcore.thresholds import Thresholds

NOW = "2026-10-05T14:10:00Z"           # 2026-10-05 14:10:00 UTC
NOW_TS = 1791209400.0


def payment(pid="p1", user="u_1", amount=50.0, event_time=NOW) -> Payment:
    return Payment(payment_id=pid, user_id=user, card_id="c", amount=amount, currency="USD", merchant_id="m",
                   mcc="5411", country="CO", device_id="d", ip_country="CO", channel="web",
                   event_time=event_time, t_scheduled_ns=0, t_sent_ns=0)


class RecordingModel:
    name, version = "fake", "1"

    def __init__(self, score=0.5):
        self.score, self.matrices = score, []

    def predict(self, X):
        self.matrices.append(X)
        return Prediction(np.full(len(X), self.score), self.name, self.version)


class FakeVelocity:
    def __init__(self, snapshots=None, fail=False):
        self.snapshots, self.fail, self.calls = snapshots or {}, fail, 0

    def get_many(self, user_ids):
        self.calls += 1
        if self.fail:
            raise ProfileUnavailable("redis down")
        return [self.snapshots.get(u) for u in user_ids]


def build(velocity=None, profiles=None, score=0.5):
    model, sink, store = RecordingModel(score), ListSink(), FakeProfileStore(profiles)
    policy = DegradedPolicy(Thresholds("v1-degraded", 0.2, 0.7), max_auto_approve_amount=500)
    batch = ScoreBatch(profiles=store, model=model, sink=sink, thresholds=Thresholds("v1", 0.3, 0.8),
                       policy=policy, clock=FixedClock(9_000))
    vel = velocity or FakeVelocity()
    return ScoreSync(velocity=vel, batch=batch), vel, store, model, sink


def row(model, i=0):
    return dict(zip(FEATURE_ORDER, model.matrices[0][i], strict=True))


def test_the_current_payment_is_added_to_the_stored_window_so_features_match_the_kafka_path():
    snap = {"f_cnt_10m": 3, "f_sum_10m": 120.0, "f_max_10m": 80.0, "f_small_tx_cnt_10m": 1, "ts": NOW_TS - 60}
    sync, _, _, model, _ = build(FakeVelocity({"u_1": snap}))
    sync([payment(amount=5.0)])
    r = row(model)
    assert (r["f_cnt_10m"], r["f_sum_10m"], r["f_max_10m"], r["f_small_tx_cnt_10m"]) == (4, 125.0, 80.0, 2)


def test_an_unknown_user_or_an_aged_window_counts_only_the_current_payment():
    old = {"f_cnt_10m": 9, "f_sum_10m": 900.0, "f_max_10m": 500.0, "f_small_tx_cnt_10m": 0,
           "ts": NOW_TS - 3600}
    sync, _, _, model, _ = build(FakeVelocity({"u_old": old}))
    sync([payment("a", "u_new", 50.0), payment("b", "u_old", 50.0)])
    assert row(model, 0)["f_cnt_10m"] == 1 and row(model, 1)["f_cnt_10m"] == 1


def test_a_micro_batch_costs_one_lookup_per_store_and_one_inference():
    sync, vel, store, model, sink = build()
    out = sync([payment("a", "u_1"), payment("b", "u_2"), payment("c", "u_3")])
    assert vel.calls == 1 and store.calls == 1 and len(model.matrices) == 1 and len(out) == 3
    assert sink.batches == 1


def test_redis_down_degrades_with_the_fallback_thresholds_instead_of_failing():
    sync, *_ = build(FakeVelocity(fail=True), score=0.75)
    d = sync([payment()])[0]
    assert d.degraded and d.threshold_set == "v1-degraded" and d.decision == "BLOCK"       # 0.75 >= 0.7


def test_a_budget_too_small_for_redis_skips_the_lookups_and_still_answers():
    sync, vel, store, _, _ = build()
    d = sync([payment()], budget_s=0.001)[0]
    assert d.degraded and vel.calls == 0 and store.calls == 0


def test_a_comfortable_budget_is_not_degraded():
    sync, *_ = build()
    assert not sync([payment()], budget_s=0.05)[0].degraded
