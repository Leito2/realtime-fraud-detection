import pytest
from scorer.application.score_batch import ScoreBatch
from scorer.domain.degraded import DegradedPolicy
from scorer.testing import FakeModel, FakeProfileStore, ListSink, RecordingMetrics, SteppingClock

from fraudcore.contracts import EnrichedPayment
from fraudcore.thresholds import Thresholds


def event(pid, amount=50.0, sched=1_000):
    return EnrichedPayment(
        payment_id=pid, user_id="u_" + pid, card_id="c", amount=amount, currency="USD", merchant_id="m",
        mcc="5411", country="CO", device_id="d", ip_country="CO", channel="web",
        event_time="2026-10-05T14:03:22.123Z", t_scheduled_ns=sched, t_sent_ns=sched + 100,
        f_cnt_10m=1, f_sum_10m=amount, f_max_10m=amount, f_small_tx_cnt_10m=0)


def build(scores, fail=False):
    metrics = RecordingMetrics()
    uc = ScoreBatch(
        profiles=FakeProfileStore(fail=fail), model=FakeModel(scores), sink=ListSink(),
        thresholds=Thresholds("v1", 0.3, 0.8),
        policy=DegradedPolicy(Thresholds("v1-degraded", 0.2, 0.7), 500),
        clock=SteppingClock(start=10_000, step=1_000), metrics=metrics)
    return uc, metrics


def test_one_observation_per_batch_with_stage_timings_in_seconds():
    uc, metrics = build([0.1, 0.5, 0.9])
    uc([event("a"), event("b"), event("c")])
    (stats,) = metrics.batches
    assert stats.size == 3 and stats.degraded is False
    assert stats.redis_s == pytest.approx(1e-6) and stats.inference_s == pytest.approx(1e-6)
    assert stats.publish_s == pytest.approx(1e-6)
    assert stats.model == "fake" and stats.version == "1"


def test_decision_mix_scores_and_end_to_end_latency_per_event():
    uc, metrics = build([0.1, 0.5, 0.9])
    uc([event("a", sched=1_000), event("b", sched=2_000), event("c", sched=3_000)])
    (stats,) = metrics.batches
    assert stats.decisions == {"APPROVE": 1, "REVIEW": 1, "BLOCK": 1}
    assert stats.scores == pytest.approx([0.1, 0.5, 0.9])
    assert len(stats.e2e_s) == 3 and all(x > 0 for x in stats.e2e_s)
    assert stats.e2e_s[0] > stats.e2e_s[2]          # scheduled earlier -> waited longer


def test_degraded_batches_are_flagged():
    uc, metrics = build([0.1], fail=True)
    uc([event("a")])
    assert metrics.batches[0].degraded is True


def test_no_metrics_object_is_needed():
    uc = ScoreBatch(profiles=FakeProfileStore(), model=FakeModel([0.1]), sink=ListSink(),
                    thresholds=Thresholds("v1", 0.3, 0.8),
                    policy=DegradedPolicy(Thresholds("d", 0.2, 0.7), 500), clock=SteppingClock())
    assert len(uc([event("a")])) == 1


def test_only_events_with_a_trace_context_are_traced_with_stage_spans():
    from scorer.testing import RecordingTracer

    tracer = RecordingTracer()
    uc = ScoreBatch(profiles=FakeProfileStore(), model=FakeModel([0.1, 0.2]), sink=ListSink(),
                    thresholds=Thresholds("v1", 0.3, 0.8),
                    policy=DegradedPolicy(Thresholds("d", 0.2, 0.7), 500),
                    clock=SteppingClock(start=10_000, step=1_000), tracer=tracer)
    traced, plain = event("a", sched=1_000), event("b", sched=2_000)
    traced.traceparent = "00-" + "a" * 32 + "-" + "b" * 16 + "-01"
    uc([traced, plain])
    ((tp, root, children),) = tracer.traces
    assert tp == traced.traceparent
    assert root.name == "payment.decision" and root.start_ns == 1_000
    assert [c.name for c in children] == ["kafka+flink transit", "profile lookup", "model inference",
                                          "publish decision"]
    assert children[0].start_ns == 1_000 and children[-1].end_ns == root.end_ns
    assert all(a.end_ns == b.start_ns for a, b in zip(children[1:], children[2:], strict=False))
