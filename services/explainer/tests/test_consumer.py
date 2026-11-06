import threading
import time

from explainer.application.explain import ExplainDecision
from explainer.domain.policy import BackpressurePolicy, CircuitBreaker
from explainer.entrypoints.kafka_consumer import DecisionConsumer

from fraudcore.contracts import DecisionEvent, ExplanationEvent
from fraudcore.features import FEATURE_ORDER


def decision(pid: str, verdict: str = "BLOCK", features: bool = True) -> bytes:
    return DecisionEvent(
        payment_id=pid, user_id="u1", decision=verdict, score=0.93, model="xgboost", model_version="3",
        threshold_set="v1", t_scheduled_ns=time.time_ns(), t_decided_ns=time.time_ns(),
        features={f: 1.0 for f in FEATURE_ORDER} if features else None,
    ).model_dump_json().encode()


class Shap:
    def contributions(self, version, features):
        return {f: (1.0 if f == "f_cnt_10m" else 0.0) for f in features}


class SlowLlm:
    def __init__(self):
        self.release = threading.Event()
        self.calls = 0

    def complete(self, prompt):
        self.calls += 1
        self.release.wait(2)
        return "BLOCK."


class Metrics:
    def observe(self, *a, **k): ...


class Sink:
    def __init__(self):
        self.events: list[ExplanationEvent] = []

    def publish(self, event):
        self.events.append(event)


def build(llm, max_inflight=1):
    explain = ExplainDecision(Shap(), llm, BackpressurePolicy(max_inflight, 60.0),
                              CircuitBreaker(3, 10.0, time.monotonic), Metrics())
    sink = Sink()
    return DecisionConsumer(None, explain, sink, max_inflight), sink


def wait_for(cond, timeout=3.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end and not cond():
        time.sleep(0.01)
    return cond()


def test_approvals_and_garbage_produce_nothing():
    consumer, sink = build(SlowLlm())
    consumer.handle(decision("p1", "APPROVE"))
    consumer.handle(b"not json")
    assert sink.events == []


def test_cases_beyond_max_inflight_are_shed_not_queued():
    llm = SlowLlm()
    consumer, sink = build(llm, max_inflight=1)
    consumer.handle(decision("p1"))                    # takes the only slot, blocks in the LLM
    assert wait_for(lambda: llm.calls == 1)
    consumer.handle(decision("p2"))                    # shed immediately
    assert [(e.payment_id, e.status) for e in sink.events] == [("p2", "skipped")]
    llm.release.set()
    assert wait_for(lambda: len(sink.events) == 2)
    assert {e.payment_id: e.status for e in sink.events}["p1"] in {"llm", "template"}
    assert llm.calls == 1


def test_a_decision_without_logged_features_is_reported_as_skipped():
    consumer, sink = build(SlowLlm())
    consumer.handle(decision("p3", features=False))
    assert wait_for(lambda: len(sink.events) == 1)
    assert sink.events[0].status == "skipped"
