from explainer.application.explain import ExplainDecision
from explainer.application.ports import LlmError, NoExplanationModel
from explainer.domain.policy import BackpressurePolicy, CircuitBreaker

from fraudcore.contracts import DecisionEvent

FEATURES = {"amount": 950.0, "f_cnt_10m": 7.0, "amount_to_avg_ratio": 9.5, "is_foreign": 0.0}
SHAP = {"amount": 0.4, "f_cnt_10m": 1.3, "amount_to_avg_ratio": 2.1, "is_foreign": -0.05}
NOW = 1_000.0


def decision(kind="BLOCK", features=FEATURES, age_s=1.0) -> DecisionEvent:
    return DecisionEvent(payment_id="p1", user_id="u1", decision=kind, score=0.93, model="fraud-champion",
                         model_version="3", threshold_set="t", t_scheduled_ns=1,
                         t_decided_ns=int((NOW - age_s) * 1e9), features=features)


class FakeShap:
    def __init__(self, missing=False):
        self.calls, self.missing = [], missing

    def contributions(self, model_version, features):
        self.calls.append(model_version)
        if self.missing:
            raise NoExplanationModel(model_version)
        return SHAP


class FakeLlm:
    def __init__(self, reply="amount_to_avg_ratio is 9.5 and f_cnt_10m is 7, score 0.93.", fail=False):
        self.reply, self.fail, self.prompts = reply, fail, []

    def complete(self, prompt):
        self.prompts.append(prompt)
        if self.fail:
            raise LlmError("gateway down")
        return self.reply


class Recorder:
    def __init__(self):
        self.events = []

    def observe(self, status, seconds, llm_seconds=None, rejected=False):
        self.events.append((status, rejected, llm_seconds is not None))


def build(llm=None, shap=None, max_inflight=2):
    metrics = Recorder()
    uc = ExplainDecision(shap=shap or FakeShap(), llm=llm or FakeLlm(),
                         policy=BackpressurePolicy(max_inflight, 30.0),
                         breaker=CircuitBreaker(3, 10.0, lambda: NOW), clock=lambda: NOW, metrics=metrics)
    return uc, metrics


def test_approved_payments_need_no_explanation():
    uc, _ = build()
    assert uc(decision("APPROVE"), inflight=0) is None


def test_a_flagged_decision_gets_an_llm_explanation_built_from_shap_facts():
    llm = FakeLlm()
    uc, metrics = build(llm)
    e = uc(decision(), inflight=0)
    assert e.status == "llm" and e.payment_id == "p1" and "9.5" in e.text
    assert e.top_features[0] == ("amount_to_avg_ratio", 2.1)
    assert "amount_to_avg_ratio" in llm.prompts[0]
    assert metrics.events == [("llm", False, True)]


def test_shap_uses_the_model_version_that_made_the_decision():
    shap = FakeShap()
    uc, _ = build(shap=shap)
    uc(decision(), inflight=0)
    assert shap.calls == ["3"]


def test_when_the_llm_fails_the_deterministic_template_answers():
    uc, _ = build(FakeLlm(fail=True))
    e = uc(decision(), inflight=0)
    assert e.status == "template" and "amount_to_avg_ratio" in e.text and e.top_features


def test_after_repeated_failures_the_breaker_stops_calling_the_llm():
    llm = FakeLlm(fail=True)
    uc, _ = build(llm)
    for _ in range(5):
        uc(decision(), inflight=0)
    assert len(llm.prompts) == 3                                   # then the circuit opened


def test_an_unfaithful_llm_text_is_replaced_by_the_template_and_counted():
    uc, metrics = build(FakeLlm(reply="It was paid from 12 countries (is_foreign is 1)."))
    e = uc(decision(), inflight=0)
    assert e.status == "template"
    assert metrics.events[-1] == ("template", True, True)


def test_when_the_llm_is_busy_the_case_is_skipped_not_queued():
    llm = FakeLlm()
    uc, _ = build(llm, max_inflight=2)
    e = uc(decision(), inflight=2)
    assert e.status == "skipped" and not llm.prompts


def test_a_stale_case_is_skipped():
    uc, _ = build()
    assert uc(decision(age_s=120.0), inflight=0).status == "skipped"


def test_a_decision_without_features_cannot_be_explained():
    uc, _ = build()
    e = uc(decision(features=None), inflight=0)
    assert e.status == "skipped" and "features" in e.text


def test_models_without_shap_still_get_an_honest_template():
    uc, _ = build(shap=FakeShap(missing=True))
    e = uc(decision(), inflight=0)
    assert e.status == "template" and "no feature attribution" in e.text.lower() and e.top_features is None
