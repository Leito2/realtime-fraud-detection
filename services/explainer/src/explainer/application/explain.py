"""Use case: a flagged decision -> an explanation. XGBoost decides, SHAP says why, the LLM only words it.

The LLM never sees more than the SHAP top-k, its text is checked against those facts, and every failure mode
(busy, stale, down, circuit open, unfaithful, no SHAP for this model) ends in a defined status instead of lag.
"""
import time
from collections.abc import Callable
from dataclasses import dataclass

from explainer.application.ports import LlmClient, LlmError, Metrics, NoExplanationModel, ShapEngine
from explainer.domain.contributions import top_contributions
from explainer.domain.faithfulness import unsupported_claims
from explainer.domain.policy import BackpressurePolicy, CircuitBreaker
from explainer.domain.prompt import build_prompt
from explainer.domain.template import render_template
from fraudcore.contracts import DecisionEvent, ExplanationEvent

TOP_K = 5


@dataclass
class ExplainDecision:
    shap: ShapEngine
    llm: LlmClient
    policy: BackpressurePolicy
    breaker: CircuitBreaker
    metrics: Metrics
    clock: Callable[[], float] = time.time

    def __call__(self, d: DecisionEvent, inflight: int = 0) -> ExplanationEvent | None:
        if d.decision == "APPROVE":
            return None
        started = self.clock()
        event, llm_s, rejected = self._explain(d, inflight, started)
        self.metrics.observe(event.status, self.clock() - started, llm_s, rejected)
        return event

    def _explain(self, d: DecisionEvent, inflight: int, started: float):
        age = started - d.t_decided_ns / 1e9
        if d.features is None:
            return self._skipped(d, "no features were logged for this decision"), None, False
        if self.policy.should_skip(age, inflight):
            return self._skipped(d, "explanation shed under load"), None, False
        try:
            shap = self.shap.contributions(d.model_version, d.features)
        except NoExplanationModel:
            text = (f"Decision {d.decision} (score {d.score:.2f}). No feature attribution is available "
                    f"for model {d.model} v{d.model_version}.")
            return ExplanationEvent(payment_id=d.payment_id, status="template", text=text), None, False
        top = top_contributions(shap, d.features, TOP_K)
        pairs = [(c.feature, c.shap) for c in top]
        template = render_template(d.decision, d.score, top)
        fallback = ExplanationEvent(payment_id=d.payment_id, status="template", text=template,
                                    top_features=pairs)
        if not self.breaker.allow():
            return fallback, None, False
        t0 = self.clock()
        try:
            text = self.llm.complete(build_prompt(d.decision, d.score, top))
        except LlmError:
            self.breaker.record(False)
            return fallback, self.clock() - t0, False
        self.breaker.record(True)
        llm_s = self.clock() - t0
        if unsupported_claims(text, top, d.score):
            return fallback, llm_s, True                      # it invented something: say it ourselves
        event = ExplanationEvent(payment_id=d.payment_id, status="llm", text=text, top_features=pairs)
        return event, llm_s, False

    @staticmethod
    def _skipped(d: DecisionEvent, why: str) -> ExplanationEvent:
        return ExplanationEvent(payment_id=d.payment_id, status="skipped", text=why)
