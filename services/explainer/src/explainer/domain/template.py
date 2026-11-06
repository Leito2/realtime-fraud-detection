"""Deterministic explanation from SHAP alone: the fallback when the LLM is down, slow or unfaithful."""
from explainer.domain.contributions import Contribution
from explainer.domain.labels import label
from explainer.domain.numbers import fmt

TEMPLATE_TOP = 3


def render_template(decision: str, score: float, contributions: list[Contribution]) -> str:
    reasons = [f"{label(c.feature)} ({c.feature}) is {fmt(c.value)}, which "
               f"{'raises' if c.shap > 0 else 'lowers'} the risk" for c in contributions[:TEMPLATE_TOP]]
    return f"Decision {decision} (score {fmt(score)}). Main factors: " + "; ".join(reasons) + "."
