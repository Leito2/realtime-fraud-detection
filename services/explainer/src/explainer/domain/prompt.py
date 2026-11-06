"""The prompt: the model gets the facts and the instruction to use nothing else."""
from explainer.domain.contributions import Contribution
from explainer.domain.labels import label
from explainer.domain.numbers import fmt


def build_prompt(decision: str, score: float, contributions: list[Contribution]) -> str:
    facts = "\n".join(f"- {c.feature} ({label(c.feature)}) = {fmt(c.value)}; effect on risk: "
                      f"{'raises' if c.shap > 0 else 'lowers'}" for c in contributions)
    return ("You explain a card-payment fraud decision to a fraud analyst in two short sentences.\n"
            f"Decision: {decision}. Fraud score: {fmt(score)} (0 to 1).\n"
            f"Facts, strongest first:\n{facts}\n"
            "Use only these facts and these exact feature names and numbers. Do not invent other numbers, "
            "features, merchants or countries. Do not give advice.")
