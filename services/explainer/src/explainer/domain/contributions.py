"""SHAP contributions of one decision, reduced to the few that matter."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Contribution:
    feature: str
    value: float          # the feature value that was served
    shap: float           # signed contribution to the score (log-odds); positive raises the fraud risk


def top_contributions(shap: dict[str, float], features: dict[str, float], k: int = 5) -> list[Contribution]:
    ranked = sorted(shap.items(), key=lambda kv: abs(kv[1]), reverse=True)[:k]
    return [Contribution(name, float(features[name]), float(value)) for name, value in ranked]
