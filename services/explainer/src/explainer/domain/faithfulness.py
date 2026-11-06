"""Gate against unsupported claims: every number and feature name in a text must come from the facts.

Deliberately simple and conservative (PLAN.md 13.1: the faithfulness audit). It cannot judge meaning, only
that nothing was invented: numbers must match a fact within a tolerance, and a feature the prompt did not
list must not be named.
"""
import re

from explainer.domain.contributions import Contribution
from explainer.domain.labels import label
from fraudcore.features import FEATURE_ORDER

NUMBER = re.compile(r"(?<![\w.])\d+(?:\.\d+)?(?![\w])")
REL_TOL = 0.10
ABS_TOL = 0.011


def _close(n: float, a: float) -> bool:
    return abs(n - a) <= max(ABS_TOL, REL_TOL * abs(a))


def unsupported_claims(text: str, contributions: list[Contribution], score: float) -> list[str]:
    allowed = [c.value for c in contributions] + [score]
    for c in contributions:                       # "last 10 minutes" in a feature's wording is not a claim
        allowed += [float(n) for n in NUMBER.findall(label(c.feature))]
    claims = [f"number {m.group()}" for m in NUMBER.finditer(text)
              if not any(_close(float(m.group()), a) for a in allowed)]
    listed = {c.feature for c in contributions}
    claims += [f"feature {f}" for f in FEATURE_ORDER if f not in listed and re.search(rf"\b{f}\b", text)]
    return claims
