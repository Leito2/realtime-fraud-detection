"""Use case: offline faithfulness audit of stored LLM explanations (PLAN.md 13).

Recomputes the SHAP facts each explanation was allowed to use and counts the texts that state something not
in them. The online gate already blocks these; the audit measures the gate's misses and the prompt's quality.
"""
from dataclasses import dataclass, field

from explainer.application.explain import TOP_K
from explainer.application.ports import NoExplanationModel, ShapEngine
from explainer.domain.contributions import top_contributions
from explainer.domain.faithfulness import unsupported_claims


@dataclass(frozen=True)
class AuditRow:
    payment_id: str
    model_version: str
    score: float
    status: str
    text: str
    features: dict[str, float] | None


@dataclass
class AuditReport:
    audited: int = 0
    unsupported: int = 0
    not_auditable: int = 0
    examples: list[tuple[str, list[str]]] = field(default_factory=list)

    @property
    def unsupported_claim_rate(self) -> float:
        return self.unsupported / self.audited if self.audited else 0.0


def audit(rows: list[AuditRow], shap: ShapEngine, max_examples: int = 10) -> AuditReport:
    report = AuditReport()
    for r in rows:
        if r.status != "llm":
            continue
        if r.features is None:
            report.not_auditable += 1
            continue
        try:
            contributions = shap.contributions(r.model_version, r.features)
        except NoExplanationModel:
            report.not_auditable += 1
            continue
        claims = unsupported_claims(r.text, top_contributions(contributions, r.features, TOP_K), r.score)
        report.audited += 1
        if claims:
            report.unsupported += 1
            if len(report.examples) < max_examples:
                report.examples.append((r.payment_id, claims))
    return report
