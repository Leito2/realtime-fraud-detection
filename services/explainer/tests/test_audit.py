from explainer.application.audit import AuditRow, audit
from explainer.application.ports import NoExplanationModel

from fraudcore.features import FEATURE_ORDER

FEATURES = {f: 0.0 for f in FEATURE_ORDER} | {"f_cnt_10m": 7.0, "amount": 950.0}


class Shap:
    def contributions(self, version, features):
        return {f: (2.0 if f == "f_cnt_10m" else 1.0 if f == "amount" else 0.0) for f in features}


class NoShap:
    def contributions(self, version, features):
        raise NoExplanationModel(version)


def row(pid, text, status="llm", features=FEATURES):
    return AuditRow(payment_id=pid, model_version="3", score=0.93, status=status, text=text,
                    features=features)


def test_faithful_llm_texts_have_zero_unsupported_claims():
    report = audit([row("p1", "Score 0.93: 7 payments in the last 10 minutes and amount 950.")], Shap())
    assert report.audited == 1 and report.unsupported == 0 and report.unsupported_claim_rate == 0.0


def test_an_invented_number_or_feature_is_counted_and_listed():
    rows = [row("p1", "Score 0.93: 7 payments in the last 10 minutes."),
            row("p2", "Score 0.93: 41 payments, also account_age_days matters.")]
    report = audit(rows, Shap())
    assert report.audited == 2 and report.unsupported == 1 and report.unsupported_claim_rate == 0.5
    assert report.examples[0][0] == "p2"
    assert any("41" in c for c in report.examples[0][1])


def test_template_and_skipped_rows_are_not_audited_and_missing_attribution_is_counted_apart():
    rows = [row("p1", "template text", status="template"), row("p2", "-", status="skipped"),
            row("p3", "anything", features=None), row("p4", "Score 0.93.")]
    report = audit(rows, Shap())
    assert report.audited == 1 and report.not_auditable == 1
    assert audit([row("p5", "Score 0.93.")], NoShap()).not_auditable == 1
