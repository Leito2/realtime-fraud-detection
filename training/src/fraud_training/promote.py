"""Promote the challenger: move the ``champion`` alias in MLflow (PLAN.md section 4.10). ``make promote``.

The scorer polls the alias and swaps the model in memory (section 3.6): no restart, no lost events. The
criterion is written down in ``config/promotion.yaml`` *before* looking at results, and checked against the
auditor's confusion counts unless ``--force`` is given.
"""
import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import mlflow
import yaml


class PromotionRefused(Exception):
    pass


@dataclass(frozen=True)
class Criteria:
    min_labelled: int = 2000            # labelled events each model must have been compared on
    min_recall_gain: float = 0.02       # absolute recall points the challenger must add
    max_fpr_increase: float = 0.005     # and at most this much more false-alarm rate


@dataclass(frozen=True)
class Verdict:
    ok: bool
    reason: str
    recall_gain: float = 0.0


@dataclass(frozen=True)
class Promotion:
    version: str
    previous: str | None


def _rates(rows, model: str) -> tuple[int, float, float]:
    """(labelled events, recall, false-alarm rate) of one model from (model, flagged, is_fraud, n) rows."""
    counts = {(flagged, is_fraud): 0 for flagged in (True, False) for is_fraud in (True, False)}
    for name, flagged, is_fraud, n in rows:
        if name == model:
            counts[(bool(flagged), bool(is_fraud))] += n
    tp, fn = counts[(True, True)], counts[(False, True)]
    fp, tn = counts[(True, False)], counts[(False, False)]
    recall = tp / (tp + fn) if tp + fn else 0.0
    fpr = fp / (fp + tn) if fp + tn else 0.0
    return tp + fn + fp + tn, recall, fpr


def evaluate_criteria(confusion_rows, c: Criteria) -> Verdict:
    """``confusion_rows``: (model, flagged, is_fraud, n) as in the ``v_confusion_by_model`` view."""
    champ, chall = _rates(confusion_rows, "champion"), _rates(confusion_rows, "challenger")
    if chall[0] == 0 or champ[0] == 0:
        return Verdict(False, "no labelled events yet for the challenger or the champion")
    gain = chall[1] - champ[1]
    if min(champ[0], chall[0]) < c.min_labelled:
        return Verdict(False, f"only {min(champ[0], chall[0])} labelled events, need {c.min_labelled}", gain)
    if gain < c.min_recall_gain:
        return Verdict(False, f"recall gain {gain:+.3f} is under {c.min_recall_gain}", gain)
    if chall[2] - champ[2] > c.max_fpr_increase:
        return Verdict(False, f"FPR rises by {chall[2] - champ[2]:.3f}, limit {c.max_fpr_increase}", gain)
    return Verdict(True, "criteria met", gain)


def promote(tracking_uri: str, model_name: str, source: str = "challenger",
            target: str = "champion") -> Promotion:
    client = mlflow.MlflowClient(tracking_uri=tracking_uri)
    try:
        version = str(client.get_model_version_by_alias(model_name, source).version)
    except mlflow.exceptions.MlflowException as exc:
        raise PromotionRefused(f"no model has the alias {source!r}") from exc
    try:
        previous = str(client.get_model_version_by_alias(model_name, target).version)
    except mlflow.exceptions.MlflowException:
        previous = None
    client.set_model_version_tag(model_name, version, "promoted_from", source)
    client.set_registered_model_alias(model_name, target, version)
    return Promotion(version, previous)


def _confusion_from_postgres(dsn: str):
    import psycopg

    with psycopg.connect(dsn) as conn:
        return conn.execute("SELECT model, flagged, is_fraud, n FROM v_confusion_by_model").fetchall()


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    default_dsn = os.environ.get("POSTGRES_DSN", "postgresql://fraud:fraud@localhost:5432/fraud")
    ap = argparse.ArgumentParser(description="Move the champion alias to the challenger version")
    ap.add_argument("--tracking-uri", default=os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    ap.add_argument("--model", default="fraud-champion")
    ap.add_argument("--source", default="challenger", help="alias whose version becomes the champion")
    ap.add_argument("--criteria", type=Path, default=Path("config/promotion.yaml"))
    ap.add_argument("--dsn", default=default_dsn)
    ap.add_argument("--force", action="store_true", help="skip the criteria check")
    a = ap.parse_args()
    if not a.force:
        criteria = Criteria(**yaml.safe_load(a.criteria.read_text(encoding="utf-8")))
        verdict = evaluate_criteria(_confusion_from_postgres(a.dsn), criteria)
        print(f"criteria: {verdict.reason} (recall gain {verdict.recall_gain:+.3f})")
        if not verdict.ok:
            sys.exit("promotion refused; use --force to override")
    result = promote(a.tracking_uri, a.model, source=a.source)
    print(f"{a.model}: champion v{result.previous} -> v{result.version}")


if __name__ == "__main__":
    main()
