"""Retrain from the audit store's feedback dataset; register the result as the challenger (PLAN.md 4.11).

``v_feedback_dataset`` joins what the scorer *served* (logged feature vectors, so no skew) with the
labels that arrived later. The new model is trained on the original history plus that feedback, verified
through ONNX like any other, and registered under the ``challenger`` alias: the shadow scorer confirms it on
live traffic before ``make promote`` moves the champion alias.
"""
import argparse
import json
import os
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import polars as pl

from fraud_training.costs import choose_thresholds
from fraud_training.dataset import build_dataset, temporal_split
from fraud_training.metrics import pr_auc, recall_at_fpr
from fraud_training.models import OnnxScorer, check_parity, fit_model, native_scores, to_onnx
from fraud_training.registry import register_model
from fraud_training.train import MODEL_NAME, Settings, _arrays, _native_files, file_digest
from fraudcore.drift import reference_from_scores
from fraudcore.features import FEATURE_ORDER, vectorize


class NotEnoughFeedback(Exception):
    pass


def feedback_arrays(rows) -> tuple[np.ndarray, np.ndarray]:
    """(features dict, is_fraud, decided_at) rows -> float32 matrix in FEATURE_ORDER and labels, by time."""
    ordered = sorted(rows, key=lambda r: r[2])
    X = np.array([vectorize(r[0]) for r in ordered], dtype=np.float32).reshape(-1, len(FEATURE_ORDER))
    return X, np.array([int(r[1]) for r in ordered])


def fetch_feedback(dsn: str) -> list[tuple[dict, bool, object]]:
    import psycopg

    with psycopg.connect(dsn) as conn:
        return conn.execute("SELECT features, is_fraud, decided_at FROM v_feedback_dataset").fetchall()


def retrain(s: Settings, rows, *, min_fraud: int = 30, use_history: bool = True, kind: str = "xgboost",
            alias: str = "challenger", valid_share: float = 0.25) -> dict:
    X, y = feedback_arrays(rows)
    if int(y.sum()) < min_fraud:
        raise NotEnoughFeedback(f"{int(y.sum())} labelled frauds in the feedback, need at least {min_fraud}")
    cut = int(len(y) * (1 - valid_share))
    Xtr, ytr, Xva, yva = X[:cut], y[:cut], X[cut:], y[cut:]
    digest = None
    if use_history:
        ds = build_dataset(pl.read_parquet(s.history))
        train, valid, _ = temporal_split(ds, s.valid_days, s.test_days)
        (Hx, Hy), (Hvx, Hvy) = _arrays(train), _arrays(valid)
        Xtr, ytr = np.concatenate([Hx, Xtr]), np.concatenate([Hy, ytr])
        Xva, yva = np.concatenate([Hvx, Xva]), np.concatenate([Hvy, yva])
        digest = file_digest(s.history)

    model = fit_model(kind, Xtr, ytr, Xva, yva, seed=s.seed)
    native = native_scores(model, Xva)
    blob = to_onnx(model, kind, len(FEATURE_ORDER))
    onnx = OnnxScorer(blob).predict(Xva)
    max_diff = check_parity(native, onnx)                                  # the registry gate
    t = choose_thresholds(onnx, yva, Xva[:, FEATURE_ORDER.index("amount")], s.costs, s.max_review_rate)
    thresholds = {"review": t.review, "block": t.block}
    metrics = {"pr_auc": pr_auc(yva, onnx), "recall_at_fpr_1pct": recall_at_fpr(yva, onnx, 0.01),
               "onnx_max_abs_diff": max_diff, "train_rows": int(len(ytr)), "valid_rows": int(len(yva))}
    source = "feedback+history" if use_history else "feedback"
    result = {"model": kind, "source": source, "metrics": metrics, "onnx_max_abs_diff": max_diff,
              "thresholds": thresholds}
    if s.register:
        version = register_model(
            tracking_uri=s.tracking_uri, artifact_root=s.artifact_root, experiment=s.experiment,
            model_name=MODEL_NAME, alias=alias, onnx_bytes=blob, features=FEATURE_ORDER,
            params={"kind": kind, "source": source}, metrics=metrics,
            artifacts={"thresholds.json": thresholds, "score_reference.json": reference_from_scores(onnx)},
            tags={"source": source, "feedback_rows": len(y), "feedback_fraud": int(y.sum()),
                  "model_kind": kind, **({"dataset_digest": digest} if digest else {})},
            run_name=f"{kind}-retrain", files=_native_files(kind, model))
        result["registry"] = {"model": MODEL_NAME, "alias": alias, "version": version}
    return result


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Retrain on history + the audit store's feedback dataset")
    ap.add_argument("--history", type=Path, default=Path("data/history_full.parquet"))
    ap.add_argument("--no-history", action="store_true", help="train on the feedback alone")
    ap.add_argument("--tracking-uri", default=Settings.tracking_uri)
    default_dsn = os.environ.get("POSTGRES_DSN", "postgresql://fraud:fraud@localhost:5432/fraud")
    ap.add_argument("--dsn", default=default_dsn)
    ap.add_argument("--min-fraud", type=int, default=30)
    a = ap.parse_args()
    s = replace(Settings(), history=a.history, tracking_uri=a.tracking_uri)
    rows = fetch_feedback(a.dsn)
    print(f"{len(rows)} feedback rows", flush=True)
    result = retrain(s, rows, min_fraud=a.min_fraud, use_history=not a.no_history)
    m = result["metrics"]
    print(f"{result['model']} ({result['source']})  PR-AUC {m['pr_auc']:.3f}  recall@1%FPR "
          f"{m['recall_at_fpr_1pct']:.3f}  onnx max|diff| {m['onnx_max_abs_diff']:.2e}")
    print(json.dumps(result.get("registry", {})))


if __name__ == "__main__":
    main()
