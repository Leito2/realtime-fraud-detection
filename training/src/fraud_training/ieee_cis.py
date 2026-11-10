"""Offline benchmark on IEEE-CIS Fraud Detection (PLAN.md 12.2, M9, optional).

    uv run python -m fraud_training.ieee_cis [--data data/ieee-cis/train_transaction.csv]

The same training code (XGBoost / LightGBM from ``fraud_training.models`` and the same metrics) on a real
public dataset, in a table *apart* from the synthetic system: it is never served or sent through Kafka. It
shows that the method works outside the project's own generator. The split is temporal (``TransactionDT``
order), like the synthetic one, so the model never trains on the future. The baseline is a one-feature rule
(the amount), the analogue of the rules baseline of the synthetic benchmark. Only ``train_transaction.csv`` is
used (the identity table covers about a quarter of the payments and is left out on purpose). Missing values
stay NaN: both libraries handle them natively, and nothing here is served, so the scorer's no-imputation rule
does not apply.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import polars as pl

from fraud_training.metrics import fpr_threshold, pr_auc, recall_at_fpr
from fraud_training.models import fit_model, native_scores

LABEL, TIME, ID = "isFraud", "TransactionDT", "TransactionID"


def prepare(df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray, list[str]]:
    """Feature matrix (float32, NaN kept), labels, event times and column names, sorted by time."""
    df = df.sort(TIME)
    names = [c for c in df.columns if c not in (LABEL, TIME, ID)]
    cols = []
    for name in names:
        series = df[name]
        if series.dtype in (pl.String, pl.Categorical):      # category -> integer code, missing stays null
            codes = series.cast(pl.Categorical).to_physical().cast(pl.Float64)
            cols.append(codes.to_numpy())
        else:
            cols.append(series.cast(pl.Float64).to_numpy())
    X = np.column_stack(cols).astype(np.float32)
    return X, df[LABEL].to_numpy().astype(np.int8), df[TIME].to_numpy().astype(np.float64), names


def temporal_split_by_time(t: np.ndarray, valid_frac: float = 0.2, test_frac: float = 0.2):
    """Index arrays (train, valid, test) by time order; ``t`` must already be sorted."""
    n = len(t)
    a, b = int(n * (1 - valid_frac - test_frac)), int(n * (1 - test_frac))
    idx = np.arange(n)
    return idx[:a], idx[a:b], idx[b:]


def amount_baseline_scores(X: np.ndarray, names: list[str]) -> np.ndarray:
    """The rule 'the bigger the amount, the riskier': the one-feature baseline."""
    return X[:, names.index("TransactionAmt")].astype(np.float32)


def _metrics(y: np.ndarray, scores: np.ndarray) -> dict:
    return {
        "pr_auc": round(pr_auc(y, scores), 4),
        "recall_at_1pct_fpr": round(recall_at_fpr(y, scores, 0.01), 4),
        "cutoff_at_1pct_fpr": round(float(fpr_threshold(y, scores, 0.01)), 4),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="IEEE-CIS offline benchmark")
    ap.add_argument("--data", default="data/ieee-cis/train_transaction.csv")
    ap.add_argument("--out", default="docs/results/ieee-cis-benchmark")
    args = ap.parse_args()

    df = pl.read_csv(args.data, infer_schema_length=20000, null_values=["", "NA"])
    X, y, t, names = prepare(df)
    tr, va, te = temporal_split_by_time(t)
    result = {
        "rows": int(len(y)), "features": len(names), "fraud_rate": round(float(y.mean()), 4),
        "train_rows": len(tr), "valid_rows": len(va), "test_rows": len(te),
        "test_fraud_rate": round(float(y[te].mean()), 4),
        "amount_rule": _metrics(y[te], amount_baseline_scores(X[te], names)),
    }
    for kind in ("xgboost", "lightgbm"):
        model = fit_model(kind, X[tr], y[tr], X[va], y[va], seed=0)
        result[kind] = _metrics(y[te], native_scores(model, X[te]))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    lines = [
        "# IEEE-CIS offline benchmark (not part of the served system)",
        "",
        f"{result['rows']:,} transactions, {result['features']} features, "
        f"fraud rate {result['fraud_rate']:.2%}. Temporal split by `TransactionDT`: {len(tr):,} train / "
        f"{len(va):,} valid / {len(te):,} test rows (test fraud rate {result['test_fraud_rate']:.2%}). "
        "Recall is at the cutoff that flags 1% of legitimate payments.",
        "",
        "| model | PR-AUC | recall at 1% FPR |",
        "|---|---|---|",
    ]
    rows = (("amount_rule", "rule: amount only"), ("xgboost", "XGBoost"), ("lightgbm", "LightGBM"))
    for key, label in rows:
        lines.append(f"| {label} | {result[key]['pr_auc']} | {result[key]['recall_at_1pct_fpr']} |")
    out.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
