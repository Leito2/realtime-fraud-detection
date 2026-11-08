"""E7 (PLAN.md 3.3, 5.6, H3): is a longer velocity window worth the price of leaving Flink SQL?

    uv run python -m fraud_training.e7_long_windows [--data data/dataset_cache.parquet]

Flink SQL cannot compute a 10-minute and a 1-hour OVER window in one query (spike S1), so v1 ships only the
10-minute features and ``slow_fraud`` (a few payments spread over hours) is almost invisible to them. Option B
(a keyed operator that keeps a day of history per user) would give every window at once. Before paying for
that, this measures the benefit: the same XGBoost on the same temporal split, with and without 1-hour and
24-hour velocity features, reporting PR-AUC, recall at 1% false positives and recall per fraud scenario.
The windows include the current payment and the boundary, like the Flink job and ``velocity_reference``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import polars as pl

from fraud_training.dataset import temporal_split
from fraud_training.metrics import fpr_threshold, pr_auc, recall_at_fpr, recall_by_scenario
from fraud_training.models import fit_model, native_scores
from fraudcore.features import FEATURE_ORDER

EXTRA = ["f_cnt_1h", "f_sum_1h", "f_cnt_24h"]


def add_long_windows(df: pl.DataFrame) -> pl.DataFrame:
    """Per-user rolling count/sum over 1 h and 24 h, current payment and exact boundary included."""
    ordered = df.sort("event_time").with_columns(pl.lit(1.0).alias("_one"))

    def roll(col: str, window: str) -> pl.Expr:
        return pl.col(col).rolling_sum_by("event_time", window_size=window, closed="both").over("user_id")

    return ordered.with_columns(
        roll("amount", "1h").alias("f_sum_1h"),
        roll("_one", "1h").cast(pl.Int64).alias("f_cnt_1h"),
        roll("_one", "24h").cast(pl.Int64).alias("f_cnt_24h"),
    ).drop("_one")


def _evaluate(train: pl.DataFrame, valid: pl.DataFrame, test: pl.DataFrame, features: list[str]) -> dict:
    def arrays(d: pl.DataFrame):
        return d.select(features).to_numpy().astype(np.float32), d["label"].to_numpy()

    Xtr, ytr = arrays(train)
    Xva, yva = arrays(valid)
    Xte, yte = arrays(test)
    model = fit_model("xgboost", Xtr, ytr, Xva, yva, seed=0)
    scores = native_scores(model, Xte)
    cutoff = fpr_threshold(yte, scores, 0.01)
    return {
        "features": len(features),
        "pr_auc": round(pr_auc(yte, scores), 4),
        "recall_at_1pct_fpr": round(recall_at_fpr(yte, scores, 0.01), 4),
        "recall_by_scenario": {
            k: round(v, 4)
            for k, v in recall_by_scenario(
                test["scenario"].to_numpy(), test["fraud_true"].to_numpy(), scores, cutoff
            ).items()
        },
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="E7: value of 1 h / 24 h velocity windows")
    ap.add_argument("--data", default="data/dataset_cache.parquet")
    ap.add_argument("--valid-days", type=int, default=10)
    ap.add_argument("--test-days", type=int, default=10)
    ap.add_argument("--out", default="docs/results/e7-long-windows")
    args = ap.parse_args()

    ds = add_long_windows(pl.read_parquet(args.data))
    train, valid, test = temporal_split(ds, args.valid_days, args.test_days)
    base = _evaluate(train, valid, test, FEATURE_ORDER)
    long = _evaluate(train, valid, test, [*FEATURE_ORDER, *EXTRA])
    result = {
        "train_rows": train.height,
        "test_rows": test.height,
        "ten_minute_only": base,
        "with_1h_and_24h": long,
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    scenarios = sorted(set(base["recall_by_scenario"]) | set(long["recall_by_scenario"]))
    lines = [
        "# E7 - value of 1 h and 24 h velocity windows",
        "",
        f"XGBoost, same temporal split ({train.height:,} train / {test.height:,} test rows), "
        "same parameters. Recall is at the cutoff that flags 1% of legitimate payments.",
        "",
        "| | 10-minute features only | + 1 h and 24 h |",
        "|---|---|---|",
        f"| PR-AUC | {base['pr_auc']} | {long['pr_auc']} |",
        f"| recall at 1% FPR | {base['recall_at_1pct_fpr']} | {long['recall_at_1pct_fpr']} |",
    ]
    lines += [
        f"| recall, {s} | {base['recall_by_scenario'].get(s, 'n/a')} | "
        f"{long['recall_by_scenario'].get(s, 'n/a')} |"
        for s in scenarios
    ]
    out.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
