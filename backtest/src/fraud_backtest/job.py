"""Offline backtest: re-score a versioned history with several models on the same test days (M6b).

Pure core (``backtest``) plus thin helpers: ``load_registry_models`` (MLflow aliases -> ONNX scorers with the
thresholds they were registered with) and ``to_markdown``. The dataset comes from the data lake with its
digest verified, so a report always names the exact bytes it ran on (PLAN.md 13.1).
"""
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from fraud_training.costs import Costs
from fraud_training.dataset import build_dataset, temporal_split
from fraud_training.evaluate import evaluate_scores
from fraud_training.models import OnnxScorer

from fraudcore.features import FEATURE_ORDER

OVERALL_METRICS = ("pr_auc", "recall_at_fpr_1pct", "cost", "review_rate", "block_rate")


@dataclass
class BacktestModel:
    name: str
    version: str
    predict: Callable[[np.ndarray], np.ndarray]
    review: float
    block: float


def backtest(history: pl.DataFrame, models: list[BacktestModel], valid_days: int = 10, test_days: int = 10,
             costs: Costs | None = None) -> pd.DataFrame:
    """Long-form table: model, version, segment (overall or fraud scenario), metric, value."""
    costs = costs or Costs()
    ds = build_dataset(history)
    train, _, test = temporal_split(ds, valid_days, test_days)
    X = test.select(FEATURE_ORDER).to_numpy().astype(np.float32)
    unseen = set(test["user_id"].unique().to_list()) - set(train["user_id"].unique().to_list())
    rows = []
    for m in models:
        result = evaluate_scores(test, m.predict(X), m.review, m.block, costs, unseen)
        for metric in OVERALL_METRICS:
            rows.append((m.name, m.version, "overall", metric, float(result[metric])))
        for scenario, recall in result["recall_by_scenario"].items():
            rows.append((m.name, m.version, scenario, "recall", float(recall)))
    return pd.DataFrame(rows, columns=["model", "version", "segment", "metric", "value"])


def to_markdown(df: pd.DataFrame, title: str = "Backtest") -> str:
    models = list(dict.fromkeys(df["model"]))
    wide = df.pivot_table(index=["segment", "metric"], columns="model", values="value", sort=False)
    lines = [f"# {title}", "", "| segment | metric | " + " | ".join(models) + " |",
             "|---|---|" + "---|" * len(models)]
    for (segment, metric), row in wide.iterrows():
        fmt = (lambda v: f"{v:,.0f}") if metric == "cost" else (lambda v: f"{v:.3f}")
        lines.append(f"| {segment} | {metric} | " + " | ".join(fmt(row[m]) for m in models) + " |")
    return "\n".join(lines) + "\n"


def load_registry_models(tracking_uri: str, model_name: str,
                         aliases: tuple[str, ...] = ("champion", "challenger"),
                         cache: Path | None = None) -> list[BacktestModel]:
    import mlflow

    client = mlflow.MlflowClient(tracking_uri=tracking_uri)
    out = []
    for alias in aliases:
        try:
            mv = client.get_model_version_by_alias(model_name, alias)
        except mlflow.exceptions.MlflowException:
            continue                                  # no challenger yet: back-test what exists
        local = Path(mlflow.artifacts.download_artifacts(artifact_uri=mv.source, tracking_uri=tracking_uri,
                                                         dst_path=str(cache) if cache else None))
        th = json.loads((local / "thresholds.json").read_text(encoding="utf-8"))
        scorer = OnnxScorer((local / "model.onnx").read_bytes())
        out.append(BacktestModel(alias, str(mv.version), scorer.predict, th["review"], th["block"]))
    return out
