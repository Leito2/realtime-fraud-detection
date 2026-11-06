"""Train, compare, export, verify and register the champion (PLAN.md section 4). ``make train``.

Rules vs XGBoost vs LightGBM, each native and (for the trees) through ONNX Runtime, on the same temporal
split. The champion is registered in MLflow only after its ONNX twin matches the native model to < 1e-5.
"""
import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
import yaml

from fraud_training.costs import Costs, choose_thresholds
from fraud_training.dataset import build_dataset, temporal_split
from fraud_training.evaluate import evaluate_scores, pick_champion
from fraud_training.models import (
    KINDS,
    OnnxScorer,
    benchmark,
    check_parity,
    fit_model,
    native_scores,
    parity_report,
    to_onnx,
)
from fraud_training.registry import register_model
from fraud_training.tune import tune
from fraudcore.drift import reference_from_scores
from fraudcore.features import FEATURE_ORDER
from fraudcore.rules import rules_score

MODEL_NAME = "fraud-champion"


@dataclass
class Settings:
    history: Path = Path("data/history.parquet")
    tracking_uri: str = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
    artifact_root: str | None = None
    experiment: str = "fraud-scoring"
    alias: str = "champion"
    trials: int = 20
    timeout_s: float = 240.0
    valid_days: int = 10
    test_days: int = 10
    max_review_rate: float = 0.02
    costs: Costs = Costs()
    results_out: Path = Path("docs/results/m3-offline-eval.json")
    thresholds_out: Path = Path("config/thresholds.yaml")
    register: bool = True
    seed: int = 0


def file_digest(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _arrays(df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    return df.select(FEATURE_ORDER).to_numpy().astype(np.float32), df["label"].to_numpy()


def _manifest(path: Path, ds: pl.DataFrame) -> dict:
    return {
        "file": path.name, "digest": file_digest(path), "rows": ds.height, "days": int(ds["day"].max()) + 1,
        "fraud_rate_true": float(ds["fraud_true"].mean()), "fraud_rate_label": float(ds["label"].mean()),
        "scenarios": {k: int(v) for k, v in ds["scenario"].value_counts().iter_rows()},
        "features": FEATURE_ORDER,
    }


def run(s: Settings) -> dict:
    history = pl.read_parquet(s.history)
    ds = build_dataset(history)
    train, valid, test = temporal_split(ds, s.valid_days, s.test_days)
    Xtr, ytr = _arrays(train)
    Xva, yva = _arrays(valid)
    Xte, _ = _arrays(test)
    unseen = set(test["user_id"].unique().to_list()) - set(train["user_id"].unique().to_list())

    def score_rules(df: pl.DataFrame) -> np.ndarray:
        return np.array([rules_score(r) for r in df.select(FEATURE_ORDER).iter_rows(named=True)])

    def table_row(valid_scores, test_scores) -> dict:
        t = choose_thresholds(valid_scores, yva, valid["amount"].to_numpy(), s.costs, s.max_review_rate)
        row = evaluate_scores(test, test_scores, t.review, t.block, s.costs, unseen)
        return {**row, "review_threshold": t.review, "block_threshold": t.block}

    models: dict[str, dict] = {"rules": table_row(score_rules(valid), score_rules(test))}
    fitted, onnx_blobs, test_scores, valid_scores, params, bench, parity = {}, {}, {}, {}, {}, {}, {}
    for kind in KINDS:
        params[kind] = tune(kind, Xtr, ytr, Xva, yva, s.trials, s.timeout_s, s.seed)
        fitted[kind] = fit_model(kind, Xtr, ytr, Xva, yva, params=params[kind], seed=s.seed)
        test_scores[kind] = native_scores(fitted[kind], Xte)
        valid_scores[kind] = native_scores(fitted[kind], Xva)
        onnx_blobs[kind] = to_onnx(fitted[kind], kind, len(FEATURE_ORDER))
        scorer = OnnxScorer(onnx_blobs[kind])
        parity[kind] = parity_report(test_scores[kind], scorer.predict(Xte))
        models[kind] = table_row(valid_scores[kind], test_scores[kind])
        models[f"{kind}_onnx"] = table_row(scorer.predict(Xva), scorer.predict(Xte))
        bench[kind] = {"native": benchmark(lambda X, m=fitted[kind]: native_scores(m, X), Xte),
                       "onnx": benchmark(scorer.predict, Xte)}

    eligible = tuple(k for k in KINDS if parity[k]["ok"])
    champion = pick_champion(models["xgboost"], models["lightgbm"], eligible)
    max_diff = check_parity(test_scores[champion], OnnxScorer(onnx_blobs[champion]).predict(Xte))   # the gate
    chosen = models[champion]
    manifest = _manifest(s.history, ds)
    results = {"champion": champion, "models": models, "onnx_max_abs_diff": max_diff, "onnx_parity": parity,
               "benchmark": bench,
               "dataset": manifest, "params": params, "costs": vars(s.costs),
               "split_days": {"valid": s.valid_days, "test": s.test_days}}

    if s.register:
        thresholds = {"review": chosen["review_threshold"], "block": chosen["block_threshold"]}
        version = register_model(
            tracking_uri=s.tracking_uri, artifact_root=s.artifact_root, experiment=s.experiment,
            model_name=MODEL_NAME, alias=s.alias, onnx_bytes=onnx_blobs[champion], features=FEATURE_ORDER,
            params={"kind": champion, **params[champion]},
            metrics={**{k: v for k, v in chosen.items() if k != "recall_by_scenario"},
                     "recall_by_scenario": chosen["recall_by_scenario"], "onnx_max_abs_diff": max_diff},
            artifacts={"thresholds.json": thresholds, "dataset_manifest.json": manifest,
                       "score_reference.json": reference_from_scores(valid_scores[champion]),
                       "eval.json": results},
            tags={"dataset_digest": manifest["digest"], "model_kind": champion},
            run_name=f"{champion}-champion",
            files=_native_files(champion, fitted[champion]))
        results["registry"] = {"model": MODEL_NAME, "alias": s.alias, "version": version}
        _write_thresholds(s.thresholds_out, f"{MODEL_NAME}-v{version}", thresholds)

    s.results_out.parent.mkdir(parents=True, exist_ok=True)
    s.results_out.write_text(json.dumps(results, indent=2, default=float), encoding="utf-8")
    return results


def _native_files(kind: str, model) -> dict[str, bytes]:
    """The native booster travels with the ONNX twin: SHAP needs it and the ONNX graph cannot provide it."""
    if kind != "xgboost":
        return {}
    return {"booster.json": bytes(model.get_booster().save_raw(raw_format="json"))}


def _write_thresholds(path: Path, name: str, th: dict) -> None:
    previous = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    degraded = previous.get("degraded", {"review": 0.20, "block": 0.70, "max_auto_approve_amount": 500})
    body = {"threshold_set": name, "review": round(th["review"], 6), "block": round(th["block"], 6),
            "degraded": degraded}
    header = "# Written by `make train`: chosen by expected cost on validation data (PLAN.md section 4.6).\n"
    path.write_text(header + yaml.safe_dump(body, sort_keys=False), encoding="utf-8")


def table(results: dict) -> str:
    rows = ["| model | PR-AUC | recall @ 1% FPR | PR-AUC unseen users | cost | review rate |",
            "|---|---|---|---|---|---|"]
    for name, m in results["models"].items():
        u = m["pr_auc_unseen_users"]
        unseen = "-" if u is None else f"{u:.3f}"
        rows.append(f"| {name} | {m['pr_auc']:.3f} | {m['recall_at_fpr_1pct']:.3f} | {unseen} | "
                    f"{m['cost']:,.0f} | {m['review_rate']:.2%} |")
    return "\n".join(rows)


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")          # MLflow prints an emoji; Windows consoles are cp1252
    ap = argparse.ArgumentParser(description="Train and register the fraud champion")
    ap.add_argument("--history", type=Path, default=Settings.history)
    ap.add_argument("--tracking-uri", default=Settings.tracking_uri)
    ap.add_argument("--trials", type=int, default=Settings.trials, help="Optuna trials per model")
    ap.add_argument("--timeout", type=float, default=Settings.timeout_s, help="seconds of Optuna per model")
    ap.add_argument("--no-register", action="store_true")
    a = ap.parse_args()
    settings = Settings(history=a.history, tracking_uri=a.tracking_uri, trials=a.trials,
                        timeout_s=a.timeout, register=not a.no_register)
    results = run(settings)
    print(table(results))
    print(f"\nchampion: {results['champion']}   onnx max |diff|: {results['onnx_max_abs_diff']:.2e}")
    print(json.dumps(results.get("registry", {})))


if __name__ == "__main__":
    main()
