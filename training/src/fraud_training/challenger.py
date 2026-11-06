"""PyTorch MLP challenger (PLAN.md section 4.8): same features and split as the champion, scored in shadow.

Preprocessing (log1p of the heavy-tailed non-negative columns, then standardisation) lives inside the
model graph, so the scaling travels with the weights into ONNX and the scorer feeds raw features: no skew.
The exported graph has the layout of the tree models (label, then N x 2 probabilities), so the scorer's
``OnnxModel`` loads it unchanged. ``mcc``/``country`` embeddings are not used: the feature contract is numeric
(``fraudcore.features.FEATURE_ORDER``), and the categorical signal is already in the derived features.
"""
import argparse
import copy
import io
import json
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import polars as pl
import torch
from torch import nn

from fraud_training.costs import choose_thresholds
from fraud_training.dataset import build_dataset, temporal_split
from fraud_training.evaluate import evaluate_scores
from fraud_training.metrics import pr_auc
from fraud_training.models import OnnxScorer, check_parity, parity_report
from fraud_training.registry import register_model
from fraud_training.train import MODEL_NAME, Settings, _arrays, _manifest
from fraudcore.drift import reference_from_scores
from fraudcore.features import FEATURE_ORDER

HEAVY_TAIL_SKEW = 2.0


class MLP(nn.Module):
    def __init__(self, n_features: int, hidden: tuple[int, ...] = (64, 32)) -> None:
        super().__init__()
        self.register_buffer("log_cols", torch.zeros(n_features, dtype=torch.bool))
        self.register_buffer("mean", torch.zeros(n_features))
        self.register_buffer("std", torch.ones(n_features))
        sizes = (n_features, *hidden)
        layers: list[nn.Module] = []
        for a, b in zip(sizes, sizes[1:], strict=False):
            layers += [nn.Linear(a, b), nn.ReLU()]
        self.body = nn.Sequential(*layers)
        self.head = nn.Linear(sizes[-1], 1)

    def prepare(self, x: torch.Tensor) -> torch.Tensor:
        x = torch.where(self.log_cols, torch.log1p(torch.clamp(x, min=0.0)), x)
        return (x - self.mean) / self.std

    def fit_scaler(self, X: np.ndarray) -> None:
        x = torch.as_tensor(X, dtype=torch.float32)
        skew = (x - x.mean(0)).pow(3).mean(0) / x.std(0).clamp(min=1e-6).pow(3)
        self.log_cols.copy_((x.min(0).values >= 0) & (skew > HEAVY_TAIL_SKEW))
        z = torch.where(self.log_cols, torch.log1p(x.clamp(min=0.0)), x)
        self.mean.copy_(z.mean(0))
        self.std.copy_(z.std(0).clamp(min=1e-6))

    def logits(self, x: torch.Tensor) -> torch.Tensor:
        return self.head(self.body(self.prepare(x))).squeeze(-1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return torch.sigmoid(self.logits(x))


def fit_mlp(Xtr, ytr, Xva, yva, epochs: int = 40, seed: int = 0, hidden: tuple[int, ...] = (64, 32),
            lr: float = 2e-3, batch_size: int = 512) -> MLP:
    """Adam on weighted BCE; keeps the epoch with the best validation PR-AUC (same criterion as the trees)."""
    torch.manual_seed(seed)
    torch.set_num_threads(1)                                    # reproducible and kind to the laptop
    model = MLP(Xtr.shape[1], hidden)
    model.fit_scaler(Xtr)
    x, y = torch.as_tensor(Xtr, dtype=torch.float32), torch.as_tensor(ytr, dtype=torch.float32)
    pos = max(float(y.sum()), 1.0)
    loss_fn = nn.BCEWithLogitsLoss(pos_weight=torch.tensor(((len(y) - pos) / pos) ** 0.5))
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    gen = torch.Generator().manual_seed(seed)
    best, best_state = -1.0, copy.deepcopy(model.state_dict())
    for _ in range(epochs):
        model.train()
        order = torch.randperm(len(y), generator=gen)
        for i in range(0, len(y), batch_size):
            idx = order[i:i + batch_size]
            opt.zero_grad()
            loss_fn(model.logits(x[idx]), y[idx]).backward()
            opt.step()
        score = pr_auc(yva, mlp_scores(model, Xva))
        if score > best:
            best, best_state = score, copy.deepcopy(model.state_dict())
    model.load_state_dict(best_state)
    return model.eval()


def mlp_scores(model: MLP, X) -> np.ndarray:
    model.eval()
    with torch.no_grad():
        return model(torch.as_tensor(X, dtype=torch.float32)).numpy().astype(np.float32)


class _Export(nn.Module):
    """(label, probabilities N x 2): the output layout the scorer reads from every model."""

    def __init__(self, mlp: MLP) -> None:
        super().__init__()
        self.mlp = mlp

    def forward(self, x):
        p = self.mlp(x)
        return (p >= 0.5).to(torch.int64), torch.stack([1 - p, p], dim=1)


def mlp_to_onnx(model: MLP, n_features: int) -> bytes:
    model.eval()
    buf = io.BytesIO()
    batch = {0: "batch"}
    torch.onnx.export(_Export(model), (torch.zeros(2, n_features),), buf, input_names=["input"],
                      output_names=["label", "probabilities"], opset_version=17, dynamo=False,
                      dynamic_axes={"input": batch, "label": batch, "probabilities": batch})
    return buf.getvalue()


def run(s: Settings, epochs: int = 40, alias: str = "challenger") -> dict:
    history = pl.read_parquet(s.history)
    ds = build_dataset(history)
    train, valid, test = temporal_split(ds, s.valid_days, s.test_days)
    (Xtr, ytr), (Xva, yva), (Xte, _) = _arrays(train), _arrays(valid), _arrays(test)
    unseen = set(test["user_id"].unique().to_list()) - set(train["user_id"].unique().to_list())

    model = fit_mlp(Xtr, ytr, Xva, yva, epochs=epochs, seed=s.seed)
    blob = mlp_to_onnx(model, len(FEATURE_ORDER))
    onnx = OnnxScorer(blob)
    parity = parity_report(mlp_scores(model, Xte), onnx.predict(Xte))
    max_diff = check_parity(mlp_scores(model, Xte), onnx.predict(Xte))        # the registry gate
    t = choose_thresholds(onnx.predict(Xva), yva, valid["amount"].to_numpy(), s.costs, s.max_review_rate)
    row = evaluate_scores(test, onnx.predict(Xte), t.review, t.block, s.costs, unseen)
    row = {**row, "review_threshold": t.review, "block_threshold": t.block}
    manifest = _manifest(s.history, ds)
    results = {"model": "mlp", "metrics": row, "onnx_parity": parity, "dataset": manifest}

    if s.register:
        thresholds = {"review": row["review_threshold"], "block": row["block_threshold"]}
        version = register_model(
            tracking_uri=s.tracking_uri, artifact_root=s.artifact_root, experiment=s.experiment,
            model_name=MODEL_NAME, alias=alias, onnx_bytes=blob, features=FEATURE_ORDER,
            params={"kind": "mlp", "epochs": epochs, "hidden": "64-32", "seed": s.seed},
            metrics={**row, "onnx_max_abs_diff": max_diff},
            artifacts={"thresholds.json": thresholds, "dataset_manifest.json": manifest,
                       "score_reference.json": reference_from_scores(onnx.predict(Xva)),
                       "eval.json": results},
            tags={"dataset_digest": manifest["digest"], "model_kind": "mlp"}, run_name="mlp-challenger")
        results["registry"] = {"model": MODEL_NAME, "alias": alias,
                               "version": version}
    s.results_out.parent.mkdir(parents=True, exist_ok=True)
    s.results_out.write_text(json.dumps(results, indent=2, default=float), encoding="utf-8")
    return results


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")          # MLflow prints an emoji; Windows consoles are cp1252
    ap = argparse.ArgumentParser(description="Train the MLP challenger and register it as 'challenger'")
    ap.add_argument("--history", type=Path, default=Settings.history)
    ap.add_argument("--tracking-uri", default=Settings.tracking_uri)
    ap.add_argument("--epochs", type=int, default=40)
    ap.add_argument("--no-register", action="store_true")
    a = ap.parse_args()
    s = replace(Settings(), history=a.history, tracking_uri=a.tracking_uri, register=not a.no_register,
                results_out=Path("docs/results/m6-challenger-eval.json"))
    r = run(s, epochs=a.epochs)
    m = r["metrics"]
    print(f"mlp  PR-AUC {m['pr_auc']:.3f}  recall@1%FPR {m['recall_at_fpr_1pct']:.3f}  cost {m['cost']:,.0f}")
    print(f"onnx max|diff| {r['onnx_parity']['max_abs_diff']:.2e}")
    print(json.dumps(r.get("registry", {})))


if __name__ == "__main__":
    main()
