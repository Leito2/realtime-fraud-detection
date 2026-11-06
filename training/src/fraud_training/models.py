"""Champion / comparison models, ONNX export, parity check and benchmark (PLAN.md sections 4.5, 4.7)."""
import time
from collections.abc import Callable

import lightgbm as lgb
import numpy as np
import onnxruntime as ort
import xgboost as xgb
from onnxmltools import convert_lightgbm, convert_xgboost
from onnxmltools.convert.common.data_types import FloatTensorType

PARITY_TOL = 1e-5
KINDS = ("xgboost", "lightgbm")


def fit_model(kind: str, Xtr, ytr, Xva, yva, params: dict | None = None, seed: int = 0):
    """Same split, same features, same imbalance handling for both libraries (early stopping on PR-AUC)."""
    params = dict(params or {})
    pos_weight = float((ytr == 0).sum() / max((ytr == 1).sum(), 1))
    if kind == "xgboost":
        defaults = dict(n_estimators=400, max_depth=6, learning_rate=0.1, subsample=0.8,
                        colsample_bytree=0.8, tree_method="hist", scale_pos_weight=pos_weight,
                        eval_metric="aucpr", random_state=seed, early_stopping_rounds=30, n_jobs=2)
        model = xgb.XGBClassifier(**{**defaults, **params})
        model.fit(Xtr, ytr, eval_set=[(Xva, yva)], verbose=False)
        return model
    if kind == "lightgbm":
        defaults = dict(n_estimators=400, max_depth=-1, num_leaves=63, learning_rate=0.1, subsample=0.8,
                        subsample_freq=1, colsample_bytree=0.8, scale_pos_weight=pos_weight,
                        random_state=seed, n_jobs=2, verbose=-1)
        model = lgb.LGBMClassifier(**{**defaults, **params})
        model.fit(Xtr, ytr, eval_set=[(Xva, yva)], eval_metric="average_precision",
                  callbacks=[lgb.early_stopping(30, verbose=False)])
        return model
    raise ValueError(f"unknown model kind {kind!r}")


def native_scores(model, X) -> np.ndarray:
    return model.predict_proba(X)[:, 1].astype(np.float32)


def to_onnx(model, kind: str, n_features: int) -> bytes:
    types = [("input", FloatTensorType([None, n_features]))]
    if kind == "xgboost":
        onx = convert_xgboost(model, initial_types=types, target_opset=15)
    elif kind == "lightgbm":
        onx = convert_lightgbm(model, initial_types=types, target_opset=15, zipmap=False)
    else:
        raise ValueError(f"unknown model kind {kind!r}")
    return onx.SerializeToString()


class OnnxScorer:
    """Scores a float32 matrix with ONNX Runtime; returns P(fraud) per row."""

    def __init__(self, onnx_bytes: bytes, intra_op_threads: int = 1) -> None:
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = intra_op_threads
        opts.log_severity_level = 3
        self._session = ort.InferenceSession(onnx_bytes, opts, providers=["CPUExecutionProvider"])
        self._input = self._session.get_inputs()[0].name
        self._probs = self._session.get_outputs()[1].name      # outputs: label, probabilities (N x 2)

    def predict(self, X: np.ndarray) -> np.ndarray:
        x = np.ascontiguousarray(X, dtype=np.float32)
        return self._session.run([self._probs], {self._input: x})[0][:, 1]


def check_parity(native: np.ndarray, onnx: np.ndarray, tol: float = PARITY_TOL) -> float:
    """Max absolute difference; a model whose ONNX twin disagrees is never registered."""
    diff = float(np.abs(np.asarray(native, dtype=np.float64) - np.asarray(onnx, dtype=np.float64)).max())
    assert diff < tol, f"ONNX parity failed: max |diff| = {diff:.2e} >= {tol:.0e}"
    return diff


def parity_report(native: np.ndarray, onnx: np.ndarray, tol: float = PARITY_TOL) -> dict:
    """Non-raising parity measurement, to compare exporters; ``check_parity`` is the gate for the champion."""
    diff = np.abs(np.asarray(native, dtype=np.float64) - np.asarray(onnx, dtype=np.float64))
    return {"max_abs_diff": float(diff.max()), "rows_over_tol": int((diff >= tol).sum()),
            "rows": int(diff.size), "ok": bool(diff.max() < tol)}


def benchmark(predict: Callable[[np.ndarray], np.ndarray], X: np.ndarray, batch_sizes=(1, 32, 500),
              repeats: int = 200) -> dict[int, dict[str, float]]:
    """Latency of one call per batch size, plus the amortised cost per event (PLAN.md 4.7)."""
    out = {}
    for b in batch_sizes:
        batch = X[:b]
        for _ in range(5):                                    # warm-up
            predict(batch)
        times = []
        for _ in range(repeats):
            t0 = time.perf_counter_ns()
            predict(batch)
            times.append(time.perf_counter_ns() - t0)
        ms = np.array(times) / 1e6
        out[b] = {"p50_ms": float(np.percentile(ms, 50)), "p99_ms": float(np.percentile(ms, 99)),
                  "per_event_p50_us": float(np.percentile(ms, 50) * 1000 / b)}
    return out
