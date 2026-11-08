"""Experiment E4 (PLAN.md 2.7, 14): the same XGBoost model served three ways.

    native   XGBoost Booster in-process (what a notebook does)
    onnx     ONNX Runtime in-process, one thread (what the scorer does)
    triton   the same ONNX file behind Triton Inference Server, over gRPC (Variant B)

``micro`` times only the model call, per batch size, on rows of the real feature table, so the number is the
price of the serving path and nothing else. ``e2e`` (``fraud_bench.run e4``) puts the scorer on each path and
measures the whole pipeline at low rates. Run ``micro`` inside the stack's network:

    docker compose --profile core --profile serving-triton --profile tools run --rm e4 micro

On an 8 GB machine only low rates with one generator are measured (PLAN.md 8.2): this is a limit of the
laptop, not of Triton, and the README says so next to the numbers.
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

import numpy as np


def summarize(seconds: list[float], batch: int) -> dict:
    """p50/p95/p99 of one call, and the cost per event, from raw call times in seconds."""
    ms = np.asarray(seconds) * 1000
    p50, p95, p99 = (float(np.percentile(ms, q)) for q in (50, 95, 99))
    return {"batch": batch, "calls": len(ms), "p50_ms": round(p50, 4), "p95_ms": round(p95, 4),
            "p99_ms": round(p99, 4), "us_per_event_p50": round(p50 * 1000 / batch, 3)}


def time_calls(predict: Callable[[np.ndarray], object], rows: np.ndarray, batch: int, iters: int,
               warmup: int, rng: np.random.Generator) -> dict:
    """Time ``iters`` calls on random contiguous slices of ``rows`` (the first ``warmup`` are discarded)."""
    starts = rng.integers(0, len(rows) - batch, size=iters + warmup)
    times: list[float] = []
    for i, s in enumerate(starts):
        X = rows[s:s + batch]
        t = time.perf_counter()
        predict(X)
        elapsed = time.perf_counter() - t
        if i >= warmup:
            times.append(elapsed)
    return summarize(times, batch)


def _booster_predict(tracking_uri: str, name: str, version: str) -> Callable[[np.ndarray], np.ndarray]:
    import mlflow
    import xgboost as xgb

    client = mlflow.MlflowClient(tracking_uri=tracking_uri, registry_uri=tracking_uri)
    mv = client.get_model_version(name, version)
    with tempfile.TemporaryDirectory() as tmp:
        path = mlflow.artifacts.download_artifacts(artifact_uri=f"{mv.source}/booster.json", dst_path=tmp,
                                                   tracking_uri=tracking_uri)
        booster = xgb.Booster(model_file=path)
    booster.set_param({"nthread": 1})                       # same single thread as the scorer's ONNX session
    return lambda X: booster.predict(xgb.DMatrix(np.ascontiguousarray(X, dtype=np.float32)))


def micro(args: argparse.Namespace) -> None:
    import polars as pl
    from scorer.adapters.hot_swap import RegisteredModel
    from scorer.adapters.mlflow_registry import MlflowRegistry
    from scorer.adapters.onnx_model import OnnxModel
    from scorer.adapters.triton_model import TritonRuntimeBuilder
    from scorer.adapters.triton_repository import TritonRepository
    from tritonclient.grpc import InferenceServerClient

    from fraudcore.features import FEATURE_ORDER

    frame = pl.read_parquet(args.data).select(FEATURE_ORDER).drop_nulls()
    rows = frame.sample(n=min(20000, frame.height), seed=7).to_numpy().astype(np.float32)
    registry = MlflowRegistry(args.mlflow, args.model, "champion")
    version = registry.current_version()
    loaded: RegisteredModel = registry.fetch(version)

    onnx = OnnxModel(loaded.onnx_bytes, args.model, version, intra_op_threads=1)
    native = _booster_predict(args.mlflow, args.model, version)
    triton = TritonRuntimeBuilder(InferenceServerClient(args.triton), TritonRepository(args.repository),
                                  "fraud_xgb", timeout_s=1.0)(loaded, args.model, version, None)

    parity = float(np.max(np.abs(onnx.predict(rows[:500]).scores - triton.predict(rows[:500]).scores)))
    if parity > 1e-5:
        sys.exit(f"onnx and triton disagree by {parity}: refusing to publish numbers")
    rng = np.random.default_rng(11)
    paths = {"native": native, "onnx": onnx.predict, "triton": triton.predict}
    results = {k: [] for k in paths}
    for batch in args.batches:
        for name, fn in paths.items():
            results[name].append(time_calls(fn, rows, batch, args.iters, args.warmup, rng))
            r = results[name][-1]
            print(f"{name:6s} batch {batch:4d}: p50 {r['p50_ms']:8.3f} ms  p95 {r['p95_ms']:8.3f} ms  "
                  f"p99 {r['p99_ms']:8.3f} ms  {r['us_per_event_p50']:9.2f} us/event", flush=True)
    out = {"model_version": version, "onnx_vs_triton_max_diff": parity, "iters": args.iters,
           "results": results}
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"wrote {args.out}")


def main() -> None:
    ap = argparse.ArgumentParser(description="E4: native vs ONNX in-process vs Triton")
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("micro", help="time the model call per batch size")
    m.add_argument("--mlflow", default="http://mlflow:5000")
    m.add_argument("--model", default="fraud-champion")
    m.add_argument("--triton", default="triton:8001")
    m.add_argument("--repository", default="/triton-models")
    m.add_argument("--data", default="/data/dataset_cache.parquet")
    m.add_argument("--batches", type=int, nargs="+", default=[1, 8, 32, 128, 500])
    m.add_argument("--iters", type=int, default=1500)
    m.add_argument("--warmup", type=int, default=200)
    m.add_argument("--out", default="/results/e4-micro.json")
    m.set_defaults(fn=micro)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
