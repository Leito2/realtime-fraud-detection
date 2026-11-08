"""E9 (PLAN.md 5.6): what does it cost to observe? Tracing off / 1% / 100% of payments.

    uv run python scripts/e9_tracing.py [--rate 300] [--duration 90] [--warmup 30] [--reps 3]

Off = no OTLP endpoint on the scorer and no traceparent on payments. 1% and 100% = the scorer exports spans
(OTel Collector + Tempo, profile ``trace``) for that share of payments, set with the generator's
``--trace-sample``. Alternates the variants per repetition. Run it on a quiet machine. Writes
docs/results/e9-tracing.{json,md}; leaves the scorer with tracing off.
"""
from __future__ import annotations

import argparse
import json
import statistics
import time

from fraud_bench.kafka_lag import LagSampler
from fraud_bench.stack import COMPOSE, ROOT, ensure_stack, run_load, sh

VARIANTS = [("off", "", "0"), ("1%", "http://otel-collector:4318", "0.01"),
            ("100%", "http://otel-collector:4318", "1.0")]


def recreate_scorer(endpoint: str) -> None:
    sh([*COMPOSE, "--profile", "core", "up", "-d", "--force-recreate", "scorer"],
       env={"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint})
    time.sleep(15)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=300)
    ap.add_argument("--duration", type=int, default=90)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--reps", type=int, default=3)
    args = ap.parse_args()

    ensure_stack()
    sh([*COMPOSE, "--profile", "trace", "up", "-d", "otel-collector", "tempo"])
    time.sleep(20)
    rows = []
    for rep in range(1, args.reps + 1):
        for name, endpoint, sample in VARIANTS:
            recreate_scorer(endpoint)
            res = run_load(args.rate, args.duration, label=f"e9-{name.strip('%')}-r{rep}",
                           warmup_s=args.warmup, warmup_rate=args.rate, hooks=[LagSampler(args.rate)],
                           gen_args=["--trace-sample", sample])
            e2e, gen = res["latency"].get("e2e_ms", {}), res["generator"]
            rows.append({"tracing": name, "rep": rep, "p50": e2e.get("p50"), "p95": e2e.get("p95"),
                         "p99": e2e.get("p99"), "lost": max(0, gen["sent"] - res["latency"].get("count", 0)),
                         "valid": gen["valid"]})
            print(rows[-1], flush=True)
    recreate_scorer("")
    sh([*COMPOSE, "--profile", "trace", "stop", "otel-collector", "tempo"])

    med = {n: statistics.median(r["p95"] for r in rows if r["tracing"] == n) for n, _, _ in VARIANTS}
    payload = {"rate": args.rate, "rows": rows, "median_p95_ms": med}
    out = ROOT / "docs" / "results"
    (out / "e9-tracing.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = ["# E9 - cost of observing (tracing)", "",
             f"{args.rate} ev/s, {args.duration} s per run, {args.warmup} s warm-up. All times in ms.", "",
             "| tracing | rep | p50 | p95 | p99 | lost | valid |", "|---|---|---|---|---|---|---|"]
    cols = ("tracing", "rep", "p50", "p95", "p99", "lost", "valid")
    lines += ["| " + " | ".join(str(r[k]) for k in cols) + " |" for r in rows]
    medians = ", ".join(f"{n} {v} ms" for n, v in med.items())
    lines += ["", f"Median p95: {medians}."]
    (out / "e9-tracing.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
