"""Load protocol runner (PLAN.md section 5.4): E1 rate steps and E2 scorer batching grid.

    uv run python -m fraud_bench.run e1 --rates 500 1000 2000 --reps 3 --duration 180 --warmup 60
    uv run python -m fraud_bench.run e2 --rate 1000 --grid 1:1 50:5 500:5 500:20

Every run gets ``docs/results/<run_id>/`` with ``manifest.json`` (machine, code, config) and ``steps.json``
(one entry per repetition). ``fraud_bench.report`` turns that folder into the README table and the knee chart.
Close heavy apps and keep the laptop on power: contention shows up as fake latency (see E3 preliminary).
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime

from fraud_bench import manifest
from fraud_bench.kafka_lag import LagSampler
from fraud_bench.stack import COMPOSE, ROOT, ensure_stack, flink_job_running, run_load, sh, wait


def to_step(rate: int, rep: int, res: dict, extra: dict | None = None) -> dict:
    """Flatten one repetition's raw result into the fields the analysis reads."""
    gen, lat = res["generator"], res["latency"]
    e2e = lat.get("e2e_ms", {})
    sent, decided = gen["sent"], lat.get("count", 0)
    return {
        "rate": rate, "rep": rep, "sent": sent, "decided": decided, "lost": max(0, sent - decided),
        "p50": e2e.get("p50", float("inf")), "p95": e2e.get("p95", float("inf")),
        "p99": e2e.get("p99", float("inf")), "max": e2e.get("max"),
        "gen_valid": bool(gen["valid"]), "gen_lag_p99_ms": gen["gen_lag_p99_ms"],
        "delivery_errors": gen["delivery_errors"],
        "lag_stable": bool(res.get("lag_stable", False)), "lag_max": res.get("lag_max"),
        "dlq_growth": res.get("dlq_growth", 0), "lag_samples": res.get("lag_samples", []),
        "docker_stats": res["docker_stats"], **(extra or {}),
    }


def _new_run(experiment: str, params: dict):
    run_id = f"{datetime.now():%Y%m%d-%H%M%S}-{experiment}"
    out = ROOT / "docs" / "results" / run_id
    out.mkdir(parents=True, exist_ok=True)
    shown = {k: v for k, v in params.items() if not callable(v)}   # argparse puts the handler in vars(args)
    (out / "manifest.json").write_text(json.dumps(manifest.collect(run_id, experiment, shown), indent=2),
                                       encoding="utf-8")
    return run_id, out


def _drain(seconds: float = 5.0) -> None:
    time.sleep(seconds)                           # let the scorer empty its backlog between steps


def e1(args: argparse.Namespace) -> None:
    run_id, out = _new_run("e1", vars(args))
    ensure_stack()
    steps = []
    for rate in args.rates:
        for rep in range(1, args.reps + 1):
            print(f"\n=== E1 {rate} ev/s, repetition {rep}/{args.reps} ===")
            res = run_load(rate, args.duration, args.procs, label=f"{rate}-r{rep}", warmup_s=args.warmup,
                           warmup_rate=rate, hooks=[LagSampler(rate)], out_dir=out / "raw")
            steps.append(to_step(rate, rep, res))
            (out / "steps.json").write_text(json.dumps(steps, indent=2), encoding="utf-8")
            _drain()
    print(f"\nresults: {out}\nrender with: uv run python -m fraud_bench.report {out.relative_to(ROOT)}")


def e2(args: argparse.Namespace) -> None:
    run_id, out = _new_run("e2", vars(args))
    steps = []
    for cell in args.grid:
        size, wait_ms = (int(x) for x in cell.split(":"))
        env = {"BATCH_MAX_MESSAGES": str(size), "BATCH_MAX_WAIT_MS": str(wait_ms)}
        ensure_stack(env)
        sh([*COMPOSE, "--profile", "core", "up", "-d", "--force-recreate", "scorer"], env=env)
        wait(flink_job_running, "Flink job RUNNING", 60)
        time.sleep(8)
        print(f"\n=== E2 batch {size} messages / {wait_ms} ms wait at {args.rate} ev/s ===")
        res = run_load(args.rate, args.duration, args.procs, label=f"b{size}w{wait_ms}", warmup_s=args.warmup,
                       warmup_rate=args.rate, hooks=[LagSampler(args.rate)], out_dir=out / "raw")
        steps.append(to_step(args.rate, 1, res, {"batch_max_messages": size, "batch_max_wait_ms": wait_ms}))
        (out / "steps.json").write_text(json.dumps(steps, indent=2), encoding="utf-8")
    print("\n| max messages | max wait (ms) | p50 | p95 | p99 | lag stable |\n|---|---|---|---|---|---|")
    for s in steps:
        print(f"| {s['batch_max_messages']} | {s['batch_max_wait_ms']} | {s['p50']} | {s['p95']} | "
              f"{s['p99']} | {s['lag_stable']} |")


def e4(args: argparse.Namespace) -> None:
    """End to end at low rates with the scorer on each serving path (PLAN.md 2.7); one generator, 8 GB."""
    run_id, out = _new_run("e4", vars(args))
    steps = []
    for variant in args.variants:
        env = {"SERVING_MODE": variant}
        profiles = ["--profile", "core"] + (["--profile", "serving-triton"] if variant == "triton" else [])
        ensure_stack(env)
        if variant == "triton":
            sh([*COMPOSE, *profiles, "up", "-d", "triton"], env=env)
            time.sleep(20)                                   # Triton starts empty; the scorer publishes
        sh([*COMPOSE, *profiles, "up", "-d", "--force-recreate", "scorer"], env=env)
        wait(flink_job_running, "Flink job RUNNING", 60)
        time.sleep(30 if variant == "triton" else 8)         # the scorer publishes and waits for Triton
        for rate in args.rates:
            for rep in range(1, args.reps + 1):
                print(f"\n=== E4 {variant} at {rate} ev/s, repetition {rep}/{args.reps} ===")
                res = run_load(rate, args.duration, 1, label=f"{variant}-{rate}-r{rep}",
                               warmup_s=args.warmup, warmup_rate=rate, hooks=[LagSampler(rate)],
                               out_dir=out / "raw")
                steps.append(to_step(rate, rep, res, {"variant": variant}))
                (out / "steps.json").write_text(json.dumps(steps, indent=2), encoding="utf-8")
                _drain()
    sh([*COMPOSE, "--profile", "core", "up", "-d", "--force-recreate", "scorer"],
       env={"SERVING_MODE": "onnx"})
    print("\n| variant | rate | rep | p50 | p95 | p99 | lost | lag stable |")
    print("|---|---|---|---|---|---|---|---|")
    for s in steps:
        print(f"| {s['variant']} | {s['rate']} | {s['rep']} | {s['p50']} | {s['p95']} | {s['p99']} | "
              f"{s['lost']} | {s['lag_stable']} |")
    print(f"\nresults: {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Open-loop load protocol")
    sub = ap.add_subparsers(dest="experiment", required=True)
    p1 = sub.add_parser("e1", help="rate steps with repetitions")
    p1.add_argument("--rates", type=int, nargs="+", default=[500, 1000, 2000, 3000, 5000])
    p1.add_argument("--reps", type=int, default=3)
    p1.add_argument("--duration", type=int, default=180)
    p1.add_argument("--warmup", type=int, default=60)
    p1.add_argument("--procs", type=int, default=1)
    p1.set_defaults(fn=e1)
    p2 = sub.add_parser("e2", help="scorer micro-batch size x wait grid")
    p2.add_argument("--rate", type=int, default=1000)
    p2.add_argument("--grid", nargs="+", default=["1:1", "50:5", "500:5", "500:20"], help="messages:wait_ms")
    p2.add_argument("--duration", type=int, default=60)
    p2.add_argument("--warmup", type=int, default=20)
    p2.add_argument("--procs", type=int, default=1)
    p2.set_defaults(fn=e2)
    p4 = sub.add_parser("e4", help="end to end with the scorer on ONNX in-process vs Triton")
    p4.add_argument("--variants", nargs="+", default=["onnx", "triton"], choices=["onnx", "triton"])
    p4.add_argument("--rates", type=int, nargs="+", default=[200, 500, 1000])
    p4.add_argument("--reps", type=int, default=3)
    p4.add_argument("--duration", type=int, default=120)
    p4.add_argument("--warmup", type=int, default=45)
    p4.set_defaults(fn=e4)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
