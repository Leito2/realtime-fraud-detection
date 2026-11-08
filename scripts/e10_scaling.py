"""E10 (PLAN.md 5.6): where does each stage saturate? Scorer processes x Flink parallelism.

    uv run python scripts/e10_scaling.py [--rate 400] [--duration 90] [--warmup 30]

At one fixed offered rate, scale the scorer to 1 and 2 processes (4 do not fit in 8 GB; one consumer group,
partitions are shared) and run the Flink job at parallelism 2 and 4. Each cell reports the end-to-end
percentiles, whether
the consumer lag stayed flat and the container CPU: the stage whose CPU sits at its limit while the lag
grows is the bottleneck. On an 8 GB laptop with one generator the 4-process rows may be limited by the
machine, not by the design; the README says so. Run it on a quiet machine; it restores the default stack.
Writes docs/results/e10-scaling.{json,md}.
"""
from __future__ import annotations

import argparse
import json
import time

from fraud_bench.kafka_lag import LagSampler
from fraud_bench.stack import (
    COMPOSE,
    ROOT,
    cancel_flink_jobs,
    ensure_stack,
    run_load,
    sh,
    submit_flink,
)


def scale_scorer(replicas: int) -> None:
    r = sh([*COMPOSE, "--profile", "core", "up", "-d", "--scale", f"scorer={replicas}", "--no-deps",
           "scorer"])
    if r.returncode:
        raise RuntimeError(r.stderr[-300:])
    time.sleep(20)                                            # new members join and the group rebalances


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=400)
    ap.add_argument("--duration", type=int, default=90)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--scorers", type=int, nargs="+", default=[1, 2])
    ap.add_argument("--parallelism", type=int, nargs="+", default=[2, 4])
    args = ap.parse_args()

    ensure_stack()
    rows = []
    for par in args.parallelism:
        cancel_flink_jobs()
        submit_flink({"PARALLELISM": par})
        for n in args.scorers:
            scale_scorer(n)
            res = run_load(args.rate, args.duration, label=f"e10-p{par}-s{n}", warmup_s=args.warmup,
                           warmup_rate=args.rate, hooks=[LagSampler(args.rate)])
            e2e, gen = res["latency"].get("e2e_ms", {}), res["generator"]
            sent, decided = gen["sent"], res["latency"].get("count", 0)
            rows.append({"flink_parallelism": par, "scorers": n, "sent": sent, "lost": max(0, sent - decided),
                         "p50": e2e.get("p50"), "p95": e2e.get("p95"), "p99": e2e.get("p99"),
                         "lag_stable": res.get("lag_stable"), "lag_max": res.get("lag_max"),
                         "gen_valid": gen["valid"], "docker_stats": res["docker_stats"]})
            print(f"flink {par} / scorers {n}: p50 {e2e.get('p50')}  p95 {e2e.get('p95')}  "
                  f"p99 {e2e.get('p99')}  lag stable {res.get('lag_stable')}", flush=True)
    scale_scorer(1)
    cancel_flink_jobs()
    submit_flink()

    out = ROOT / "docs" / "results"
    payload = {"rate": args.rate, "duration_s": args.duration, "rows": rows}
    (out / "e10-scaling.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = ["# E10 - scaling the scorer and Flink", "",
             f"{args.rate} ev/s offered, {args.duration} s per cell after a {args.warmup} s warm-up, "
             "open loop.", "",
             "| Flink parallelism | scorers | p50 (ms) | p95 (ms) | p99 (ms) | lag stable | lost | valid |",
             "|---|---|---|---|---|---|---|---|"]
    lines += [f"| {r['flink_parallelism']} | {r['scorers']} | {r['p50']} | {r['p95']} | "
              f"{r['p99']} | {r['lag_stable']} | {r['lost']} | {r['gen_valid']} |" for r in rows]
    (out / "e10-scaling.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
