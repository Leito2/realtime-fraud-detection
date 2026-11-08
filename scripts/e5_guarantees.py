"""E5 (PLAN.md 5.6): what does exactly-once cost in latency? Quantifies ADR-4.

    uv run python scripts/e5_guarantees.py [--rate 200] [--duration 90] [--warmup 30]

Same pipeline, same load, four configurations of the Flink sink on payments-enriched:
at-least-once (the default, checkpoint every 10 s) and exactly-once with a checkpoint every 1, 5 and 10 s.
Exactly-once publishes inside a Kafka transaction that commits when the checkpoint completes, so a row
becomes visible to a ``read_committed`` consumer up to one checkpoint interval late: the interval is the
latency floor. The scorer is recreated with ``isolation.level=read_committed`` for the exactly-once rows.
Run it on a quiet machine; it leaves the stack on the default job. Writes docs/results/e5-guarantees.*
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


def _exactly_once(interval: str) -> dict:
    # rows become visible in bursts one checkpoint apart: the collector must wait longer than that gap
    return {"label": f"exactly-once, checkpoint {interval}", "isolation": "read_committed",
            "idle_s": 2 * int(interval.split()[0]) + 8,
            "flink": {"DELIVERY_GUARANTEE": "exactly-once", "CHECKPOINT_INTERVAL": interval}}


VARIANTS = [
    {"label": "at-least-once, checkpoint 10 s", "flink": {}, "isolation": "read_uncommitted"},
    _exactly_once("1 s"),
    _exactly_once("5 s"),
    _exactly_once("10 s"),
]


def recreate_scorer(isolation: str) -> None:
    sh([*COMPOSE, "--profile", "core", "up", "-d", "--force-recreate", "scorer"],
       env={"KAFKA_ISOLATION_LEVEL": isolation})
    time.sleep(15)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=200)
    ap.add_argument("--duration", type=int, default=90)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--only", help="run only variants whose label contains this text (rerun one row)")
    args = ap.parse_args()

    ensure_stack()
    rows = []
    for v in VARIANTS:
        if args.only and args.only not in v["label"]:
            continue
        cancel_flink_jobs()
        submit_flink(v["flink"])
        recreate_scorer(v["isolation"])
        label = "e5-" + v["label"].replace(" ", "").replace(",", "-")
        res = run_load(args.rate, args.duration, label=label, warmup_s=args.warmup,
                       warmup_rate=args.rate, hooks=[LagSampler(args.rate)],
                       idle_timeout_s=v.get("idle_s", 8))
        e2e, gen = res["latency"].get("e2e_ms", {}), res["generator"]
        sent, decided = gen["sent"], res["latency"].get("count", 0)
        rows.append({"variant": v["label"], "sent": sent, "decided": decided, "lost": max(0, sent - decided),
                     "p50": e2e.get("p50"), "p95": e2e.get("p95"), "p99": e2e.get("p99"),
                     "gen_valid": gen["valid"], "lag_stable": res.get("lag_stable")})
        print(f"{v['label']:34s} p50 {e2e.get('p50')}  p95 {e2e.get('p95')}  p99 {e2e.get('p99')}",
              flush=True)
    cancel_flink_jobs()
    submit_flink()
    recreate_scorer("")                                       # back to the default configuration

    out = ROOT / "docs" / "results"
    if args.only and (out / "e5-guarantees.json").exists():
        old = json.loads((out / "e5-guarantees.json").read_text(encoding="utf-8"))["rows"]
        fresh = {r["variant"]: r for r in rows}
        rows = [fresh.pop(r["variant"], r) for r in old] + list(fresh.values())
    payload = {"rate": args.rate, "duration_s": args.duration, "rows": rows}
    (out / "e5-guarantees.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = ["# E5 - cost of exactly-once (ADR-4)", "",
             f"{args.rate} ev/s, {args.duration} s per row after a {args.warmup} s warm-up, open loop.", "",
             "| configuration | p50 (ms) | p95 (ms) | p99 (ms) | lost | valid run |",
             "|---|---|---|---|---|---|"]
    lines += [f"| {r['variant']} | {r['p50']} | {r['p95']} | {r['p99']} | {r['lost']} | {r['gen_valid']} |"
              for r in rows]
    (out / "e5-guarantees.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
