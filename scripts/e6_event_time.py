"""E6 (PLAN.md 5.6): what do watermarks cost? Window on processing time vs on event time.

    uv run python scripts/e6_event_time.py [--rate 200] [--duration 90] [--warmup 30]

Same pipeline and load; only the Flink window changes:
processing time (the default job) and event time with 0.5 s and 2 s of allowed out-of-orderness
(flink/sql/e6_event_time.sql). Event time makes the window exact for late or reordered payments, but an
OVER window can emit a row only after the watermark passes it, so the out-of-orderness bound is added to
the latency.
The generator emits in order, so no payment is late here: the table shows the price of the guarantee, not a
benefit. ``lost`` counts payments that never got a decision (late ones would be dropped by the window).
Run it on a quiet machine; it leaves the stack on the default job. Writes docs/results/e6-event-time.*
"""
from __future__ import annotations

import argparse
import json

from fraud_bench.kafka_lag import LagSampler
from fraud_bench.stack import ROOT, cancel_flink_jobs, ensure_stack, run_load, submit_flink


def _event_time(delay: str) -> dict:
    return {"label": f"event time, {delay} s out-of-orderness", "sql": "e6_event_time.sql",
            "flink": {"WATERMARK_DELAY": delay}}


VARIANTS = [{"label": "processing time", "sql": None, "flink": {}}, _event_time("0.5"), _event_time("2")]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=200)
    ap.add_argument("--duration", type=int, default=90)
    ap.add_argument("--warmup", type=int, default=30)
    args = ap.parse_args()

    ensure_stack()
    rows = []
    for v in VARIANTS:
        cancel_flink_jobs()
        submit_flink(v["flink"], sql=v["sql"])
        label = "e6-" + v["label"].replace(" ", "").replace(",", "-")
        res = run_load(args.rate, args.duration, label=label, warmup_s=args.warmup,
                       warmup_rate=args.rate, hooks=[LagSampler(args.rate)])
        e2e, gen = res["latency"].get("e2e_ms", {}), res["generator"]
        sent, decided = gen["sent"], res["latency"].get("count", 0)
        rows.append({"variant": v["label"], "sent": sent, "decided": decided, "lost": max(0, sent - decided),
                     "p50": e2e.get("p50"), "p95": e2e.get("p95"), "p99": e2e.get("p99"),
                     "gen_valid": gen["valid"]})
        print(f"{v['label']:38s} p50 {e2e.get('p50')}  p95 {e2e.get('p95')}  p99 {e2e.get('p99')}",
              flush=True)
    cancel_flink_jobs()
    submit_flink()

    out = ROOT / "docs" / "results"
    payload = {"rate": args.rate, "duration_s": args.duration, "rows": rows}
    (out / "e6-event-time.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = ["# E6 - cost of event-time windows", "",
             f"{args.rate} ev/s, {args.duration} s per row after a {args.warmup} s warm-up, open loop.", "",
             "| window | p50 (ms) | p95 (ms) | p99 (ms) | lost | valid run |", "|---|---|---|---|---|---|"]
    lines += [f"| {r['variant']} | {r['p50']} | {r['p95']} | {r['p99']} | {r['lost']} | {r['gen_valid']} |"
              for r in rows]
    (out / "e6-event-time.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
