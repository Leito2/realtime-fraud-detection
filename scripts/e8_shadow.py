"""E8 (PLAN.md 5.6, ADR-6): does the shadow challenger change the champion's latency?

    uv run python scripts/e8_shadow.py [--rate 300] [--duration 90] [--warmup 30] [--reps 3]

Same load, the shadow scorer (its own consumer group, publishes only to shadow-scores) stopped and running,
alternating off/on so slow drift of the machine hits both. Reports the median p95 of each side and the
difference. Run it on a quiet machine. Writes docs/results/e8-shadow.{json,md}; leaves the shadow stopped.
"""
from __future__ import annotations

import argparse
import json
import statistics
import time

from fraud_bench.kafka_lag import LagSampler
from fraud_bench.stack import COMPOSE, ROOT, ensure_stack, run_load, sh


def set_shadow(on: bool) -> None:
    if on:
        sh([*COMPOSE, "--profile", "core", "--profile", "ops", "up", "-d", "scorer-shadow"])
        time.sleep(40)                                     # model load + group join
    else:
        sh([*COMPOSE, "--profile", "ops", "stop", "scorer-shadow"])
        time.sleep(5)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=300)
    ap.add_argument("--duration", type=int, default=90)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--reps", type=int, default=3)
    args = ap.parse_args()

    ensure_stack()
    rows = []
    for rep in range(1, args.reps + 1):
        for on in (False, True):
            set_shadow(on)
            tag = "on" if on else "off"
            res = run_load(args.rate, args.duration, label=f"e8-shadow-{tag}-r{rep}", warmup_s=args.warmup,
                           warmup_rate=args.rate, hooks=[LagSampler(args.rate)])
            e2e, gen = res["latency"].get("e2e_ms", {}), res["generator"]
            rows.append({"shadow": tag, "rep": rep, "p50": e2e.get("p50"), "p95": e2e.get("p95"),
                         "p99": e2e.get("p99"), "lost": max(0, gen["sent"] - res["latency"].get("count", 0)),
                         "valid": gen["valid"]})
            print(rows[-1], flush=True)
    set_shadow(False)

    med = {t: statistics.median(r["p95"] for r in rows if r["shadow"] == t) for t in ("off", "on")}
    payload = {"rate": args.rate, "rows": rows, "median_p95_ms": med,
               "delta_p95_ms": round(med["on"] - med["off"], 3)}
    out = ROOT / "docs" / "results"
    (out / "e8-shadow.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = ["# E8 - impact of the shadow challenger on the champion", "",
             f"{args.rate} ev/s, {args.duration} s per run, {args.warmup} s warm-up. All times in ms.", "",
             "| shadow | rep | p50 | p95 | p99 | lost | valid |", "|---|---|---|---|---|---|---|"]
    cols = ("shadow", "rep", "p50", "p95", "p99", "lost", "valid")
    lines += ["| " + " | ".join(str(r[k]) for k in cols) + " |" for r in rows]
    lines += ["", f"Median p95: off {med['off']} ms, on {med['on']} ms,",
              f"difference {payload['delta_p95_ms']} ms."]
    (out / "e8-shadow.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
