"""E7 (PLAN.md 3.3): what does option B cost per event? Times the operator logic without a cluster.

    uv run python -m fraud_bench.option_b [--users 2000] [--events 200000] [--gap 600] [--warm 100000]

Replays a stream where each user pays every ``--gap`` seconds on average, so the 24 h buffer fills the way it
would in production, and reports the time of one ``UserWindows.add`` call. This is the Python cost only:
PyFlink thread-mode bridge (JVM to Python serialization) is not included and is not measured here.
Writes docs/results/e7-option-b.{json,md}.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import random
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]


def load_state_module():
    spec = importlib.util.spec_from_file_location("user_state", ROOT / "flink" / "pyflink" / "user_state.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> None:
    ap = argparse.ArgumentParser(description="E7: cost of the option B operator logic")
    ap.add_argument("--users", type=int, default=2000)
    ap.add_argument("--events", type=int, default=200_000)
    ap.add_argument("--gap", type=float, default=600.0, help="mean seconds between payments of one user")
    ap.add_argument("--warm", type=int, default=100_000, help="events replayed before timing (fills buffers)")
    args = ap.parse_args()

    mod = load_state_module()
    rnd = random.Random(7)
    states: dict[int, object] = {}
    ts = 0.0
    step = args.gap / args.users                        # global inter-arrival time
    times: list[float] = []
    for i in range(args.warm + args.events):
        ts += rnd.expovariate(1 / step)
        u = rnd.randrange(args.users)
        st = states.get(u) or states.setdefault(u, mod.UserWindows())
        amount = rnd.uniform(1, 200)
        t0 = time.perf_counter()
        st.add(ts, amount)
        dt = time.perf_counter() - t0
        if i >= args.warm:
            times.append(dt)
    sizes = [s.size() for s in states.values()]
    us = np.asarray(times) * 1e6
    res = {"users": args.users, "mean_gap_s": args.gap, "events_timed": len(us),
           "p50_us": round(float(np.percentile(us, 50)), 2), "p95_us": round(float(np.percentile(us, 95)), 2),
           "p99_us": round(float(np.percentile(us, 99)), 2), "mean_us": round(float(us.mean()), 2),
           "events_per_second_one_core": round(1e6 / float(us.mean())),
           "buffer_events_median": int(np.median(sizes)), "buffer_events_max": int(max(sizes))}
    out = ROOT / "docs" / "results"
    (out / "e7-option-b.json").write_text(json.dumps(res, indent=2), encoding="utf-8")
    md = ["# E7 - cost of option B (keyed operator, all windows)", "",
          f"{args.users} users, one payment per user every {args.gap:.0f} s on average, 24 h buffers full "
          f"before timing. Time of one `UserWindows.add` in CPython, no Flink around it.", "",
          "| measure | value |", "|---|---|",
          f"| p50 | {res['p50_us']} us |", f"| p95 | {res['p95_us']} us |", f"| p99 | {res['p99_us']} us |",
          f"| mean | {res['mean_us']} us |",
          f"| events/s on one core (mean) | {res['events_per_second_one_core']:,} |",
          f"| buffer, median / max events per user | {res['buffer_events_median']} / "
          f"{res['buffer_events_max']} |"]
    (out / "e7-option-b.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))


if __name__ == "__main__":
    main()
