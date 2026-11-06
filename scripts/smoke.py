"""End-to-end smoke: bring up the core profile, push open-loop load, print p50/p95/p99 (M1).

Usage: python scripts/smoke.py [--rate 2000] [--duration 30]      (also: make smoke)
Writes docs/results/smoke-<rate>.json and docs/results/docker-stats-<rate>.txt.
The load protocol with several rates and repetitions is `bench` (M4).
"""
from __future__ import annotations

import argparse

from fraud_bench.stack import ensure_stack, report, run_load


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=2000)
    ap.add_argument("--duration", type=int, default=30)
    ap.add_argument("--procs", type=int, default=1)
    args = ap.parse_args()
    ensure_stack()
    report(run_load(args.rate, args.duration, args.procs))


if __name__ == "__main__":
    main()
