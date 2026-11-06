"""E3 (PLAN.md section 5.6): what does Flink's network buffer timeout cost in latency?

Usage: python scripts/e3_buffer_timeout.py [--rate 500] [--duration 20] [--values "100 ms" "10 ms" ...]
Cancels the running feature job, resubmits it with execution.buffer-timeout set per value, and measures each.
Writes docs/results/e3-buffer-timeout.json. Run it on a quiet machine: other CPU work contaminates the result.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.request

from fraud_bench.stack import COMPOSE, ROOT, ensure_stack, flink_job_running, run_load, wait


def cancel_jobs() -> None:
    with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=5) as r:
        jobs = json.load(r)["jobs"]
    for j in jobs:
        if j["state"] == "RUNNING":
            req = urllib.request.Request(f"http://localhost:8081/jobs/{j['jid']}?mode=cancel", method="PATCH")
            urllib.request.urlopen(req, timeout=5).read()
    wait(lambda: not flink_job_running(), "job cancelled", 60)


def submit(buffer_timeout: str) -> None:
    run = subprocess.run([*COMPOSE, "--profile", "core", "run", "--rm", "-T", "flink-job"], cwd=ROOT,
                         env={**os.environ, "BUFFER_TIMEOUT": buffer_timeout}, capture_output=True,
                         text=True, encoding="utf-8")
    if run.returncode:
        raise SystemExit(run.stderr)
    wait(flink_job_running, "job RUNNING", 120)
    time.sleep(5)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=500)
    ap.add_argument("--duration", type=int, default=20)
    ap.add_argument("--values", nargs="+", default=["100 ms", "10 ms", "5 ms", "1 ms"])
    args = ap.parse_args()

    ensure_stack()
    rows = []
    for value in args.values:
        cancel_jobs()
        submit(value)
        res = run_load(args.rate, args.duration, label=f"e3-{value.replace(' ', '')}")
        e2e = res["latency"].get("e2e_ms", {})
        gen = res["generator"]
        rows.append({"buffer_timeout": value, "count": res["latency"].get("count", 0), **e2e,
                     "gen_lag_p99_ms": gen["gen_lag_p99_ms"], "valid": gen["valid"]})
        print(f"{value:>8}: p50 {e2e.get('p50')}  p95 {e2e.get('p95')}  p99 {e2e.get('p99')}")
    out = ROOT / "docs" / "results" / "e3-buffer-timeout.json"
    out.write_text(json.dumps({"rate": args.rate, "duration_s": args.duration, "rows": rows}, indent=2))
    print()
    print("| buffer-timeout | p50 (ms) | p95 (ms) | p99 (ms) |")
    print("|---|---|---|---|")
    for r in rows:
        print(f"| {r['buffer_timeout']} | {r.get('p50')} | {r.get('p95')} | {r.get('p99')} |")


if __name__ == "__main__":
    main()
