"""End-to-end smoke: bring up the core profile, push open-loop load, print p50/p95/p99 (PLAN.md §11.3, M1).

Usage: python scripts/smoke.py [--rate 2000] [--duration 30]      (also: make smoke)
Writes docs/results/smoke-<rate>.json and docs/results/docker-stats-<rate>.txt.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose"]
TOOLS = [*COMPOSE, "--profile", "tools", "run", "--rm", "-T"]
STATS_FORMAT = "{{.Name}}" + "\t" + "{{.MemUsage}}" + "\t" + "{{.CPUPerc}}"


def sh(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=ROOT, text=True, encoding="utf-8", capture_output=True, **kw)


def flink_job_running() -> bool:
    try:
        with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=3) as r:
            jobs = json.load(r)["jobs"]
    except Exception:
        return False
    return any(j["state"] == "RUNNING" for j in jobs)


def wait(cond, what: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(2)
    sys.exit(f"timeout waiting for {what}")


def last_json(text: str) -> dict:
    for line in reversed(text.strip().splitlines()):
        if line.startswith("{"):
            return json.loads(line)
    sys.exit(f"no JSON in output:\n{text}")


def ensure_stack() -> None:
    print("starting core profile ...")
    up = sh([*COMPOSE, "--profile", "core", "up", "-d", "--build"])
    if up.returncode:
        sys.exit(up.stderr)
    wait(flink_job_running, "Flink job RUNNING", 180)
    time.sleep(8)                                    # scorer joins its consumer group


def run_load(rate: int, duration: int, procs: int = 1, label: str | None = None) -> dict:
    """Warm up, then push open-loop load while the collector listens. Returns the measured summary."""
    print("warm-up (discarded) ...")
    sh([*TOOLS, "generator", "--rate", "500", "--duration", "5"])

    collector = subprocess.Popen([*TOOLS, "latency", "--idle-timeout", "8"], cwd=ROOT, text=True,
                                 encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in collector.stdout:                    # wait until the collector owns its partitions
        if line.strip() == "READY":
            break
    print(f"load: {rate} ev/s for {duration} s ...")
    gen = subprocess.Popen([*TOOLS, "generator", "--rate", str(rate), "--duration", str(duration),
                            "--procs", str(procs)], cwd=ROOT, text=True, encoding="utf-8",
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    time.sleep(duration / 2 + 3)
    stats = sh(["docker", "stats", "--no-stream", "--format", STATS_FORMAT]).stdout
    gen_out = gen.communicate()[0]
    col_out = collector.communicate()[0]
    result = {"generator": last_json(gen_out), "latency": last_json(col_out), "docker_stats": stats}
    name = label or str(rate)
    out = ROOT / "docs" / "results"
    out.mkdir(parents=True, exist_ok=True)
    summary = {k: result[k] for k in ("generator", "latency")}
    (out / f"smoke-{name}.json").write_text(json.dumps(summary, indent=2))
    (out / f"docker-stats-{name}.txt").write_text(stats, encoding="utf-8")
    return result


def report(result: dict) -> None:
    gen_r, lat = result["generator"], result["latency"]
    e2e, scorer = lat.get("e2e_ms", {}), lat.get("scorer_ms", {})
    lost = gen_r["sent"] - lat.get("count", 0)
    print()
    print(f"sent {gen_r['sent']}  decided {lat.get('count', 0)}  lost {lost}  "
          f"delivery_errors {gen_r['delivery_errors']}")
    verdict = "VALID" if gen_r["valid"] else "INVALID (>5 ms)"
    print(f"gen_lag p99 {gen_r['gen_lag_p99_ms']} ms -> run {verdict}")
    print(f"{'':12}{'p50':>9}{'p95':>9}{'p99':>9}{'max':>9}   (ms)")
    for name, d in (("end-to-end", e2e), ("to decided", scorer)):
        if d:
            print(f"{name:12}{d['p50']:>9}{d['p95']:>9}{d['p99']:>9}{d['max']:>9}")
    print()
    print("docker stats (mid-load):")
    print(result["docker_stats"])


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
