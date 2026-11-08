"""Docker-compose plumbing shared by smoke and E1-E3: stack up, open-loop load, results."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Protocol

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ["docker", "compose"]
TOOLS = [*COMPOSE, "--profile", "tools", "run", "--rm", "-T"]
STATS_FORMAT = "{{.Name}}" + "\t" + "{{.MemUsage}}" + "\t" + "{{.CPUPerc}}"


class Hooks(Protocol):
    """Observers of the measurement window (lag sampler, DLQ counter)."""

    def start(self) -> None: ...
    def stop(self) -> dict: ...


def sh(cmd: list[str], env: dict | None = None, **kw) -> subprocess.CompletedProcess:
    import os

    full_env = {**os.environ, **env} if env else None
    return subprocess.run(cmd, cwd=ROOT, text=True, encoding="utf-8", capture_output=True, env=full_env, **kw)


def flink_job_running() -> bool:
    try:
        with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=3) as r:
            jobs = json.load(r)["jobs"]
    except Exception:
        return False
    return any(j["state"] == "RUNNING" for j in jobs)


def cancel_flink_jobs() -> None:
    """Cancel every RUNNING job on the session cluster (the next submit starts from the latest offsets)."""
    with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=5) as r:
        jobs = json.load(r)["jobs"]
    for j in jobs:
        if j["state"] == "RUNNING":
            req = urllib.request.Request(f"http://localhost:8081/jobs/{j['jid']}?mode=cancel", method="PATCH")
            urllib.request.urlopen(req, timeout=5).read()
    wait(lambda: not flink_job_running(), "job cancelled", 60)


def submit_flink(variant: dict | None = None, sql: str | None = None) -> None:
    """Submit the feature job with experiment overrides (see flink/submit-job.sh): BUFFER_TIMEOUT,
    CHECKPOINT_INTERVAL, DELIVERY_GUARANTEE, WATERMARK_DELAY. ``sql`` names a file under /opt/flink/sql."""
    cmd = [*COMPOSE, "--profile", "core", "run", "--rm", "-T", "flink-job"]
    if sql:
        cmd += ["/opt/flink/submit-job.sh", f"/opt/flink/sql/{sql}"]   # run replaces the command
    run = subprocess.run(cmd, cwd=ROOT, env={**os.environ, **{k: str(v) for k, v in (variant or {}).items()}},
                         capture_output=True, text=True, encoding="utf-8")
    if run.returncode:
        raise RuntimeError(f"flink submit failed: {run.stderr.strip()[-300:]}")
    wait(flink_job_running, "job RUNNING", 120)
    time.sleep(5)


def wait(cond, what: str, timeout: float, every: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(every)
    sys.exit(f"timeout waiting for {what}")


def last_json(text: str) -> dict:
    for line in reversed(text.strip().splitlines()):
        if line.startswith("{"):
            return json.loads(line)
    sys.exit(f"no JSON in output:\n{text}")


def ensure_stack(env: dict | None = None) -> None:
    print("starting core profile ...")
    up = sh([*COMPOSE, "--profile", "core", "up", "-d", "--build"], env=env)
    if up.returncode:
        sys.exit(up.stderr)
    wait(flink_job_running, "Flink job RUNNING", 180)
    time.sleep(8)                                    # scorer joins its consumer group


def run_load(rate: int, duration: int, procs: int = 1, label: str | None = None, warmup_s: int = 5,
             warmup_rate: int | None = None, hooks: list[Hooks] | None = None,
             out_dir: Path | None = None, idle_timeout_s: int = 8) -> dict:
    """Warm up (discarded), then push open-loop load while the collector listens. Returns the measurements."""
    print(f"warm-up {warmup_s} s (discarded) ...")
    sh([*TOOLS, "generator", "--rate", str(warmup_rate or min(rate, 500)), "--duration", str(warmup_s),
        "--procs", str(procs)])

    collector = subprocess.Popen([*TOOLS, "latency", "--idle-timeout", str(idle_timeout_s)], cwd=ROOT,
                                 text=True, encoding="utf-8", stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
    for line in collector.stdout:                    # wait until the collector owns its partitions
        if line.strip() == "READY":
            break
    print(f"load: {rate} ev/s for {duration} s ...")
    for h in hooks or []:
        h.start()
    gen = subprocess.Popen([*TOOLS, "generator", "--rate", str(rate), "--duration", str(duration),
                            "--procs", str(procs)], cwd=ROOT, text=True, encoding="utf-8",
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    time.sleep(duration / 2 + 3)
    stats = sh(["docker", "stats", "--no-stream", "--format", STATS_FORMAT]).stdout
    gen_out = gen.communicate()[0]
    extra: dict = {}
    for h in hooks or []:
        extra.update(h.stop())
    col_out = collector.communicate()[0]
    result = {"generator": last_json(gen_out), "latency": last_json(col_out), "docker_stats": stats, **extra}
    name = label or str(rate)
    out = out_dir or (ROOT / "docs" / "results")
    out.mkdir(parents=True, exist_ok=True)
    (out / f"smoke-{name}.json").write_text(json.dumps({k: result[k] for k in ("generator", "latency")},
                                                       indent=2))
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
