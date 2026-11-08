"""Run one failure experiment (PLAN.md 7):  uv run chaos F1   (make chaos EXP=F1)

Steady load -> fault at ``fault_at_s`` -> observe -> drain -> verify with the correctness verifier -> report.
The verdict is about the hypothesis written in ``chaos/experiments/<id>.yaml`` *before* the run.
"""
from __future__ import annotations

import argparse
import random
import subprocess
import sys
import threading
import time

from fraud_bench.kafka_lag import end_offsets_total
from fraud_bench.stack import (
    COMPOSE,
    ROOT,
    TOOLS,
    cancel_flink_jobs,
    flink_job_running,
    sh,
    submit_flink,
    wait,
)
from pydantic import ValidationError

from fraud_chaos import faults, registry
from fraud_chaos.experiment import Experiment, Outcome, judge, load
from fraud_chaos.report import write_report
from fraud_chaos.verify import Decided, longest_gap_s, read_topic, recovery_s, timeline, verify
from fraudcore.contracts import Payment

BOOTSTRAP = "localhost:9092"
OUT_DIR = ROOT / "docs" / "resilience"


# A freshly (re)started service needs a couple of minutes before its latency settles (seen in the plain-load
# check: p95 of 0.3-2.6 s for 150 s, then 26-45 ms). That stretch is load, not part of the measured baseline.
WARMUP_S = 120
# Services recreated for the experiment (new environment, cold model and SHAP) take longer: in F8 the scorer
# and explainer were still at a p95 of 6 s three minutes after the restart and settled at 33 ms soon after.
RECREATE_WARMUP_S = 120


def warmup_s(exp: Experiment) -> int:
    return WARMUP_S + (RECREATE_WARMUP_S if exp.recreate else 0)

PROFILES = ["--profile", "core", "--profile", "ops", "--profile", "chaos"]


def up(*services: str, env: dict | None = None) -> None:
    r = sh([*COMPOSE, *PROFILES, "up", "-d", "--no-deps", *services], env=env)
    if r.returncode:
        raise RuntimeError(f"compose up {services} failed: {r.stderr.strip()[:300]}")


def ensure_stack(exp: Experiment) -> None:
    if exp.needs:
        up(*exp.needs)
    if exp.flink:
        cancel_flink_jobs()
        submit_flink(exp.flink)                            # e.g. exactly-once (F10)
    if exp.recreate:
        up(*exp.recreate, env={k: str(v) for k, v in exp.env.items()})
        time.sleep(15)                                     # the recreated services join their groups
    wait(flink_job_running, "Flink job RUNNING", 120)


def reset_stack(exp: Experiment) -> None:
    """Leave the stack as the next experiment expects it: services up, default environment."""
    sh([*COMPOSE, "start", "kafka", "redis", "taskmanager", "mlflow"])
    if exp.flink:
        cancel_flink_jobs()
        submit_flink()                                     # back to the default job
    if exp.recreate:
        up(*exp.recreate)


def start_load(exp: Experiment, seed: int) -> subprocess.Popen:
    duration = warmup_s(exp) + exp.fault_at_s + exp.observe_s
    return subprocess.Popen(
        [*TOOLS, "generator", "--rate", str(exp.rate), "--duration", str(duration),
         "--stream-seed", str(seed),
         "--users", "20000", "--zipf", "0.7"], cwd=ROOT, text=True, encoding="utf-8",
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def drain(topic: str = "decisions", quiet_s: float = 20, timeout_s: float = 180) -> None:
    """Wait until the decisions topic stops growing: everything that can be decided has been."""
    last, since, end = -1, time.monotonic(), time.monotonic() + timeout_s
    while time.monotonic() < end:
        now = end_offsets_total(BOOTSTRAP, topic)
        if now != last:
            last, since = now, time.monotonic()
        elif time.monotonic() - since >= quiet_s:
            return
        time.sleep(3)


# Only the groups a baseline depends on. The async consumers (auditor, shadow scorer) may be stopped on
# purpose to free memory on a small machine, and a stopped group keeps a lag that nobody will drain.
QUIET_GROUPS = "scorer-champion|flink-features|feature-writer|explainer"
QUIET_QUERY = f'sum(kafka_consumergroup_lag{{consumergroup=~"{QUIET_GROUPS}"}})'


def total_consumer_lag(prometheus: str = "http://localhost:9090") -> float | None:
    """Sum of every consumer group's lag as Prometheus sees it, or None if it cannot be read."""
    import json
    import urllib.parse
    import urllib.request

    url = f"{prometheus}/api/v1/query?" + urllib.parse.urlencode({"query": QUIET_QUERY})
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            result = json.load(r)["data"]["result"]
        return float(result[0]["value"][1]) if result else 0.0
    except (OSError, ValueError, KeyError, IndexError):
        return None


def wait_quiet(max_lag: float = 100, steady_s: float = 15, timeout_s: float = 240) -> bool:
    """Do not start measuring while the previous experiment is still being digested: a baseline taken over a
    backlog (seen in F3/F6b: p95 of 1-4 s before any fault) makes every later number meaningless."""
    end, ok_since = time.monotonic() + timeout_s, None
    while time.monotonic() < end:
        lag = total_consumer_lag()
        if lag is not None and lag <= max_lag:
            ok_since = ok_since or time.monotonic()
            if time.monotonic() - ok_since >= steady_s:
                return True
        else:
            ok_since = None
        time.sleep(3)
    return False


def is_valid_payment(row: dict) -> bool:
    """Only well-formed payments count as produced: junk is not a payment that anyone owes a decision."""
    try:
        Payment.model_validate(row)
    except ValidationError:
        return False
    return True


def to_decided(rows: list[tuple[float, dict]]) -> list[Decided]:
    return [Decided(r["payment_id"], r["t_decided_ns"] / 1e9, (r["t_decided_ns"] - r["t_scheduled_ns"]) / 1e6,
                    r.get("decision", "APPROVE"), bool(r.get("degraded", False)),
                    str(r.get("model_version", "")))
            for _, r in rows]


def run_experiment(exp: Experiment, restore: bool = True) -> dict:
    print(f"== {exp.id}: {exp.title}\n   hypothesis: {exp.hypothesis}", flush=True)
    ensure_stack(exp)
    if not wait_quiet():
        print("   WARNING: consumers still had a backlog after 240 s; baseline may be polluted", flush=True)
    champion_before = registry.champion_version() if exp.fault["type"] == "mlflow_corrupt" else None
    dlq_before = end_offsets_total(BOOTSTRAP, "dlq")
    t_start = time.time()
    load = start_load(exp, seed=random.randint(1, 10**6))
    time.sleep(warmup_s(exp) + exp.fault_at_s)
    fault_s = time.time()
    done: dict = {}

    def inject() -> None:
        try:
            done["what"] = faults.inject(exp.fault)
        except Exception as exc:                       # the report must say it, not hide it
            done["what"] = f"INJECTION FAILED: {exc}"

    thread = threading.Thread(target=inject)
    thread.start()
    thread.join()
    print(f"   fault at +{exp.fault_at_s} s: {done['what']}", flush=True)
    load_out = load.communicate()[0]
    t_end = time.time()
    if champion_before:
        registry.restore_champion(champion_before)
    if restore:
        reset_stack(exp)
    print("   draining ...", flush=True)
    drain()

    produced = [r["payment_id"] for _, r in read_topic(BOOTSTRAP, "payments", t_start - 2, t_end + 5)
                if is_valid_payment(r)]
    decided = to_decided(read_topic(BOOTSTRAP, "decisions", t_start - 2))
    produced_ids = set(produced)                      # built once: a set per element made this quadratic
    in_run = [d for d in decided if d.payment_id in produced_ids]
    before = [d.latency_ms for d in in_run if fault_s - exp.fault_at_s <= d.decided_s < fault_s]
    base_p95 = sorted(before)[int(0.95 * len(before))] if before else float("nan")
    tl = timeline(in_run, fault_s, lo=-exp.fault_at_s + 15, hi=exp.observe_s + 60)
    after = [d for d in in_run if fault_s <= d.decided_s <= t_end]
    window_end = fault_s + exp.p95_window_s if exp.p95_window_s else t_end

    def p95_of(rows: list) -> float:
        lat = sorted(d.latency_ms for d in rows)
        return lat[int(0.95 * len(lat))] if lat else float("nan")

    p95_after = p95_of([d for d in after if d.decided_s <= window_end])
    p95_rest = p95_of([d for d in after if d.decided_s > window_end])
    versions_before = {d.model_version for d in in_run if d.decided_s < fault_s}
    versions_after = {d.model_version for d in after}
    template_share = None
    if "template_share_min" in exp.expect:
        expl = [row for _, row in read_topic(BOOTSTRAP, "explanations", fault_s)]
        llm_or_template = [x for x in expl if x.get("status") in ("llm", "template")]
        if llm_or_template:
            template_share = sum(x["status"] == "template" for x in llm_or_template) / len(llm_or_template)
    out = Outcome(
        verification=verify(produced, decided), timeline=tl,
        recovery_s=recovery_s(tl, exp.budget_ms), longest_gap_s=longest_gap_s(in_run, fault_s, t_end),
        baseline_p95_ms=base_p95,
        degraded_share_after=(sum(d.degraded for d in after) / len(after)) if after else 0.0,
        dlq_growth=end_offsets_total(BOOTSTRAP, "dlq") - dlq_before,
        p95_after_ms=p95_after, p95_rest_ms=p95_rest,
        versions_changed=bool(versions_before) and not versions_after <= versions_before,
        template_share=template_share,
        extra={"fault": done["what"], "generator": load_out.strip().splitlines()[-1] if load_out else ""})
    ok, checks = judge(exp, out)
    path = write_report(exp, out, ok, checks, OUT_DIR, fault_s=fault_s)
    print("   " + "\n   ".join(checks), flush=True)
    print(f"   hypothesis {'CONFIRMED' if ok else 'REFUTED'} -> {path.relative_to(ROOT)}", flush=True)
    return {"id": exp.id, "confirmed": ok}


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Failure experiment with a written hypothesis")
    ap.add_argument("experiment", help="experiment id, e.g. F1 (file chaos/experiments/F1.yaml)")
    ap.add_argument("--no-restore", action="store_true")
    args = ap.parse_args()
    result = run_experiment(load(args.experiment), restore=not args.no_restore)
    sys.exit(0 if result["confirmed"] else 1)


if __name__ == "__main__":
    main()
