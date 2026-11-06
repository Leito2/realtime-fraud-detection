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
from fraud_bench.stack import COMPOSE, ROOT, TOOLS, flink_job_running, sh, wait
from pydantic import ValidationError

from fraud_chaos import faults, registry
from fraud_chaos.experiment import Experiment, Outcome, judge, load
from fraud_chaos.report import write_report
from fraud_chaos.verify import Decided, longest_gap_s, read_topic, recovery_s, timeline, verify
from fraudcore.contracts import Payment

BOOTSTRAP = "localhost:9092"
OUT_DIR = ROOT / "docs" / "resilience"


PROFILES = ["--profile", "core", "--profile", "ops", "--profile", "chaos"]


def up(*services: str, env: dict | None = None) -> None:
    r = sh([*COMPOSE, *PROFILES, "up", "-d", "--no-deps", *services], env=env)
    if r.returncode:
        raise RuntimeError(f"compose up {services} failed: {r.stderr.strip()[:300]}")


def ensure_stack(exp: Experiment) -> None:
    if exp.needs:
        up(*exp.needs)
    if exp.recreate:
        up(*exp.recreate, env={k: str(v) for k, v in exp.env.items()})
        time.sleep(15)                                     # the recreated services join their groups
    wait(flink_job_running, "Flink job RUNNING", 120)


def reset_stack(exp: Experiment) -> None:
    """Leave the stack as the next experiment expects it: services up, default environment."""
    sh([*COMPOSE, "start", "kafka", "redis", "taskmanager", "mlflow"])
    if exp.recreate:
        up(*exp.recreate)


def start_load(exp: Experiment, seed: int) -> subprocess.Popen:
    duration = exp.fault_at_s + exp.observe_s
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
    champion_before = registry.champion_version() if exp.fault["type"] == "mlflow_corrupt" else None
    dlq_before = end_offsets_total(BOOTSTRAP, "dlq")
    t_start = time.time()
    load = start_load(exp, seed=random.randint(1, 10**6))
    time.sleep(exp.fault_at_s)
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
    in_run = [d for d in decided if d.payment_id in set(produced)]
    before = [d.latency_ms for d in in_run if d.decided_s < fault_s]
    base_p95 = sorted(before)[int(0.95 * len(before))] if before else float("nan")
    tl = timeline(in_run, fault_s, lo=-exp.fault_at_s + 15, hi=exp.observe_s + 60)
    after = [d for d in in_run if fault_s <= d.decided_s <= t_end]
    p95_after = sorted(d.latency_ms for d in after)[int(0.95 * len(after))] if after else float("nan")
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
        dlq_growth=end_offsets_total(BOOTSTRAP, "dlq") - dlq_before, p95_after_ms=p95_after,
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
