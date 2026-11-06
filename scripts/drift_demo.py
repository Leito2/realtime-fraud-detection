"""Drift demo (PLAN.md 4.11): the whole online MLOps loop in one command. ``make drift-demo``.

    A steady traffic            -> the champion works
    B new fraud pattern appears -> recall of the champion falls, fraud gets approved
    retrain from the feedback   -> registered as the challenger; the shadow scorer picks it up
    C compare in shadow         -> champion vs challenger on the same live traffic
    promote (criteria checked)  -> the scorer swaps the model in memory, no restart
    D recovery                  -> the new champion catches the pattern

Needs ``make up PROFILE=ops``, profiles loaded (``make profiles``) and a trained champion (``make train``).
Traffic is generated at a low rate on purpose: model behaviour, not latency, is under test, and a moderately
skewed population keeps the per-user intensity close to the regime the models were trained on (PLAN.md 16).
Watch Grafana D4 (http://localhost:3000) while it runs.
"""
import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Per-user intensity decides whether the live feature distribution looks like the training one: 6 ev/s with a
# moderately skewed population gives f_cnt_10m and z-scores close to training (checked, PLAN.md 16 H9).
ZIPF = "0.7"
PSQL = ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "fraud", "-d", "fraud", "-At", "-F", ","]

STATS_SQL = """
SELECT count(*) FILTER (WHERE l.is_fraud),
       count(*) FILTER (WHERE l.is_fraud AND d.decision <> 'APPROVE'),
       count(*) FILTER (WHERE l.is_fraud AND l.scenario = 'new_pattern'),
       count(*) FILTER (WHERE l.is_fraud AND l.scenario = 'new_pattern' AND d.decision <> 'APPROVE'),
       count(*) FILTER (WHERE NOT l.is_fraud AND d.decision <> 'APPROVE'),
       count(*) FILTER (WHERE NOT l.is_fraud),
       count(*) FILTER (WHERE l.is_fraud AND s.decision <> 'APPROVE'),
       count(*) FILTER (WHERE l.is_fraud AND l.scenario = 'new_pattern' AND s.decision <> 'APPROVE'),
       count(*), min(d.model_version), max(d.model_version)
FROM decisions d JOIN labels l USING (payment_id) LEFT JOIN shadow_scores s USING (payment_id)
"""


@dataclass
class Phase:
    name: str
    fraud: int
    caught: int
    new_total: int
    new_caught: int
    legit_flagged: int
    legit: int
    shadow_caught: int
    shadow_new_caught: int
    total: int
    models: str

    @property
    def recall(self) -> float:
        return self.caught / self.fraud if self.fraud else float("nan")

    @property
    def new_recall(self) -> float:
        return self.new_caught / self.new_total if self.new_total else float("nan")

    @property
    def fpr(self) -> float:
        return self.legit_flagged / self.legit if self.legit else float("nan")


def run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=ROOT, text=True, encoding="utf-8", errors="replace", capture_output=True,
                          check=True, **kw)


def sql(query: str) -> str:
    return run(PSQL + ["-c", query]).stdout.strip()


def traffic(seconds: int, rate: int, stream_seed: int, extra: list[str]) -> None:
    run(["docker", "compose", "--profile", "tools", "run", "--rm", "-T", "generator", "--rate", str(rate),
         "--duration", str(seconds), "--users", "20000", "--zipf", ZIPF, "--scenarios",
         "--fraud-rate", "0.02", "--hard-legit-rate", "0.01", "--stream-seed", str(stream_seed), *extra])
    time.sleep(10)                                          # let the auditor catch up


def snapshot(name: str) -> Phase:
    f = sql(STATS_SQL).split(",")
    return Phase(name, *[int(x) for x in f[:9]], models=f"{f[9]}..{f[10]}")


def table(phases: list[Phase]) -> str:
    rows = ["| phase | champion | frauds | recall | new-pattern recall | legit flagged | challenger recall |",
            "|---|---|---|---|---|---|---|"]
    for p in phases:
        shadow = f"{p.shadow_caught / p.fraud:.1%}" if p.fraud else "-"
        new = f"{p.new_recall:.1%} ({p.new_caught}/{p.new_total})" if p.new_total else "-"
        rows.append(f"| {p.name} | {p.models} | {p.fraud} | {p.recall:.1%} | {new} | {p.fpr:.1%} | "
                    f"{shadow} |")
    return "\n".join(rows)


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rate", type=int, default=6)
    ap.add_argument("--steady", type=int, default=240, help="seconds of phase A")
    ap.add_argument("--drift", type=int, default=360, help="seconds of phase B")
    ap.add_argument("--compare", type=int, default=360, help="seconds of phase C (shadow comparison)")
    ap.add_argument("--recover", type=int, default=240, help="seconds of phase D")
    ap.add_argument("--inject-rate", type=float, default=0.05)
    ap.add_argument("--out", type=Path, default=ROOT / "docs" / "results" / "m6-drift-demo.md")
    a = ap.parse_args()
    inject = ["--inject-scenario", "new_pattern", "--inject-after", "0", "--inject-rate", str(a.inject_rate)]

    sql("TRUNCATE decisions, labels, shadow_scores")
    phases: list[Phase] = []
    print("A. steady traffic", flush=True)
    traffic(a.steady, a.rate, 101, [])
    phases.append(snapshot("A steady"))
    print(table(phases), flush=True)

    print("B. new fraud pattern", flush=True)
    sql("TRUNCATE decisions, labels, shadow_scores")
    traffic(a.drift, a.rate, 102, inject)
    phases.append(snapshot("B new pattern"))
    print(table(phases), flush=True)

    print("retrain from the feedback dataset", flush=True)
    out = run(["uv", "run", "retrain", "--min-fraud", "20", "--match-flag-rate", "auto"]).stdout
    print(out, flush=True)
    time.sleep(15)                                          # the shadow scorer polls the alias every 5 s

    print("C. shadow comparison", flush=True)
    sql("TRUNCATE decisions, labels, shadow_scores")
    traffic(a.compare, a.rate, 103, inject)
    phases.append(snapshot("C champion vs challenger"))
    print(table(phases), flush=True)

    print("promote (criteria checked)", flush=True)
    promoted = subprocess.run(["uv", "run", "promote"], cwd=ROOT, text=True, encoding="utf-8",
                              errors="replace", capture_output=True)
    print(promoted.stdout + promoted.stderr, flush=True)
    if promoted.returncode != 0:
        print("criteria not met: the demo stops here, the champion stays", flush=True)
    else:
        time.sleep(15)
        print("D. recovery", flush=True)
        sql("TRUNCATE decisions, labels, shadow_scores")
        traffic(a.recover, a.rate, 104, inject)
        phases.append(snapshot("D new champion"))

    report = "# Drift demo\n\n" + table(phases) + "\n\n" + promoted.stdout.strip() + "\n"
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(report, encoding="utf-8")
    print("\n" + report)


if __name__ == "__main__":
    main()
