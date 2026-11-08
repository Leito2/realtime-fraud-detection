"""An experiment is data: a written hypothesis, a load, one fault, and what must hold afterwards."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from fraud_chaos.verify import Timeline, Verification

EXPERIMENTS_DIR = Path(__file__).resolve().parents[2] / "experiments"


@dataclass(frozen=True)
class Experiment:
    id: str
    title: str
    hypothesis: str
    fault: dict
    rate: int = 400                 # about half of the sustained rate (PLAN.md 5.4): not saturated
    fault_at_s: int = 60            # load runs this long before the fault
    observe_s: int = 120            # and this long after it
    budget_ms: float = 80.0
    p95_window_s: int | None = None  # judge p95 over this many seconds after the fault (None: until the end)
    needs: list[str] = field(default_factory=list)       # extra compose services the experiment requires
    env: dict = field(default_factory=dict)              # compose variables for the services it recreates
    recreate: list[str] = field(default_factory=list)    # services restarted with ``env`` before the load
    expect: dict = field(default_factory=dict)
    mechanism: str = ""


def load(exp_id: str, directory: Path = EXPERIMENTS_DIR) -> Experiment:
    raw = yaml.safe_load((directory / f"{exp_id}.yaml").read_text(encoding="utf-8"))
    return Experiment(**raw)


@dataclass
class Outcome:
    verification: Verification
    timeline: Timeline
    recovery_s: float | None
    longest_gap_s: float
    baseline_p95_ms: float
    degraded_share_after: float
    dlq_growth: int = 0
    p95_after_ms: float = float("nan")
    p95_rest_ms: float = float("nan")      # the observation after the judged window, reported, not judged
    versions_changed: bool = False
    template_share: float | None = None
    extra: dict = field(default_factory=dict)


def judge(exp: Experiment, out: Outcome) -> tuple[bool, list[str]]:
    """(hypothesis confirmed, one line per check). A check that fails refutes the hypothesis."""
    e, checks, ok = exp.expect, [], True

    def check(passed: bool, text: str) -> None:
        nonlocal ok
        ok = ok and passed
        checks.append(("PASS " if passed else "FAIL ") + text)

    if "lost_max" in e:
        check(out.verification.lost_count <= e["lost_max"],
              f"lost {out.verification.lost_count} (limit {e['lost_max']})")
    if "duplicates_max" in e:
        check(out.verification.duplicate_extra <= e["duplicates_max"],
              f"duplicated {out.verification.duplicate_extra} (limit {e['duplicates_max']})")
    if "recovery_s_max" in e:
        r = out.recovery_s
        check(r is not None and r <= e["recovery_s_max"],
              f"recovery {'never' if r is None else f'{r:.0f} s'} (limit {e['recovery_s_max']} s)")
    if "gap_s_max" in e:
        check(out.longest_gap_s <= e["gap_s_max"],
              f"longest silence {out.longest_gap_s:.1f} s (limit {e['gap_s_max']} s)")
    if "degraded_min" in e:
        check(out.degraded_share_after >= e["degraded_min"],
              f"degraded share {out.degraded_share_after:.0%} (at least {e['degraded_min']:.0%})")
    if "p95_after_ms_max" in e:
        check(out.p95_after_ms <= e["p95_after_ms_max"],
              f"p95 during the fault {out.p95_after_ms:.1f} ms (limit {e['p95_after_ms_max']} ms)")
    if e.get("versions_unchanged"):
        check(not out.versions_changed, "the serving model version did not change")
    if "template_share_min" in e:
        share = out.template_share
        check(share is not None and share >= e["template_share_min"],
              f"explanations by template {'n/a' if share is None else f'{share:.0%}'} "
              f"(at least {e['template_share_min']:.0%})")
    if "dlq_growth_min" in e:
        check(out.dlq_growth >= e["dlq_growth_min"],
              f"DLQ grew by {out.dlq_growth} (at least {e['dlq_growth_min']})")
    return ok, checks
