"""Correctness verifier (PLAN.md 7.2): what went in versus what came out, after a failure.

``produced`` are the payment ids that reached the ``payments`` topic (the record of what the generator got a
delivery confirmation for). ``decided`` is every decision that reached the ``decisions`` topic. The functions
here are pure; ``read_topic`` (Kafka) feeds them.

* lost       - produced but never decided (the failure that costs money)
* duplicated - decided more than once (at-least-once delivery allows it; it must stay bounded)
* unknown    - decided but never produced (a bug, or traffic from another run)
"""
from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Decided:
    payment_id: str
    decided_s: float          # wall clock of the decision, in seconds
    latency_ms: float         # decided - scheduled, as the scorer measured it
    decision: str = "APPROVE"
    degraded: bool = False
    model_version: str = ""


@dataclass(frozen=True)
class Verification:
    produced: int
    decided_unique: int
    lost: list[str]
    duplicated: dict[str, int]
    unknown: int

    @property
    def duplicate_extra(self) -> int:
        return sum(n - 1 for n in self.duplicated.values())

    @property
    def lost_count(self) -> int:
        return len(self.lost)


def verify(produced: list[str], decided: list[Decided]) -> Verification:
    sent = set(produced)
    counts = Counter(d.payment_id for d in decided)
    return Verification(
        produced=len(sent), decided_unique=len(sent & counts.keys()), lost=sorted(sent - counts.keys()),
        duplicated={k: n for k, n in counts.items() if n > 1 and k in sent},
        unknown=sum(1 for k in counts if k not in sent))


@dataclass
class Timeline:
    """Per-second view of the decision stream around a fault."""

    seconds: list[int] = field(default_factory=list)       # seconds since the fault (negative = before)
    count: list[int] = field(default_factory=list)
    p95_ms: list[float] = field(default_factory=list)
    degraded: list[int] = field(default_factory=list)


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))] if ordered else math.nan


def timeline(decided: list[Decided], fault_s: float, lo: int = -60, hi: int = 300) -> Timeline:
    buckets: dict[int, list[Decided]] = {}
    for d in decided:
        sec = math.floor(d.decided_s - fault_s)
        if lo <= sec <= hi:
            buckets.setdefault(sec, []).append(d)
    out = Timeline()
    for sec in range(lo, hi + 1):
        group = buckets.get(sec, [])
        out.seconds.append(sec)
        out.count.append(len(group))
        out.p95_ms.append(_p95([d.latency_ms for d in group]))
        out.degraded.append(sum(1 for d in group if d.degraded))
    return out


def recovery_s(tl: Timeline, budget_ms: float, steady_for_s: int = 5, min_rate: float = 0.5,
               baseline_rate: float | None = None, relative_to_baseline: bool = True,
               slack: float = 1.5) -> float | None:
    """Seconds after the fault until decisions flow at a normal rate with p95 back to normal for
    ``steady_for_s`` consecutive seconds. ``None`` if that never happens inside the window.

    "Normal" is the larger of the SLO budget and ``slack`` times the median per-second p95 before the fault,
    so a machine that already ran above the budget before the fault is judged against itself, not against a
    target it never met (``relative_to_baseline=False`` keeps the strict budget)."""
    pre = [c for s, c in zip(tl.seconds, tl.count, strict=True) if s < 0]
    base = baseline_rate if baseline_rate is not None else (sum(pre) / len(pre) if pre else 0.0)
    limit = budget_ms
    if relative_to_baseline:
        before = sorted(p for s, p in zip(tl.seconds, tl.p95_ms, strict=True) if s < 0 and not math.isnan(p))
        if before:
            limit = max(budget_ms, slack * before[len(before) // 2])
    ok = [(s >= 0 and c >= min_rate * base and not math.isnan(p) and p <= limit)
          for s, c, p in zip(tl.seconds, tl.count, tl.p95_ms, strict=True)]
    run = 0
    for i, good in enumerate(ok):
        run = run + 1 if good else 0
        if run >= steady_for_s:
            return float(tl.seconds[i] - steady_for_s + 1)
    return None


def longest_gap_s(decided: list[Decided], start_s: float, end_s: float) -> float:
    """Longest stretch inside [start, end] with no decision at all: the outage as a caller would see it."""
    times = sorted(d.decided_s for d in decided if start_s <= d.decided_s <= end_s)
    if not times:
        return end_s - start_s
    points = [start_s, *times, end_s]
    return max(b - a for a, b in zip(points, points[1:], strict=False))


def read_topic(bootstrap: str, topic: str, since_s: float, until_s: float | None = None,
               idle_s: float = 6.0) -> list[tuple[float, dict]]:
    """(kafka timestamp in seconds, JSON value) for every message at or after ``since_s``."""
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"chaos-verify-{topic}-{int(since_s)}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    try:
        parts = sorted(c.list_topics(topic, timeout=10).topics[topic].partitions)
        wanted = [TopicPartition(topic, p, int(since_s * 1000)) for p in parts]
        c.assign(c.offsets_for_times(wanted, timeout=10))
        out: list[tuple[float, dict]] = []
        idle = 0.0
        while idle < idle_s:
            msgs = c.consume(num_messages=2000, timeout=1.0)
            idle = 0.0 if msgs else idle + 1.0
            for m in msgs:
                if m.error() or m.value() is None:
                    continue
                ts = m.timestamp()[1] / 1000
                if until_s is not None and ts > until_s:
                    continue
                try:
                    out.append((ts, json.loads(m.value())))
                except ValueError:                  # JSON or UTF-8 error: a poison pill, not a reading
                    continue
        return out
    finally:
        c.close()
