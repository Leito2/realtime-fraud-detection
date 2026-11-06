"""Live traffic with fraud and hard-legit episodes, plus the label the bank will eventually know.

Pure and deterministic (no Kafka, no clock): ``next`` is called once per scheduled tick. Episode steps take
the slots of normal traffic, so the offered rate stays what the open-loop schedule says (PLAN.md §5.3).
Only episodes that fit within ``max_span_s`` start: live windows are wall-clock, so an episode lasting an hour
would only be a long idle test. ``slow_fraud`` and ``geo_jump`` therefore stay offline (PLAN.md §3.3, E7).
"""
import heapq
import random

from generator.logic import Population, build_payment, next_payment
from generator.scenarios import (
    AVG_FRAUD_EPISODE_PAYMENTS,
    AVG_LEGIT_EPISODE_PAYMENTS,
    FRAUD_KINDS,
    LEGIT_HARD_KINDS,
    Episode,
    make_episode,
)

SECOND_NS = 1_000_000_000
TRIES = 12


class LiveTraffic:
    def __init__(self, pop: Population, rng: random.Random, *, fraud_rate: float = 0.01,
                 hard_legit_rate: float = 0.01, miss_rate: float = 0.05, false_alarm_rate: float = 0.0005,
                 max_span_s: float = 900.0) -> None:
        self._pop, self._rng = pop, rng
        self._p_fraud = fraud_rate / AVG_FRAUD_EPISODE_PAYMENTS      # chance per tick of starting an episode
        self._p_legit = hard_legit_rate / AVG_LEGIT_EPISODE_PAYMENTS
        self._miss, self._false_alarm, self._max_span = miss_rate, false_alarm_rate, max_span_s
        self._pending: list[tuple[int, int, dict]] = []     # (due_ns, seq, step) as a min-heap
        self._seq = 0

    def next(self, due_ns: int, sent_ns: int) -> tuple[dict, dict]:
        """The payment due at this tick and its label message."""
        self._maybe_start(due_ns)
        if self._pending and self._pending[0][0] <= due_ns:
            step = heapq.heappop(self._pending)[2]
            payment = build_payment(step["habits"], step["uid"], self._rng, amount=step["amount"],
                                    country=step["country"], device=step["device"],
                                    ip_country=step["ip_country"], t_scheduled_ns=due_ns, t_sent_ns=sent_ns)
            scenario, truth = step["scenario"], step["fraud"]
        else:
            payment = next_payment(self._pop, self._rng, due_ns, sent_ns)
            scenario, truth = "normal", False
        return payment, {"payment_id": payment["payment_id"], "is_fraud": self._known(truth),
                         "scenario": scenario}

    def _known(self, truth: bool) -> bool:
        """What the bank knows: some fraud is never reported, a few legitimate payments get disputed."""
        return self._rng.random() >= self._miss if truth else self._rng.random() < self._false_alarm

    def _maybe_start(self, now_ns: int) -> None:
        if self._p_fraud and self._rng.random() < self._p_fraud:
            self._start(self._rng.choice(FRAUD_KINDS), now_ns)
        if self._p_legit and self._rng.random() < self._p_legit:
            self._start(self._rng.choice(LEGIT_HARD_KINDS), now_ns)

    def _start(self, kind: str, now_ns: int) -> None:
        rng = self._rng
        uid = self._pop.pick_user(rng) if rng.random() < 0.5 else rng.choice(self._pop.user_ids)
        habits = self._pop.habits(uid)
        episode = self._fitting_episode(kind, habits)
        if episode is None:
            return
        new_device = f"d_{uid[2:]}_new{self._rng.randint(0, 99_999)}"
        for s in episode.steps:
            self._seq += 1
            step = {"habits": habits, "uid": uid, "amount": s.amount, "country": s.country,
                    "device": new_device if s.device == "NEW" else s.device, "ip_country": s.ip_country,
                    "scenario": kind, "fraud": episode.is_fraud}
            heapq.heappush(self._pending, (now_ns + int(s.offset_s * SECOND_NS), self._seq, step))

    def _fitting_episode(self, kind: str, habits: dict) -> Episode | None:
        for _ in range(TRIES):
            episode = make_episode(kind, habits, self._rng)
            if episode.steps[-1].offset_s <= self._max_span:
                return episode
        return None
