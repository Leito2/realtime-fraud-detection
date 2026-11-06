"""Degraded-mode policy (PLAN.md §7.4): neither fail-open nor fail-closed. Pure business rule."""
from dataclasses import dataclass

from fraudcore.contracts import Decision
from fraudcore.thresholds import Thresholds


@dataclass(frozen=True)
class DegradedPolicy:
    """Used when profiles are unavailable: stricter thresholds, and no auto-approval of large amounts."""

    thresholds: Thresholds
    max_auto_approve_amount: float

    def decide(self, score: float, amount: float) -> Decision:
        decision = self.thresholds.decide(score)
        if decision == "APPROVE" and amount >= self.max_auto_approve_amount:
            return "REVIEW"
        return decision
