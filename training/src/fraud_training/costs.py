"""From score to decision by expected cost (PLAN.md section 4.6).

Missed fraud costs the payment amount, a review costs analyst time (and catches the fraud), and blocking a
legitimate customer costs friction. Thresholds are the pair that minimises the total on validation data,
subject to a cap on the share of payments sent to review (analyst capacity).
"""
from dataclasses import dataclass

import numpy as np

from fraudcore.thresholds import Thresholds


@dataclass(frozen=True)
class Costs:
    review_cost: float = 5.0
    block_friction_cost: float = 25.0


def expected_cost(scores: np.ndarray, y: np.ndarray, amount: np.ndarray, review: float, block: float,
                  costs: Costs) -> float:
    approved = scores < review
    reviewed = (scores >= review) & (scores < block)
    blocked = scores >= block
    missed = float(amount[approved & (y == 1)].sum())
    return (missed + costs.review_cost * float(reviewed.sum())
            + costs.block_friction_cost * float((blocked & (y == 0)).sum()))


def choose_thresholds(scores: np.ndarray, y: np.ndarray, amount: np.ndarray, costs: Costs,
                      max_review_rate: float, name: str = "cost-optimal") -> Thresholds:
    """Exhaustive search over score quantiles, O(candidates^2) using prefix sums over the sorted scores."""
    n = len(scores)
    order = np.argsort(scores, kind="stable")
    s, ys, am = scores[order], y[order], amount[order]
    miss = np.r_[0.0, np.cumsum(np.where(ys == 1, am, 0.0))]          # cost of approving s[:i]
    legit = np.r_[0, np.cumsum(ys == 0)]                              # legit payments in s[:i]

    cands = np.unique(np.clip(np.quantile(s, np.linspace(0.5, 0.9995, 400)), 1e-6, 1.0))
    cands = np.unique(np.r_[cands, 1.0])
    idx = np.searchsorted(s, cands, side="left")                      # first position with score >= cand

    best = (np.inf, 0, 1)
    for i in range(len(cands) - 1):
        j = np.arange(i + 1, len(cands))
        review_rate = (idx[j] - idx[i]) / n
        cost = (miss[idx[i]] + costs.review_cost * (idx[j] - idx[i])
                + costs.block_friction_cost * (legit[n] - legit[idx[j]]))
        cost = np.where(review_rate <= max_review_rate, cost, np.inf)
        k = int(np.argmin(cost))
        if cost[k] < best[0]:
            best = (float(cost[k]), i, i + 1 + k)
    if not np.isfinite(best[0]):
        raise ValueError("no threshold pair satisfies the review-rate cap")
    return Thresholds(name, float(cands[best[1]]), float(cands[best[2]]))


def match_flag_rate(scores: np.ndarray, flag_rate: float, cost_optimal: Thresholds) -> Thresholds:
    """Operating point with the same flagged share as another model: ``review`` is the score quantile that
    flags ``flag_rate`` of the payments. Needed to compare a challenger with the champion at equal workload:
    recall at different flag rates says nothing (PLAN.md 16, H11). ``block`` keeps its cost-optimal value
    unless that would not be above ``review``."""
    review = float(np.quantile(scores, 1.0 - flag_rate))
    block = cost_optimal.block if cost_optimal.block > review else review + (1.0 - review) / 2
    return Thresholds(f"{cost_optimal.name}@flag-rate", review, block)
