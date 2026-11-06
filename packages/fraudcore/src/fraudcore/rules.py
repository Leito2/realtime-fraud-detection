"""Rules baseline: what a team without ML would deploy (PLAN.md section 4.4). First row of the results."""
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class Rule:
    name: str
    weight: float
    reads: tuple[str, ...]
    fires: Callable[[dict], bool]


RULES = [
    Rule("burst", 0.6, ("f_cnt_10m",), lambda r: r["f_cnt_10m"] > 5),
    Rule("foreign_and_large", 0.6, ("is_foreign", "amount_to_avg_ratio"),
         lambda r: r["is_foreign"] > 0 and r["amount_to_avg_ratio"] > 3),
    Rule("very_large", 0.5, ("amount_to_avg_ratio",), lambda r: r["amount_to_avg_ratio"] > 5),
    Rule("card_testing", 0.5, ("f_small_tx_cnt_10m",), lambda r: r["f_small_tx_cnt_10m"] >= 4),
    Rule("ip_mismatch_foreign", 0.3, ("ip_country_mismatch", "is_foreign"),
         lambda r: r["ip_country_mismatch"] > 0 and r["is_foreign"] > 0),
]


def rules_score(row: dict) -> float:
    """Sum of the weights of the rules that fire, capped at 1. Not a probability: only its ranking matters."""
    return min(1.0, sum(rule.weight for rule in RULES if rule.fires(row)))
