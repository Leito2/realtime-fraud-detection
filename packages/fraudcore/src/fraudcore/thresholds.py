"""Score → decision bands (PLAN.md §4.6). Pure: no I/O, so training and serving share it."""
from dataclasses import dataclass

from fraudcore.contracts import Decision


@dataclass(frozen=True)
class Thresholds:
    name: str
    review: float
    block: float

    def __post_init__(self) -> None:
        if not 0.0 <= self.review < self.block <= 1.0:
            raise ValueError(f"need 0 <= review < block <= 1, got {self.review}, {self.block}")

    def decide(self, score: float) -> Decision:
        if score >= self.block:
            return "BLOCK"
        if score >= self.review:
            return "REVIEW"
        return "APPROVE"
