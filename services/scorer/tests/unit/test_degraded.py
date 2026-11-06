from scorer.domain.degraded import DegradedPolicy

from fraudcore.thresholds import Thresholds

POLICY = DegradedPolicy(thresholds=Thresholds("deg", review=0.2, block=0.7), max_auto_approve_amount=500)


def test_uses_the_conservative_thresholds():
    assert POLICY.decide(score=0.25, amount=10) == "REVIEW"       # would be APPROVE at review=0.3
    assert POLICY.decide(score=0.75, amount=10) == "BLOCK"


def test_large_amounts_are_never_auto_approved():
    assert POLICY.decide(score=0.01, amount=500) == "REVIEW"
    assert POLICY.decide(score=0.01, amount=499.99) == "APPROVE"


def test_block_is_kept_for_large_amounts():
    assert POLICY.decide(score=0.9, amount=10_000) == "BLOCK"
