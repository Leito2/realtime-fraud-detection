import pytest

from fraudcore.thresholds import Thresholds


def test_decide_uses_half_open_bands():
    t = Thresholds(name="v1", review=0.3, block=0.8)
    assert t.decide(0.0) == "APPROVE"
    assert t.decide(0.2999) == "APPROVE"
    assert t.decide(0.3) == "REVIEW"
    assert t.decide(0.7999) == "REVIEW"
    assert t.decide(0.8) == "BLOCK"
    assert t.decide(1.0) == "BLOCK"


def test_review_threshold_must_be_below_block():
    with pytest.raises(ValueError):
        Thresholds(name="bad", review=0.9, block=0.5)
