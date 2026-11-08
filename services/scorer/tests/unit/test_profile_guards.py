import threading
import time

import pytest
from scorer.adapters.deadline_profiles import DeadlineProfiles
from scorer.application.guarded_profiles import BreakerProfiles
from scorer.application.ports import ProfileUnavailable


class Inner:
    def __init__(self):
        self.calls = 0
        self.fail = False

    def get_many(self, user_ids):
        self.calls += 1
        if self.fail:
            raise ProfileUnavailable("down")
        return [{"u": u} for u in user_ids]


class Clock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


def test_a_healthy_store_is_passed_through():
    inner = Inner()
    breaker = BreakerProfiles(inner, open_s=1.0, now_s=Clock(), failures=1)
    assert breaker.get_many(["a", "b"]) == [{"u": "a"}, {"u": "b"}] and inner.calls == 1


def test_after_a_failure_the_store_is_not_called_again_until_the_pause_is_over():
    """With Redis stopped every call blocked ~4 s in DNS (F3): after one failure, do not pay that again."""
    inner, clock = Inner(), Clock()
    breaker = BreakerProfiles(inner, open_s=1.0, now_s=clock, failures=1)
    inner.fail = True
    with pytest.raises(ProfileUnavailable):
        breaker.get_many(["a"])
    for _ in range(5):
        clock.t += 0.1
        with pytest.raises(ProfileUnavailable):
            breaker.get_many(["a"])
    assert inner.calls == 1


def test_one_probe_after_the_pause_closes_the_breaker_when_it_succeeds():
    inner, clock = Inner(), Clock()
    breaker = BreakerProfiles(inner, open_s=1.0, now_s=clock, failures=1)
    inner.fail = True
    with pytest.raises(ProfileUnavailable):
        breaker.get_many(["a"])
    inner.fail = False
    clock.t += 1.01
    assert breaker.get_many(["a"]) == [{"u": "a"}]
    assert breaker.get_many(["b"]) == [{"u": "b"}] and inner.calls == 3


def test_a_failed_probe_opens_the_breaker_again():
    inner, clock = Inner(), Clock()
    breaker = BreakerProfiles(inner, open_s=1.0, now_s=clock, failures=1)
    inner.fail = True
    with pytest.raises(ProfileUnavailable):
        breaker.get_many(["a"])
    clock.t += 1.01
    with pytest.raises(ProfileUnavailable):
        breaker.get_many(["a"])                          # the probe
    clock.t += 0.5
    with pytest.raises(ProfileUnavailable):
        breaker.get_many(["a"])                          # still paused
    assert inner.calls == 2


def test_a_call_that_outlasts_its_deadline_becomes_unavailable_without_waiting_for_it():
    release = threading.Event()

    class Stuck:
        def get_many(self, user_ids):
            release.wait(5)
            return []

    guarded = DeadlineProfiles(Stuck(), deadline_s=0.05)
    t = time.perf_counter()
    with pytest.raises(ProfileUnavailable):
        guarded.get_many(["a"])
    assert time.perf_counter() - t < 1.0
    release.set()


def test_a_fast_call_and_the_inner_stores_own_errors_pass_through():
    inner = Inner()
    guarded = DeadlineProfiles(inner, deadline_s=1.0)
    assert guarded.get_many(["a"]) == [{"u": "a"}]
    inner.fail = True
    with pytest.raises(ProfileUnavailable, match="down"):
        guarded.get_many(["a"])


def test_a_single_blip_does_not_open_the_breaker_but_consecutive_failures_do():
    inner, clock = Inner(), Clock()
    breaker = BreakerProfiles(inner, open_s=1.0, now_s=clock, failures=3)
    inner.fail = True
    for _ in range(2):
        with pytest.raises(ProfileUnavailable):
            breaker.get_many(["a"])
    inner.fail = False
    assert breaker.get_many(["a"]) == [{"u": "a"}]          # the streak was reset by a success
    inner.fail = True
    for _ in range(3):
        with pytest.raises(ProfileUnavailable):
            breaker.get_many(["a"])
    calls = inner.calls
    with pytest.raises(ProfileUnavailable):
        breaker.get_many(["a"])
    assert inner.calls == calls                              # open: no call reached the store
