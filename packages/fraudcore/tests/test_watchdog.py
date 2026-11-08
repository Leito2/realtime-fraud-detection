import json

from fraudcore.watchdog import ConsumerWatchdog


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def stats(state="up", join="steady"):
    return json.dumps({"cgrp": {"state": state, "join_state": join}})


def test_a_consumer_that_keeps_reporting_steady_never_expires():
    clock = Clock()
    wd = ConsumerWatchdog(max_unhealthy_s=60, clock=clock)
    for _ in range(10):
        clock.now += 30
        wd.on_stats(stats())
    assert not wd.expired()


def test_a_consumer_stuck_rejoining_expires_after_the_limit_not_before():
    """Observed after a broker restart (F5): the group member never re-joined and nothing exited."""
    clock = Clock()
    wd = ConsumerWatchdog(max_unhealthy_s=60, clock=clock)
    wd.on_stats(stats())
    clock.now += 59
    wd.on_stats(stats(state="wait-coord", join="wait-join"))
    assert not wd.expired()
    clock.now += 2
    wd.on_stats(stats(state="up", join="init"))
    assert wd.expired() and "join_state=init" in wd.reason()


def test_recovery_inside_the_limit_resets_the_clock():
    clock = Clock()
    wd = ConsumerWatchdog(max_unhealthy_s=60, clock=clock)
    clock.now += 50
    wd.on_stats(stats(state="down", join="init"))
    clock.now += 5
    wd.on_stats(stats())
    clock.now += 50
    assert not wd.expired()


def test_a_consumer_that_stops_reporting_at_all_also_expires():
    """A main loop blocked in a call never reaches the statistics callback."""
    clock = Clock()
    wd = ConsumerWatchdog(max_unhealthy_s=60, clock=clock)
    clock.now += 61
    assert wd.expired() and "no statistics" in wd.reason()


def test_unreadable_statistics_do_not_count_as_healthy():
    clock = Clock()
    wd = ConsumerWatchdog(max_unhealthy_s=60, clock=clock)
    clock.now += 61
    wd.on_stats("not json")
    assert wd.expired()


def test_guard_adds_statistics_and_exits_when_the_consumer_never_becomes_steady():
    import threading

    from fraudcore.watchdog import guard

    done = threading.Event()
    codes = []
    conf = guard({"group.id": "g"}, max_unhealthy_s=0.05, poll_s=0.01,
                 exit_fn=lambda c: (codes.append(c), done.set()))
    assert conf["group.id"] == "g" and conf["statistics.interval.ms"] == 5000 and callable(conf["stats_cb"])
    assert done.wait(2.0) and codes == [3]
