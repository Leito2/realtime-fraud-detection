"""Liveness judgement for a Kafka group member (PLAN.md 7, experiment F5).

After the broker was restarted, some consumers (librdkafka) sat in "rejoining group" for ever: no error, no
exit, no progress, and the orchestrator had no reason to restart them. The fix is not to guess why the client
wedged but to make the wedge visible: the client's own statistics say whether the member is a steady member of
its group, and a member that has not been one for ``max_unhealthy_s`` is restarted by the orchestrator.

This module only judges; it does no I/O, so the rule is unit-tested without a broker. The services wire it to
``statistics.interval.ms`` / ``stats_cb`` and exit when it expires.
"""
from __future__ import annotations

import json
import time
from collections.abc import Callable


class ConsumerWatchdog:
    def __init__(self, max_unhealthy_s: float = 60.0, clock: Callable[[], float] = time.monotonic):
        self.max_unhealthy_s = max_unhealthy_s
        self._clock = clock
        self._last_healthy = clock()                     # the start-up grace period is one full window
        self._last_stats = clock()
        self._last_seen = "no statistics yet"

    def on_stats(self, raw: str | bytes) -> None:
        """Feed it the JSON string librdkafka passes to ``stats_cb``."""
        now = self._clock()
        try:
            cgrp = json.loads(raw)["cgrp"]
            state, join = cgrp["state"], cgrp["join_state"]
        except (ValueError, KeyError, TypeError):
            return                                       # unreadable is not healthy: the clock keeps running
        self._last_stats = now
        self._last_seen = f"state={state} join_state={join}"
        if state == "up" and join == "steady":
            self._last_healthy = now

    def expired(self) -> bool:
        return self._clock() - self._last_healthy > self.max_unhealthy_s

    def reason(self) -> str:
        silent = self._clock() - self._last_stats
        if silent > self.max_unhealthy_s:
            return f"no statistics for {silent:.0f} s (the consume loop is blocked)"
        return f"not a steady group member for over {self.max_unhealthy_s:.0f} s ({self._last_seen})"


def guard(conf: dict, max_unhealthy_s: float = 60.0, exit_fn: Callable[[int], None] | None = None,
          poll_s: float = 5.0) -> dict:
    """Return ``conf`` extended so the process exits (code 3) when its consumer wedges.

    Adds ``statistics.interval.ms`` and a ``stats_cb`` feeding a :class:`ConsumerWatchdog`, and starts a
    daemon thread that exits when the watchdog expires. The thread, not the callback, decides, so a consume
    loop blocked in some call (which never reaches the callback) is caught too. ``exit_fn`` is injectable.
    """
    import os
    import sys
    import threading

    wd = ConsumerWatchdog(max_unhealthy_s)
    do_exit = exit_fn or os._exit

    def watch() -> None:
        while True:
            time.sleep(poll_s)
            if wd.expired():
                print(f"consumer watchdog: {wd.reason()}; exiting so the orchestrator restarts this service",
                      file=sys.stderr, flush=True)
                do_exit(3)
                return

    threading.Thread(target=watch, name="consumer-watchdog", daemon=True).start()
    # A dead member is only noticed after session.timeout.ms; librdkafka defaults to 45 s, which was the whole
    # silence seen in F2 with one of two scorers killed. 10 s with a 2 s heartbeat tolerates a short stall.
    return {"session.timeout.ms": 10000, "heartbeat.interval.ms": 2000, **conf,
            "statistics.interval.ms": 5000, "stats_cb": wd.on_stats}
