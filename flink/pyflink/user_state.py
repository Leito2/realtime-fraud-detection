"""Option B of spike S1 (PLAN.md 3.3): all velocity windows from one per-user buffer.

A PyFlink ``KeyedProcessFunction`` keyed by ``user_id`` keeps one ``UserWindows`` per key and calls
``add`` for each payment. Flink SQL cannot give a 10-minute and a 1-hour OVER window in one job;
this does, exactly, in one pass, at the price of running Python inside the operator. The class has
no Flink import so it is unit-tested (``tests/test_option_b_state.py``) and timed
(``fraud_bench.option_b``) without a cluster. The windows include the current payment and the exact
boundary, like the SQL job and ``fraudcore.features.velocity_reference``.
"""
from __future__ import annotations

from collections import deque
from itertools import islice

SMALL_TX_AMOUNT = 10.0                                  # pinned to flink/sql/*.sql by tests/test_features
WINDOWS_S = {"10m": 600, "1h": 3600, "24h": 86400}


class UserWindows:
    """One user's payments over the last 24 h, with a running aggregate per window."""

    def __init__(self) -> None:
        self._events: deque[tuple[float, float]] = deque()          # (ts, amount), oldest first
        self._start = dict.fromkeys(WINDOWS_S, 0)                    # oldest event index per window
        self._cnt = dict.fromkeys(WINDOWS_S, 0)
        self._sum = dict.fromkeys(WINDOWS_S, 0.0)
        self._small = dict.fromkeys(WINDOWS_S, 0)
        self._dropped = 0                                            # events already popped from the left

    def size(self) -> int:
        return len(self._events)

    def add(self, ts: float, amount: float) -> dict:
        self._events.append((ts, amount))
        for name, span in WINDOWS_S.items():
            self._cnt[name] += 1
            self._sum[name] += amount
            self._small[name] += amount < SMALL_TX_AMOUNT
            lo = self._start[name]
            while True:                                              # expire what left this window
                t, a = self._events[lo - self._dropped]
                if t >= ts - span:
                    break
                self._cnt[name] -= 1
                self._sum[name] -= a
                self._small[name] -= a < SMALL_TX_AMOUNT
                lo += 1
            self._start[name] = lo
        while self._start["24h"] > self._dropped:                    # nothing older than 24 h is needed
            self._events.popleft()
            self._dropped += 1
        return self._features()

    def _features(self) -> dict:
        row = {}
        for name in WINDOWS_S:
            row[f"f_cnt_{name}"] = self._cnt[name]
            row[f"f_sum_{name}"] = float(self._sum[name])
            row[f"f_small_tx_cnt_{name}"] = self._small[name]
            lo = self._start[name] - self._dropped
            row[f"f_max_{name}"] = float(max(a for _, a in islice(self._events, lo, None)))
        return row
