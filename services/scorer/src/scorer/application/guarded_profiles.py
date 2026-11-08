"""Circuit breaker around the profile store (PLAN.md 7.4, experiment F3).

With Redis stopped, every lookup blocked for about four seconds in name resolution, which no socket
timeout covers, so the scorer stalled once per micro-batch instead of degrading. After one failure the
breaker answers "unavailable" at once, which the use case already turns into the degraded policy, and lets
a single probe through every ``open_s`` to find out whether the store is back.
"""
from collections.abc import Callable, Sequence

from scorer.application.ports import ProfileStore, ProfileUnavailable


class BreakerProfiles:
    def __init__(self, inner: ProfileStore, open_s: float, now_s: Callable[[], float]) -> None:
        self._inner = inner
        self._open_s = open_s
        self._now = now_s
        self._retry_at: float | None = None             # None = closed

    def get_many(self, user_ids: Sequence[str]) -> list[dict]:
        if self._retry_at is not None and self._now() < self._retry_at:
            raise ProfileUnavailable("profile store paused after a failure (circuit open)")
        try:
            result = self._inner.get_many(user_ids)
        except ProfileUnavailable:
            self._retry_at = self._now() + self._open_s
            raise
        self._retry_at = None
        return result
