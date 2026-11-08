"""A hard deadline for a profile lookup (PLAN.md 7.4, experiment F3).

``socket_timeout`` does not bound name resolution or a connect to a vanished container, so a lookup can
block for seconds even with a 5 ms timeout. The lookup runs on a worker thread and the caller waits at most
``deadline_s``; a call that overruns is abandoned (the worker finishes on its own) and reported unavailable.
"""
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout

from scorer.application.ports import ProfileStore, ProfileUnavailable


class DeadlineProfiles:
    def __init__(self, inner: ProfileStore, deadline_s: float, workers: int = 4) -> None:
        self._inner = inner
        self._deadline_s = deadline_s
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="profile-lookup")

    def get_many(self, user_ids: Sequence[str]) -> list[dict]:
        future = self._pool.submit(self._inner.get_many, list(user_ids))
        try:
            return future.result(timeout=self._deadline_s)
        except FutureTimeout as exc:
            future.cancel()
            ms = self._deadline_s * 1000
            raise ProfileUnavailable(f"profile lookup exceeded its {ms:.0f} ms deadline") from exc
