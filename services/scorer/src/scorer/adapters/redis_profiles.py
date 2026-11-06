"""ProfileStore over Redis: one pipelined HGETALL per micro-batch (PLAN.md ADR-2)."""
from collections.abc import Sequence

import redis

from fraudcore.keys import profile_key
from scorer.application.ports import ProfileUnavailable

_TEXT_FIELDS = {"home_country"}


def parse_profile(raw: dict[bytes, bytes]) -> dict:
    out: dict = {}
    for k, v in raw.items():
        name, value = k.decode(), v.decode()
        out[name] = value if name in _TEXT_FIELDS else float(value)
    return out


class RedisProfileStore:
    def __init__(self, client: redis.Redis) -> None:
        self._r = client

    def get_many(self, user_ids: Sequence[str]) -> list[dict]:
        pipe = self._r.pipeline(transaction=False)
        for uid in user_ids:
            pipe.hgetall(profile_key(uid))
        try:
            raw = pipe.execute()
        except redis.RedisError as exc:                    # timeouts, lost connection: hidden from the core
            raise ProfileUnavailable(str(exc)) from exc
        return [parse_profile(r) for r in raw]
