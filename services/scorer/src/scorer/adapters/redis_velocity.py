"""VelocityStore over Redis: the Feature Writer's ``vel:{user}`` hashes, one pipelined HGETALL per call."""
from collections.abc import Sequence

import redis

from fraudcore.keys import velocity_key
from scorer.application.ports import ProfileUnavailable

_INT = ("f_cnt_10m", "f_small_tx_cnt_10m")
_FLOAT = ("f_sum_10m", "f_max_10m", "ts")


def parse_velocity(raw: dict[bytes, bytes]) -> dict | None:
    """Hash -> typed snapshot; ``None`` when the user has no state or it predates the ``ts`` field."""
    data = {k.decode(): v.decode() for k, v in raw.items()}
    if not data or "ts" not in data:
        return None
    return {**{f: int(float(data[f])) for f in _INT}, **{f: float(data[f]) for f in _FLOAT}}


class RedisVelocityStore:
    def __init__(self, client: redis.Redis) -> None:
        self._r = client

    def get_many(self, user_ids: Sequence[str]) -> list[dict | None]:
        pipe = self._r.pipeline(transaction=False)
        for uid in user_ids:
            pipe.hgetall(velocity_key(uid))
        try:
            raw = pipe.execute()
        except redis.RedisError as exc:
            raise ProfileUnavailable(str(exc)) from exc
        return [parse_velocity(r) for r in raw]
