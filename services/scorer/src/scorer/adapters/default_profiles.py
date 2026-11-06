"""M1 profile store: everyone is a new user. Replaced by the Redis store in M2."""
from collections.abc import Sequence

from fraudcore.features import PROFILE_DEFAULTS


class DefaultProfileStore:
    def get_many(self, user_ids: Sequence[str]) -> list[dict]:
        return [dict(PROFILE_DEFAULTS) for _ in user_ids]
