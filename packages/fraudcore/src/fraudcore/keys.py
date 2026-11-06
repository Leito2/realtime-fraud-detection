"""Redis key layout (PLAN.md §3.5). Shared so the writer, the builder and the readers cannot drift apart."""

VELOCITY_TTL_S = 24 * 3600


def profile_key(user_id: str) -> str:
    return f"profile:{user_id}"


def velocity_key(user_id: str) -> str:
    return f"vel:{user_id}"
