"""Pure helpers: profile row -> Redis hash, and chunking for pipelines."""
from collections.abc import Iterable, Iterator
from itertools import islice

from fraudcore.keys import profile_key


def to_redis(row: dict) -> tuple[str, dict[str, str]]:
    mapping = {k: str(v) for k, v in row.items() if k != "user_id" and v is not None}
    return profile_key(row["user_id"]), mapping


def batched[T](items: Iterable[T], n: int) -> Iterator[list[T]]:
    it = iter(items)
    while chunk := list(islice(it, n)):
        yield chunk
