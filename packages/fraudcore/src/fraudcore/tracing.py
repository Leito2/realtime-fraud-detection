"""W3C trace context (traceparent) helpers, shared by generator and scorer (PLAN.md section 6.6)."""
import random
from dataclasses import dataclass

_HEX = set("0123456789abcdef")


@dataclass(frozen=True)
class Traceparent:
    trace_id: str
    span_id: str
    sampled: bool


def new_traceparent(rng: random.Random) -> str:
    """A fresh sampled context. Uses the caller's RNG so a seeded load run is reproducible."""
    trace_id = f"{rng.getrandbits(128) or 1:032x}"
    span_id = f"{rng.getrandbits(64) or 1:016x}"
    return f"00-{trace_id}-{span_id}-01"


def parse_traceparent(value: str | bytes | None) -> Traceparent | None:
    """Parse ``00-<trace>-<span>-<flags>``; anything malformed (or all-zero ids) is not a context."""
    if isinstance(value, bytes):
        value = value.decode("ascii", errors="replace")
    if not value:
        return None
    parts = value.split("-")
    sizes = [2, 32, 16, 2]
    if len(parts) != 4 or parts[0] != "00" or [len(p) for p in parts] != sizes:
        return None
    if not all(set(p) <= _HEX for p in parts[1:]) or int(parts[1], 16) == 0 or int(parts[2], 16) == 0:
        return None
    return Traceparent(parts[1], parts[2], bool(int(parts[3], 16) & 1))
