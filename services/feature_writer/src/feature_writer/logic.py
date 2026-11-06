"""Pure: collapse a batch of enriched events into the latest velocity hash per user (last value wins).

``ts`` is the event time of that state: the synchronous scorer uses it to know if the window is still open.
"""
from fraudcore.contracts import EnrichedPayment
from fraudcore.features import VELOCITY
from fraudcore.keys import velocity_key


def latest_per_user(events: list[EnrichedPayment]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for e in events:                                    # arrival order per user = partition order
        d = e.model_dump()
        out[velocity_key(e.user_id)] = {**{f: str(d[f]) for f in VELOCITY}, "payment_id": e.payment_id,
                                "ts": str(e.event_time.timestamp())}
    return out
