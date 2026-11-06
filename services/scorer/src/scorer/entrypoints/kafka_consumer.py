"""Driving adapter: micro-batch consume loop (PLAN.md §3.6). Kafka concepts (offsets, DLQ) stay here."""
import threading
from collections.abc import Callable

from pydantic import ValidationError

from fraudcore.contracts import EnrichedPayment


def run(consumer, dlq: Callable[[bytes, str], None], score: Callable[[list[EnrichedPayment]], object],
        max_messages: int, max_wait_s: float, stop: threading.Event) -> None:
    while not stop.is_set():
        msgs = consumer.consume(num_messages=max_messages, timeout=max_wait_s)
        if not msgs:
            continue
        events: list[EnrichedPayment] = []
        for m in msgs:
            if m.error():
                continue
            try:
                events.append(EnrichedPayment.model_validate_json(m.value()))
            except ValidationError as exc:
                dlq(m.value(), f"invalid enriched payment: {exc.error_count()} errors")
        if events:
            score(events)                      # raises if delivery failed → offsets stay uncommitted
        consumer.commit(asynchronous=False)    # only after decisions were flushed (at-least-once, ADR-4)
