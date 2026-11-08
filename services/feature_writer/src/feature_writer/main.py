"""payments-enriched -> Redis `vel:{user}` in pipelines; own consumer group, off the critical path."""
import argparse
import os
import signal
import threading

import redis
from confluent_kafka import Consumer
from pydantic import ValidationError

from feature_writer.logic import latest_per_user
from fraudcore.contracts import EnrichedPayment
from fraudcore.keys import VELOCITY_TTL_S
from fraudcore.watchdog import guard


def main() -> None:
    ap = argparse.ArgumentParser(description="Feature writer")
    ap.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP", "kafka:29092"))
    ap.add_argument("--redis", default=os.environ.get("REDIS_URL", "redis://redis:6379/0"))
    ap.add_argument("--topic", default="payments-enriched")
    ap.add_argument("--max-messages", type=int, default=500)
    ap.add_argument("--max-wait-s", type=float, default=0.05)
    args = ap.parse_args()

    consumer = Consumer(guard({"bootstrap.servers": args.bootstrap, "group.id": "feature-writer",
                               "enable.auto.commit": False, "auto.offset.reset": "latest"}))
    consumer.subscribe([args.topic])
    r = redis.Redis.from_url(args.redis)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    print(f"feature_writer consuming {args.topic}", flush=True)
    while not stop.is_set():
        msgs = consumer.consume(num_messages=args.max_messages, timeout=args.max_wait_s)
        events = []
        for m in msgs:
            if m.error():
                continue
            try:
                events.append(EnrichedPayment.model_validate_json(m.value()))
            except ValidationError:
                continue                                # the scorer already routes bad events to the DLQ
        if events:
            pipe = r.pipeline(transaction=False)
            for key, mapping in latest_per_user(events).items():
                pipe.hset(key, mapping=mapping)
                pipe.expire(key, VELOCITY_TTL_S)
            pipe.execute()
        if msgs:
            consumer.commit(asynchronous=False)         # idempotent writes: replays are harmless
    consumer.close()


if __name__ == "__main__":
    main()
