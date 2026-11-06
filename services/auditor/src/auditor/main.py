"""Thin shell: poll Kafka in batches, write to Postgres, commit only after the write (ADR-4)."""
import logging
import os
import signal
import threading
import time

from confluent_kafka import Consumer

from auditor.ingest import ingest
from auditor.rows import TOPICS
from auditor.store import PostgresStore

log = logging.getLogger("auditor")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("BOOTSTRAP", "kafka:29092"), "group.id": "auditor",
        "enable.auto.commit": False, "auto.offset.reset": "earliest",
    })
    consumer.subscribe(list(TOPICS))
    store = PostgresStore(os.environ.get("POSTGRES_DSN", "postgresql://fraud:fraud@postgres:5432/fraud"))
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    total = bad = 0
    print("auditor consuming", ", ".join(TOPICS), flush=True)
    try:
        while not stop.is_set():
            msgs = consumer.consume(num_messages=1000, timeout=0.5)
            batch = [(m.topic(), m.value()) for m in msgs if m.error() is None and m.value() is not None]
            if not batch:
                continue
            result = None
            while result is None and not stop.is_set():      # same batch until it lands: nothing skipped
                try:
                    result = ingest(store, batch)
                except Exception:
                    log.exception("write failed; retrying the same batch")
                    time.sleep(2)
            if result is None:
                break
            consumer.commit(asynchronous=False)
            total, bad = total + result.written, bad + result.bad
            log.info("written=%d bad=%d (total %d)", result.written, result.bad, total)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
