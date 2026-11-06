"""Consume decisions, print percentile summary on idle (PLAN.md §3.12). Single host → single clock."""
import argparse
import os
import time

import orjson
from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, Consumer

from latency.logic import LatencyStats, e2e_ms


def main() -> None:
    ap = argparse.ArgumentParser(description="End-to-end latency collector")
    ap.add_argument("--topic", default="decisions")
    ap.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP", "kafka:29092"))
    ap.add_argument("--idle-timeout", type=float, default=10.0, help="stop after this many idle seconds")
    ap.add_argument("--max-wait", type=float, default=300.0, help="give up if nothing arrives")
    args = ap.parse_args()

    started_ns = time.time_ns()                 # ignore decisions of payments scheduled before we started
    consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": f"latency-{started_ns}",
                         "auto.offset.reset": "latest", "enable.auto.commit": False})
    assigned = False

    def on_assign(_c, partitions) -> None:
        nonlocal assigned
        assigned = True

    consumer.subscribe([args.topic], on_assign=on_assign)
    stats, skipped, last_msg = LatencyStats(), 0, None
    ready_printed = False
    t_start = time.monotonic()
    while True:
        msg = consumer.poll(0.2)
        if assigned and not ready_printed:
            print("READY", flush=True)
            ready_printed = True
        now = time.monotonic()
        if msg is None or msg.error():
            if last_msg is not None and now - last_msg > args.idle_timeout:
                break
            if last_msg is None and now - t_start > args.max_wait:
                break
            continue
        d = orjson.loads(msg.value())
        if d["t_scheduled_ns"] < started_ns:
            skipped += 1
            continue
        last_msg = now
        ts_type, ts_ms = msg.timestamp()
        if ts_type != TIMESTAMP_LOG_APPEND_TIME:
            raise SystemExit("decisions topic must use LogAppendTime (infra/kafka/create-topics.sh)")
        stats.record(e2e_ms(ts_ms, d["t_scheduled_ns"]), (d["t_decided_ns"] - d["t_scheduled_ns"]) / 1e6)
    consumer.close()
    print(orjson.dumps({**stats.summary(), "skipped_before_start": skipped}).decode(), flush=True)


if __name__ == "__main__":
    main()
