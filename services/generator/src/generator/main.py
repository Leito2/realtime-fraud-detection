"""Open-loop load generator (PLAN.md §3.1, §5.3). I/O shell around ``logic`` and ``schedule``."""
import argparse
import multiprocessing as mp
import os
import random
import time

import orjson
from confluent_kafka import Producer

from generator.logic import Population, next_payment
from generator.schedule import gen_lag_ms, scheduled_ns

GEN_LAG_LIMIT_MS = 5.0      # PLAN.md §5.3: above this p99 the run is invalid


def run_worker(idx: int, args: argparse.Namespace, out: mp.Queue) -> None:
    rate = args.rate / args.procs
    pop = Population(args.users, args.seed, worker=idx, workers=args.procs)
    rng = random.Random(args.seed * 1000 + idx)
    errors = 0

    def on_delivery(err, _msg) -> None:
        nonlocal errors
        if err is not None:
            errors += 1

    producer = Producer({
        "bootstrap.servers": args.bootstrap, "linger.ms": 1, "compression.type": "lz4", "acks": 1,
        "queue.buffering.max.messages": 200_000,
    })
    total = int(args.duration * rate)
    t0 = time.time_ns() + 500_000_000          # let every worker start before the first event is due
    lags: list[float] = []
    for i in range(total):
        due = scheduled_ns(t0, i, rate)
        wait = (due - time.time_ns()) / 1e9
        if wait > 0.0005:
            time.sleep(wait - 0.0003)
        while time.time_ns() < due:             # fine wait; if we are already late, send at once (open loop)
            pass
        sent = time.time_ns()
        event = next_payment(pop, rng, due, sent)
        while True:
            try:
                producer.produce(args.topic, key=event["user_id"], value=orjson.dumps(event),
                                 on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.001)
        producer.poll(0)
        lags.append(gen_lag_ms(due, sent))
    producer.flush(30)
    lags.sort()
    out.put({"sent": total, "errors": errors, "lags": lags})


def main() -> None:
    ap = argparse.ArgumentParser(description="Open-loop payment generator")
    ap.add_argument("--rate", type=float, default=2000, help="target events/s (all processes)")
    ap.add_argument("--duration", type=float, default=30, help="seconds")
    ap.add_argument("--users", type=int, default=100_000)
    ap.add_argument("--procs", type=int, default=1)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--topic", default="payments")
    ap.add_argument("--bootstrap", default=os.environ.get("BOOTSTRAP", "kafka:29092"))
    ap.add_argument("--offline", metavar="PARQUET", help="write a simulated history here instead of Kafka")
    ap.add_argument("--days", type=int, default=60, help="--offline: simulated days")
    ap.add_argument("--payments-per-day", type=int, default=50_000, help="--offline: payments per day")
    args = ap.parse_args()

    if args.offline:
        from datetime import UTC, datetime, timedelta  # lazy: polars is only needed offline

        from generator.offline import generate_history

        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        pop = Population(args.users, args.seed)
        start = today - timedelta(days=args.days)
        df = generate_history(pop, args.seed, start, args.days, args.payments_per_day)
        df.write_parquet(args.offline)
        print(orjson.dumps({"rows": df.height, "path": args.offline}).decode(), flush=True)
        return

    out: mp.Queue = mp.Queue()
    procs = [mp.Process(target=run_worker, args=(i, args, out)) for i in range(args.procs)]
    for p in procs:
        p.start()
    results = [out.get() for _ in procs]
    for p in procs:
        p.join()

    lags = sorted(x for r in results for x in r["lags"])
    p99 = lags[int(0.99 * (len(lags) - 1))] if lags else 0.0
    summary = {"sent": sum(r["sent"] for r in results), "delivery_errors": sum(r["errors"] for r in results),
               "target_rate": args.rate, "gen_lag_p99_ms": round(p99, 3), "valid": p99 <= GEN_LAG_LIMIT_MS}
    print(orjson.dumps(summary).decode(), flush=True)


if __name__ == "__main__":
    main()
