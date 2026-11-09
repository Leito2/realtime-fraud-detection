"""F5 isolation: after a broker restart, which stage is silent and for how long.

Runs 200 ev/s for 40 s, stops Kafka for 20 s, starts it, and samples the end offsets of ``payments``,
``payments-enriched`` and ``decisions`` every 2 s for 170 s. The stage that stays flat longest is the one
that rejoins slowly (producer -> Flink -> scorer). Needs the stack up; run it on a quiet machine.

    uv run python scripts/f5_isolate.py > docs/results/f5-isolate.md
"""

import subprocess
import sys
import threading
import time

from confluent_kafka import Consumer, TopicPartition
from fraud_bench.stack import COMPOSE, ROOT, TOOLS

BOOTSTRAP = "localhost:9092"
TOPICS = ["payments", "payments-enriched", "decisions"]
EVERY = 2.0
TOTAL_S = 170


def sample(rows: list, stop: threading.Event) -> None:
    """One long-lived consumer with short timeouts, so a dead broker costs 1.5 s per probe, not 10 s."""
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "f5-isolate",
                         "enable.auto.commit": False})
    start, last = time.monotonic(), dict.fromkeys(TOPICS)
    while not stop.is_set():
        tick = time.monotonic()
        row = {"t": round(tick - start)}
        for topic in TOPICS:
            try:
                parts = consumer.list_topics(topic, timeout=1.5).topics[topic].partitions
                now = sum(consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=1.5)[1]
                          for p in parts)
            except Exception:
                now = None
            prev = last[topic]
            row[topic] = "down" if now is None else ("-" if prev is None else now - prev)
            if now is not None:
                last[topic] = now
        row["dt"] = time.monotonic() - tick
        rows.append(row)
        stop.wait(max(0.0, EVERY - row["dt"]))


def main() -> None:
    subprocess.run([*COMPOSE, "--profile", "core", "up", "-d", "--no-deps", "kafka"], cwd=ROOT, check=False)
    load = subprocess.Popen([*TOOLS, "generator", "--rate", "200", "--duration", str(TOTAL_S),
                             "--users", "20000", "--zipf", "0.7", "--stream-seed", "5"], cwd=ROOT,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    rows: list = []
    stop = threading.Event()
    t0 = time.monotonic()
    probe = threading.Thread(target=sample, args=(rows, stop), daemon=True)
    probe.start()
    time.sleep(40)
    fault_at = round(time.monotonic() - t0)
    try:
        subprocess.run([*COMPOSE, "stop", "kafka"], cwd=ROOT, check=False, capture_output=True)
        time.sleep(20)
    finally:                                                 # never leave the broker down
        subprocess.run([*COMPOSE, "start", "kafka"], cwd=ROOT, check=False, capture_output=True)
    restarted_at = round(time.monotonic() - t0)
    load.wait(timeout=TOTAL_S + 60)
    time.sleep(10)
    stop.set()
    probe.join(5)
    print(f"# F5 isolation (stop at t={fault_at} s, start returned at t={restarted_at} s)\n")
    print("| t (s) | payments (+records) | payments-enriched (+records) | decisions (+records) |")
    print("|---|---|---|---|")
    for r in rows:
        print(f"| {r['t']} | {r['payments']} | {r['payments-enriched']} | {r['decisions']} |")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
