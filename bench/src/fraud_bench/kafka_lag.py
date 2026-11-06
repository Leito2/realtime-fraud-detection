"""Consumer-group lag and topic growth, sampled from the host while the load runs (PLAN.md section 5.4).

Only the scorer group is sampled: it commits after every micro-batch. The Flink group commits on checkpoints
(every 10 s), so its committed offset trails by design; Flink falling behind shows up as lost/late decisions.
"""
from __future__ import annotations

import threading
import time

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from fraud_bench.analysis import lag_stable


def _partitions(consumer: Consumer, topic: str) -> list[int]:
    return sorted(consumer.list_topics(topic, timeout=10).topics[topic].partitions)


def end_offsets_total(bootstrap: str, topic: str) -> int:
    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": "bench-probe"})
    try:
        return sum(probe.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
                   for p in _partitions(probe, topic))
    finally:
        probe.close()


def group_lag(admin: AdminClient, probe: Consumer, group: str, topic: str) -> int:
    parts = [TopicPartition(topic, p) for p in _partitions(probe, topic)]
    future = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, parts)])[group]
    committed = {tp.partition: tp.offset for tp in future.result(timeout=10).topic_partitions}
    lag = 0
    for tp in parts:
        end = probe.get_watermark_offsets(tp, timeout=10)[1]
        lag += max(0, end - max(committed.get(tp.partition, 0), 0))
    return lag


class LagSampler:
    """Hook for ``run_load``: samples the scorer group's lag and counts what lands in the DLQ."""

    def __init__(self, rate: int, bootstrap: str = "localhost:9092", group: str = "scorer-champion",
                 topic: str = "payments-enriched", every_s: float = 2.0) -> None:
        self.rate, self.bootstrap, self.every_s = rate, bootstrap, every_s
        self.group, self.topic = group, topic
        self.samples: list[tuple[float, float]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._dlq_before = 0

    def start(self) -> None:
        self._dlq_before = end_offsets_total(self.bootstrap, "dlq")
        self._t0 = time.monotonic()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        probe = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "bench-probe"})
        try:
            while not self._stop.wait(self.every_s):
                try:
                    self.samples.append((time.monotonic() - self._t0,
                                         group_lag(admin, probe, self.group, self.topic)))
                except Exception:
                    continue                      # a failed sample is a gap, not a result
        finally:
            probe.close()

    def stop(self) -> dict:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=15)
        return {
            "lag_samples": self.samples,
            "lag_stable": lag_stable(self.samples, self.rate),
            "lag_max": max((lag for _, lag in self.samples), default=None),
            "dlq_growth": end_offsets_total(self.bootstrap, "dlq") - self._dlq_before,
        }
