"""Composition root: the only module that knows every concrete class (ADR-0002)."""
import argparse
import signal
import threading

from confluent_kafka import Consumer, Producer

from scorer import config
from scorer.adapters.default_profiles import DefaultProfileStore
from scorer.adapters.kafka_decisions import KafkaDecisionSink
from scorer.adapters.rule_model import RuleModel
from scorer.adapters.system_clock import SystemClock
from scorer.application.score_batch import ScoreBatch
from scorer.entrypoints import kafka_consumer


def main() -> None:
    ap = argparse.ArgumentParser(description="Fraud scorer")
    ap.add_argument("--mode", choices=["champion", "shadow"], default="champion")
    args = ap.parse_args()
    cfg = config.load(mode=args.mode)

    producer = Producer({"bootstrap.servers": cfg.bootstrap, "linger.ms": 1, "compression.type": "lz4",
                         "acks": 1})
    consumer = Consumer({
        "bootstrap.servers": cfg.bootstrap, "group.id": cfg.group, "enable.auto.commit": False,
        "auto.offset.reset": "earliest", "fetch.wait.max.ms": 5,
    })
    consumer.subscribe([cfg.input_topic])

    score = ScoreBatch(
        profiles=DefaultProfileStore(), model=RuleModel(), sink=KafkaDecisionSink(producer, cfg.output_topic),
        thresholds=cfg.thresholds, policy=cfg.policy, clock=SystemClock())

    def to_dlq(raw: bytes, why: str) -> None:
        producer.produce(cfg.dlq_topic, value=raw, headers={"error": why})

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    print(f"scorer[{args.mode}] consuming {cfg.input_topic} → {cfg.output_topic}", flush=True)
    try:
        kafka_consumer.run(consumer, to_dlq, score, cfg.max_messages, cfg.max_wait_s, stop)
    finally:
        producer.flush(5)
        consumer.close()


if __name__ == "__main__":
    main()
