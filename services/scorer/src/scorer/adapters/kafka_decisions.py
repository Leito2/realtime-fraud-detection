"""DecisionSink over Kafka. Flushes before returning so offsets are committed only after delivery (ADR-4)."""
from collections.abc import Sequence

from fraudcore.contracts import DecisionEvent


class DeliveryFailed(Exception):
    pass


class KafkaDecisionSink:
    def __init__(self, producer, topic: str) -> None:
        self._producer, self._topic = producer, topic

    def publish_batch(self, decisions: Sequence[DecisionEvent]) -> None:
        failed: list[str] = []

        def on_delivery(err, _msg) -> None:
            if err is not None:
                failed.append(str(err))

        for d in decisions:
            self._producer.produce(self._topic, key=d.payment_id, value=d.model_dump_json(),
                                   on_delivery=on_delivery)
            self._producer.poll(0)
        remaining = self._producer.flush(10)
        if failed or remaining:
            raise DeliveryFailed(f"{len(failed)} failed, {remaining} undelivered: {failed[:1]}")
