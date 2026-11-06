"""ExplanationSink over Kafka: best effort, a lost explanation is acceptable (decision already shipped)."""
from fraudcore.contracts import ExplanationEvent


class KafkaExplanationSink:
    def __init__(self, producer, topic: str) -> None:
        self._producer, self._topic = producer, topic

    def publish(self, event: ExplanationEvent) -> None:
        self._producer.produce(self._topic, key=event.payment_id, value=event.model_dump_json())
        self._producer.poll(0)
