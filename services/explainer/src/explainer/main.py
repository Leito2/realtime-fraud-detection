"""Composition root: the only place that knows every concrete class."""
import logging
import signal
import threading
import time

from confluent_kafka import Consumer, Producer

from explainer.adapters.kafka_explanations import KafkaExplanationSink
from explainer.adapters.llm import MockLlm, OpenAiCompatLlm
from explainer.adapters.mlflow_booster import MlflowBoosterSource
from explainer.adapters.prometheus_metrics import serve
from explainer.adapters.xgboost_shap import XgboostShap
from explainer.application.explain import ExplainDecision
from explainer.config import Settings, load
from explainer.domain.policy import BackpressurePolicy, CircuitBreaker
from explainer.entrypoints.kafka_consumer import DecisionConsumer


def build_llm(s: Settings):
    if s.provider == "mock":
        return MockLlm()
    return OpenAiCompatLlm(s.llm_base_url, s.llm_model, timeout_s=s.llm_timeout_s)   # ollama, gateway


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    s = load()
    metrics = serve(s.metrics_port)
    explain = ExplainDecision(
        shap=XgboostShap(MlflowBoosterSource(s.tracking_uri, s.model_name)), llm=build_llm(s),
        policy=BackpressurePolicy(s.max_inflight, s.max_age_s),
        breaker=CircuitBreaker(s.breaker_failures, s.breaker_cooldown_s, time.monotonic),
        metrics=metrics)
    consumer = Consumer({"bootstrap.servers": s.bootstrap, "group.id": s.group, "enable.auto.commit": True,
                         "auto.offset.reset": "latest"})
    consumer.subscribe([s.input_topic])
    sink = KafkaExplanationSink(Producer({"bootstrap.servers": s.bootstrap, "linger.ms": 20}), s.output_topic)
    loop = DecisionConsumer(consumer, explain, sink, s.max_inflight, on_inflight=metrics.inflight.set)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    print(f"explainer: {s.input_topic} -> {s.output_topic} (llm={s.provider})", flush=True)
    loop.run(stop)


if __name__ == "__main__":
    main()
