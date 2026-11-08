"""Composition root: the only module that knows every concrete class (ADR-0002)."""
import argparse
import asyncio
import logging
import signal
import threading
import time

import redis
from confluent_kafka import Consumer, Producer

from fraudcore.watchdog import guard
from scorer import config
from scorer.adapters.deadline_profiles import DeadlineProfiles
from scorer.adapters.hot_swap import HotSwapModel
from scorer.adapters.kafka_decisions import KafkaDecisionSink
from scorer.adapters.mlflow_registry import MlflowRegistry
from scorer.adapters.otel_tracer import from_env as tracer_from_env
from scorer.adapters.postgres_decisions import PostgresDecisionLookup
from scorer.adapters.prometheus_metrics import serve as serve_metrics
from scorer.adapters.redis_profiles import RedisProfileStore
from scorer.adapters.rule_model import RuleModel
from scorer.adapters.system_clock import SystemClock
from scorer.application.guarded_profiles import BreakerProfiles
from scorer.application.score_batch import ScoreBatch
from scorer.entrypoints import grpc_server, kafka_consumer
from scorer.serving import runtime_builder
from scorer.sync_wiring import build_sync_scorer


def main() -> None:
    ap = argparse.ArgumentParser(description="Fraud scorer")
    ap.add_argument("--mode", choices=["champion", "shadow", "grpc"], default="champion",
                    help="grpc: synchronous scoring API (PLAN.md 14) instead of the Kafka consumer")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    cfg = config.load(mode="champion" if args.mode == "grpc" else args.mode)
    if args.mode == "grpc":
        return serve_grpc(cfg)

    producer = Producer({"bootstrap.servers": cfg.bootstrap, "linger.ms": 1, "compression.type": "lz4",
                         "acks": 1})
    consumer = Consumer(guard({
        "bootstrap.servers": cfg.bootstrap, "group.id": cfg.group, "enable.auto.commit": False,
        "auto.offset.reset": "earliest", "fetch.wait.max.ms": 5,
        "isolation.level": cfg.isolation_level,        # read_committed with a transactional Flink sink (F10)
        "partition.assignment.strategy": "cooperative-sticky",  # peers keep their partitions (F2)
    }))
    consumer.subscribe([cfg.input_topic])

    t = cfg.redis_timeout_s
    redis_client = redis.Redis.from_url(cfg.redis_url, socket_timeout=t, socket_connect_timeout=t)
    # The rule model serves until the registry has a champion; then the poller swaps it in, no restart.
    registry = MlflowRegistry(cfg.tracking_uri, cfg.model_name, cfg.model_alias, cfg.model_cache_dir)
    model = HotSwapModel(registry, cfg.model_name, fallback=RuleModel(), poll_s=cfg.poll_s,
                         intra_op_threads=cfg.onnx_threads, build=runtime_builder(cfg))
    model.start()
    metrics = serve_metrics(cfg.metrics_port)
    score = ScoreBatch(
        profiles=BreakerProfiles(
            DeadlineProfiles(RedisProfileStore(redis_client), cfg.redis_deadline_s),
            cfg.redis_breaker_open_s, time.monotonic, cfg.redis_breaker_failures), model=model,
        sink=KafkaDecisionSink(producer, cfg.output_topic),
        thresholds=cfg.thresholds, policy=cfg.policy, clock=SystemClock(), metrics=metrics,
        log_features=cfg.log_features and args.mode == "champion",
        tracer=tracer_from_env(f"scorer-{args.mode}"))

    def to_dlq(raw: bytes, why: str) -> None:
        producer.produce(cfg.dlq_topic, value=raw, headers={"error": why})

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    print(f"scorer[{args.mode}] consuming {cfg.input_topic} → {cfg.output_topic}", flush=True)
    try:
        kafka_consumer.run(consumer, to_dlq, score, cfg.max_messages, cfg.max_wait_s, stop)
    finally:
        model.stop()
        producer.flush(5)
        consumer.close()


def serve_grpc(cfg: config.Settings) -> None:
    """Same model runtime, features and thresholds as the Kafka path, behind gRPC (see sync_wiring)."""
    sync = build_sync_scorer(cfg, cfg.grpc_metrics_port)
    lookup = PostgresDecisionLookup(cfg.postgres_dsn) if cfg.postgres_dsn else None
    servicer = grpc_server.FraudScoringServicer(sync.score, lookup, cfg.grpc_stream_max,
                                                cfg.grpc_stream_wait_s, cfg.grpc_workers)

    async def run() -> None:
        server = grpc_server.build_server(servicer, cfg.grpc_api_key)
        server.add_insecure_port(f"0.0.0.0:{cfg.grpc_port}")
        await server.start()
        await server.health_status.set("", grpc_server.SERVING)
        await server.health_status.set(server.service_name, grpc_server.SERVING)
        print(f"scorer[grpc] serving {server.service_name} on :{cfg.grpc_port}", flush=True)
        loop = asyncio.get_running_loop()
        stopped = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stopped.set)
            except NotImplementedError:                      # Windows event loops
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stopped.set))
        await stopped.wait()
        await server.health_status.enter_graceful_shutdown()
        await server.stop(grace=5)

    try:
        asyncio.run(run())
    finally:
        sync.close()


if __name__ == "__main__":
    main()
