"""Composition of the synchronous scoring path, shared by the gRPC server and the REST ``POST /score``.

One wiring, two doors: both call the same ``ScoreSync`` over the same model runtime, so E11 compares the
transports and nothing else.
"""
import time
from dataclasses import dataclass

import redis
from confluent_kafka import Producer

from scorer import config
from scorer.adapters.deadline_profiles import DeadlineProfiles
from scorer.adapters.hot_swap import HotSwapModel
from scorer.adapters.kafka_decisions import FireAndForgetDecisionSink
from scorer.adapters.mlflow_registry import MlflowRegistry
from scorer.adapters.prometheus_metrics import serve as serve_metrics
from scorer.adapters.redis_profiles import RedisProfileStore
from scorer.adapters.redis_velocity import RedisVelocityStore
from scorer.adapters.rule_model import RuleModel
from scorer.adapters.system_clock import SystemClock
from scorer.application.guarded_profiles import BreakerProfiles
from scorer.application.score_batch import ScoreBatch
from scorer.application.score_sync import ScoreSync


@dataclass
class SyncScorer:
    score: ScoreSync
    model: HotSwapModel
    producer: Producer

    def close(self) -> None:
        self.model.stop()
        self.producer.flush(5)


def build_sync_scorer(cfg: config.Settings, metrics_port: int | None) -> SyncScorer:
    producer = Producer({"bootstrap.servers": cfg.bootstrap, "linger.ms": 1, "acks": 1})
    t = cfg.redis_timeout_s
    redis_client = redis.Redis.from_url(cfg.redis_url, socket_timeout=t, socket_connect_timeout=t)
    registry = MlflowRegistry(cfg.tracking_uri, cfg.model_name, cfg.model_alias, cfg.model_cache_dir)
    model = HotSwapModel(registry, cfg.model_name, fallback=RuleModel(), poll_s=cfg.poll_s,
                         intra_op_threads=cfg.onnx_threads)
    model.start()
    kwargs = {"metrics": serve_metrics(metrics_port)} if metrics_port else {}
    batch = ScoreBatch(
        profiles=BreakerProfiles(
            DeadlineProfiles(RedisProfileStore(redis_client), cfg.redis_deadline_s),
            cfg.redis_breaker_open_s, time.monotonic, cfg.redis_breaker_failures), model=model,
        sink=FireAndForgetDecisionSink(producer, cfg.output_topic), thresholds=cfg.thresholds,
        policy=cfg.policy, clock=SystemClock(), log_features=cfg.log_features, **kwargs)
    return SyncScorer(ScoreSync(velocity=RedisVelocityStore(redis_client), batch=batch), model, producer)
