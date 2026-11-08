"""Loads config/*.yaml + env into typed settings. Outer ring: the core only receives the resulting values."""
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from fraudcore.thresholds import Thresholds
from scorer.domain.degraded import DegradedPolicy


@dataclass(frozen=True)
class Settings:
    bootstrap: str
    group: str
    input_topic: str
    output_topic: str
    dlq_topic: str
    redis_url: str
    redis_timeout_s: float
    redis_deadline_s: float
    redis_breaker_open_s: float
    max_messages: int
    max_wait_s: float
    tracking_uri: str
    model_name: str
    model_alias: str
    poll_s: float
    onnx_threads: int
    model_cache_dir: Path | None
    metrics_port: int
    log_features: bool | str
    grpc_port: int
    grpc_stream_max: int
    grpc_stream_wait_s: float
    grpc_workers: int
    grpc_metrics_port: int
    grpc_api_key: str | None
    postgres_dsn: str | None
    thresholds: Thresholds
    policy: DegradedPolicy


def _log_features(default: bool | str) -> bool | str:
    """LOG_FEATURES: 0/false; 1/true/all (every decision: feedback dataset); flagged (REVIEW/BLOCK only)."""
    raw = os.environ.get("LOG_FEATURES")
    if not raw:
        return default
    raw = raw.lower()
    return "flagged" if raw == "flagged" else raw in {"1", "true", "yes", "all"}


def load(config_dir: Path | None = None, mode: str = "champion") -> Settings:
    d = Path(config_dir or os.environ.get("CONFIG_DIR", "config"))
    scorer = yaml.safe_load((d / "scorer.yaml").read_text(encoding="utf-8"))
    th = yaml.safe_load((d / "thresholds.yaml").read_text(encoding="utf-8"))
    kafka = scorer["kafka"]
    shadow = mode == "shadow"
    grpc = scorer["grpc"]
    deg = th["degraded"]
    return Settings(
        bootstrap=os.environ.get("BOOTSTRAP", kafka["bootstrap"]),
        group=f"scorer-{mode}",
        input_topic=kafka["input_topic"],
        output_topic=kafka["shadow_topic"] if shadow else kafka["output_topic"],
        dlq_topic=kafka["dlq_topic"],
        redis_url=os.environ.get("REDIS_URL", scorer["redis"]["url"]),
        redis_timeout_s=scorer["redis"]["timeout_ms"] / 1000,
        redis_deadline_s=scorer["redis"].get("deadline_ms", 25) / 1000,
        redis_breaker_open_s=scorer["redis"].get("breaker_open_s", 1.0),
        max_messages=int(os.environ.get("BATCH_MAX_MESSAGES") or scorer["batch"]["max_messages"]),
        max_wait_s=float(os.environ.get("BATCH_MAX_WAIT_MS") or scorer["batch"]["max_wait_ms"]) / 1000,
        tracking_uri=os.environ.get("MLFLOW_TRACKING_URI", scorer["mlflow"]["tracking_uri"]),
        model_name=scorer["model"]["name"],
        model_alias=scorer["model"]["challenger_alias" if shadow else "champion_alias"],
        poll_s=float(scorer["model"]["poll_seconds"]),
        onnx_threads=int(scorer["onnx"]["intra_op_num_threads"]),
        log_features=_log_features(scorer.get("log_features", False)),
        grpc_port=int(os.environ.get("GRPC_PORT", grpc["port"])),
        grpc_stream_max=int(grpc["stream_max_messages"]),
        grpc_stream_wait_s=float(grpc["stream_max_wait_ms"]) / 1000,
        grpc_workers=int(grpc["workers"]),
        grpc_metrics_port=int(os.environ.get("GRPC_METRICS_PORT", grpc["metrics_port"])),
        grpc_api_key=os.environ.get("GRPC_API_KEY") or None,
        postgres_dsn=os.environ.get("POSTGRES_DSN") or None,
        metrics_port=int(os.environ.get("METRICS_PORT", 9101 if shadow else 9100)),
        model_cache_dir=Path(os.environ["MODEL_CACHE_DIR"]) if os.environ.get("MODEL_CACHE_DIR") else None,
        thresholds=Thresholds(th["threshold_set"], th["review"], th["block"]),
        policy=DegradedPolicy(Thresholds(f"{th['threshold_set']}-degraded", deg["review"], deg["block"]),
                              deg["max_auto_approve_amount"]),
    )
