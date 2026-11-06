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
    max_messages: int
    max_wait_s: float
    thresholds: Thresholds
    policy: DegradedPolicy


def load(config_dir: Path | None = None, mode: str = "champion") -> Settings:
    d = Path(config_dir or os.environ.get("CONFIG_DIR", "config"))
    scorer = yaml.safe_load((d / "scorer.yaml").read_text(encoding="utf-8"))
    th = yaml.safe_load((d / "thresholds.yaml").read_text(encoding="utf-8"))
    kafka = scorer["kafka"]
    shadow = mode == "shadow"
    deg = th["degraded"]
    return Settings(
        bootstrap=os.environ.get("BOOTSTRAP", kafka["bootstrap"]),
        group=f"scorer-{mode}",
        input_topic=kafka["input_topic"],
        output_topic=kafka["shadow_topic"] if shadow else kafka["output_topic"],
        dlq_topic=kafka["dlq_topic"],
        redis_url=os.environ.get("REDIS_URL", scorer["redis"]["url"]),
        redis_timeout_s=scorer["redis"]["timeout_ms"] / 1000,
        max_messages=scorer["batch"]["max_messages"],
        max_wait_s=scorer["batch"]["max_wait_ms"] / 1000,
        thresholds=Thresholds(th["threshold_set"], th["review"], th["block"]),
        policy=DegradedPolicy(Thresholds(f"{th['threshold_set']}-degraded", deg["review"], deg["block"]),
                              deg["max_auto_approve_amount"]),
    )
