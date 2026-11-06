"""Loads config/explainer.yaml + env into typed settings. Outer ring: the core only receives the values."""
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

PROVIDERS = {"mock", "ollama", "gateway"}


@dataclass(frozen=True)
class Settings:
    bootstrap: str
    input_topic: str
    output_topic: str
    group: str
    tracking_uri: str
    model_name: str
    max_inflight: int
    max_age_s: float
    breaker_failures: int
    breaker_cooldown_s: float
    provider: str
    llm_base_url: str
    llm_model: str
    llm_timeout_s: float
    metrics_port: int


def load(config_dir: Path | None = None) -> Settings:
    d = Path(config_dir or os.environ.get("CONFIG_DIR", "config"))
    cfg = yaml.safe_load((d / "explainer.yaml").read_text(encoding="utf-8"))
    kafka, llm, policy, breaker = cfg["kafka"], cfg["llm"], cfg["policy"], cfg["breaker"]
    provider = os.environ.get("LLM_PROVIDER", llm["provider"])
    if provider not in PROVIDERS:
        raise ValueError(f"LLM_PROVIDER must be one of {sorted(PROVIDERS)}, got {provider!r}")
    return Settings(
        bootstrap=os.environ.get("BOOTSTRAP", kafka["bootstrap"]),
        input_topic=kafka["input_topic"], output_topic=kafka["output_topic"], group=kafka["group"],
        tracking_uri=os.environ.get("MLFLOW_TRACKING_URI", cfg["mlflow"]["tracking_uri"]),
        model_name=cfg["mlflow"]["model_name"],
        max_inflight=int(policy["max_inflight"]), max_age_s=float(policy["max_age_s"]),
        breaker_failures=int(breaker["failures"]), breaker_cooldown_s=float(breaker["cooldown_s"]),
        provider=provider,
        llm_base_url=os.environ.get("LLM_BASE_URL", llm["base_url"]),
        llm_model=os.environ.get("LLM_MODEL", llm["model"]),
        llm_timeout_s=float(llm["timeout_s"]),
        metrics_port=int(os.environ.get("METRICS_PORT", cfg["metrics_port"])),
    )
