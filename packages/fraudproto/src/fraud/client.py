"""Client helpers for FraudScoring (PLAN.md 14.3): a reused channel, retries only on UNAVAILABLE, keepalive.

A channel is expensive and multiplexes many calls, so callers create one and keep it. Every call carries a
deadline: the checkout path must not wait for an answer that arrives too late to use.
"""
import json

import grpc

DEFAULT_DEADLINE_S = 0.05

SERVICE_CONFIG = json.dumps({"methodConfig": [{
    "name": [{"service": "fraud.v1.FraudScoring"}],
    "retryPolicy": {"maxAttempts": 3, "initialBackoff": "0.005s", "maxBackoff": "0.05s",
                    "backoffMultiplier": 2,
                    "retryableStatusCodes": ["UNAVAILABLE"]},         # never DEADLINE_EXCEEDED
}]})

CHANNEL_OPTIONS = [
    ("grpc.enable_retries", 1),
    ("grpc.service_config", SERVICE_CONFIG),
    ("grpc.keepalive_time_ms", 30_000),
    ("grpc.keepalive_timeout_ms", 5_000),
    ("grpc.keepalive_permit_without_calls", 1),
]


def api_key_metadata(api_key: str | None) -> tuple[tuple[str, str], ...]:
    return (("x-api-key", api_key),) if api_key else ()


def open_channel(target: str) -> grpc.Channel:
    return grpc.insecure_channel(target, options=CHANNEL_OPTIONS)


def open_aio_channel(target: str) -> grpc.aio.Channel:
    return grpc.aio.insecure_channel(target, options=CHANNEL_OPTIONS)
