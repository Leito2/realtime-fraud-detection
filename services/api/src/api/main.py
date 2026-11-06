"""Composition root: wires the real stores and serves with uvicorn (1 worker)."""
import os

import uvicorn
from scorer import config
from scorer.sync_wiring import build_sync_scorer

from api.app import create_app
from api.stores import MlflowChampion, PostgresDecisions, RedisProfiles


def build():
    cfg = config.load()
    sync = build_sync_scorer(cfg, int(os.environ.get("API_METRICS_PORT", "9104")))
    return create_app(
        PostgresDecisions(os.environ.get("POSTGRES_DSN", "postgresql://fraud:fraud@postgres:5432/fraud")),
        RedisProfiles(os.environ.get("REDIS_URL", "redis://redis:6379/0")),
        MlflowChampion(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
                       os.environ.get("MODEL_NAME", "fraud-champion")),
        scorer=sync.score)


def main() -> None:
    uvicorn.run(build(), host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), workers=1)


if __name__ == "__main__":
    main()
