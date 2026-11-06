"""HTTP surface (PLAN.md 3.7). Thin on purpose: it reads stores through small protocols and holds no logic.

It is a separate service from the scorer so no HTTP server sits in the consume loop. A dispute (P2) asks
``GET /decisions/{payment_id}`` and gets the decision, the label if known, and the explanation.
"""
from typing import Protocol

from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


class DecisionStore(Protocol):
    def decision(self, payment_id: str) -> dict | None: ...


class ProfileStore(Protocol):
    def features(self, user_id: str) -> dict | None: ...


class ModelInfo(Protocol):
    def champion(self) -> dict | None: ...


def create_app(decisions: DecisionStore, profiles: ProfileStore, model: ModelInfo) -> FastAPI:
    app = FastAPI(title="fraud admin api", version="1")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/decisions/{payment_id}")
    def get_decision(payment_id: str) -> dict:
        found = decisions.decision(payment_id)
        if found is None:
            raise HTTPException(404, f"no decision for payment {payment_id}")
        return found

    @app.get("/users/{user_id}/features")
    def get_features(user_id: str) -> dict:
        found = profiles.features(user_id)
        if found is None:
            raise HTTPException(404, f"no profile for user {user_id}")
        return found

    @app.get("/model")
    def get_model() -> dict:
        found = model.champion()
        if found is None:
            raise HTTPException(503, "no champion registered")
        return found

    return app
