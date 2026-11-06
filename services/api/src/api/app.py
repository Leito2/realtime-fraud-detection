"""HTTP surface (PLAN.md 3.7). Thin on purpose: it reads stores through small protocols and holds no logic.

It is a separate service from the scorer so no HTTP server sits in the consume loop. A dispute (P2) asks
``GET /decisions/{payment_id}`` and gets the decision, the label if known, and the explanation.
"""
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Literal, Protocol

from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from fraudcore.contracts import DecisionEvent, Payment

SCORE_BUDGET_S = 0.05            # same default deadline as the gRPC API (PLAN.md 14)
ScoreFn = Callable[[Sequence[Payment], float | None], list[DecisionEvent]]


class ScoreIn(BaseModel):
    """Same fields as the gRPC ScoreRequest, so E11 compares transports and not payloads."""

    payment_id: str
    user_id: str
    card_id: str
    amount: float = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    merchant_id: str
    mcc: str
    country: str = Field(min_length=2, max_length=2)
    device_id: str
    ip_country: str = Field(min_length=2, max_length=2)
    channel: Literal["web", "app", "pos"]
    event_time: datetime | None = None


class ScoreOut(BaseModel):
    payment_id: str
    decision: str
    score: float
    model_version: str
    threshold_set: str
    degraded: bool
    server_latency_us: int


class DecisionStore(Protocol):
    def decision(self, payment_id: str) -> dict | None: ...


class ProfileStore(Protocol):
    def features(self, user_id: str) -> dict | None: ...


class ModelInfo(Protocol):
    def champion(self) -> dict | None: ...


def create_app(decisions: DecisionStore, profiles: ProfileStore, model: ModelInfo,
               scorer: ScoreFn | None = None) -> FastAPI:
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

    @app.post("/score")
    def score(body: ScoreIn) -> ScoreOut:
        if scorer is None:
            raise HTTPException(503, "synchronous scoring is not configured")
        payment = Payment(**{**body.model_dump(), "event_time": body.event_time or datetime.now(UTC)},
                          t_scheduled_ns=0, t_sent_ns=0)
        started = time.perf_counter()
        d = scorer([payment], SCORE_BUDGET_S)[0]
        return ScoreOut(payment_id=d.payment_id, decision=d.decision, score=d.score,
                        model_version=f"{d.model}:{d.model_version}", threshold_set=d.threshold_set,
                        degraded=d.degraded, server_latency_us=int((time.perf_counter() - started) * 1e6))

    @app.get("/model")
    def get_model() -> dict:
        found = model.champion()
        if found is None:
            raise HTTPException(503, "no champion registered")
        return found

    return app
