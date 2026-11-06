"""Event contracts v1 (PLAN.md §2.5). Topics: payments → payments-enriched → decisions."""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

Decision = Literal["APPROVE", "REVIEW", "BLOCK"]


class Payment(BaseModel):
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
    event_time: datetime
    t_scheduled_ns: int = Field(ge=0)       # open-loop schedule (measurement, PLAN §5)
    t_sent_ns: int = Field(ge=0)


class EnrichedPayment(Payment):
    f_cnt_10m: int = Field(ge=1)            # includes the current payment (OVER window, ADR-1/ADR-3)
    f_sum_10m: float
    f_max_10m: float
    f_small_tx_cnt_10m: int = Field(ge=0)
    t_flink_out_ns: int | None = None
    # W3C trace context from the Kafka header, never serialised: it is metadata, not business data.
    traceparent: str | None = Field(default=None, exclude=True)


class DecisionEvent(BaseModel):
    payment_id: str
    user_id: str
    decision: Decision
    score: float = Field(ge=0, le=1)
    model: str
    model_version: str
    threshold_set: str
    degraded: bool = False
    t_scheduled_ns: int
    t_decided_ns: int
    # Served feature vector, logged on request so retraining uses what the model really saw.
    features: dict[str, float] | None = None


class LabelEvent(BaseModel):
    """What the bank learns about a payment later. Own topic (``labels``), not in ``payments`` (ADR-5)."""

    payment_id: str
    is_fraud: bool
    scenario: str | None = None


class ExplanationEvent(BaseModel):
    """Async explanation of a decision (PLAN.md §3.10). ``skipped`` = shed under backpressure."""

    payment_id: str
    status: Literal["llm", "template", "skipped"]
    text: str | None = None
    top_features: list[tuple[str, float]] | None = None     # (feature, signed SHAP contribution)
