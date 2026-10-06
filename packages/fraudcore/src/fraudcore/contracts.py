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


EntityType = Literal["user", "card", "device", "ip_country", "merchant"]


class EntityEdge(BaseModel):
    """Relationship published on topic `entity-edges` for P4's GraphRAG (PLAN §13)."""
    src_type: EntityType
    src_id: str
    rel: Literal["USES_CARD", "USES_DEVICE", "CONNECTS_FROM", "PAYS"]
    dst_type: EntityType
    dst_id: str
    payment_id: str
    decision: Decision
    score: float = Field(ge=0, le=1)
    event_time: datetime


def entity_edges(payment: Payment, decision: DecisionEvent) -> list[EntityEdge]:
    """Explode one scored payment into the user-centred edges of the fraud graph."""
    common = {"payment_id": payment.payment_id, "decision": decision.decision,
              "score": decision.score, "event_time": payment.event_time}
    links = [("USES_CARD", "card", payment.card_id), ("USES_DEVICE", "device", payment.device_id),
             ("CONNECTS_FROM", "ip_country", payment.ip_country), ("PAYS", "merchant", payment.merchant_id)]
    return [
        EntityEdge(src_type="user", src_id=payment.user_id, rel=rel, dst_type=kind, dst_id=dst_id, **common)
        for rel, kind, dst_id in links
    ]
