"""Protobuf <-> domain contracts. The only place that knows both vocabularies."""
from datetime import UTC, datetime

from fraud.v1 import scoring_pb2 as pb
from fraudcore.contracts import DecisionEvent, Payment
from scorer.application.ports import StoredDecision

CHANNELS = {pb.CHANNEL_WEB: "web", pb.CHANNEL_APP: "app", pb.CHANNEL_POS: "pos"}
DECISIONS = {"APPROVE": pb.DECISION_APPROVE, "REVIEW": pb.DECISION_REVIEW, "BLOCK": pb.DECISION_BLOCK}


class InvalidRequest(ValueError):
    """The request cannot be a payment; maps to INVALID_ARGUMENT."""


def to_payment(r: pb.ScoreRequest, now: datetime | None = None) -> Payment:
    if r.channel not in CHANNELS:
        raise InvalidRequest("channel is required (WEB, APP or POS)")
    stamped = r.HasField("event_time")
    event_time = r.event_time.ToDatetime(tzinfo=UTC) if stamped else (now or datetime.now(UTC))
    try:
        return Payment(payment_id=r.payment_id, user_id=r.user_id, card_id=r.card_id, amount=r.amount,
                       currency=r.currency, merchant_id=r.merchant_id, mcc=r.mcc, country=r.country,
                       device_id=r.device_id, ip_country=r.ip_country, channel=CHANNELS[r.channel],
                       event_time=event_time, t_scheduled_ns=0, t_sent_ns=0)
    except ValueError as exc:                       # pydantic.ValidationError is a ValueError
        raise InvalidRequest(str(exc).splitlines()[0]) from exc


def from_decision(d: DecisionEvent, server_latency_us: int) -> pb.ScoreResponse:
    return pb.ScoreResponse(payment_id=d.payment_id, decision=DECISIONS[d.decision], score=d.score,
                            model_version=f"{d.model}:{d.model_version}", threshold_set=d.threshold_set,
                            degraded=d.degraded, server_latency_us=server_latency_us)


def from_stored(d: StoredDecision) -> pb.ScoreResponse:
    return pb.ScoreResponse(payment_id=d.payment_id, decision=DECISIONS[d.decision], score=d.score,
                            model_version=d.model_version, threshold_set=d.threshold_set, degraded=d.degraded)
