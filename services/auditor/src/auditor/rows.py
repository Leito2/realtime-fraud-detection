"""Kafka message -> (table, row). Pure: validation lives in the shared contracts, not here."""
import json

from pydantic import ValidationError

from fraudcore.contracts import DecisionEvent, ExplanationEvent, LabelEvent

TOPICS = ("decisions", "labels", "shadow-scores", "explanations")


class BadMessage(Exception):
    pass


def _decision(raw: bytes) -> dict:
    d = DecisionEvent.model_validate_json(raw)
    return {"payment_id": d.payment_id, "user_id": d.user_id, "decision": d.decision, "score": d.score,
            "model_version": f"{d.model}:{d.model_version}", "threshold_set": d.threshold_set,
            "degraded": d.degraded, "features": json.dumps(d.features) if d.features is not None else None}


def _shadow(raw: bytes) -> dict:
    d = DecisionEvent.model_validate_json(raw)
    return {"payment_id": d.payment_id, "model_version": f"{d.model}:{d.model_version}", "score": d.score,
            "decision": d.decision}


def _label(raw: bytes) -> dict:
    return LabelEvent.model_validate_json(raw).model_dump()


def _explanation(raw: bytes) -> dict:
    e = ExplanationEvent.model_validate_json(raw)
    return {"payment_id": e.payment_id, "status": e.status, "text": e.text,
            "top_features": json.dumps(e.top_features) if e.top_features is not None else None}


_BUILDERS = {"decisions": ("decisions", _decision), "shadow-scores": ("shadow_scores", _shadow),
             "labels": ("labels", _label), "explanations": ("explanations", _explanation)}


def to_row(topic: str, raw: bytes) -> tuple[str, dict]:
    if topic not in _BUILDERS:
        raise BadMessage(f"no table for topic {topic!r}")
    table, build = _BUILDERS[topic]
    try:
        return table, build(raw)
    except (ValidationError, ValueError) as exc:
        raise BadMessage(str(exc)[:200]) from exc
