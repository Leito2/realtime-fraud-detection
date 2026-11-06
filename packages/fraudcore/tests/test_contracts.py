from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fraudcore.contracts import DecisionEvent, EnrichedPayment, Payment

BASE = dict(payment_id="p1", user_id="u_000123", card_id="c1", amount=42.5, currency="USD",
            merchant_id="m1", mcc="5411", country="CO", device_id="d1", ip_country="CO", channel="web",
            event_time=datetime(2026, 10, 6, tzinfo=UTC), t_scheduled_ns=1, t_sent_ns=2)


def test_enriched_payment_roundtrip():
    e = EnrichedPayment(**BASE, f_cnt_10m=3, f_sum_10m=120.0, f_max_10m=60.0, f_small_tx_cnt_10m=0)
    assert EnrichedPayment.model_validate_json(e.model_dump_json()) == e


def test_window_count_includes_current_payment():
    with pytest.raises(ValidationError):
        EnrichedPayment(**BASE, f_cnt_10m=0, f_sum_10m=0, f_max_10m=0, f_small_tx_cnt_10m=0)


def test_decision_labels_are_closed_set():
    with pytest.raises(ValidationError):
        DecisionEvent(payment_id="p1", user_id="u", decision="MAYBE", score=0.5, model="xgb",
                      model_version="1", threshold_set="v1", t_scheduled_ns=1, t_decided_ns=2)


def test_decision_features_are_optional_and_roundtrip():
    base = dict(payment_id="p1", user_id="u", decision="APPROVE", score=0.1, model="m", model_version="1",
                threshold_set="v1", t_scheduled_ns=1, t_decided_ns=2)
    assert DecisionEvent(**base).features is None
    d = DecisionEvent(**base, features={"amount": 10.0})
    assert DecisionEvent.model_validate_json(d.model_dump_json()).features == {"amount": 10.0}


def test_label_event_is_a_separate_contract_from_the_payment():
    from fraudcore.contracts import LabelEvent

    assert "is_fraud" not in Payment.model_fields                      # ADR-5: no label leakage in payments
    label = LabelEvent(payment_id="p1", is_fraud=True, scenario="burst")
    assert LabelEvent.model_validate_json(label.model_dump_json()) == label
