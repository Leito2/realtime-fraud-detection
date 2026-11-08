"""Train/serve parity (PLAN.md §4.3): Flink's OVER window == fraudcore.velocity_reference.

Needs the stack (`make up`); skipped when Kafka is not reachable. Run: `make parity`.
All payments of a user arrive within seconds, so no payment expires: this pins the semantics that matter
(current payment included, per-user isolation, count/sum/max/small-payment count). Expiry is covered by the
reference's unit tests.

Each payment is flushed on its own. Flink's RANGE window treats rows processed in the same millisecond as
peers and aggregates them together, so two payments of one user that reach Flink in the same fetch batch both
see each other (measured in M2, PLAN.md §3.3 spike S1). Sequential sends keep this test on the intended
semantics; the burst effect is documented, not hidden.
"""
import json
import random
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from fraudcore.contracts import EnrichedPayment
from fraudcore.features import VELOCITY, velocity_reference

BOOTSTRAP = "localhost:9092"


def _stack_up() -> bool:
    try:
        topics = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=2).topics
    except Exception:
        return False
    return "payments-enriched" in topics


def _payment(pid: str, user: str, amount: float) -> dict:
    return {"payment_id": pid, "user_id": user, "card_id": "c", "amount": amount, "currency": "USD",
            "merchant_id": "m", "mcc": "5411", "country": "CO", "device_id": "d", "ip_country": "CO",
            "channel": "web", "event_time": "2026-10-05T14:03:22.123Z", "t_scheduled_ns": 0, "t_sent_ns": 0}


@pytest.mark.integration
def test_flink_velocity_matches_the_offline_reference():
    if not _stack_up():
        pytest.skip("stack not running (make up)")
    run = uuid.uuid4().hex[:8]
    rng = random.Random(7)
    users = [f"par_{run}_{i}" for i in range(12)]
    sent = [_payment(f"{run}-{n}", rng.choice(users), round(rng.choice([2.5, 7.0, 15.0, 80.0, 400.0]), 2))
            for n in range(120)]

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"parity-{run}",
                         "auto.offset.reset": "latest", "enable.auto.commit": False})
    assigned = []
    consumer.subscribe(["payments-enriched"], on_assign=lambda _c, p: assigned.extend(p))
    deadline = time.monotonic() + 20
    while not assigned and time.monotonic() < deadline:
        consumer.poll(0.2)
    assert assigned, "consumer never got partitions"
    settle = time.monotonic() + 2.0        # "latest" resolves on the first fetch: earlier events are skipped
    while time.monotonic() < settle:
        consumer.poll(0.2)

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 0})
    for p in sent:                                           # per-user order = partition order
        producer.produce("payments", key=p["user_id"], value=json.dumps(p))
        producer.flush(10)
        time.sleep(0.01)          # RANGE windows treat rows of one millisecond as peers (CLAUDE.md gotcha)

    got: dict[str, EnrichedPayment] = {}
    deadline = time.monotonic() + 60
    wanted = {p["payment_id"] for p in sent}
    while len(got) < len(wanted) and time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is None or msg.error():
            continue
        event = EnrichedPayment.model_validate_json(msg.value())
        if event.payment_id in wanted:
            got[event.payment_id] = event
    consumer.close()
    assert len(got) == len(wanted), f"only {len(got)}/{len(wanted)} enriched events arrived"

    expected = velocity_reference([{"user_id": p["user_id"], "amount": p["amount"], "ts": i * 0.01}
                                   for i, p in enumerate(sent)])
    mismatches = []
    for p, exp in zip(sent, expected, strict=True):
        actual = got[p["payment_id"]].model_dump()
        for f in VELOCITY:
            if abs(actual[f] - exp[f]) > 1e-6:
                mismatches.append((p["payment_id"], f, actual[f], exp[f]))
    assert not mismatches, f"{len(mismatches)} parity mismatches, first: {mismatches[:3]}"
