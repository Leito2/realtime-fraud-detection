"""The auditor end to end: Kafka -> Postgres, idempotent (ADR-4). Needs `make up PROFILE=ops`."""
import json
import time
import uuid

import pytest
from confluent_kafka import Producer

psycopg = pytest.importorskip("psycopg")

DSN = "postgresql://fraud:fraud@localhost:5432/fraud"
BOOTSTRAP = "localhost:9092"


def _connect():
    try:
        return psycopg.connect(DSN, connect_timeout=2, autocommit=True)
    except Exception:
        pytest.skip("Postgres is not reachable (make up PROFILE=ops)")


def _wait_for(conn, sql: str, args: tuple, want: int, timeout: float = 20.0) -> int:
    deadline, n = time.time() + timeout, -1
    while time.time() < deadline:
        n = conn.execute(sql, args).fetchone()[0]
        if n >= want:
            return n
        time.sleep(0.5)
    return n


def test_a_decision_and_its_label_land_in_postgres_once_even_if_sent_twice():
    conn = _connect()
    pid = f"it-{uuid.uuid4().hex}"
    decision = {"payment_id": pid, "user_id": "u_it", "decision": "BLOCK", "score": 0.97,
                "model": "fraud-champion", "model_version": "it", "threshold_set": "it", "degraded": False,
                "t_scheduled_ns": 1, "t_decided_ns": 2, "features": {"amount": 12.5}}
    label = {"payment_id": pid, "is_fraud": True, "scenario": "burst"}
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for _ in range(2):                                                    # at-least-once: duplicates happen
        producer.produce("decisions", key=pid, value=json.dumps(decision))
        producer.produce("labels", key=pid, value=json.dumps(label))
    producer.flush(10)

    if _wait_for(conn, "SELECT count(*) FROM decisions WHERE payment_id = %s", (pid,), 1) < 1:
        pytest.skip("auditor is not running (docker compose --profile ops up -d auditor)")
    _wait_for(conn, "SELECT count(*) FROM labels WHERE payment_id = %s", (pid,), 1)
    time.sleep(2)                                                         # let the duplicates arrive too
    assert conn.execute("SELECT count(*) FROM decisions WHERE payment_id = %s", (pid,)).fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM labels WHERE payment_id = %s", (pid,)).fetchone()[0] == 1
    features = conn.execute("SELECT features FROM decisions WHERE payment_id = %s", (pid,)).fetchone()[0]
    assert features == {"amount": 12.5}
    confusion = conn.execute("SELECT n FROM v_confusion_by_model WHERE model = 'champion' AND flagged "
                             "AND is_fraud").fetchone()
    assert confusion and confusion[0] >= 1
