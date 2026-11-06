import json

import pytest
from auditor.ingest import ingest
from auditor.rows import BadMessage, to_row

DECISION = {"payment_id": "p1", "user_id": "u1", "decision": "BLOCK", "score": 0.93,
            "model": "fraud-champion", "model_version": "3", "threshold_set": "fraud-champion-v3",
            "degraded": False,
            "t_scheduled_ns": 1, "t_decided_ns": 2, "features": {"amount": 10.0}}


def raw(obj) -> bytes:
    return json.dumps(obj).encode()


class FakeStore:
    def __init__(self, fail_first: bool = False) -> None:
        self.writes: list[tuple[str, list[dict]]] = []
        self._fail = fail_first

    def write(self, table: str, rows: list[dict]) -> None:
        if self._fail:
            self._fail = False
            raise ConnectionError("db down")
        self.writes.append((table, rows))


def test_a_decision_becomes_a_decisions_row_with_model_and_features():
    table, row = to_row("decisions", raw(DECISION))
    assert table == "decisions"
    assert row["payment_id"] == "p1" and row["decision"] == "BLOCK" and row["score"] == 0.93
    assert row["model_version"] == "fraud-champion:3" and row["threshold_set"] == "fraud-champion-v3"
    assert json.loads(row["features"]) == {"amount": 10.0}


def test_decision_without_features_stores_null():
    d = {k: v for k, v in DECISION.items() if k != "features"}
    assert to_row("decisions", raw(d))[1]["features"] is None


def test_shadow_scores_keep_the_challenger_decision_apart_from_the_champion():
    table, row = to_row("shadow-scores", raw(DECISION))
    assert table == "shadow_scores"
    assert set(row) == {"payment_id", "model_version", "score", "decision"}


def test_a_label_becomes_a_labels_row():
    table, row = to_row("labels", raw({"payment_id": "p1", "is_fraud": True, "scenario": "burst"}))
    assert (table, row) == ("labels", {"payment_id": "p1", "is_fraud": True, "scenario": "burst"})


def test_an_explanation_becomes_an_explanations_row():
    msg = {"payment_id": "p1", "status": "template", "text": "high amount", "top_features": [["amount", 1.2]]}
    table, row = to_row("explanations", raw(msg))
    assert table == "explanations" and row["status"] == "template"
    assert json.loads(row["top_features"]) == [["amount", 1.2]]


@pytest.mark.parametrize("topic", ["decisions", "labels", "shadow-scores", "explanations"])
def test_garbage_is_a_bad_message_not_a_crash(topic):
    with pytest.raises(BadMessage):
        to_row(topic, b"{not json")
    with pytest.raises(BadMessage):
        to_row(topic, raw({"unexpected": 1}))


def test_unknown_topics_are_refused():
    with pytest.raises(BadMessage):
        to_row("payments", raw({}))


def test_ingest_groups_rows_by_table_and_counts_bad_messages():
    store = FakeStore()
    label = raw({"payment_id": "p1", "is_fraud": False})
    result = ingest(store, [("decisions", raw(DECISION)), ("labels", label), ("decisions", b"junk"),
                            ("decisions", raw({**DECISION, "payment_id": "p2"}))])
    assert result.written == 3 and result.bad == 1
    tables = {t: len(rows) for t, rows in store.writes}
    assert tables == {"decisions": 2, "labels": 1}


def test_ingest_lets_a_store_failure_propagate_so_offsets_are_not_committed():
    with pytest.raises(ConnectionError):
        ingest(FakeStore(fail_first=True), [("decisions", raw(DECISION))])
