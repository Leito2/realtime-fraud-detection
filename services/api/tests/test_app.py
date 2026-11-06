import pytest
from api.app import create_app
from fastapi.testclient import TestClient


class Decisions:
    def decision(self, payment_id):
        if payment_id != "p1":
            return None
        return {"payment_id": "p1", "decision": "BLOCK", "score": 0.97, "model_version": "3",
                "label_is_fraud": True, "explanation_status": "template",
                "explanation": "BLOCK because f_cnt_10m is 7."}


class Profiles:
    def features(self, user_id):
        return {"profile": {"avg_amount_30d": "42.0"}, "velocity": {}} if user_id == "u1" else None


class Model:
    def __init__(self, found=True):
        self.found = found

    def champion(self):
        return {"name": "fraud-champion", "alias": "champion", "version": "3"} if self.found else None


@pytest.fixture
def client():
    return TestClient(create_app(Decisions(), Profiles(), Model()))


def test_a_dispute_gets_the_decision_with_its_label_and_explanation(client):
    body = client.get("/decisions/p1").json()
    assert body["decision"] == "BLOCK" and body["label_is_fraud"] is True
    assert body["explanation"].startswith("BLOCK because")


def test_unknown_payment_and_user_are_404(client):
    assert client.get("/decisions/nope").status_code == 404
    assert client.get("/users/nope/features").status_code == 404


def test_user_features_come_from_the_online_store(client):
    assert client.get("/users/u1/features").json()["profile"]["avg_amount_30d"] == "42.0"


def test_model_endpoint_reports_the_champion_or_503_when_none_is_registered():
    ok = TestClient(create_app(Decisions(), Profiles(), Model()))
    none = TestClient(create_app(Decisions(), Profiles(), Model(found=False)))
    assert ok.get("/model").json()["version"] == "3"
    assert none.get("/model").status_code == 503


def test_health_and_metrics_are_served(client):
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/metrics").status_code == 200
