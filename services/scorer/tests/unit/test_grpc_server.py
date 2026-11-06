import asyncio
import time

import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from grpc_health.v1 import health_pb2, health_pb2_grpc
from prometheus_client import CollectorRegistry
from scorer.application.ports import StoredDecision
from scorer.entrypoints.grpc_interceptors import MetricsInterceptor
from scorer.entrypoints.grpc_server import FraudScoringServicer, build_server

from fraud.v1 import scoring_pb2 as pb
from fraud.v1 import scoring_pb2_grpc as pb_grpc
from fraudcore.contracts import DecisionEvent


class FakeScore:
    """Stands in for ScoreSync: records the micro-batches and the budget it was given."""

    def __init__(self, delay_s: float = 0.0):
        self.delay_s, self.batches, self.budgets = delay_s, [], []

    def __call__(self, payments, budget_s=None):
        time.sleep(self.delay_s)
        self.batches.append([p.payment_id for p in payments])
        self.budgets.append(budget_s)
        return [DecisionEvent(payment_id=p.payment_id, user_id=p.user_id, decision="REVIEW", score=0.55,
                              model="xgboost", model_version="7", threshold_set="v1",
                              degraded=budget_s < 0.003, t_scheduled_ns=1, t_decided_ns=2) for p in payments]


class FakeLookup:
    def get(self, payment_id):
        if payment_id == "p-known":
            return StoredDecision("p-known", "BLOCK", 0.97, "fraud-champion:7", "v1", False)
        return None


def request(pid="p1", channel=pb.CHANNEL_WEB, amount=25.0) -> pb.ScoreRequest:
    ts = Timestamp()
    ts.GetCurrentTime()
    return pb.ScoreRequest(payment_id=pid, user_id="u1", card_id="c1", amount=amount, currency="USD",
                           merchant_id="m1", mcc="5411", country="CO", device_id="d1", ip_country="CO",
                           channel=channel, event_time=ts)


class Harness:
    def __init__(self, score=None, lookup="default", api_key=None):
        self.score = score or FakeScore()
        lookup = FakeLookup() if lookup == "default" else lookup
        self.registry = CollectorRegistry()
        self.servicer = FraudScoringServicer(self.score, lookup, stream_max=8, stream_wait_s=0.05)
        self.server = build_server(self.servicer, api_key, MetricsInterceptor(self.registry))

    async def __aenter__(self):
        port = self.server.add_insecure_port("127.0.0.1:0")
        await self.server.start()
        self.channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        self.stub = pb_grpc.FraudScoringStub(self.channel)
        return self

    async def __aexit__(self, *exc):
        await self.channel.close()
        await self.server.stop(0)

    def handled(self, method, code):
        return self.registry.get_sample_value("grpc_server_handled_total", {"method": method, "code": code})


def run(coro):
    return asyncio.run(coro)


def test_unary_score_returns_the_decision_with_model_identity_and_server_latency():
    async def go():
        async with Harness() as h:
            r = await h.stub.Score(request("p1"), timeout=2)
            assert (r.payment_id, r.decision, r.model_version) == ("p1", pb.DECISION_REVIEW, "xgboost:7")
            assert r.server_latency_us > 0 and not r.degraded
            assert h.score.budgets[0] <= 2          # the use case is told how much time is left
    run(go())


def test_an_invalid_request_is_invalid_argument_not_an_internal_error():
    async def go():
        async with Harness() as h:
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await h.stub.Score(request(channel=pb.CHANNEL_UNSPECIFIED), timeout=2)
            assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT and "channel" in e.value.details()
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await h.stub.Score(request(amount=-1.0), timeout=2)
            assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
            assert h.handled("Score", "INVALID_ARGUMENT") == 2
    run(go())


def test_an_expired_deadline_is_deadline_exceeded():
    async def go():
        async with Harness(FakeScore(delay_s=0.4)) as h:
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await h.stub.Score(request(), timeout=0.05)
            assert e.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    run(go())


def test_a_tiny_remaining_budget_reaches_the_use_case_so_it_can_degrade():
    async def go():
        async with Harness() as h:
            r = await h.stub.Score(request(), timeout=0.002)       # less than a Redis round trip
            assert r.degraded or h.score.budgets[0] < 0.003
    try:
        run(go())
    except grpc.aio.AioRpcError as exc:
        assert exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED       # the deadline can also win the race


def test_streaming_answers_every_request_in_order_and_groups_them_into_micro_batches():
    async def go():
        async with Harness() as h:
            call = h.stub.ScoreStream(timeout=5)
            for i in range(6):
                await call.write(request(f"s{i}"))
            await call.done_writing()
            ids = [r.payment_id async for r in call]
            assert ids == [f"s{i}" for i in range(6)]
            assert len(h.score.batches) < 6 and sum(map(len, h.score.batches)) == 6
    run(go())


def test_get_decision_found_not_found_and_unimplemented():
    async def go():
        async with Harness() as h:
            r = await h.stub.GetDecision(pb.GetDecisionRequest(payment_id="p-known"), timeout=2)
            assert (r.decision, r.model_version) == (pb.DECISION_BLOCK, "fraud-champion:7")
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await h.stub.GetDecision(pb.GetDecisionRequest(payment_id="nope"), timeout=2)
            assert e.value.code() == grpc.StatusCode.NOT_FOUND
        async with Harness(lookup=None) as h:
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await h.stub.GetDecision(pb.GetDecisionRequest(payment_id="x"), timeout=2)
            assert e.value.code() == grpc.StatusCode.UNIMPLEMENTED
    run(go())


def test_health_check_answers_without_an_api_key():
    async def go():
        async with Harness(api_key="s3cret") as h:
            await h.server.health_status.set("", health_pb2.HealthCheckResponse.SERVING)
            health = health_pb2_grpc.HealthStub(h.channel)
            r = await health.Check(health_pb2.HealthCheckRequest(service=""), timeout=2)
            assert r.status == health_pb2.HealthCheckResponse.SERVING
    run(go())


def test_the_api_key_is_required_when_configured():
    async def go():
        async with Harness(api_key="s3cret") as h:
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await h.stub.Score(request(), timeout=2)
            assert e.value.code() == grpc.StatusCode.UNAUTHENTICATED
            with pytest.raises(grpc.aio.AioRpcError) as e:
                await h.stub.Score(request(), timeout=2, metadata=(("x-api-key", "wrong"),))
            assert e.value.code() == grpc.StatusCode.UNAUTHENTICATED
            ok = await h.stub.Score(request(), timeout=2, metadata=(("x-api-key", "s3cret"),))
            assert ok.payment_id == "p1"
    run(go())


def test_metrics_count_calls_by_method_and_status_code():
    async def go():
        async with Harness() as h:
            await h.stub.Score(request(), timeout=2)
            await h.stub.GetDecision(pb.GetDecisionRequest(payment_id="p-known"), timeout=2)
            with pytest.raises(grpc.aio.AioRpcError):
                await h.stub.GetDecision(pb.GetDecisionRequest(payment_id="nope"), timeout=2)
            assert h.handled("Score", "OK") == 1 and h.handled("GetDecision", "OK") == 1
            assert h.handled("GetDecision", "NOT_FOUND") == 1
            assert h.registry.get_sample_value("grpc_server_handling_seconds_count", {"method": "Score"}) == 1
    run(go())
