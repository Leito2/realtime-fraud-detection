"""gRPC entrypoint into the ``ScoreSync`` use case (PLAN.md 14, ADR-0002): translate, call, translate back.

The scoring itself is blocking (Redis, ONNX), so it runs in a small thread pool and the event loop only
shuffles messages. The caller's deadline is honoured twice: the loop stops waiting when it expires (gRPC
cancels the handler and the client sees DEADLINE_EXCEEDED), and the time left decides whether Redis is worth
asking at all.
"""
import asyncio
import time
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from fraud.v1 import scoring_pb2 as pb
from fraud.v1 import scoring_pb2_grpc as pb_grpc
from fraudcore.contracts import Payment
from scorer.application.ports import DecisionLookup
from scorer.application.score_sync import ScoreSync
from scorer.entrypoints.grpc_interceptors import ApiKeyInterceptor, MetricsInterceptor
from scorer.entrypoints.grpc_mapping import InvalidRequest, from_decision, from_stored, to_payment

SERVING = health_pb2.HealthCheckResponse.SERVING
DEFAULT_BUDGET_S = 0.05            # the proto says clients MUST set a deadline; this covers those that do not


class FraudScoringServicer(pb_grpc.FraudScoringServicer):
    def __init__(self, score: ScoreSync, lookup: DecisionLookup | None = None, stream_max: int = 32,
                 stream_wait_s: float = 0.005, workers: int = 4) -> None:
        self._score, self._lookup = score, lookup
        self._stream_max, self._stream_wait_s = stream_max, stream_wait_s
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="score")

    def _payments(self, requests: list[pb.ScoreRequest]) -> list[Payment]:
        return [to_payment(r) for r in requests]

    async def _decide(self, payments: list[Payment], context) -> list[pb.ScoreResponse]:
        remaining = context.time_remaining()
        budget = DEFAULT_BUDGET_S if remaining is None else remaining
        started = time.perf_counter()
        loop = asyncio.get_running_loop()
        decisions = await loop.run_in_executor(self._pool, self._score, payments, budget)
        latency_us = int((time.perf_counter() - started) * 1e6)
        return [from_decision(d, latency_us) for d in decisions]

    async def Score(self, request, context):
        try:
            payments = self._payments([request])
        except InvalidRequest as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        return (await self._decide(payments, context))[0]

    async def ScoreStream(self, request_iterator: AsyncIterator, context):
        """Bidirectional: requests are grouped into micro-batches (N messages or T ms) for one inference."""
        queue: asyncio.Queue = asyncio.Queue()

        async def pump() -> None:
            try:
                async for request in request_iterator:
                    await queue.put(request)
            finally:
                await queue.put(None)

        reader = asyncio.create_task(pump())
        loop, closed = asyncio.get_running_loop(), False
        try:
            while not closed:
                first = await queue.get()
                if first is None:
                    break
                batch, deadline = [first], loop.time() + self._stream_wait_s
                while len(batch) < self._stream_max:
                    try:
                        item = await asyncio.wait_for(queue.get(), max(0.0, deadline - loop.time()))
                    except TimeoutError:
                        break
                    if item is None:
                        closed = True
                        break
                    batch.append(item)
                try:
                    payments = self._payments(batch)
                except InvalidRequest as exc:
                    await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
                for response in await self._decide(payments, context):
                    yield response
        finally:
            reader.cancel()

    async def GetDecision(self, request, context):
        if self._lookup is None:
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, "no audit store configured")
        loop = asyncio.get_running_loop()
        try:
            stored = await loop.run_in_executor(self._pool, self._lookup.get, request.payment_id)
        except Exception as exc:
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"audit store unavailable: {type(exc).__name__}")
        if stored is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"no decision for payment {request.payment_id}")
        return from_stored(stored)


def build_server(servicer: FraudScoringServicer, api_key: str | None = None, metrics=None) -> grpc.aio.Server:
    interceptors = [metrics or MetricsInterceptor()]
    if api_key:
        interceptors.append(ApiKeyInterceptor(api_key))
    server = grpc.aio.server(interceptors=interceptors, options=[
        ("grpc.keepalive_time_ms", 30_000), ("grpc.keepalive_timeout_ms", 5_000),
        ("grpc.http2.min_ping_interval_without_data_ms", 10_000),
        ("grpc.http2.max_pings_without_data", 0)])
    pb_grpc.add_FraudScoringServicer_to_server(servicer, server)
    status = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(status, server)
    service = pb.DESCRIPTOR.services_by_name["FraudScoring"].full_name
    reflection.enable_server_reflection((service, health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,
                                         reflection.SERVICE_NAME), server)
    server.health_status = status                           # set SERVING once the model is ready
    server.service_name = service
    return server
