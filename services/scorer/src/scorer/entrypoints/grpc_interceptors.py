"""Server interceptors (PLAN.md 14.3): Prometheus metrics per method and status code, and an API key check."""
import asyncio
import hmac
import time

import grpc
from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram

BUCKETS = [.0005, .001, .002, .005, .01, .02, .05, .1, .25, 1]


def _rewrap(handler, unary_unary=None, stream_stream=None):
    """Same handler shape (serializers included) around different behaviour."""
    if handler.unary_unary:
        return grpc.unary_unary_rpc_method_handler(unary_unary or handler.unary_unary,
                                                   handler.request_deserializer, handler.response_serializer)
    return grpc.stream_stream_rpc_method_handler(stream_stream or handler.stream_stream,
                                                 handler.request_deserializer, handler.response_serializer)


class MetricsInterceptor(grpc.aio.ServerInterceptor):
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._handled = Counter("grpc_server_handled_total", "RPCs finished, by method and status code",
                                ["method", "code"], registry=registry)
        self._seconds = Histogram("grpc_server_handling_seconds", "Time to handle an RPC", ["method"],
                                  buckets=BUCKETS, registry=registry)

    def _finish(self, method: str, started: float, code: str) -> None:
        self._handled.labels(method, code).inc()
        self._seconds.labels(method).observe(time.perf_counter() - started)

    @staticmethod
    def _code(context, error: BaseException | None) -> str:
        if isinstance(error, grpc.aio.AbortError):
            return context.code().name
        if error is None:
            return "OK"
        left = context.time_remaining()
        if left is not None and left <= 0:
            return "DEADLINE_EXCEEDED"
        return "CANCELLED" if isinstance(error, GeneratorExit | asyncio.CancelledError) else "UNKNOWN"

    async def intercept_service(self, continuation, details):
        handler = await continuation(details)
        if handler is None or not (handler.unary_unary or handler.stream_stream):
            return handler
        method = details.method.rsplit("/", 1)[-1]

        async def unary(request, context):
            started, error = time.perf_counter(), None
            try:
                return await handler.unary_unary(request, context)
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._finish(method, started, self._code(context, error))

        async def stream(request_iterator, context):
            started, error = time.perf_counter(), None
            try:
                async for response in handler.stream_stream(request_iterator, context):
                    yield response
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._finish(method, started, self._code(context, error))

        return _rewrap(handler, unary_unary=unary, stream_stream=stream)


class ApiKeyInterceptor(grpc.aio.ServerInterceptor):
    """Rejects calls whose ``x-api-key`` metadata does not match. Health and reflection stay open."""

    OPEN_PREFIXES = ("/grpc.health.", "/grpc.reflection.")

    def __init__(self, key: str) -> None:
        self._key = key.encode()

    async def intercept_service(self, continuation, details):
        handler = await continuation(details)
        if handler is None or details.method.startswith(self.OPEN_PREFIXES):
            return handler
        supplied = dict(details.invocation_metadata or ()).get("x-api-key", "").encode()
        if hmac.compare_digest(supplied, self._key):
            return handler

        async def deny_unary(request, context):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing or invalid x-api-key")

        async def deny_stream(request_iterator, context):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing or invalid x-api-key")
            yield  # pragma: no cover  (makes this an async generator)

        return _rewrap(handler, unary_unary=deny_unary, stream_stream=deny_stream)
