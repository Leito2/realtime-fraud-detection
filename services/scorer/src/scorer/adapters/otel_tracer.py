"""Tracer adapter on OpenTelemetry (PLAN.md section 6.6): continues the generator's sampled traces.

Spans are created after the fact with the timestamps the use case measured, so unsampled events (99%) pay
nothing and only the sampled 1% cost a few span exports.
"""
import os
from collections.abc import Sequence

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from fraudcore.tracing import parse_traceparent
from scorer.application.ports import NullTracer, SpanData

_PROPAGATOR = TraceContextTextMapPropagator()


class OtelTracer:
    def __init__(self, provider: TracerProvider) -> None:
        self._tracer = provider.get_tracer("scorer")

    def record(self, traceparent: str, root: SpanData, children: Sequence[SpanData]) -> None:
        context = parse_traceparent(traceparent)
        if context is None or not context.sampled:
            return
        parent = _PROPAGATOR.extract({"traceparent": traceparent})
        root_span = self._tracer.start_span(root.name, context=parent, start_time=root.start_ns)
        child_ctx = trace.set_span_in_context(root_span)
        for child in children:
            span = self._tracer.start_span(child.name, context=child_ctx, start_time=child.start_ns)
            span.end(end_time=child.end_ns)
        root_span.end(end_time=root.end_ns)


def from_env(service_name: str = "scorer") -> OtelTracer | NullTracer:
    """OTLP over HTTP when OTEL_EXPORTER_OTLP_ENDPOINT is set (the `trace` compose profile); else a no-op."""
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return NullTracer()
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces")))
    return OtelTracer(provider)
