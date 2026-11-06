from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from scorer.adapters.otel_tracer import OtelTracer
from scorer.application.ports import SpanData

TRACE, PARENT = "a" * 32, "b" * 16


def tracer_and_exporter():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return OtelTracer(provider), exporter


def test_spans_continue_the_generators_trace_with_the_recorded_timestamps():
    tracer, exporter = tracer_and_exporter()
    tracer.record(f"00-{TRACE}-{PARENT}-01", SpanData("payment.decision", 1_000, 9_000),
                  [SpanData("kafka+flink transit", 1_000, 4_000), SpanData("model inference", 4_000, 9_000)])
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert set(spans) == {"payment.decision", "kafka+flink transit", "model inference"}
    root = spans["payment.decision"]
    assert f"{root.context.trace_id:032x}" == TRACE and f"{root.parent.span_id:016x}" == PARENT
    assert (root.start_time, root.end_time) == (1_000, 9_000)
    for name in ("kafka+flink transit", "model inference"):
        assert spans[name].parent.span_id == root.context.span_id
        assert f"{spans[name].context.trace_id:032x}" == TRACE
    assert spans["model inference"].start_time == 4_000


def test_unsampled_or_malformed_contexts_produce_no_spans():
    tracer, exporter = tracer_and_exporter()
    tracer.record(f"00-{TRACE}-{PARENT}-00", SpanData("x", 1, 2), [])
    tracer.record("garbage", SpanData("x", 1, 2), [])
    assert exporter.get_finished_spans() == ()
