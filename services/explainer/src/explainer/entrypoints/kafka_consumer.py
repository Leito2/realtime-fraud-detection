"""Entrypoint: decisions topic -> explain use case -> explanations topic.

At-most-once on purpose (auto commit, no retries): an explanation is optional, the decision already shipped.
Load shedding happens here and in the use case: when ``max_inflight`` explanations are running, new cases are
answered inline with status ``skipped`` instead of waiting for the slow LLM.
"""
import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor

from pydantic import ValidationError

from explainer.application.explain import ExplainDecision
from explainer.application.ports import ExplanationSink
from fraudcore.contracts import DecisionEvent, ExplanationEvent

log = logging.getLogger("explainer")


class DecisionConsumer:
    def __init__(self, consumer, explain: ExplainDecision, sink: ExplanationSink, max_inflight: int,
                 on_inflight: Callable[[int], None] = lambda n: None) -> None:
        self._consumer, self._explain, self._sink = consumer, explain, sink
        self._max = max_inflight
        self._on_inflight = on_inflight
        self._inflight = 0
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=max_inflight, thread_name_prefix="explain")

    def run(self, stop: threading.Event, poll_s: float = 0.5) -> None:
        try:
            while not stop.is_set():
                for msg in self._consumer.consume(num_messages=100, timeout=poll_s):
                    if msg.error() is None and msg.value() is not None:
                        self.handle(msg.value())
        finally:
            self._pool.shutdown(wait=True, cancel_futures=True)
            self._consumer.close()

    def handle(self, raw: bytes) -> None:
        try:
            decision = DecisionEvent.model_validate_json(raw)
        except ValidationError:
            log.warning("unparseable decision dropped")
            return
        if decision.decision == "APPROVE":
            return
        with self._lock:
            busy = self._inflight >= self._max
            if not busy:
                self._inflight += 1
        self._on_inflight(self._inflight)
        if busy:                                     # use case answers "skipped" without touching the LLM
            self._publish(self._explain(decision, inflight=self._max))
            return
        self._pool.submit(self._work, decision).add_done_callback(self._done)

    def _work(self, decision: DecisionEvent) -> ExplanationEvent | None:
        with self._lock:
            inflight = self._inflight
        return self._explain(decision, inflight=inflight - 1)

    def _done(self, future: Future) -> None:
        with self._lock:
            self._inflight -= 1
        self._on_inflight(self._inflight)
        if future.exception() is not None:
            log.error("explanation failed", exc_info=future.exception())
            return
        self._publish(future.result())

    def _publish(self, event: ExplanationEvent | None) -> None:
        if event is None:
            return
        try:
            self._sink.publish(event)
        except Exception:
            log.exception("explanation not published")      # best effort
