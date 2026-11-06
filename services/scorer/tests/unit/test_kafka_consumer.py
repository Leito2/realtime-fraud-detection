import threading

from scorer.entrypoints.kafka_consumer import run


class Msg:
    def __init__(self, value: bytes, err=None, headers=None) -> None:
        self._v, self._e, self._h = value, err, headers

    def headers(self):
        return self._h

    def value(self):
        return self._v

    def error(self):
        return self._e


class FakeConsumer:
    def __init__(self, batches, stop: threading.Event) -> None:
        self.batches, self.stop, self.log = list(batches), stop, []

    def consume(self, num_messages, timeout):
        if not self.batches:
            self.stop.set()
            return []
        return self.batches.pop(0)

    def commit(self, asynchronous):
        self.log.append("commit")


GOOD = (b'{"payment_id":"p1","user_id":"u","card_id":"c","amount":5,"currency":"USD","merchant_id":"m",'
        b'"mcc":"1","country":"CO","device_id":"d","ip_country":"CO","channel":"web",'
        b'"event_time":"2026-10-05T14:03:22.123Z","t_scheduled_ns":1,"t_sent_ns":2,'
        b'"f_cnt_10m":1,"f_sum_10m":5,"f_max_10m":5,"f_small_tx_cnt_10m":1}')


def test_valid_events_are_scored_then_offsets_committed():
    stop = threading.Event()
    c = FakeConsumer([[Msg(GOOD)]], stop)
    seen = []
    run(c, dlq=lambda raw, why: seen.append(("dlq", why)),
        score=lambda evs: (c.log.append("score"), seen.append(len(evs))), max_messages=500, max_wait_s=0.005,
        stop=stop)
    assert c.log == ["score", "commit"]      # commit strictly after scoring (at-least-once)
    assert seen == [1]


def test_invalid_json_goes_to_dlq_and_does_not_block_the_batch():
    stop = threading.Event()
    c = FakeConsumer([[Msg(b"not json"), Msg(GOOD)]], stop)
    dlq, scored = [], []
    run(c, dlq=lambda raw, why: dlq.append(raw), score=scored.append, max_messages=500, max_wait_s=0.005,
        stop=stop)
    assert dlq == [b"not json"] and len(scored[0]) == 1


def test_empty_polls_do_not_score_or_commit():
    stop = threading.Event()
    c = FakeConsumer([[]], stop)
    run(c, dlq=lambda *_: None, score=lambda evs: c.log.append("score"), max_messages=500, max_wait_s=0.005,
        stop=stop)
    assert c.log == []


def test_traceparent_header_is_attached_to_the_event_but_not_serialised():
    stop = threading.Event()
    tp = "00-" + "a" * 32 + "-" + "b" * 16 + "-01"
    c = FakeConsumer([[Msg(GOOD, headers=[("traceparent", tp.encode()), ("other", b"x")]), Msg(GOOD)]], stop)
    seen = []
    run(c, dlq=lambda *_: None, score=lambda evs: seen.extend(evs), max_messages=500, max_wait_s=0.005,
        stop=stop)
    assert [e.traceparent for e in seen] == [tp, None]
    assert "traceparent" not in seen[0].model_dump()
