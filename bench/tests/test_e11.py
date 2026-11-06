import asyncio
import time

from fraud_bench.e11 import Sample, open_loop, percentile, summarize, to_json_body, to_proto


def test_percentile_picks_the_rank_and_survives_empty_input():
    values = sorted(float(i) for i in range(1, 101))
    assert percentile(values, 0.5) == 51.0 and percentile(values, 0.99) == 100.0
    assert percentile([], 0.5) != percentile([], 0.5)          # NaN


def test_latency_is_measured_from_the_scheduled_instant_not_from_when_the_client_got_to_it():
    s = Sample(scheduled=10.0, started=10.2, done=10.25)
    assert round(s.latency_ms) == 250 and round(s.start_lag_ms) == 200


def test_open_loop_follows_its_schedule_and_counts_every_request():
    seen = []

    async def send(i):
        seen.append(i)

    t0 = time.perf_counter()
    samples = asyncio.run(open_loop(rate=200, duration_s=0.5, send=send))
    assert len(samples) == 100 and sorted(seen) == list(range(100))
    assert time.perf_counter() - t0 >= 0.45                     # 100 requests at 200/s take half a second


def test_a_slow_server_shows_up_as_latency_even_when_requests_overlap():
    async def send(i):
        await asyncio.sleep(0.05)

    samples = asyncio.run(open_loop(rate=100, duration_s=0.5, send=send))
    s = summarize("x", 100, samples)
    # Windows asyncio timers are coarse (see e11.py docstring), so the start-lag validity flag is not asserted here:
    # test_a_client_that_fell_behind_its_schedule_invalidates_the_run covers it deterministically.
    assert 30 <= s.p50 <= 120


def test_errors_are_counted_by_code_and_excluded_from_the_percentiles():
    ok = Sample(0.0, 0.0, 0.010)
    bad = Sample(0.1, 0.1, 0.9, error="StatusCode.DEADLINE_EXCEEDED")
    s = summarize("grpc", 10, [ok, bad])
    assert s.errors == {"StatusCode.DEADLINE_EXCEEDED": 1} and s.p99 == 10.0


def test_a_client_that_fell_behind_its_schedule_invalidates_the_run():
    late = [Sample(i * 0.01, i * 0.01 + 0.05, i * 0.01 + 0.06) for i in range(50)]
    assert not summarize("rest", 100, late).valid


def test_warmup_is_discarded():
    samples = [Sample(i * 0.1, i * 0.1, i * 0.1 + 0.001 * (1 if i >= 10 else 500)) for i in range(20)]
    s = summarize("grpc", 10, samples, warmup_s=1.0)
    assert s.count == 10 and s.p99 < 5


def test_the_same_payment_builds_both_wire_formats():
    p = {"payment_id": "p1", "user_id": "u1", "card_id": "c1", "amount": 12.5, "currency": "USD",
         "merchant_id": "m1", "mcc": "5411", "country": "CO", "device_id": "d1", "ip_country": "CO",
         "channel": "app", "event_time": "x", "t_scheduled_ns": 0, "t_sent_ns": 0}
    assert set(to_json_body(p)) == {"payment_id", "user_id", "card_id", "amount", "currency", "merchant_id",
                                    "mcc", "country", "device_id", "ip_country", "channel"}
    assert to_proto(p).ByteSize() < len(str(to_json_body(p)))
