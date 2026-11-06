from latency.logic import LatencyStats, e2e_ms


def test_e2e_is_broker_timestamp_minus_scheduled():
    # broker LogAppendTime is in ms, t_scheduled in ns (single host clock, PLAN.md §3.12)
    assert e2e_ms(broker_ts_ms=1_700_000_000_050, t_scheduled_ns=1_700_000_000_000_000_000) == 50.0


def test_percentiles_over_a_known_distribution():
    s = LatencyStats()
    for ms in range(1, 101):                    # 1..100 ms
        s.record(float(ms), float(ms) / 2)
    r = s.summary()
    assert r["count"] == 100
    assert 49 <= r["e2e_ms"]["p50"] <= 51
    assert 94 <= r["e2e_ms"]["p95"] <= 96
    assert 98 <= r["e2e_ms"]["p99"] <= 100
    assert 24 <= r["scorer_ms"]["p50"] <= 26


def test_negative_latencies_are_clamped_not_crashing():
    s = LatencyStats()
    s.record(-3.0, -1.0)                        # clock quantisation can make a tiny negative
    assert s.summary()["e2e_ms"]["p50"] == 0.0


def test_empty_summary():
    assert LatencyStats().summary() == {"count": 0}
