import numpy as np
from fraud_bench.e4 import summarize, time_calls


def test_summarize_reports_percentiles_in_ms_and_cost_per_event():
    seconds = [0.001] * 90 + [0.010] * 10
    s = summarize(seconds, batch=10)
    assert s["calls"] == 100 and s["p50_ms"] == 1.0 and s["p99_ms"] == 10.0
    assert s["us_per_event_p50"] == 100.0                      # 1 ms for 10 events


def test_time_calls_discards_warmup_and_hands_the_model_the_requested_batch_size():
    seen = []
    rows = np.zeros((1000, 4), dtype=np.float32)

    def predict(X):
        seen.append(X.shape)

    r = time_calls(predict, rows, batch=32, iters=50, warmup=10, rng=np.random.default_rng(0))
    assert r["calls"] == 50 and len(seen) == 60 and set(seen) == {(32, 4)}
