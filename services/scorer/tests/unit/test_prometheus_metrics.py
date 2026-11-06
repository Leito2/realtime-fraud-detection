from prometheus_client import CollectorRegistry
from scorer.adapters.prometheus_metrics import E2E_BUCKETS, PrometheusMetrics
from scorer.application.ports import BatchStats


def stats(**over):
    base = dict(size=3, redis_s=0.001, inference_s=0.002, publish_s=0.003, degraded=False,
                model="fraud-champion",
                version="4", decisions={"APPROVE": 2, "BLOCK": 1}, scores=[0.1, 0.2, 0.95],
                e2e_s=[0.012, 0.045, 0.2])
    return BatchStats(**{**base, **over})


def value(reg, name, labels=None):
    return reg.get_sample_value(name, labels or {})


def test_exports_the_catalog_names_with_units():
    reg = CollectorRegistry()
    PrometheusMetrics(reg).observe_batch(stats())
    assert value(reg, "fraud_e2e_latency_seconds_count") == 3
    assert value(reg, "fraud_e2e_latency_seconds_bucket", {"le": "0.05"}) == 2      # 12 ms and 45 ms
    assert value(reg, "fraud_stage_latency_seconds_sum", {"stage": "inference"}) == 0.002
    assert value(reg, "fraud_stage_latency_seconds_count", {"stage": "redis"}) == 1
    assert value(reg, "fraud_batch_size_sum") == 3
    assert value(reg, "fraud_decisions_total", {"decision": "APPROVE"}) == 2
    assert value(reg, "fraud_decisions_total", {"decision": "BLOCK"}) == 1
    assert value(reg, "fraud_score_count") == 3


def test_model_version_is_a_gauge_and_degraded_batches_are_counted():
    reg = CollectorRegistry()
    m = PrometheusMetrics(reg)
    m.observe_batch(stats(version="4"))
    m.observe_batch(stats(version="5", degraded=True))
    assert value(reg, "fraud_model_info", {"model": "fraud-champion", "version": "5"}) == 1
    assert value(reg, "fraud_model_info", {"model": "fraud-champion", "version": "4"}) == 0
    assert value(reg, "fraud_degraded_batches_total") == 1


def test_buckets_are_dense_around_the_80_ms_budget():
    assert sum(1 for b in E2E_BUCKETS if 0.04 <= b <= 0.1) >= 6 and 0.08 in E2E_BUCKETS
