"""Dashboards and alerts are code: they must parse and may only use metrics that something really exports."""
import json
import re
from pathlib import Path

import pytest
import yaml
from prometheus_client import CollectorRegistry
from scorer.adapters.prometheus_metrics import PrometheusMetrics
from scorer.application.ports import BatchStats

ROOT = Path(__file__).resolve().parents[1]
OBS = ROOT / "observability"

# Metric names of the third-party exporters, as seen on the running stack.
EXTERNAL = {
    "kafka_consumergroup_lag", "kafka_topic_partition_current_offset", "redis_memory_used_bytes",
    "flink_taskmanager_job_task_busyTimeMsPerSecond",
    "flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
    "flink_taskmanager_job_task_numRecordsInPerSecond", "flink_taskmanager_job_task_numRecordsOutPerSecond",
    "flink_jobmanager_job_lastCheckpointDuration", "flink_jobmanager_job_numberOfFailedCheckpoints",
    "flink_jobmanager_job_numberOfCompletedCheckpoints",
}
TOKEN = re.compile(r"\b((?:fraud|kafka|redis|flink)_[A-Za-z0-9_]+)")


def exported_names() -> set[str]:
    reg = CollectorRegistry()
    PrometheusMetrics(reg).observe_batch(BatchStats(
        size=1, redis_s=.001, inference_s=.001, publish_s=.001, degraded=True, model="m", version="1",
        decisions={"APPROVE": 1}, scores=[0.1], e2e_s=[0.01]))
    return {s.name for metric in reg.collect() for s in metric.samples}


def metrics_in(expr: str) -> set[str]:
    return set(TOKEN.findall(expr))


def dashboard_exprs() -> list[tuple[str, str]]:
    out = []
    for path in sorted((OBS / "grafana" / "dashboards").glob("*.json")):
        for panel in json.loads(path.read_text(encoding="utf-8"))["panels"]:
            for target in panel.get("targets", []):
                out.append((path.name, target["expr"]))
    return out


def rule_exprs() -> list[tuple[str, str]]:
    out = []
    for path in sorted((OBS / "prometheus").glob("*rules.yml")):
        for group in yaml.safe_load(path.read_text(encoding="utf-8"))["groups"]:
            for rule in group["rules"]:
                out.append((rule.get("alert") or rule["record"], rule["expr"]))
    return out


def test_exporter_metric_names_are_the_catalog_of_the_scorer():
    names = exported_names()
    assert {"fraud_e2e_latency_seconds_bucket", "fraud_stage_latency_seconds_bucket",
            "fraud_decisions_total", "fraud_model_info", "fraud_batch_size_sum"} <= names


@pytest.mark.parametrize(("where", "expr"), dashboard_exprs() + rule_exprs())
def test_every_metric_in_a_dashboard_or_rule_exists(where, expr):
    known = exported_names() | EXTERNAL | recorded_names()
    unknown = {m for m in metrics_in(expr) if m not in known and m.replace(":", "_") not in known}
    assert not unknown, f"{where}: unknown metrics {unknown}"


def recorded_names() -> set[str]:
    return {name for name, expr in rule_exprs() if ":" in name}


def test_dashboards_cover_the_questions_of_the_plan():
    titles = {json.loads(p.read_text(encoding="utf-8"))["title"]
              for p in (OBS / "grafana" / "dashboards").glob("*.json")}
    assert {"D1 Overview", "D2 Latency Breakdown", "D3 Kafka & Flink"} <= titles


def test_dashboard_uids_are_unique_and_use_the_provisioned_datasource():
    uids = []
    for p in (OBS / "grafana" / "dashboards").glob("*.json"):
        d = json.loads(p.read_text(encoding="utf-8"))
        uids.append(d["uid"])
        for panel in d["panels"]:
            assert panel["datasource"]["uid"] == "prom"
    assert len(uids) == len(set(uids))


def test_alert_catalog_has_the_planned_alerts_with_severity_labels():
    alerts = {}
    for path in (OBS / "prometheus").glob("*rules.yml"):
        for group in yaml.safe_load(path.read_text(encoding="utf-8"))["groups"]:
            alerts.update({r["alert"]: r for r in group["rules"] if "alert" in r})
    assert {"LatencySLOBurn", "ConsumerLagGrowing", "FlinkCheckpointsFailing", "DlqNotEmpty",
            "BlockRateAnomaly", "ScorerDegraded"} <= set(alerts)
    assert all(a["labels"]["severity"] in {"critical", "warning", "info"} for a in alerts.values())
    assert alerts["LatencySLOBurn"]["labels"]["severity"] == "critical"


def test_prometheus_loads_the_rule_files_and_scrapes_the_scorer():
    cfg = yaml.safe_load((OBS / "prometheus" / "prometheus.yml").read_text(encoding="utf-8"))
    assert cfg["rule_files"]
    assert "scorer" in {j["job_name"] for j in cfg["scrape_configs"]}
