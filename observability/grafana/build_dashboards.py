"""Generate the Grafana dashboards as JSON (dashboards as code, PLAN.md section 6.4).

    uv run python observability/grafana/build_dashboards.py

Editing a Python list is easier to review than 800 lines of exported JSON, and tests/test_observability.py
checks that every metric the panels query really exists. The JSON output is committed and provisioned.
"""
import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
DS = {"type": "prometheus", "uid": "prom"}
PG = {"type": "grafana-postgresql-datasource", "uid": "pg"}
BUDGET = 0.08


def q(metric: str, quantile: float, by: str = "", window: str = "1m") -> str:
    group = f"sum by (le{', ' + by if by else ''})"
    return f"histogram_quantile({quantile}, {group} (rate({metric}_bucket[{window}])))"


class Board:
    def __init__(self, uid: str, title: str) -> None:
        self.uid, self.title, self.panels, self._id = uid, title, [], 0
        self._x = self._y = self._row_h = 0

    def _place(self, w: int, h: int) -> dict:
        if self._x + w > 24:
            self._x, self._y, self._row_h = 0, self._y + self._row_h, 0
        pos = {"x": self._x, "y": self._y, "w": w, "h": h}
        self._x += w
        self._row_h = max(self._row_h, h)
        return pos

    def _panel(self, kind: str, title: str, targets: list[tuple[str, str]], unit: str, w: int, h: int,
               **extra) -> None:
        self._id += 1
        self.panels.append({
            "id": self._id, "type": kind, "title": title, "datasource": DS, "gridPos": self._place(w, h),
            "targets": [{"expr": e, "legendFormat": legend, "refId": chr(65 + i), "datasource": DS}
                        for i, (e, legend) in enumerate(targets)],
            "fieldConfig": {"defaults": {"unit": unit, **extra.pop("defaults", {})}, "overrides": []},
            **extra,
        })

    def series(self, title: str, targets, unit: str = "short", w: int = 12, h: int = 8, **extra) -> None:
        self._panel("timeseries", title, targets, unit, w, h, **extra)

    def stat(self, title: str, expr: str, unit: str = "short", w: int = 4, h: int = 4, **defaults) -> None:
        self._panel("stat", title, [(expr, "")], unit, w, h, defaults={**defaults})

    def heatmap(self, title: str, expr: str, w: int = 12, h: int = 8) -> None:
        self._panel("heatmap", title, [(expr, "{{le}}")], "s", w, h,
                    options={"calculate": False, "yAxis": {"unit": "s"}})
        self.panels[-1]["targets"][0]["format"] = "heatmap"

    def table(self, title: str, expr: str, w: int = 8, h: int = 4) -> None:
        self._panel("table", title, [(expr, "")], "short", w, h)
        self.panels[-1]["targets"][0]["format"] = "table"
        self.panels[-1]["targets"][0]["instant"] = True

    def sql(self, title: str, query: str, kind: str = "timeseries", unit: str = "short", w: int = 12,
            h: int = 8, **extra) -> None:
        """Panel over the audit store (PostgreSQL). Time series need a ``time`` column."""
        self._id += 1
        fmt = "table" if kind == "table" else "time_series"
        self.panels.append({
            "id": self._id, "type": kind, "title": title, "datasource": PG, "gridPos": self._place(w, h),
            "targets": [{"rawSql": query, "rawQuery": True, "editorMode": "code", "format": fmt, "refId": "A",
                         "datasource": PG}],
            "fieldConfig": {"defaults": {"unit": unit}, "overrides": []}, **extra})

    def build(self) -> dict:
        return {"uid": self.uid, "title": self.title, "schemaVersion": 39, "version": 1, "refresh": "5s",
                "time": {"from": "now-15m", "to": "now"}, "tags": ["fraud"], "editable": False,
                "panels": self.panels}


def overview() -> Board:
    b = Board("fraud-overview", "D1 Overview")
    b.stat("Throughput (ev/s)", "fraud:throughput:rate1m", "ops")
    b.stat("p95 end to end", q("fraud_e2e_latency_seconds", 0.95), "s",
           thresholds={"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                    {"color": "red", "value": BUDGET}]})
    b.stat("SLO: decisions under 80 ms (5 m)", "fraud:slo_good_ratio:rate5m", "percentunit",
           thresholds={"mode": "absolute", "steps": [{"color": "red", "value": None},
                                                    {"color": "green", "value": 0.95}]})
    b.stat("Scorer lag (messages)", 'sum(kafka_consumergroup_lag{consumergroup="scorer-champion"})')
    b.stat("BLOCK rate", "fraud:block_ratio:rate10m", "percentunit")
    b.table("Serving model", "fraud_model_info == 1")
    b.series("End-to-end latency", [(q("fraud_e2e_latency_seconds", 0.5), "p50"),
                                    (q("fraud_e2e_latency_seconds", 0.95), "p95"),
                                    (q("fraud_e2e_latency_seconds", 0.99), "p99")], "s")
    b.series("Decision mix", [("sum by (decision) (rate(fraud_decisions_total[1m]))", "{{decision}}")], "ops",
             fieldConfig={"defaults": {"unit": "ops", "custom": {"stacking": {"mode": "normal"}}},
                          "overrides": []})
    b.series("Degraded batches (no profiles)",
             [("rate(fraud_degraded_batches_total[1m])", "degraded")], "ops")
    b.series("Score distribution", [("sum by (le) (rate(fraud_score_bucket[1m]))", "{{le}}")], "short")
    return b


def latency() -> Board:
    b = Board("fraud-latency", "D2 Latency Breakdown")
    stages = "fraud_stage_latency_seconds"
    b.series("Where the time goes (p95 per micro-batch stage)",
             [(q(stages, 0.95, "stage"), "{{stage}}")], "s")
    b.series("Mean stage time per micro-batch",
             [(f"sum by (stage) (rate({stages}_sum[1m])) / sum by (stage) (rate({stages}_count[1m]))",
               "{{stage}}")], "s",
             fieldConfig={"defaults": {"unit": "s", "custom": {"stacking": {"mode": "normal"}}},
                          "overrides": []})
    b.heatmap("End-to-end latency heatmap", "sum by (le) (rate(fraud_e2e_latency_seconds_bucket[30s]))")
    mean_batch = "rate(fraud_batch_size_sum[1m]) / rate(fraud_batch_size_count[1m])"
    b.series("Micro-batch size", [(mean_batch, "mean"), (q("fraud_batch_size", 0.95), "p95")])
    b.series("Batch size vs p95 latency", [(mean_batch, "batch size"),
                                           (q("fraud_e2e_latency_seconds", 0.95) + " * 1000", "p95 (ms)")])
    return b


def streaming() -> Board:
    b = Board("fraud-streaming", "D3 Kafka & Flink")
    b.series("Consumer lag by partition (scorer)",
             [('kafka_consumergroup_lag{consumergroup="scorer-champion"}', "p{{partition}}")])
    b.series("Consumer lag by group",
             [("sum by (consumergroup) (kafka_consumergroup_lag)", "{{consumergroup}}")])
    b.series("Messages in per topic (ev/s)",
             [("sum by (topic) (rate(kafka_topic_partition_current_offset[1m]))", "{{topic}}")], "ops")
    b.series("Flink busy time (ms per s)",
             [("avg by (task_name) (flink_taskmanager_job_task_busyTimeMsPerSecond)", "{{task_name}}")], "ms")
    b.series("Flink backpressure (ms per s)",
             [("avg by (task_name) (flink_taskmanager_job_task_backPressuredTimeMsPerSecond)",
               "{{task_name}}")], "ms")
    b.series("Flink records in / out per second",
             [("sum(flink_taskmanager_job_task_numRecordsInPerSecond)", "in"),
              ("sum(flink_taskmanager_job_task_numRecordsOutPerSecond)", "out")], "ops")
    b.series("Checkpoint duration", [("flink_jobmanager_job_lastCheckpointDuration", "last")], "ms")
    b.series("Checkpoints completed / failed",
             [("flink_jobmanager_job_numberOfCompletedCheckpoints", "completed"),
              ("flink_jobmanager_job_numberOfFailedCheckpoints", "failed")])
    b.series("Redis memory", [("redis_memory_used_bytes", "used")], "bytes")
    return b


RECALL_BY_MODEL = """
SELECT time, caught::float / NULLIF(fraud, 0) AS champion,
       shadow_caught::float / NULLIF(shadow_fraud, 0) AS challenger
FROM (
  SELECT date_trunc('minute', d.decided_at) AS time,
         count(*) FILTER (WHERE l.is_fraud) AS fraud,
         count(*) FILTER (WHERE l.is_fraud AND d.decision <> 'APPROVE') AS caught,
         count(*) FILTER (WHERE l.is_fraud AND s.payment_id IS NOT NULL) AS shadow_fraud,
         count(*) FILTER (WHERE l.is_fraud AND s.decision <> 'APPROVE') AS shadow_caught
  FROM decisions d JOIN labels l USING (payment_id) LEFT JOIN shadow_scores s USING (payment_id)
  WHERE $__timeFilter(d.decided_at) GROUP BY 1) t
ORDER BY 1
"""
FRAUD_OUTCOME = """
SELECT date_trunc('minute', d.decided_at) AS time,
       count(*) FILTER (WHERE l.is_fraud AND d.decision = 'APPROVE') AS "fraud approved (missed)",
       count(*) FILTER (WHERE l.is_fraud AND d.decision <> 'APPROVE') AS "fraud flagged",
       count(*) FILTER (WHERE NOT l.is_fraud AND d.decision <> 'APPROVE') AS "legit flagged (friction)"
FROM decisions d JOIN labels l USING (payment_id)
WHERE $__timeFilter(d.decided_at) GROUP BY 1 ORDER BY 1
"""
BY_VERSION = """
SELECT model_version AS "model", labelled, fraud_blocked, fraud_reviewed, fraud_missed, legit_blocked,
       legit_reviewed, round(review_rate::numeric, 3) AS review_rate
FROM v_business_tradeoff ORDER BY model_version
"""
CONFUSION = "SELECT model, flagged, is_fraud AS fraud, n FROM v_confusion_by_model ORDER BY 1, 2, 3"
AGREEMENT = """
SELECT date_trunc('minute', d.decided_at) AS time,
       avg(((d.decision <> 'APPROVE') = (s.decision <> 'APPROVE'))::int) AS "champion/challenger agreement"
FROM decisions d JOIN shadow_scores s USING (payment_id)
WHERE $__timeFilter(d.decided_at) GROUP BY 1 ORDER BY 1
"""


def model_business() -> Board:
    b = Board("fraud-model", "D4 Model & Business")
    b.stat("Score PSI vs training", "fraud_score_psi", "short",
           thresholds={"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                    {"color": "orange", "value": 0.1},
                                                    {"color": "red", "value": 0.25}]})
    b.table("Serving model", "fraud_model_info == 1")
    b.series("Score PSI (alert at 0.25)", [("fraud_score_psi", "psi")], "short", w=16)
    b.sql("Recall online: champion vs challenger (per minute)", RECALL_BY_MODEL, unit="percentunit")
    b.sql("Fraud outcome per minute (labelled)", FRAUD_OUTCOME)
    b.sql("Champion / challenger agreement", AGREEMENT, unit="percentunit")
    b.sql("Business trade-off by champion version", BY_VERSION, kind="table", w=24, h=5)
    b.sql("Confusion by model", CONFUSION, kind="table", w=24, h=7)
    return b


EXPLAINED = """
SELECT date_trunc('minute', created_at) AS time, status, count(*) AS n
FROM explanations WHERE $__timeFilter(created_at) GROUP BY 1, 2 ORDER BY 1
"""


def explanations() -> Board:
    b = Board("fraud-explanations", "D5 Explanations")
    b.stat("Explained / s", "sum(rate(explanations_total[1m]))", "ops")
    total = "clamp_min(sum(rate(explanations_total[5m])), 1e-9)"
    b.stat("Skipped (shed) share", f'sum(rate(explanations_total{{status="skipped"}}[5m])) / {total}',
           "percentunit")
    b.stat("LLM texts rejected as unfaithful",
           f"sum(rate(explanations_rejected_total[5m])) / {total}", "percentunit")
    b.stat("In flight", "explainer_inflight")
    b.series("Explanations by status",
             [("sum by (status) (rate(explanations_total[1m]))", "{{status}}")], "ops")
    b.series("LLM latency", [(q("explainer_llm_latency_seconds", 0.5), "p50"),
                             (q("explainer_llm_latency_seconds", 0.95), "p95")], "s")
    b.series("Time to explain one decision", [(q("explainer_seconds", 0.95), "p95")], "s")
    b.series("Explainer lag (messages)", [('sum(kafka_consumergroup_lag{consumergroup="explainer"})', "lag")])
    b.sql("Stored explanations by status (per minute)", EXPLAINED)
    return b


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for board in (overview(), latency(), streaming(), model_business(), explanations()):
        (OUT / f"{board.uid}.json").write_text(json.dumps(board.build(), indent=2) + "\n", encoding="utf-8")
        print("wrote", board.uid)


if __name__ == "__main__":
    main()
