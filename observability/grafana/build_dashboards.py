"""Generate the Grafana dashboards as JSON (dashboards as code, PLAN.md section 6.4).

    uv run python observability/grafana/build_dashboards.py

Editing a Python list is easier to review than 800 lines of exported JSON, and tests/test_observability.py
checks that every metric the panels query really exists. The JSON output is committed and provisioned.
"""
import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
DS = {"type": "prometheus", "uid": "prom"}
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


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for board in (overview(), latency(), streaming()):
        (OUT / f"{board.uid}.json").write_text(json.dumps(board.build(), indent=2) + "\n", encoding="utf-8")
        print("wrote", board.uid)


if __name__ == "__main__":
    main()
