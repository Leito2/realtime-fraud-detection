# Runbooks

One entry per symptom an operator can see. Each follows the same shape: **Symptom → Dashboard or alert →
Diagnosis → Action → Verification**. The entries come from the failure experiments in `docs/resilience/`
(F1-F9); the experiment that produced the evidence is named in each one. Alerts are defined in
`observability/prometheus/slo_rules.yml`; dashboards are in Grafana (http://localhost:3000, regenerate with
`uv run python observability/grafana/build_dashboards.py`).

## 1. Decisions stop for tens of seconds after a broker or TaskManager loss

- **Symptom:** `decisions` throughput drops to zero; later the lag drains in a burst.
- **Dashboard or alert:** `ConsumerLagGrowing`, `FlinkCheckpointsFailing`; Grafana "fraud-streaming" (lag per
  group, Flink checkpoints).
- **Diagnosis:** `docker compose ps -a` shows which of `kafka`, `taskmanager`, `jobmanager` is not running.
  A container stopped by `docker kill` is not restarted by Docker's restart policy, so it stays down until
  someone starts it. Then check the Flink job: `curl -s localhost:8081/jobs/overview`.
- **Action:** `docker compose start <service>`. If the job is not `RUNNING` after the TaskManager is back,
  `docker compose --profile core up -d flink-job` (the submit script is idempotent). Flink restarts from the
  last checkpoint by itself with the fixed-delay strategy; the detection time is bounded by
  `heartbeat.timeout` (10 s in `flink/conf/config.yaml`).
- **Verification:** lag per consumer group returns to under 100 messages and `fraud:throughput:rate1m`
  matches the input rate. Zero lost events is checked with the correctness verifier
  (`make chaos EXP=F1` runs it); duplicates are expected after a restart (at-least-once, ADR-4) and are
  discarded downstream by `payment_id`.
- **Evidence:** F1, F5.

## 2. A scorer, auditor, explainer or feature writer is up but consumes nothing

- **Symptom:** the process is alive, its consumer group lag grows, no errors in the log.
- **Dashboard or alert:** `ConsumerLagGrowing`; `kafka_consumergroup_lag` for that group.
- **Diagnosis:** after a Kafka restart, librdkafka can stay in a group join that never completes. Each
  consumer runs a watchdog (`fraudcore.watchdog.guard`) over its statistics callback; if the group state is
  not `up/steady` for 60 s it exits with code 3. Look for `exit code 3` in `docker compose ps -a` or
  `docker inspect`.
- **Action:** none if the orchestrator restarts the container (compose `restart: unless-stopped`). If it is not
  restarted, `docker compose restart <service>`.
- **Verification:** the group appears in `kafka-consumer-groups --describe` with an assigned member and lag falling.
- **Evidence:** F1 round 2, F2.

## 3. `ScorerDegraded` fires: decisions carry `degraded=true`

- **Symptom:** the share of decisions made without a profile rises; the BLOCK/REVIEW mix changes.
- **Dashboard or alert:** `ScorerDegraded`; Grafana "fraud-overview" (degraded share).
- **Diagnosis:** the scorer could not read the profile from Redis within its deadline (30 ms) or the
  circuit breaker is open (three consecutive failures, one probe every second). Check
  `docker compose ps redis` and `docker compose exec redis redis-cli ping`. Redis keeps its data in the
  `redis-data` volume with AOF `everysec`; after a restart `DBSIZE` should be close to the profile count
  (about 75 000 with the default simulation).
- **Action:** start Redis if it is down. If `DBSIZE` is 0 (volume lost), reload the profiles:
  `MSYS_NO_PATHCONV=1 docker compose --profile tools run --rm -T profile-builder --history /data/history.parquet`.
- **Verification:** `rate(fraud_degraded_batches_total[5m])` returns to 0 within one probe interval after Redis answers.
  The policy while degraded is conservative, so an extended degraded period is a business risk, not only a
  technical one: report it.
- **Evidence:** F3 (Redis stopped, silence 3.1 s once the breaker opens), F4 (Redis slow).

## 4. p95 above 80 ms, `LatencySLOBurn`

- **Symptom:** SLO burn alert; the latency panel shows p95 above the budget.
- **Dashboard or alert:** `LatencySLOBurn`; "fraud-latency" (stage breakdown: Kafka in, Flink, scorer, publish).
- **Diagnosis:** find the stage that grew. (a) Flink stage large and periodic (every few minutes, 0.3-3.7 s):
  checkpoint or RocksDB stalls, a known open finding (PLAN §16); correlate with checkpoint duration. (b) Scorer
  stage large: `docker stats` for CPU; a recently started container needs about two minutes before latency
  settles. (c) Kafka CPU near 200 %: the load is above what one broker on this machine sustains
  (400 ev/s produced an 8 s baseline in the chaos runs); reduce the input rate.
- **Action:** remove the cause above; for (a) there is no fix yet, only the measurement.
- **Verification:** `fraud:slo_good_ratio:rate5m` above 0.95.
- **Evidence:** F9 (overload, recovery 101 s), E1 in the README.

## 5. `DlqNotEmpty`: messages keep landing in `dlq`

- **Symptom:** the `dlq` topic grows.
- **Dashboard or alert:** `DlqNotEmpty`.
- **Diagnosis:** read a few messages: `docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic dlq --from-beginning --max-messages 5 --property print.headers=true`.
  Records that are not valid JSON, or valid JSON without `payment_id`, `user_id` or numeric `amount`, are
  routed there by the Flink job `m8_validated.sql` with the header `error`. Find the producer.
- **Action:** fix the producer. Do not replay the DLQ blindly: replay only after the cause is fixed and only
  the records that are well-formed payments.
- **Verification:** the DLQ offset stops moving; `payments` and `decisions` counts match (verifier).
- **Evidence:** F6 and F6b. A wrong-shaped JSON value used to crash-loop the old Flink job; the raw-text
  parse removed that failure.

## 6. Decisions arrive but the model version is wrong or never changes

- **Symptom:** `model_version` in `decisions` is not the registry `champion`.
- **Dashboard or alert:** "fraud-model" (served version, scores).
- **Diagnosis:** `curl -s localhost:5000/api/2.0/mlflow/registered-models/alias?name=fraud-champion&alias=champion`.
  If MLflow is down or its artifact is corrupt, the scorer keeps serving the version it already loaded and
  logs the failed reload.
- **Action:** restore MLflow (`docker compose start mlflow`); if the artifact is corrupt, re-register the champion
  (`make train TRAIN_ARGS='--trials 0'` retrains but does not touch `config/thresholds.yaml` if you restore the
  official one; do not overwrite `docs/results/m3-offline-eval.json`). Use `make promote` for deliberate swaps.
- **Verification:** `model_version` in new decisions equals the alias.
- **Evidence:** F7.

## 7. `BlockRateAnomaly` or `ScoreDrift`

- **Symptom:** BLOCK share above 3 %, or PSI of scores above 0.25.
- **Dashboard or alert:** `BlockRateAnomaly`, `ScoreDrift`; "fraud-model".
- **Diagnosis:** distinguish data from model: `make drift-report` (Evidently) compares the live window with the
  training reference. A degraded scorer (runbook 3) also moves the block rate.
- **Action:** if the data shifted, train a challenger on recent data (`uv run retrain --match-flag-rate auto`)
  and promote it only through `make promote`, which requires the criteria in `config/promotion.yaml`.
- **Verification:** the drift demo (`make drift-demo`) shows the full cycle: shift injected, alert, retrain,
  promotion, recovery.
- **Evidence:** `docs/results/m6-drift-demo.md`.

## Known gaps

- Periodic Flink stalls (checkpoint duration 0.8-1.5 s correlating with p95 spikes) are measured but not fixed.
- Redis AOF rewrite under load has not been observed.
- Exactly-once (F10) is not implemented; the pipeline is at-least-once with idempotent consumers.
- Slow-burn fraud (`slow_fraud`) is invisible to a 10-minute window; see PLAN §3.3 and experiment E7.
