# 🛡️ Real-time Fraud Detection Platform

> Streaming fraud scoring — Kafka · Flink SQL · Redis · XGBoost/ONNX · Grafana — measured end to end with
> open-loop load and percentiles. **Headline (measured):** 200 events/s sustained at p95 38.6 ms end to end on a single 8 GB laptop; the bottleneck is the Flink TaskManager.

**Status:** v1.0, milestones M0-M10 built and measured. Full design in [`PLAN.md`](PLAN.md) (Spanish). Courses behind it: Flink, stream engines,
Redis feature serving, Triton/ONNX, Grafana & latency engineering (Learning vault).

## TL;DR — Results at a Glance
Every number is a measurement from `docs/results/` on one 8 GB Windows laptop (WSL2 about 5 GB), one generator.

| | Result | Where |
|---|---|---|
| Sustained throughput | **200 ev/s at p95 38.6 ms** (300 ev/s: p95 64 ms in the final run, 81 ms in an earlier one: borderline); bottleneck: Flink TaskManager CPU | [E1](#e1---how-much-does-this-laptop-sustain-make-bench) |
| Model vs rules | PR-AUC 0.590 vs 0.164, recall at 1% FPR 0.674 vs 0.260 | [Rules vs models](#rules-vs-models-test-days-never-seen-in-training) |
| ONNX vs native XGBoost | 0.021 ms vs 0.468 ms per call at batch 1 (22x); parity ONNX-Triton 1.2e-7; Triton adds about 0.7-0.9 ms | [E4](#serving-guarantees-windows-and-scaling-e4-e7-e10) |
| Longer windows | `slow_fraud` recall 1.1% -> 21.1% with 1 h / 24 h features | E7 |
| gRPC vs REST | p50 3.5 vs 5.4 ms, p99 8.6 vs 12.3 ms at 100 req/s (stream: p50 8.3 ms, slower than unary) | [E11](#scorer-batching-shadow-tracing-and-the-api-e2-e8-e9-e11) |
| Exactly-once | 0 duplicates after killing the TaskManager (1539 with at-least-once); p95 25 ms -> 1.1 s | [E5, F10](#resilience-what-happens-when-things-break-f0-f10) |
| Failure experiments | 11 fault experiments plus a control: 10 of the 11 confirmed; F1 (39 s restart) refuted; 0 events lost in all | [F0-F10](#resilience-what-happens-when-things-break-f0-f10) |
| Shadow challenger | not free here: p50 +0.4-5.5 ms with the shadow on (E8) | E8 |

The target of the plan (80 ms p95 at thousands of events per second) is **not** met on this hardware: the honest
number is 200 ev/s. The design is not the limit so much as one laptop running Kafka, Flink and the scorer together.

## Part I — The Big Picture
### 1. The Problem: Fraud Is a Real-time Problem
### 2. Core Concepts Primer
Event logs and partitions · per-event vs micro-batch processing · event vs processing time · state, checkpoints and
delivery guarantees · online vs offline features and train/serve skew · tail latency and coordinated omission ·
champion/challenger, shadow mode, drift
### Key technologies at a glance
Chosen, and each one measured or tested somewhere in Part IV (why each was chosen over its alternatives: section 5).
- **Kafka** — durable log that carries every payment event (one broker, topics keyed by `user_id`).
- **Flink SQL** — computes each user's recent behaviour with per-event `OVER` windows.
- **Redis** — serves the slow-changing user profile in one round trip per micro-batch.
- **XGBoost + ONNX Runtime** — fast, explainable scoring on CPU (LightGBM and a PyTorch challenger compared; native XGBoost and Triton measured as serving alternatives).
- **gRPC + REST** — synchronous scoring API for a checkout that needs an answer right away (benchmarked against each other, E11).
- **MLflow** — model registry with a `champion` alias; the scorer hot-swaps models without losing events.
- **Shadow mode, Evidently and Postgres** — challengers run on real traffic in their own consumer group; the audit store joins decisions with labels; drift is detected with Evidently.
- **SeaweedFS (S3 API)** — versioned data lake for backtests.
- **SHAP + an LLM that only phrases facts** — explanations that cannot invent causes (`mock`/`ollama`, $0).
- **Prometheus, Grafana, OpenTelemetry/Tempo** — metrics, dashboards, optional tracing.
- **Toxiproxy and the `chaos` framework** — fault injection with a written hypothesis per experiment.
- **Python 3.12, uv, Polars, pydantic** — one workspace; `fraudcore` is the shared contract for training and serving.
### 3. What This Project Demonstrates
### 4. Architecture

#### How the code is organized
The architecture works at two levels, and each level has its own pattern.

**System level: event-driven services.** Each program that runs (generator, scorer, explainer, auditor, …)
is its own service under `services/`. Services talk through Kafka topics and never call each other on the
critical path. Each one can be scaled, restarted or broken on purpose without touching the others. Flink
jobs, infrastructure and dashboards live next to them, grouped by technology, because they hold no
business logic.

**Code level: Clean Architecture where it pays.** Inside the scorer and the explainer, business rules sit
at the center and know nothing about Kafka, Redis, ONNX or the LLM. Those are adapters plugged in from
outside, and imports only point inward. That's what lets the same scoring logic serve Kafka, gRPC and
REST, and run on ONNX Runtime or Triton, without being written twice. Services that are just a
consume → transform → write loop stay as plain modules: adding layers to them would add files, not
clarity.

The shared package `fraudcore` holds the rules every part of the system must agree on (feature
definitions, event contracts, thresholds). Training and serving import the same code, which is how
train/serve skew is prevented by construction. Full reasoning:
[ADR-0002](docs/adr/0002-code-organization-clean-architecture.md).

#### The final pipeline
Two paths with different rules. The **critical path** has a p95 budget of 80 ms and nothing slow is allowed on it;
the **async path** may be slow or down without changing a single decision.
```
CRITICAL PATH
generator ──payments──► Flink SQL ──payments-enriched──► scorer ──decisions──► latency collector
 open-loop   key=user_id  10-min OVER window,             micro-batches, ONNX,     percentiles
                          current payment included        profile from Redis
                                                              ▲
 profile builder (batch, 30 days) ──────────► Redis ──────────┘

ASYNC PATH (each its own consumer group)
decisions + shadow-scores + labels ──► auditor ──► Postgres (audit store, drift, promotion criteria)
payments-enriched ──► shadow scorer (challenger) ──► shadow-scores
decisions ──► explainer (SHAP facts -> LLM wording -> gate) ;  feature writer ──► Redis (state for the sync path)

SYNCHRONOUS ENTRYPOINTS (same ScoreSync use case): gRPC Score / ScoreStream and REST POST /score
MODEL LIFECYCLE: training ──► MLflow registry (champion alias, thresholds, score reference) ──► scorer hot swap
```
A payment is scheduled at a fixed time (`t_scheduled_ns`), whatever the system is doing. Latency is the
broker timestamp of its decision minus that scheduled time, so a slow system cannot hide its own delay
(*coordinated omission*).

#### Features: computed twice, checked against each other
Fraud shows in recent behaviour, so each payment is enriched with its user's last 10 minutes: how many payments,
their total, the largest, and how many were tiny (card testing). Flink computes this online. Training computes
it offline with `fraudcore.velocity_reference`. If the two ever differ, the model degrades silently
(*train/serve skew*), so `make parity` replays payments through the real Flink job and compares every value.
Slow-changing facts (30-day average amount, home country, known devices) come from a batch Profile Builder
that writes to Redis; the scorer reads them for a whole micro-batch in one round trip.

One thing the parity test found: Flink treats payments of the same user processed in the same millisecond as one
group, so in a burst the earlier payment can "see" the later one. It is exact for normally spaced payments and
documented in [PLAN §3.3](PLAN.md) as the reason to measure an alternative (E7).

#### How much does Flink's buffer timeout cost? (E3)
Flink ships records between operators in network buffers and flushes a buffer when it is full or when a timeout
expires. The timeout is a direct latency floor for sparse traffic. Sweep at 200 ev/s (the rate this laptop sustains,
see E1), 20 s per value after a warm-up, one run per value, nothing else running; raw data in
`docs/results/e3-buffer-timeout.json`:

| buffer-timeout | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|
| 100 ms | 65 | 111 | 130 |
| 10 ms | 18 | 30 | 70 |
| 5 ms | 14 | 22 | 29 |
| 1 ms | 15 | 37 | 239 |

(Final measurement block, quiet machine; an earlier sweep gave 78 / 23 / 20 / 19 ms of p50, same ranking.) The
default (100 ms) puts the median at about 4x the one with a 1-10 ms timeout. Between 1, 5 and 10 ms the p50 is
within 4 ms and the p95 differences (22-37 ms) are run-to-run noise at this single repetition, so the data does not
say which of the three is best, only that the default is the expensive choice. An earlier sweep at 500 ev/s is not
used: that rate is above what this machine sustains, so every row was queueing.

### 5. Design Decisions (ADRs)
Only the decisions that had real alternatives. The full text (context, decision, consequences) is in
[`docs/adr/`](docs/adr/); where a number settled the choice, the experiment is named.

| Decision | Alternatives considered | Why this one | Evidence |
|---|---|---|---|
| Features are computed by Flink and travel **inside the event** (`payments-enriched`) | Scorer reads velocity from Redis while Flink writes it | Reading Redis races with the write (the scorer can miss the current payment) and creates train/serve skew; enriched events have no race and an exact offline twin | [ADR-0003](docs/adr/0003-enriched-events-not-redis-lookups.md), parity test |
| Redis only for the slow **profile**, one pipelined read per micro-batch | Redis for everything; Feast; Dragonfly/Valkey | One round trip per batch; a Redis failure degrades the decision instead of stopping it | [ADR-0004](docs/adr/0004-redis-only-for-the-profile.md), F3, F4 |
| Flink SQL with **`OVER` windows** | Tumbling windows; Spark Structured Streaming; Kafka Streams; Bytewax/Faust | Tumbling windows add up to 10 min of latency; `OVER` emits per event. Known cost: same-millisecond peers in a burst (option B, a keyed operator, was built and measured but not adopted) | [ADR-0005](docs/adr/0005-over-windows.md), E3, E7 |
| **At-least-once** + dedupe by `payment_id` on the critical path | Exactly-once transactional sink | Exactly-once makes a row visible only after a checkpoint: p95 25 ms becomes 1.1 s. Both were measured | [ADR-0006](docs/adr/0006-at-least-once-with-idempotency.md), E5, F10 |
| Labels in their **own topic** | Label inside the payment | No leakage into features or logs, by construction | [ADR-0007](docs/adr/0007-labels-in-their-own-topic.md) |
| Challenger in **shadow** with its own consumer group | Canary or A/B on live traffic | Sees real traffic and cannot decide anything; not free on one laptop (p50 +0.4-5.5 ms) | [ADR-0008](docs/adr/0008-shadow-has-its-own-consumer-group.md), E8 |
| **JSON** in v1 | Avro + Schema Registry | One container fewer on 8 GB; Flink SQL reads JSON natively; serialization was never the measured bottleneck | [ADR-0009](docs/adr/0009-json-serialization.md), E1 |
| **XGBoost** as champion | LightGBM; rules only; a PyTorch challenger | Better PR-AUC on the synthetic data (0.590 vs rules 0.164) and on IEEE-CIS (0.472 vs LightGBM 0.250) | [Why XGBoost](#why-xgboost-and-not-lightgbm), IEEE-CIS |
| **ONNX Runtime in-process** | Native XGBoost; Triton; a Go scorer | 0.021 ms vs 0.468 ms per call at batch 1 (22x), parity 1e-7; Triton adds a hop and wins nothing with one client | [ADR-0016](docs/adr/0016-triton-as-a-measured-variant.md), E4 |
| SHAP from the native booster; the LLM **only phrases** the facts | Let the LLM reason; templates only | An LLM that reasons about a payment can invent causes; a gate rejects text that cites anything but the facts | [ADR-0010](docs/adr/0010-shap-from-the-native-booster.md), F8 |
| A registered model **carries its thresholds** | Thresholds only in config | A hot swap that changes the model but not the cutoffs silently changes business behaviour | [ADR-0011](docs/adr/0011-model-carries-its-thresholds.md) |
| gRPC/REST rebuild velocity **from stored state** | A second Flink-like window in the API | Same feature semantics, one model implementation; the residual error is reported (exact for 89.1% of payments) | [ADR-0012](docs/adr/0012-sync-velocity-from-stored-state.md), sync-vs-Flink |
| **gRPC** unary for synchronous scoring | REST/JSON; gRPC streaming | p50 3.5 vs 5.4 ms; the stream is slower (one handler, responses in order) | E11 |
| **SeaweedFS** behind the S3 API | MinIO | MinIO stopped publishing usable images in 2025; code uses boto3 + a SHA-256 manifest, so the server is replaceable | [ADR-0013](docs/adr/0013-seaweedfs-instead-of-minio.md) |
| Profile lookup with a **deadline and circuit breaker** | Plain socket timeout | A stopped Redis blocked ~4 s in name resolution, which no socket timeout covers; the breaker bounds it (2.7 s) and keeps p95 at baseline when Redis is slow | [ADR-0014](docs/adr/0014-profile-store-breaker-and-deadline.md), F3, F4 |
| Every consumer under a **watchdog** with a 10 s session timeout | Default librdkafka behaviour | Consumers could sit in a group join forever after a broker restart; now they restart in about a minute | [ADR-0015](docs/adr/0015-consumer-watchdog-and-session-timeout.md), F1, F2, F5 |
| **Clean Architecture** only in scorer and explainer | Everywhere; nowhere | The other services are consume-transform-write loops where layers add files, not clarity | [ADR-0002](docs/adr/0002-code-organization-clean-architecture.md) |
| **Open-loop** load, latency from the scheduled time | Closed-loop; Prometheus-only latency | A closed loop hides queueing (coordinated omission) | [How we measure](#how-we-measure) |
| **MLflow** registry; **Postgres** audit store | Weights & Biases; custom S3 registry; ClickHouse/DuckDB | Local, $0, aliases give a one-move promotion; the audit store is a join between decisions, shadow scores and labels | PLAN (MLflow, audit store sections) |

Decisions the measurements **contradicted** are kept in the open: the plan's 80 ms at thousands of events per second
(real: 200 ev/s, bottleneck the Flink TaskManager), the hypothesis F1 (Flink restart 39 s, not 30 s) and the first
F5 run (see Resilience).

### 6. The Journey of a Payment
1. The **generator** schedules a payment at a fixed instant and publishes it to `payments`, keyed by `user_id` (labels go to `labels`, never in the payment).
2. **Flink** keeps the user's last 10 minutes and emits one row to `payments-enriched` with the count, sum, maximum and tiny-payment count *including this payment*.
3. The **scorer** reads a micro-batch, fetches the users' 30-day profile from **Redis** in one round trip (with a deadline and a circuit breaker), builds the feature vector with the same code training used, and scores it with **ONNX Runtime**.
4. The registered model's own **thresholds** turn the score into a decision, and the decision, with the model version and whether it was `degraded`, goes to `decisions`.
5. In parallel, and without being able to slow step 3: the **shadow scorer** scores the same event with the challenger; the **auditor** joins decisions, shadow scores and labels in Postgres; the **explainer** turns the top SHAP facts into a sentence; the **feature writer** keeps the state the synchronous path needs.
6. A checkout that cannot wait for Kafka calls **gRPC `Score`** (or REST `POST /score`): the same use case, with velocity rebuilt from the stored state.
7. The **latency collector** subtracts the scheduled time from the decision's broker timestamp: that is the number in Part IV.

## Part II — Components (Concept → How it works here → Technical details)
Every component is its own container or package; the detail of each lives in the section named in the last column.

| Component | What it does | Technology | Where it is explained |
|---|---|---|---|
| `generator` | Open-loop synthetic payments with fraud scenarios, labels to their own topic | Python, confluent-kafka | [The data](#the-data-honestly) |
| Flink job (`flink/sql`) | Per-user velocity features, one row per payment | Flink SQL, `OVER` windows | [Features](#features-computed-twice-checked-against-each-other), E3 |
| Profile builder | 30-day profile per user into Redis | Polars, Redis | [Features](#features-computed-twice-checked-against-each-other) |
| `scorer` | Micro-batch scoring, thresholds, degraded mode, hot swap; also gRPC and REST entrypoints | ONNX Runtime, Clean Architecture | [From score to decision](#from-score-to-decision), E2, E4, E11 |
| Shadow scorer | Same binary, challenger model, own consumer group | `--mode shadow` | [Shadow challenger](#shadow-challenger-promotion-and-the-drift-demo), E8 |
| `auditor` | Joins decisions, shadow scores and labels | Postgres | [Shadow challenger](#shadow-challenger-promotion-and-the-drift-demo) |
| `explainer` | SHAP facts phrased by an LLM, gated | SHAP, mock/Ollama | [Explanations](#explanations-xgboost-decides-the-llm-words-it) |
| Training and registry | Rules / XGBoost / LightGBM, ONNX parity gate, thresholds at a cost, `champion` alias | XGBoost, MLflow | [The model](#rules-vs-models-test-days-never-seen-in-training), [Model lifecycle](#model-lifecycle-so-far) |
| Data lake and backtests | Versioned history, backtests, drift reports | SeaweedFS (S3), Evidently | [Backtesting](#backtesting-on-a-versioned-data-lake) |
| Observability | Metrics, dashboards, alerts, optional traces | Prometheus, Grafana, OTel/Tempo | [Observability](#observability), E9 |
| `bench` and `chaos` | Open-loop experiments E1-E11 and fault experiments F0-F10 | Python, Toxiproxy | [How we measure](#how-we-measure), [Resilience](#resilience-what-happens-when-things-break-f0-f10) |

## Part III — The Model (features & parity, cost-based thresholds, offline evaluation, lifecycle, drift demo)

### The data, honestly
There is no public streaming fraud dataset, so the history is simulated (`make history`): 60 days, 600,000
payments, 20,000 users, 0.84% fraud. Making it *hard* matters more than making it big.
Six fraud scenarios (burst, geo jump, card testing, atypical amount, account takeover, a slow fraud that stays
under every velocity rule) are mixed with four kinds of legitimate behaviour that look suspicious (traveller,
bill burst, new phone, big purchase). About 5% of fraud is never reported and 0.05% of legitimate payments are
disputed, so the labels are noisy like real ones. If a rules engine had scored 0.95 PR-AUC here, the generator would
have been too easy and would have been fixed before training. Splits are temporal: days 1-40 train, 41-50
validation, 51-60 test.

### Rules vs models (test days, never seen in training)
| model | PR-AUC | recall @ 1% FPR | PR-AUC, unseen users | cost | review rate |
|---|---|---|---|---|---|
| rules baseline | 0.164 | 0.260 | 0.323 | 255,530 | 1.29% |
| **XGBoost (champion)** | 0.590 | 0.674 | 0.557 | 38,785 | 0.90% |
| XGBoost through ONNX Runtime | 0.590 | 0.674 | 0.557 | 38,785 | 0.90% |
| LightGBM | 0.503 | 0.579 | 0.401 | 44,612 | 1.75% |
| LightGBM through ONNX Runtime | 0.503 | 0.579 | 0.401 | 44,618 | 1.75% |

*Recall @ 1% FPR*: of the fraud, how much do we catch while bothering 1 in 100 legitimate customers. *Cost* is
the simulated business cost on the test days with thresholds chosen on validation (missed fraud costs its amount, a
review costs 5, blocking a legitimate customer costs 25, review capacity capped at 2% of traffic).

What each approach sees, as recall at the same 1% FPR (ground-truth scenario, not the noisy label):

| scenario | rules | XGBoost |
|---|---|---|
| account_takeover | 0.82 | 1.00 |
| atypical_amount | 0.04 | 0.56 |
| burst | 0.46 | 0.61 |
| card_testing | 0.58 | 0.95 |
| geo_jump | 0.06 | 0.32 |
| slow_fraud | 0.08 | 0.01 |

The model is not magic. `slow_fraud` (three payments spaced 15-45 minutes apart) is invisible to a 10-minute
window and stays at about 1%; seeing it needs a longer window (`f_cnt_1h`, PLAN §3.3 option B, experiment E7).

### Why XGBoost and not LightGBM
Same split, same features, same Optuna budget (15 trials each). XGBoost won on every column, and LightGBM has a
second problem: its ONNX export is **not faithful**. On 100,000 test rows, 4 differed from the native model by more
than 1e-5 (largest difference 0.062), because the converter stores split
thresholds as float32 and a value sitting exactly on a split flips sides. XGBoost's maximum difference is
8.9e-08. The training job treats parity as a gate: a model that fails it cannot become champion.

### What ONNX Runtime buys (XGBoost, one CPU thread in the scorer)
| batch size | native p50 (ms) | ONNX p50 (ms) | speed-up | native us/event | ONNX us/event |
|---|---|---|---|---|---|
| 1 | 1.35 | 0.08 | 17.2x | 1347.5 | 78.1 |
| 32 | 2.14 | 0.47 | 4.5x | 66.9 | 14.7 |
| 500 | 4.75 | 6.25 | 0.8x | 9.5 | 12.5 |

For a single event ONNX Runtime is about 17x faster, because the native Python wrapper has a large fixed cost per call. At
batch 500 the native library (2 threads) beats one ONNX thread. The scorer sees micro-batches of tens of events, where ONNX wins; the batch-size sweep is experiment E2.

### From score to decision
A score is not a decision. Two thresholds, `review` and `block`, are chosen by minimising expected cost on the validation days
under the review-capacity cap, and written to `config/thresholds.yaml` (`make train`). For the champion the chosen
pair is review >= 0.70, block >= 0.98. The same cost routine is
applied to every model in the table, so the cost column compares like with like.

### Model lifecycle (so far)
`make train` fits both libraries, checks ONNX parity, picks the champion, logs everything to MLflow (parameters, metrics, the
`.onnx` file, its feature signature, the thresholds, the SHA-256 of the training data) and moves the `champion` alias.
The scorer polls that alias every 5 seconds, downloads the new version, **refuses** it if its feature order differs from
`fraudcore.FEATURE_ORDER`, and swaps it with a single reference assignment: a batch is scored entirely by the old or entirely by
the new model, and each decision records which one. Every downloaded version is cached on disk, so the scorer keeps running if MLflow is down or restarts.
Until a champion exists the scorer serves the rules fallback.

### Shadow challenger, promotion and the drift demo
A challenger (a retrained XGBoost or the PyTorch MLP, `make train-challenger`) never touches a decision. It runs in its own
consumer group, scores the same enriched events, and writes its scores to a separate topic, so it can be slow or dead without
moving the champion's p95 (ADR-0008). The auditor joins both models' scores with the labels that arrive later and
`make promote` moves the `champion` alias only if the challenger meets written criteria. The criteria compare the two models at the
**same flag rate**: a model that looks better only because it flags more customers is not better.

`make drift-demo` runs the whole loop against the live stack: (A) steady traffic, (B) a new fraud pattern the champion never
saw, (C) a retrain from the feedback labels runs as challenger in shadow, (D) promotion and recovery.

| phase | champion | frauds | recall | new-pattern recall | legit flagged |
|---|---|---|---|---|---|
| A steady | v7 | 24 | 66.7% | - | 9.2% |
| B new pattern | v7 | 112 | 50.0% | 46.9% (38/81) | 9.0% |
| C champion vs challenger | v7 (challenger v8 in shadow) | 88 | 35.2% | 25.0% (15/60) | 8.5% |
| D new champion | v8 | 51 | 51.0% | 59.4% (19/32) | 6.3% |

The challenger caught 51.1% in shadow against 35.2% for the champion on the same events, the criteria were met (recall gain
+0.159), and after the alias moved the new-pattern recall went from 25.0% to 59.4% without restarting anything. Two honest
caveats. The sample is small (tens of frauds per phase), so read the direction, not the decimals. And the live "legit flagged"
column (6-9%) is far above the 1% false-positive rate of the offline evaluation: the demo's traffic regime (few users, high
fraud share, cold profiles) is not the 60-day history the thresholds were chosen on. The lesson of the demo is the mechanism,
not a recall figure.

### Backtesting on a versioned data lake
Training histories and backtest datasets live in an S3-compatible bucket with versioning, next to a manifest that records the
SHA-256, row count and schema of every object. (PLAN named MinIO; MinIO stopped publishing images and binaries in 2025, so the
`s3` service is SeaweedFS behind the same API, and nothing in the code depends on that choice.) `make datalake` uploads a
dataset, `make backtest` replays a registered model over it from a container, and `make drift-report` runs an Evidently report
over a reference window and a current window.

A backtest is a table per model and per fraud scenario, not a single number. The first one compared champion v3 (XGBoost) with
challenger v4 (a small PyTorch MLP) on the same 10 held-out days:

| segment | metric | champion | challenger |
|---|---|---|---|
| overall | PR-AUC | 0.590 | 0.594 |
| overall | recall @ 1% FPR | 0.674 | 0.668 |
| atypical_amount | recall | 0.560 | 0.040 |
| geo_jump | recall | 0.320 | 0.420 |
| burst | recall | 0.609 | 0.658 |

The aggregate says "equal". The scenario rows say the challenger has forgotten one kind of fraud (unusually large or small
amounts) and learned another better. That is the reason promotion looks at segments and at live shadow results, not only at PR-AUC.

### Explanations: XGBoost decides, the LLM words it
A score of 0.87 is not something an analyst can act on. The explainer consumes `decisions`, keeps `REVIEW` and `BLOCK`, and
produces a short text saying why. The division of labour is strict:

1. **Facts come from the model.** XGBoost's own `pred_contribs` gives exact TreeSHAP values for the booster that made the decision
   (the ONNX graph cannot, so training registers the native booster next to it). The top 5 become the only facts.
2. **The LLM only phrases them.** The prompt contains those facts and nothing else; the model never sees the payment.
3. **A gate checks the wording.** Every number in the text must match a fact (10% tolerance) and no feature outside the top 5 may be
   named. A text that fails is replaced by a deterministic template built from the same facts, and counted in `explanations_rejected_total`.

It is a best-effort side path, so every way of failing has a defined outcome instead of lag: more than 2 explanations in flight or
a decision older than 30 s is `skipped`; an LLM error or an open circuit breaker (3 failures, 20 s cooldown) is a `template`; a
model without a native booster (the MLP) is a `template` that says no attribution exists. Delivery is at-most-once; the decision
had already shipped. The scorer must log features for the decisions it flags (`LOG_FEATURES=flagged`, or `1` for everything).

On the live stack with the free `mock` provider, 90 s of traffic produced 67 `llm`, 1 `skipped` (the first call, while the booster
was being fetched) and 1 `template` explanation; with the LLM endpoint unreachable the same traffic produced templates (and a few
`skipped` while the breaker was open) and the pipeline did not notice. `uv run explainer-audit` re-derives the facts of stored LLM
explanations and counts unsupported claims: 0 of 67 for `mock`, which is faithful by construction, so that number says the audit
works, not that a real model is faithful. The rate for a real model (Ollama) belongs to the measurement phase. Dashboard D5 shows
explanations by status, LLM latency, shed share and explainer lag; `GET /decisions/{payment_id}` on the admin API returns the
decision with its label and explanation for a dispute.

## Part IV — Proof (how we measure, results E1–E10, bottleneck analysis, observability, resilience F1–F10)

### How we measure
`make bench` runs the load protocol of [PLAN §5](PLAN.md): open-loop rate steps, a discarded warm-up, three repetitions
per step, and a manifest with the git SHA, Docker limits and effective configuration next to the raw data. A step
counts as **sustained** only if all four hold: median p95 <= 80 ms; the scorer's consumer lag is not growing and
there is no standing backlog; the generator kept its schedule (p99 lag <= 5 ms, else the run is invalid); and there
are no lost events, delivery errors or DLQ growth. `uv run python -m fraud_bench.report docs/results/<run>`
turns a run into the table and the p95-vs-rate "knee" chart. The rules live in `bench/src/fraud_bench/analysis.py`
and are unit-tested, so "sustained" cannot quietly change meaning between runs.

Machine matters, and not only the code: while a game was running on the same laptop, the p50 at 500 ev/s went from
16 ms (M1) to 1 s, and the validity rule flagged it (generator lag 100 ms). Those runs are not published. The E3 sweep above has the same warning.

### Observability
Prometheus scrapes the scorer, Flink, Kafka and Redis every 5 s; Grafana (profile `ops`) loads three dashboards
from code (`observability/grafana/build_dashboards.py`): **D1 Overview** (throughput, p50/p95/p99, SLO, lag, decision mix,
serving model), **D2 Latency Breakdown** (time per stage, heatmap, batch size vs latency) and **D3 Kafka & Flink**
(lag per partition, busy time, backpressure, checkpoints). A test fails if a panel or an alert uses a metric
nothing exports, and every expression was checked against the live Prometheus parser.

The SLO is "95% of decisions published in under 80 ms". Alerts fire on **burn rate** in two windows at once
(5 minutes and 1 hour, so a blip does not page and a slow leak does), plus growing consumer lag, failing Flink
checkpoints, a non-empty DLQ, a BLOCK rate outside its band, and a scorer running without profiles.

Tracing is optional (`make trace-up`: OTel Collector + Tempo). The generator marks 1% of payments with a W3C `traceparent`
header, **Flink SQL passes Kafka headers through unchanged** (checked), and the scorer turns each sampled
payment into spans: transit through Kafka and Flink, profile lookup, inference and publish. Spans are built after the fact
from timestamps the scorer already measures, so the other 99% of events pay nothing.

Measured on this stack: MLflow takes ~370 MB (its default job-executor pool is disabled; it was 900 MB), Grafana ~110 MB,
Prometheus ~65 MB, both exporters under 30 MB.

### E1 - how much does this laptop sustain? (`make bench`)

Open-loop rate steps, three repetitions each, a 60 s discarded warm-up and 180 s of load per run, one generator,
on the 8 GB laptop with only the pipeline running (no dashboards, no async scorers). Raw steps, manifest and the
knee chart are in `docs/results/e1-*`.

| rate (ev/s) | reps | p50 (ms) | p95 (ms) | p95 range | p99 (ms) | sustained | failing |
|---|---|---|---|---|---|---|---|
| 100 | 3 | 13.9 | 22.9 | 20.4-476 | 200 | no | gen_lag_valid, p95_within_budget |
| 200 | 3 | 15.6 | 38.6 | 34.6-45.4 | 231 | yes | - |
| 300 | 3 | 19.3 | 64.4 | 53.5-413 | 135 | yes (highest) | - |
| 400 | 3 | 25.1 | 326 | 122-1,409 | 704 | no | p95_within_budget |
| 500 | 3 | 63.2 | 35,848 | 578-49,644 | 42,533 | no | gen_lag_valid, p95_within_budget |
| 600 | 3 | 25,641 | 60,031 | 9,388-60,031 | 60,031 | no | gen_lag_valid, p95_within_budget |

This is the E1 of the final measurement block (quiet machine, after the last fixes). An earlier full run of the same
protocol (kept in `docs/results/e1-report-first-run.md`) gave: 100 ev/s p95 22.9 ms, **200 ev/s p95 38.6 ms (37.8-41.1)**, 300 ev/s p95 81 ms (62.8-96.3, not sustained),
400 ev/s p95 3,750 ms, 500 ev/s 17,007 ms and 600 ev/s 52,560 ms.

**Sustained throughput: 200 ev/s at p95 38.6 ms in both runs.** 300 ev/s is borderline: the median p95 was 64 ms in the
final run (one repetition at 413 ms) and 81 ms in the earlier one, so it passed the 80 ms rule once and missed it once.
The 100 ev/s step of the final run failed the generator-validity rule (a load-generator hiccup, p95 range 20-476 ms), not
the pipeline. From 400 ev/s the queues grow without bound in both runs. The container CPU shows where, in both runs. Final run (mean of the samples per step): at 200 ev/s the TaskManager is at 44% of a core;
at 300 ev/s 115% (max 175%) against 23% for the scorer; at 400 ev/s 121% against 29% for the scorer, with Kafka also busy
(124% mean at 400 ev/s). The earlier run read 100-131% for the TaskManager and under 3% for Redis. So **the bottleneck is the Flink job (one TaskManager on this laptop), not the scorer or the feature store**.
The M1 smoke (500 ev/s, p95 96 ms, placeholder scorer) is an earlier measurement; 500 ev/s does not fit the budget in this run (p95 17 s, generator lag invalid).
The `[X]k events/s` of the headline is therefore not reachable on this hardware, and this section is the real number.

### Serving, guarantees, windows and scaling (E4-E7, E10)

All numbers below were measured on this 8 GB laptop with one generator; where a run is noisy it says so. The
raw files are in [`docs/results/`](docs/results/).

**E4 - the same model, three ways** (`make e4`; ADR-0016). Time of the model call alone, in ms. ONNX Runtime
in-process is the fastest at every batch size the scorer uses; Triton adds a network hop (about 0.7-0.9 ms at
small batches) and wins nothing with a single client, because its dynamic batching has nothing to merge. Parity
ONNX vs Triton: max difference 1.2e-7. The comparison under many concurrent clients is not measured.

| batch | native p50 | native p99 | ONNX p50 | ONNX p99 | Triton p50 | Triton p99 |
|---|---|---|---|---|---|---|
| 1 | 0.468 | 11.7282 | 0.0205 | 0.0714 | 0.9255 | 1.8596 |
| 8 | 0.5774 | 20.2659 | 0.0333 | 0.0752 | 0.7279 | 1.9082 |
| 32 | 0.5424 | 9.2748 | 0.0747 | 0.2405 | 0.7032 | 1.463 |
| 128 | 0.8243 | 21.4424 | 0.2565 | 0.7743 | 0.9166 | 1.5966 |
| 500 | 1.7622 | 17.2914 | 0.7928 | 1.6515 | 1.4195 | 3.959 |

End to end (scorer on ONNX in-process vs as a Triton client; quiet machine, 2 repetitions per cell, compare the p50: the tails move between repetitions):

| variant | rate (ev/s) | rep | p50 | p95 | p99 | lost | lag stable |
|---|---|---|---|---|---|---|---|
| onnx | 200 | 1 | 18.895 | 82.943 | 690.175 | 0 | True |
| onnx | 200 | 2 | 17.647 | 47.647 | 188.671 | 0 | True |
| onnx | 400 | 1 | 23.535 | 111.551 | 217.087 | 0 | True |
| onnx | 400 | 2 | 24.815 | 88.831 | 183.935 | 0 | True |
| triton | 200 | 1 | 19.903 | 1580.031 | 5787.647 | 0 | True |
| triton | 200 | 2 | 24.223 | 825.343 | 2639.871 | 0 | True |
| triton | 400 | 1 | 28.207 | 152.319 | 355.839 | 0 | True |
| triton | 400 | 2 | 32.511 | 204.543 | 400.639 | 0 | True |

**IEEE-CIS (offline, not served).** The same training code and metrics on the public IEEE-CIS Fraud Detection dataset
(590,540 transactions, 391 features, 3.50% fraud), temporal split by `TransactionDT` (354,324 train / 118,108 valid /
118,108 test). Untuned, one run, `train_transaction.csv` only. Recall is at the cutoff that flags 1% of legitimate
payments ([`docs/results/ieee-cis-benchmark.md`](docs/results/ieee-cis-benchmark.md), `uv run python -m fraud_training.ieee_cis`).

| model | PR-AUC | recall at 1% FPR |
|---|---|---|
| rule: amount only | 0.0365 | 0.0079 |
| XGBoost | 0.4723 | 0.3925 |
| LightGBM | 0.25 | 0.2306 |

XGBoost beats the one-feature rule by 13x in PR-AUC on real data, so the method holds outside the generator. LightGBM
with the shared default parameters is clearly worse here; no tuning was attempted, so no claim is made about why.

**E5 - what exactly-once costs** (`make e5`; ADR-4). Same load, Flink sink at-least-once vs transactional. A row
becomes visible when its checkpoint commits, so the checkpoint interval is the latency floor.

| configuration | p50 (ms) | p95 (ms) | p99 (ms) | lost | valid run |
|---|---|---|---|---|---|
| at-least-once, checkpoint 10 s | 15.055 | 25.055 | 44.063 | 0 | True |
| exactly-once, checkpoint 1 s | 642.047 | 1089.535 | 1146.879 | 0 | True |
| exactly-once, checkpoint 5 s | 2844.671 | 5787.647 | 8937.471 | 0 | True |
| exactly-once, checkpoint 10 s | 5562.367 | 10043.391 | 10649.599 | 0 | True |

**E6 - what event-time windows cost** (`make e6`). The watermark delay is added to every decision. The rows counted
as lost are the last events still held by the watermark when the load stops (0.5 s x 200 ev/s = 100 and 2 s x 200
= 400, next to the 101 and 410 measured), not missing data.

| window | p50 (ms) | p95 (ms) | p99 (ms) | lost | valid run |
|---|---|---|---|---|---|
| processing time | 15.927 | 29.935 | 75.967 | 0 | True |
| event time, 0.5 s out-of-orderness | 831.999 | 1055.743 | 1221.631 | 101 | True |
| event time, 2 s out-of-orderness | 2338.815 | 2562.047 | 2787.327 | 410 | True |

**E7 - do longer velocity windows pay off?** (`make e7`; PLAN §3.3). Same XGBoost, same temporal split, with and
without 1 h and 24 h counts and sums. `slow_fraud`, a few payments spread over hours, goes from nearly invisible
to a fifth detected. Flink SQL cannot give both windows in one job; option B (a keyed operator,
`flink/pyflink/user_state.py`) matches the 10-minute reference exactly and costs 16.06 us per event in CPython (monotonic-deque maximum). The PyFlink
thread-mode bridge between the JVM and Python is **not** measured, so option B is neither adopted nor ruled out.

| | 10-minute features only | + 1 h and 24 h |
|---|---|---|
| PR-AUC | 0.5679 | 0.6176 |
| recall at 1% FPR | 0.6425 | 0.7322 |
| recall, account_takeover | 0.9863 | 0.9863 |
| recall, atypical_amount | 0.64 | 0.64 |
| recall, burst | 0.5827 | 0.7519 |
| recall, card_testing | 0.9013 | 0.9333 |
| recall, geo_jump | 0.3 | 0.44 |
| recall, slow_fraud | 0.0111 | 0.2111 |

**E10 - scaling** (`make e10`; 400 ev/s, because 2000 ev/s saturated this machine and four scorer processes do not
fit in 8 GB). One scorer is enough at this rate and a second process does not lower the p95 here. The cell marked
invalid is a run where the generator itself fell behind its schedule. Where each stage saturates needs a larger machine.

| Flink parallelism | scorers | p50 (ms) | p95 (ms) | p99 (ms) | lag stable | lost | valid |
|---|---|---|---|---|---|---|---|
| 2 | 1 | 18.175 | 56.191 | 116.223 | True | 0 | True |
| 2 | 2 | 21.951 | 185.471 | 679.423 | True | 1 | False |
| 4 | 1 | 18.255 | 53.279 | 125.311 | True | 0 | True |
| 4 | 2 | 20.399 | 70.911 | 139.007 | True | 0 | True |

**Synchronous velocity vs the Flink window** (`make sync-vs-flink`; ADR-0012). The REST/gRPC path computes velocity
from the Feature Writer's last snapshot. It matches the Flink window exactly for most payments and overcounts for the
rest (events that just left the window are still in the snapshot); the undercount from the writer's delay stays
below 0.2%. Full table: [`docs/results/m7c-sync-vs-flink.md`](docs/results/m7c-sync-vs-flink.md).

### Scorer batching, shadow, tracing and the API (E2, E8, E9, E11)

Same machine, same rule: one generator, only the pipeline running, 200 ev/s for E2/E8/E9 (E1's sustained rate).
Single-run cells are marked; a run is `valid` only if the generator kept its schedule.

**E2 - micro-batch size x wait** (`fraud_bench.run e2`). One run per cell, so differences of a few ms are noise
(E1 shows p95 moving by that much between identical runs). In the final block the 50- and 500-message batches with a
5 ms wait are best (p95 29 ms), against 49 ms for a batch of 1 and for 500 messages with a 20 ms wait; an earlier
run (`docs/results/e2-batching-first-run.md`) ranked the cells differently (p95 33-81 ms), which is the single-repetition noise.

| max messages | max wait (ms) | p50 | p95 | p99 | lag stable |
|---|---|---|---|---|---|
| 1 | 1 | 10.311 | 49.311 | 164.351 | True |
| 50 | 5 | 15.167 | 29.167 | 77.183 | True |
| 500 | 5 | 15.487 | 29.487 | 73.535 | True |
| 500 | 20 | 25.951 | 48.959 | 84.991 | True |

**E8 - does the shadow challenger touch the champion?** (`scripts/e8_shadow.py`; ADR-6). The shadow is a separate
consumer group that only writes to `shadow-scores`, so the expectation was "no effect". On this 8 GB laptop it is
not zero: with the shadow on, the p50 is 0.4-5.5 ms higher in each of the three pairs, and the median p95 is 265 ms
against 54 ms (rerun on a quiet machine after the final fixes; one "on" run is invalid, and the p95 swings between
repetitions, 94-273 ms with the shadow on, so treat the p95 as a hint, not a measurement). The shadow competes
for the same CPU; the isolation is logical (own group, own topic), not physical.

| shadow | rep | p50 | p95 | p99 | lost | valid |
|---|---|---|---|---|---|---|
| off | 1 | 15.279 | 31.279 | 82.303 | 0 | True |
| on | 1 | 18.319 | 273.407 | 805.375 | 0 | True |
| off | 2 | 16.511 | 53.503 | 138.623 | 0 | True |
| on | 2 | 22.015 | 94.015 | 155.135 | 0 | True |
| off | 3 | 22.911 | 115.967 | 182.911 | 0 | True |
| on | 3 | 23.263 | 265.471 | 970.751 | 0 | False |

**E9 - cost of observing** (`scripts/e9_tracing.py`). The rerun in the final measurement block was discarded: all nine runs were invalid (p95 0.8-4 s even with tracing off; the cause was not investigated), so the table below is the earlier measurement. Tracing off, 1% and 100% of payments carrying a `traceparent`
and exporting spans. The host had a bad stretch during this experiment (several runs invalid, one "off" run with a
3.6 s p95), so the tails are not usable. The p50 of the clean repetitions (2 and 3) is stable: about 20 ms off,
19-20 ms at 1% and 24 ms at 100%. Sampling 1% is free at this resolution; tracing everything costs about 4 ms of p50.

| tracing | rep | p50 | p95 | p99 | lost | valid |
|---|---|---|---|---|---|---|
| off | 1 | 77.439 | 3778.559 | 5480.447 | 0 | False |
| 1% | 1 | 577.023 | 1663.999 | 2396.159 | 0 | False |
| 100% | 1 | 1278.975 | 4145.151 | 4730.879 | 0 | False |
| off | 2 | 19.903 | 66.943 | 199.935 | 0 | True |
| 1% | 2 | 19.551 | 52.543 | 100.543 | 0 | True |
| 100% | 2 | 23.951 | 132.991 | 420.095 | 0 | False |
| off | 3 | 20.767 | 3651.583 | 6549.503 | 0 | True |
| 1% | 3 | 19.391 | 50.399 | 90.431 | 0 | True |
| 100% | 3 | 23.631 | 302.847 | 652.799 | 0 | False |

**E11 - REST/JSON vs gRPC** (`python -m fraud_bench.e11`). Same model, same use case behind both
entrypoints, one client process at 100 req/s (the rate a single Python client holds without falling behind its own
schedule), 3 repetitions. gRPC unary is about a third faster at the median and at the p99 (3.5 vs 5.4 ms p50; the earlier run gave 4.1 vs 6.3 ms).

| mode | p50 | p95 | p99 |
|---|---|---|---|
| rest | 5.35 | 8.30 | 12.27 |
| grpc | 3.54 | 5.39 | 8.59 |
| grpc-stream | 8.30 | 10.00 | 12.19 |

Median of the valid repetitions of the final measurement block (quiet machine). Per run (the bidirectional stream
is slower than unary: one stream is one handler, so responses queue in order; the first REST run is invalid: the
client stalled right after the container started, start lag 1.7 s):

| mode | rep | p50 | p95 | p99 | start lag p99 | valid | failed requests |
|---|---|---|---|---|---|---|---|
| rest | 1 | 39549 | 90139 | 103267 | 1677 | False | 1 |
| grpc | 1 | 4.97 | 14.50 | 38.36 | 3.66 | True | 0 |
| grpc-stream | 1 | 8.33 | 10.00 | 12.19 | 1.34 | True | 0 |
| rest | 2 | 5.30 | 8.56 | 12.78 | 1.33 | True | 0 |
| grpc | 2 | 3.49 | 5.39 | 8.59 | 1.26 | True | 0 |
| grpc-stream | 2 | 8.30 | 10.20 | 15.64 | 1.39 | True | 0 |
| rest | 3 | 5.40 | 8.03 | 11.76 | 1.41 | True | 0 |
| grpc | 3 | 3.54 | 5.18 | 6.88 | 1.26 | True | 0 |
| grpc-stream | 3 | 8.27 | 9.82 | 11.75 | 1.26 | True | 0 |

### Resilience: what happens when things break (F0-F10)

Each experiment has a hypothesis written *before* the run, a fault injected at a fixed second under steady load
(200 ev/s, about half of what this laptop sustains), and a verdict computed from the topics: every produced
`payment_id` is compared with the decisions, so "lost" and "duplicated" are counts, not impressions. Recovery is
the time until the rate and the p95 are back to the run's own pre-fault level for 5 s. Reports, with the chart
and the full numbers, are in [`docs/resilience/`](docs/resilience/); the operational side is in
[`docs/runbooks.md`](docs/runbooks.md). `make chaos EXP=F1` runs one.

F0 is the control: the same run with nothing broken. It is what makes the other rows readable. An earlier round
of F2, F4 and F8 "failed" or "passed" because the laptop was swapping, not because of the fault; those runs are
kept next to the clean ones as `*-dirty-baseline` and are not used.

| # | Fault | Verdict | Lost | Extra copies | Longest silence | Recovery | What it showed |
|---|---|---|---|---|---|---|---|
| F0 | none (control) | confirmed | 0 | 0 | 1.0 s | 4 s | the floor: p95 25 ms, nothing degraded |
| F1 | Flink TaskManager killed | **refuted** | 0 | 1539 | 29.8 s | 39 s (limit 30 s) | nothing lost, duplicates are the at-least-once price |
| F2 | one of two scorers killed | confirmed | 0 | 5 | 12.0 s | 5 s | survivor takes over after one session timeout |
| F3 | Redis stopped 30 s | confirmed | 0 | 0 | 0.4 s | 0 s | breaker: 27% of decisions degraded, none late |
| F4 | Redis +20 ms | confirmed | 0 | 0 | 0.8 s | 3 s | deadline keeps p95 at baseline, 48% degraded |
| F5 | Kafka broker restarted (20 s down) | confirmed (3 clean runs) | 0 | 2-3 | 40.9-41.6 s | 43-44 s (limit 120 s) | the 130 s and 79.9 s runs were polluted: every service up, or a baseline with a leftover backlog ([report](docs/resilience/F5-busy-host.md)) |
| F6 | malformed payments | confirmed | 0 | 0 | 2.0 s | 5 s | invalid rows go to the DLQ, not into silence |
| F6b | malformed enriched rows | confirmed | 0 | 0 | 0.5 s | 1 s | the scorer's DLQ takes them |
| F7 | MLflow down + corrupt champion | confirmed | 0 | 0 | 3.3 s | 5 s | the previous model keeps serving |
| F8 | LLM endpoint dies | confirmed | 0 | 0 | 0.6 s | 4 s | decisions never wait for the explainer |
| F9 | 2x overload for 60 s | confirmed | 0 | 0 | 0.5 s | 5 s | lag grows, then drains; no component died |
| F10 | TaskManager killed, exactly-once | confirmed | 0 | **0** | 24.1 s | 26 s | zero duplicates, at the cost of latency |

Three results are worth reading as numbers rather than verdicts:

* **At-least-once vs exactly-once (F1 vs F10, E5).** Killing the TaskManager produced 1539 duplicate decisions with
  the default sink and 0 with a transactional sink. The price is latency, because a row becomes visible only when its
  checkpoint commits: at 200 ev/s the p95 is 25 ms at-least-once and 1.1 s, 5.8 s and 10.0 s with a checkpoint every 1,
  5 and 10 s (E5, table below). The duplicates are harmless here because downstream consumers dedupe by `payment_id` (the auditor inserts with `ON CONFLICT DO NOTHING`, ADR-4),
  which is why at-least-once stays the default.
* **A dead consumer is noticed after `session.timeout.ms` (F2).** With librdkafka's default of 45 s the surviving
  scorer was silent for 48 s; with 10 s it takes over in 12 s (ADR-0015). The cost is a false rebalance when a
  process stalls for more than 10 s, which does happen on a busy 8 GB laptop.
* **Redis down is not an outage (F3, F4).** A hard deadline and a circuit breaker on the profile lookup (ADR-0014)
  turn "Redis is gone" into "27% of decisions use the conservative degraded policy for a few seconds"; before them
  every micro-batch blocked for about 4 s in name resolution.

One hypothesis stays refuted: the Flink restart takes 39 s, not 30. F5 was refuted on a loaded host (130 s of
silence with every service up), then three runs with only the pipeline running gave 79.9 s (that run warned of a
leftover backlog in its baseline), 41.6 s and 40.9 s, with 43-44 s recovery in the last two.
A separate isolation run shows which stage waits: Flink resumes in the first sample after the broker is back, the
scorer's consumer group takes about 12 s more to rejoin ([`docs/results/f5-isolate.md`](docs/results/f5-isolate.md)).
Nothing was lost in any run.

## Part V — Run It Yourself

### Prerequisites
Docker Desktop (WSL2 backend), [uv](https://docs.astral.sh/uv/), GNU make (optional), ~15 GB free disk.
Recommended `%UserProfile%\.wslconfig`: `[wsl2] memory=5GB processors=6 swap=2GB`.

```bash
python scripts/doctor.py        # prerequisites
uv sync --all-packages --dev    # or: make setup
uv run pytest -q                # M0: contracts + feature definitions
make up PROFILE=core            # Kafka, Flink, Redis, Prometheus, scorer
make smoke                      # end-to-end load test: prints p50/p95/p99   (RATE=500 make smoke for a lighter run)
```

First measurement (M1, 8 GB laptop, placeholder rule scorer; raw files in `docs/results/`):

| Load | p50 | p95 | p99 | Lost | Note |
|---|---|---|---|---|---|
| 500 ev/s | 16 ms | 96 ms | 487 ms | 0 | |
| 2 000 ev/s | 1 065 ms | 2 439 ms | 2 931 ms | 0 | saturated: queues build up (CPU-bound, see PLAN §8) |

The 80 ms target is not met yet. Finding where the time goes is the job of M4.

## Part VI — Reflection (lessons, limitations, future work, glossary)

### Limitations (what was not measured or not achieved)
* **Hardware.** 8 GB with WSL2 at about 5 GB. Rates above 200 ev/s were not sustained; four scorer processes, Triton under many clients and any run above one generator were not measured. Larger-machine numbers need larger-machine runs.
* **Noisy host.** Several runs had to be repeated or discarded because the host was swapping; the experiments that still show wide tails (E4 end to end, E8, E9) say so next to the numbers.
* **gRPC streaming** was only measured at 100 req/s from one Python client; higher rates were not driven (E11).
* **F5:** the silence after a broker restart is about 41 s on a clean baseline (20 s down plus about 21 s of restart and rejoin; the scorer group is the stage that waits, Flink resumes at once, [isolation](docs/results/f5-isolate.md)); a loaded host or a leftover backlog stretches it to 80-130 s. **F1:** the Flink restart takes 39 s, not the 30 s hypothesized.
* **Option B** (PyFlink keyed operator): correct and cheap in plain Python (16 us/event). Through PyFlink's process-mode bridge it runs at 6.3k-7.9k ev/s per slot (upper bound: Flink keyed state not included) ([E7 bridge](docs/results/e7-option-b-bridge.md)); thread mode would not start (pemja native library not found, 4 attempts). Not adopted: the cost is a second runtime in the Flink image, not throughput.
* **Real data only offline.** The served system runs on a synthetic generator. The same training code was also run once on the public IEEE-CIS dataset (offline table, below); it was not streamed through Kafka and the model was not tuned there.
* The shadow challenger is isolated logically, not physically: it shares the CPU with the champion (E8).

### Lessons
* A control run (F0) and a quiet baseline are what make a failure experiment readable; two early verdicts changed once the machine stopped swapping.
* Configuration plumbing fails silently: the Flink job ignored the exactly-once override because compose forwarded one variable, and a single-broker Kafka needs `transaction.state.log.replication.factor=1` or the transactional sink hangs forever.
* Benchmark harnesses have bugs too: a collector idle timeout shorter than the checkpoint gap produced fake losses, and an asyncio write race made a gRPC client report 100% errors.

### CV phrase (measured numbers only)
* **Real-time fraud scoring pipeline** (Kafka -> Flink SQL windowed features -> Redis online store -> XGBoost on ONNX Runtime) that sustains **200 events/s at p95 38.6 ms end to end** on a single 8 GB laptop, measured open loop with three repetitions per step; the bottleneck was identified as the Flink TaskManager (115-121% of a core at 300-400 ev/s in the final run while the scorer stayed at 23-29%).
* **Model serving and lifecycle:** XGBoost on ONNX Runtime is 22x faster than the native booster per call at batch 1 (0.021 vs 0.468 ms; parity 1.2e-7 against Triton); it beats a rules baseline at **0.674 vs 0.260 recall at 1% FPR** (PR-AUC 0.590 vs 0.164); the champion hot-swaps through the MLflow registry, and a challenger in shadow mode costs 0.4-5.5 ms of champion p50 on this hardware (E8).
* **Contract-first gRPC scoring API** benchmarked against REST/JSON on the same model: **p99 8.6 ms vs 12.3 ms**, p50 3.5 vs 5.4 ms at 100 req/s (final measurement block, valid repetitions).
* **Reliability:** 11 failure experiments (plus a control) with written hypotheses and 0 lost events in all of them; exactly-once recovery from a killed Flink TaskManager in 26 s with 0 duplicates (1539 with at-least-once), at a latency cost measured in E5; two hypotheses refuted and published with their real numbers.


## License
MIT
