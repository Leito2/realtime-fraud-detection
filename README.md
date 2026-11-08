# 🛡️ Real-time Fraud Detection Platform

> Streaming fraud scoring — Kafka · Flink SQL · Redis · XGBoost/ONNX · Grafana — measured end to end with
> open-loop load and percentiles. **Headline (to be measured):** _{X}k events/s at p95 {Y} ms on a single 8 GB laptop._

**Status:** 🟡 M0 bootstrap. Full design in [`PLAN.md`](PLAN.md) (Spanish). Courses behind it: Flink, stream engines,
Redis feature serving, Triton/ONNX, Grafana & latency engineering (Learning vault).

## TL;DR — Results at a Glance
_Filled from `docs/results/` after milestone M4._

## Part I — The Big Picture
### 1. The Problem: Fraud Is a Real-time Problem
### 2. Core Concepts Primer
Event logs and partitions · per-event vs micro-batch processing · event vs processing time · state, checkpoints and
delivery guarantees · online vs offline features and train/serve skew · tail latency and coordinated omission ·
champion/challenger, shadow mode, drift
### Key technologies at a glance
- **Kafka** — durable log that carries every payment event.
- **Flink** — computes each user's recent behaviour in real time.
- **Redis** — serves user features in under a millisecond.
- **XGBoost + ONNX Runtime** — fast, explainable scoring on CPU.
- **gRPC** — low-latency service-to-service scoring API, used when a checkout needs an answer right away.
- **Shadow mode and drift monitoring** — new models are tested on real traffic before they replace the current one.
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

#### The pipeline today (M1)
```
generator ──payments──► Flink SQL (10-min OVER window) ──payments-enriched──► scorer ──decisions──► latency collector
 open-loop      key=user_id     one row per payment, current one included      micro-batches         percentiles
```
A payment is scheduled at a fixed time (`t_scheduled_ns`), whatever the system is doing. Latency is the
broker timestamp of its decision minus that scheduled time, so a slow system cannot hide its own delay
(*coordinated omission*). The scorer is a placeholder rule for now; the real model arrives in M3.

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

#### Preliminary: how much does Flink's buffer timeout cost? (E3)
Flink ships records between operators in network buffers and flushes a buffer when it is full or when a timeout
expires. The timeout is a direct latency floor for sparse traffic. A first sweep at 500 ev/s (20 s per value, raw
data in `docs/results/e3-buffer-timeout.json`):

| buffer-timeout | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|
| 100 ms | 288 | 1434 | 1523 |
| 10 ms | 23 | 74 | 155 |
| 5 ms | 30 | 799 | 1355 |
| 1 ms | 51 | 730 | 1301 |

Only the first conclusion is safe: a 100 ms timeout adds roughly 10x. The 5 ms and 1 ms rows are **not valid**: I
was running unrelated CPU work on the same laptop during the sweep, and those p95 values are contention, not the
timeout. E3 is repeated on a quiet machine in M4 before any number from it is quoted.

### 5. Design Decisions (ADRs)
### 6. The Journey of a Payment

## Part II — Components (Concept → How it works here → Technical details)
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
| F5 | Kafka broker restarted (20 s down) | **refuted** | 0 | 4 | 130 s | 140 s (limit 120 s) | nothing lost, but the pipeline is silent far longer than the outage |
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
  5 and 10 s (E5, table below). The duplicates are harmless here because the scorer dedupes by `payment_id` (ADR-4),
  which is why at-least-once stays the default.
* **A dead consumer is noticed after `session.timeout.ms` (F2).** With librdkafka's default of 45 s the surviving
  scorer was silent for 48 s; with 10 s it takes over in 12 s (ADR-0015). The cost is a false rebalance when a
  process stalls for more than 10 s, which does happen on a busy 8 GB laptop.
* **Redis down is not an outage (F3, F4).** A hard deadline and a circuit breaker on the profile lookup (ADR-0014)
  turn "Redis is gone" into "27% of decisions use the conservative degraded policy for a few seconds"; before them
  every micro-batch blocked for about 4 s in name resolution.

Two hypotheses were refuted and stay refuted: the Flink restart takes 39 s, not 30, and a 20 s broker restart
leaves the decisions topic silent for 130 s, far longer than the outage. Neither lost an event. The cause of the
130 s was not isolated; it is listed as open in PLAN §16 rather than explained by a guess.

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

## License
MIT
