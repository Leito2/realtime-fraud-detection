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
