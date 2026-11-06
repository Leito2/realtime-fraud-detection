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
## Part IV — Proof (how we measure, results E1–E10, bottleneck analysis, observability, resilience F1–F10)
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
