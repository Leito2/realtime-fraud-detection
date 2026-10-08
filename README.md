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
make up PROFILE=core            # Kafka, Flink, Redis, Prometheus
```

## Part VI — Reflection (lessons, limitations, future work, glossary)

## License
MIT
