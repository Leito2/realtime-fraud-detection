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
### 3. What This Project Demonstrates
### 4. Architecture
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
