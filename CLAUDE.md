# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

Streaming fraud-scoring platform (Kafka → Flink SQL → Redis → XGBoost/ONNX → decisions), measured end to end with open-loop load and percentiles. **Only milestone M0 (bootstrap) plus the gRPC contract exists.** Most of the tree is `.gitkeep` placeholders (`services/*`, `flink/`, `bench/`, `chaos/`, `training/`, `infra/{mlflow,toxiproxy,triton}`); real code today is the `fraudcore` package, `proto/`, `scripts/doctor.py`, and the Compose/config skeleton. `docker-compose.yml` has infra only — app services are added per milestone from a single image (`docker/app.Dockerfile`).

`PLAN.md` (Spanish, ~1280 lines) is the source of truth for design, milestones (§11), risks and experiments; code comments cite it as `PLAN.md §x.y` / `ADR-n`. `README.md` is a progressive skeleton (theory first, technical detail last) that grows with each milestone — each milestone must write its README sections. Per ADR-0001, `PLAN.md`'s ADR summaries become `docs/adr/NNNN-*.md` when their milestone is implemented.

## Commands

Python 3.12, `uv` workspace (root is a virtual package; members are `packages/*`). Docker Desktop (WSL2) for the stack.

```bash
uv sync --all-packages --dev        # make setup
uv run pytest -q                    # make test (testpaths: packages/fraudcore/tests, tests)
uv run pytest packages/fraudcore/tests/test_features.py::test_vectorize_rejects_missing_feature -q   # single test
uv run ruff check .                 # make lint   (CI runs lint then tests)
uv run ruff format .                # make fmt
python scripts/doctor.py            # make doctor — stdlib-only prerequisite check (Docker, uv, RAM, WSL limit, ports, disk)
make up PROFILE=core                # docker compose --profile core up -d; PROFILE = core|ops|explain|chaos
make down                           # tears down all profiles
```

`make smoke|train|bench|chaos|promote` are stubs that exit 1 until later milestones. Ruff: line length 110, rules `E,F,I,UP,B`. CI (`.github/workflows/ci.yml`) is just lint + pytest; `buf lint` / `buf breaking` are planned for the gRPC milestone (M7c) per `buf.yaml`.

## Architecture (big picture)

Two paths with different rules (PLAN §2.2):
- **Critical path** (target p95 ≤ 80 ms): generator → `payments` → Flink SQL → `payments-enriched` → scorer (+ Redis profile lookup) → `decisions`. Anything added here must justify its milliseconds — no LLM calls, no synchronous Postgres writes.
- **Async path**: shadow scorer, explainer, auditor, feature writer, latency collector. Each is its own Kafka consumer group and may be slow or down without affecting decisions.

Design decisions that shape the code (details in PLAN §2.3):
- **Enriched events, not Redis lookups for velocity features (ADR-1/3):** Flink uses `OVER` windows to emit one row per event whose features *already include the current payment* (`f_cnt_10m ≥ 1`). Only slow-changing *profile* features come from Redis (`HGET` pipelined once per micro-batch, ADR-2), populated by a batch Profile Builder.
- **At-least-once + idempotency on the critical path (ADR-4):** transactional exactly-once would add checkpoint-interval latency; dedupe by `payment_id`. Exactly-once is measured as a separate experiment.
- **Labels never travel in `payments` (ADR-5)** — separate `labels` topic to avoid leakage; shadow challenger has its own consumer group (ADR-6); JSON serialization in v1 (ADR-7).
- Kafka topics are keyed by `user_id` (`payments`, `payments-enriched`, 12 partitions) so per-user windows stay partition-local; created by `infra/kafka/create-topics.sh` (auto-create is disabled).
- Latency is measured open-loop: `t_scheduled_ns` (the schedule, not the actual send) → `t_decided_ns`, to avoid coordinated omission. These timestamps are measurement fields, not business data.

### Shared contracts — change these in lockstep

- `packages/fraudcore/src/fraudcore/contracts.py`: pydantic (v2) models for the v1 event contracts (`Payment` → `EnrichedPayment` → `DecisionEvent`). Mirrors PLAN §2.5.
- `packages/fraudcore/src/fraudcore/features.py`: single source of truth for `FEATURE_ORDER`, defaults, and derived features. Training, scorer and API must all import from here (structural defense against train/serve skew); `vectorize` raises on missing/NaN rather than imputing. `PROFILE_DEFAULTS` must match what training uses for new users.
- `proto/fraud/v1/scoring.proto`: gRPC contract (`FraudScoring`: `Score`, `ScoreStream`, `GetDecision`). `tests/test_proto_contract.py` compiles it with `grpc_tools.protoc` and asserts the service methods and that `ScoreRequest` covers every `Payment` field except `t_scheduled_ns`/`t_sent_ns`. Adding a field to `Payment` requires adding it to `ScoreRequest`.

### Code organization (ADR-0002)

Event-driven services at the system level; **Clean Architecture inside `scorer` and `explainer` only** (`domain/` → `application/` with use cases + `ports.py` Protocols → `adapters/` + `entrypoints/` → `main.py` composition root). Other services stay thin: pure functions + a `main.py` doing I/O. Imports point inward only (to be enforced with `import-linter`). Ports on the critical path are batch-shaped (`get_many`, `predict(matrix)`, `publish_batch`). The gRPC server and REST `POST /score` are entrypoints into the scorer's `score_sync` use case, not separate scoring implementations. `fraudcore` must stay free of I/O and infrastructure; model loading/telemetry go in a separate `fraudinfra` package. Each service becomes a uv workspace member (own `pyproject.toml`) when it gets code — don't add `services/*` to `[tool.uv.workspace]` before then, since uv rejects members without a `pyproject.toml`.

Config lives in YAML/`.env`, not code: `config/scorer.yaml` (micro-batch size/wait, ONNX threads, Redis timeout, model alias), `config/thresholds.yaml`, `config/experiments/E1.yaml`, `.env.example`. Compose services have hard `mem_limit`s sized for an 8 GB laptop (WSL2 ~5 GB); keep new services within the budget in PLAN §8. The `explain` profile references a sibling `../llm-gateway` project (P0) outside this repo.

Definition of done for milestones (PLAN §11.2): CI green, affected profile runs in 8 GB without `OOMKilled`, README sections written, config in YAML/`.env`, $0 spent (`LLM_PROVIDER=mock|ollama` only).
