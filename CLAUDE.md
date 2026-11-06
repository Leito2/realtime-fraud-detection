# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

Streaming fraud-scoring platform (Kafka → Flink SQL → Redis → XGBoost/ONNX → decisions), measured end to end with open-loop load and percentiles. **Milestones M0-M5 are done** (walking skeleton; real-time features with a Flink-vs-`fraudcore` parity test; offline training with rules/XGBoost/LightGBM, cost-based thresholds, ONNX parity gate and MLflow registry with a hot-swapping scorer; the `bench` load protocol; Prometheus/Grafana/alerts and optional tracing). Measured E1/E2/E3 numbers need a quiet machine and are not final until run (see README). Later milestones are still `.gitkeep` placeholders (`bench/`, `chaos/`, `training/`, `infra/{mlflow,toxiproxy,triton}`, other `services/*`). App services are added per milestone from a single image (`docker/app.Dockerfile`); every service that gets code is added by explicit path to `[tool.uv.workspace].members` in the root `pyproject.toml`. Confirmed design decisions: PLAN.md §15.

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
make smoke                          # end-to-end load test, prints p50/p95/p99 (needs Docker; RATE=500 for lighter)
make profiles                       # simulate history + load user profiles into Redis (stack must be up)
make parity                         # Flink OVER window vs fraudcore.velocity_reference (stack must be up; skipped otherwise)
make history                        # simulate 60 days with fraud scenarios -> data/history_full.parquet (600k payments)
make train                          # rules/XGBoost/LightGBM, ONNX parity gate, register champion in MLflow (needs `make up`); TRAIN_ARGS='--trials 0' for a quick run
make bench                          # E1 rate steps x3 + manifest + sustained throughput (QUIET MACHINE ONLY); uv run python -m fraud_bench.report docs/results/<run>
make trace-up                       # core + ops + tracing (OTel Collector, Tempo); Grafana on :3000, MLflow :5000, Prometheus :9090
uv run python observability/grafana/build_dashboards.py   # regenerate Grafana dashboards from code
uv run python scripts/e3_buffer_timeout.py   # experiment E3 (Flink buffer-timeout sweep)
uv run lint-imports --config services/scorer/pyproject.toml   # ADR-0002 dependency rule (also in make lint and CI)
```

`make chaos|promote` are stubs that exit 1 until later milestones. Ruff: line length 110, rules `E,F,I,UP,B`. CI (`.github/workflows/ci.yml`) is just lint + pytest; `buf lint` / `buf breaking` are planned for the gRPC milestone (M7c) per `buf.yaml`.

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
- `packages/fraudcore/src/fraudcore/features.py`: single source of truth for `FEATURE_ORDER`, defaults, derived features, and `velocity_reference` (offline twin of the Flink window; `WINDOW_S`/`SMALL_TX_AMOUNT` are pinned to `flink/sql/*.sql` by a test). `profiles.py` (Polars) builds the 30-day profile for both Redis and training; `keys.py` holds the Redis key layout. Training, scorer and API must all import from here (structural defense against train/serve skew); `vectorize` raises on missing/NaN rather than imputing. `PROFILE_DEFAULTS` must match what training uses for new users.
- `proto/fraud/v1/scoring.proto`: gRPC contract (`FraudScoring`: `Score`, `ScoreStream`, `GetDecision`). `tests/test_proto_contract.py` compiles it with `grpc_tools.protoc` and asserts the service methods and that `ScoreRequest` covers every `Payment` field except `t_scheduled_ns`/`t_sent_ns`. Adding a field to `Payment` requires adding it to `ScoreRequest`.

### Code organization (ADR-0002)

Event-driven services at the system level; **Clean Architecture inside `scorer` and `explainer` only** (`domain/` → `application/` with use cases + `ports.py` Protocols → `adapters/` + `entrypoints/` → `main.py` composition root). Other services stay thin: pure functions + a `main.py` doing I/O. Imports point inward only (to be enforced with `import-linter`). Ports on the critical path are batch-shaped (`get_many`, `predict(matrix)`, `publish_batch`). The gRPC server and REST `POST /score` are entrypoints into the scorer's `score_sync` use case, not separate scoring implementations. `fraudcore` must stay free of I/O and infrastructure; model loading/telemetry go in a separate `fraudinfra` package. Each service becomes a uv workspace member (own `pyproject.toml`) when it gets code — don't add `services/*` to `[tool.uv.workspace]` before then, since uv rejects members without a `pyproject.toml`.

Config lives in YAML/`.env`, not code: `config/scorer.yaml` (micro-batch size/wait, ONNX threads, Redis timeout, model alias), `config/thresholds.yaml`, `config/experiments/E1.yaml`, `.env.example`. Compose services have hard `mem_limit`s sized for an 8 GB laptop (WSL2 ~5 GB); keep new services within the budget in PLAN §8. The `explain` profile references a sibling `../llm-gateway` project (P0) outside this repo.

Definition of done for milestones (PLAN §11.2): CI green, affected profile runs in 8 GB without `OOMKilled`, README sections written, config in YAML/`.env`, $0 spent (`LLM_PROVIDER=mock|ollama` only).

## CV results template (fill at M10 with measured numbers only)

Every placeholder must come from a reproducible experiment (PLAN §5.10). If a target is missed, publish the real number plus the bottleneck. Don't use estimated values.

* **Real-time fraud scoring pipeline** (Kafka → Flink SQL windowed features → Redis online store → XGBoost on ONNX Runtime) sustaining **`[X]k` events/s at `[Y]` ms p95 end to end** on a single 8 GB laptop. Latency was measured open-loop and corrected for coordinated omission; the bottleneck was identified as `[Flink / scorer CPU / Kafka]`.
* **Model serving and lifecycle:** XGBoost exported to ONNX (**`[Z]×` faster** than native inference, output parity < 1e-5). It beats a rules baseline at **`[R]`% recall at 1% FPR** (PR-AUC `[A]` vs `[B]`). A PyTorch challenger runs in shadow mode with **no measurable impact on champion p95**, and hot model swaps go through the MLflow registry without losing events.
* **Contract-first gRPC scoring API** (protobuf, bidirectional streaming, deadlines, health checks), benchmarked against REST/JSON on the same model: **p99 `[g]` ms vs `[r]` ms** `[or: "parity — inference dominates; serialization is <[s]% of the latency budget"]`.
* **Reliability and governance:** **0 lost events and recovery in `[T]` s** after killing the Flink TaskManager (`[N]` failure scenarios tested with written hypotheses). Evidently drift detection catches an injected fraud-pattern shift within `[M]` min. Backtests run in containers over versioned S3-compatible storage (MinIO). An audit of LLM-generated explanations found **`[H]`% unsupported claims** and fed a prompt fix that cut them to `[H2]`%.

Short version: *Kafka + Flink fraud scoring: **`[X]k` ev/s at p95 `[Y]` ms**, measured end to end; ONNX `[Z]×` faster than native; exactly-once recovery in `[T]` s.*

| Placeholder | Source |
|---|---|
| X, Y, bottleneck | E1 load steps (3 repetitions), M4 |
| Z, parity | ONNX export + benchmark, M3 |
| R, A, B | Offline evaluation, rules vs XGB, M3 |
| Shadow impact | E8, M6 |
| g, r, s | E11, M7c |
| T, N | F1–F10, M8 |
| M | Drift demo + Evidently, M6/M6b |
| H, H2 | Explanation audit (`judgekit`), PLAN §13 |

## Known gotchas

- **Flink `RANGE` OVER windows treat rows processed in the same millisecond as peers** (they aggregate together, so a burst from one user sees "future" payments). Exact semantics only for spaced payments; the parity test sends one payment at a time on purpose. Details and the plan (option B, E7) in PLAN.md §3.3.
- Windows consoles use cp1252: scripts that print non-ASCII must reconfigure stdout (see `scripts/doctor.py`).
- `docker compose run` replaces `command` with its arguments, so run-to-completion tools (`generator`, `latency`, `profile-builder`) use `entrypoint:`.
- `docker compose up` re-runs one-shot containers; `flink/submit-job.sh` is idempotent for that reason (a second SQL job would duplicate every event).
- Shell heredocs containing `\n` escapes or unbalanced quotes have been unreliable in this environment; write multi-line files with the editor tools.
