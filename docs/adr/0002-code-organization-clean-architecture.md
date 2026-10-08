# ADR-0002: Code organization — event-driven services, Clean Architecture where it pays

- **Status:** accepted
- **Date:** 2026-10-08

## Context
The repo holds several kinds of things: Python services that run as separate processes, Flink SQL jobs,
infrastructure and observability config, training scripts, and benchmark/chaos tooling. Two questions
were being mixed up:

1. **System level** — which programs exist and how they talk to each other.
2. **Code level** — how the code *inside* one program is organized.

Clean Architecture and Hexagonal Architecture answer question 2 only. Applying one of them uniformly to
every service would add layers to services that are just a consume → transform → write loop. Applying
none would leave the services with real business rules and swappable dependencies (the scorer and the
explainer) hard to test and hard to extend.

The critical path has a p95 budget of 80 ms and depends on micro-batching (one Redis round trip and one
ONNX call per batch, PLAN §2.6). Any abstraction on that path must keep the batch shape.

## Decision

### System level: event-driven services in a monorepo
- One folder per deployable process under `services/`; they communicate through Kafka topics (and
  gRPC/HTTP for synchronous scoring). Each is its own consumer group.
- Non-code artifacts are grouped by technology (`flink/`, `infra/`, `observability/`, `config/`) because
  they contain no business logic to organize.
- Shared code lives in `packages/` as uv workspace members. Each service becomes a workspace member with
  its own `pyproject.toml` (`src/<name>/` + `tests/`) when it gets code.

### Shared packages: keep the domain pure
- **`fraudcore`** is the *Entities* ring shared by every application (training, scorer, API, explainer):
  feature definitions, event contracts, thresholds/decision rules. It has **no I/O and no infrastructure
  dependencies**. It is the structural defense against train/serve skew.
- **`fraudinfra`** (new, created when first needed) holds shared infrastructure: model loading from the
  MLflow registry, OpenTelemetry/Prometheus helpers, Kafka header propagation. This moves `model_io.py`
  and `telemetry.py` out of `fraudcore`, where PLAN §10.2 originally placed them.

### Code level: Clean Architecture, applied by need
A service gets the full layout when it has **business rules worth isolating and more than one
implementation of a dependency or more than one way of being called**:

| Service | Layout | Reason |
|---|---|---|
| `scorer` | Clean Architecture | Called from Kafka, gRPC and REST; model runs on ONNX in-process or Triton; champion/shadow modes |
| `explainer` | Clean Architecture | LLM provider `mock`/`ollama` plus a deterministic template fallback; SHAP backend |
| `api` | Thin | Routes call query functions; `POST /score` calls the scorer's use case |
| `generator`, `feature_writer`, `profile_builder`, `auditor`, `latency` | Thin | Pipes with little logic: pure functions + `main.py` that does the I/O |

Layout for a Clean Architecture service (scorer shown):

```
services/scorer/src/scorer/
├── domain/        # service-specific rules (e.g. degraded-mode / fail-open policy, PLAN §7.4); pure
├── application/   # use cases + the ports (typing.Protocol) they need
│   ├── ports.py         # ModelRuntime, ProfileStore, VelocityStore, DecisionSink
│   ├── score_batch.py   # Kafka path: enriched events → decisions
│   └── score_sync.py    # gRPC/REST path: raw payment → fetch velocity + profile → decision
├── adapters/      # implementations of the ports (driven side)
│   ├── onnx_model.py · triton_model.py
│   ├── redis_store.py
│   └── kafka_decisions.py
├── entrypoints/   # what calls the use cases (driving side)
│   ├── kafka_consumer.py
│   └── grpc_server.py
└── main.py        # composition root: reads config, picks adapters by --mode, starts an entrypoint
```

Mapping to the Clean Architecture rings: Entities = `fraudcore` + `domain/`; Use Cases = `application/`;
Interface Adapters = `adapters/` + `entrypoints/`; Frameworks & Drivers = the libraries themselves
(`onnxruntime`, `confluent-kafka`, `grpc.aio`, `redis`) plus `main.py`.

Rules:
1. **Dependency rule:** imports point inward only. `domain` imports nothing from the service;
   `application` imports `domain` and `fraudcore`; `adapters` and `entrypoints` import `application`;
   only `main.py` knows every layer. Enforced with `import-linter` layer contracts in CI once the scorer
   exists.
2. **Batch-shaped ports on the critical path:** `ProfileStore.get_many(user_ids)`,
   `ModelRuntime.predict(matrix)`, `DecisionSink.publish_batch(decisions)` — never a per-event call that
   hides N round trips.
3. **One scoring core, several processes:** the gRPC server (PLAN §14) and the REST `POST /score` are
   entrypoints into the scorer's application layer, deployed as their own containers from the same image.
   There is no separate scoring implementation per protocol.
4. Use cases are tested with in-memory fakes of the ports; adapters get integration tests against the
   real infrastructure.

## Consequences
- The decision logic (features → score → decision) exists once and is tested without Kafka, Redis or
  ONNX. Swapping ONNX for Triton (PLAN §2.7) or `mock` for `ollama` is a new adapter plus a line in
  `main.py`.
- Thin services stay readable; nobody has to cross four folders to follow a 60-line consumer.
- Two layouts coexist in `services/`. The table above is the rule for which one a service uses; a thin
  service that grows real rules or a second implementation of a dependency moves to the full layout.
- `fraudcore`'s event contracts use pydantic, a library inside the Entities ring. Accepted on purpose:
  pydantic is stable, has no I/O, and validation is part of the contract. Infrastructure libraries are
  not allowed there.
- `api` depends on the `scorer` package for `POST /score`; both are workspace members, so this is an
  ordinary import.

## Alternatives considered
- **Clean Architecture in every service:** rejected; most services would be layers with one function each.
- **Layer-first repo** (`domain/`, `application/`, `infrastructure/` at the top level for the whole
  system): rejected; it hides which processes exist and couples services that deploy independently.
- **Hexagonal vocabulary** (ports/adapters, driving/driven) instead of Clean Architecture: equivalent in
  practice. Clean Architecture was chosen for its explicit Entities/Use Cases split, which maps directly
  to `fraudcore` (shared rules) vs. each service's use cases. The `ports.py` name is kept because the term is
  standard in both.
