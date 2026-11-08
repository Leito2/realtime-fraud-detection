.DEFAULT_GOAL := help
PROFILE ?= core
.PHONY: proto datalake backtest drift-report drift-demo help doctor setup up down ps logs test lint fmt smoke profiles parity bench chaos train train-challenger promote history trace-up shadow e4 e5 e6 e10 sync-vs-flink

help:      ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-9s %s\n", $$1, $$2}'
doctor:    ## Check prerequisites (Docker, uv, RAM, WSL limit, ports, disk)
	python scripts/doctor.py
setup:     ## Install Python dependencies (uv workspace)
	uv sync --all-packages --dev
up:        ## Start a profile: make up PROFILE=core|ops|explain|chaos
	docker compose --profile $(PROFILE) up -d
down:      ## Stop everything
	docker compose --profile core --profile ops --profile explain --profile chaos down
ps:        ## Show running services
	docker compose ps
logs:      ## Tail logs
	docker compose logs -f --tail=100
test:      ## Unit + contract tests
	uv run pytest -q
proto:     ## Regenerate the gRPC Python stubs from proto/fraud/v1/scoring.proto
	uv run python -m grpc_tools.protoc -I proto --python_out=packages/fraudproto/src --grpc_python_out=packages/fraudproto/src --pyi_out=packages/fraudproto/src proto/fraud/v1/scoring.proto
lint:      ## Lint + dependency rule (ADR-0002)
	uv run ruff check .
	uv run lint-imports --config services/scorer/pyproject.toml
	uv run lint-imports --config services/explainer/pyproject.toml
fmt:       ## Format
	uv run ruff format .
trace-up:  ## Core + ops + tracing (OTel Collector, Tempo); the scorer then exports 1% traces
	OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 docker compose --profile core --profile ops --profile trace up -d
profiles:  ## Simulate 20 days of history and load user profiles into Redis (needs `make up`)
	docker compose --profile tools run --rm -T generator --offline /data/history.parquet --days 20 --payments-per-day 20000
	docker compose --profile tools run --rm -T profile-builder --history /data/history.parquet
parity:    ## Feature parity: Flink output vs fraudcore reference (needs `make up`)
	uv run pytest tests/integration -m integration -q
smoke:     ## End-to-end smoke: p50/p95/p99 at RATE ev/s (default 2000)
	uv run python scripts/smoke.py --rate $(or $(RATE),2000)
history:   ## Simulate 60 days (600k payments, fraud scenarios) for training: data/history_full.parquet
	uv run python -m generator.main --offline data/history_full.parquet --days 60 --payments-per-day 10000 --users 20000
train:     ## Train rules/XGBoost/LightGBM, verify ONNX, register the champion in MLflow (needs `make up`)
	uv run python -m fraud_training.train --history data/history_full.parquet $(TRAIN_ARGS)
train-challenger:  ## Train the PyTorch MLP and register it as the `challenger` alias (needs `uv sync --extra nn`)
	uv run train-challenger --history data/history_full.parquet $(TRAIN_ARGS)
promote:   ## Move the champion alias to MODEL (default challenger) if config/promotion.yaml is met; FORCE=1 skips the check
	uv run promote --source $(or $(MODEL),challenger) $(if $(FORCE),--force,)
shadow:    ## Labelled traffic in the regime the models were trained on, for the champion-vs-challenger views (RATE=20 DURATION=300)
	docker compose --profile tools run --rm -T generator --rate $(or $(RATE),20) --duration $(or $(DURATION),300) --users 20000 --zipf 0 --scenarios
datalake:  ## Publish data/history_full.parquet to MinIO as a new object version (with SHA-256 manifest)
	uv run backtest publish data/history_full.parquet
backtest:  ## Champion vs challenger on the versioned dataset in MinIO -> docs/results/m6b-backtest.md
	uv run backtest run $(BACKTEST_ARGS)
drift-report:  ## Evidently: training features vs the ones the scorer served (start it with LOG_FEATURES=1)
	uv run backtest drift $(DRIFT_ARGS)
drift-demo:  ## The whole online MLOps loop: new fraud pattern, retrain, shadow, promote, recovery (~25 min)
	uv run python scripts/drift_demo.py $(DEMO_ARGS)
bench:     ## Load protocol E1 (rate steps x3, p95/p99, sustained throughput); BENCH_ARGS to override
	uv run python -m fraud_bench.run e1 $(BENCH_ARGS)
chaos:     ## Failure experiment with a written hypothesis: make chaos EXP=F1 (report in docs/resilience/)
	uv run chaos $(EXP)
e4:        ## E4: native vs ONNX in-process vs Triton, per batch size (stack + serving-triton up; QUIET MACHINE)
	docker compose --profile core --profile serving-triton --profile tools run --rm -T e4 micro $(E4_ARGS)
e5:        ## E5: cost of exactly-once vs at-least-once (checkpoint 1/5/10 s) -> docs/results/e5-guarantees.md
	uv run python scripts/e5_guarantees.py $(E5_ARGS)
e6:        ## E6: processing-time vs event-time window (watermark cost) -> docs/results/e6-event-time.md
	uv run python scripts/e6_event_time.py $(E6_ARGS)
e10:       ## E10: scorer processes x Flink parallelism at a fixed rate -> docs/results/e10-scaling.md
	uv run python scripts/e10_scaling.py $(E10_ARGS)
sync-vs-flink:  ## ADR-0012: error of synchronous velocity vs the Flink window -> docs/results/m7c-sync-vs-flink.md
	uv run python -m fraud_bench.sync_vs_flink $(SVF_ARGS)
