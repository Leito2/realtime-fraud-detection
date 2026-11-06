.DEFAULT_GOAL := help
PROFILE ?= core
.PHONY: help doctor setup up down ps logs test lint fmt smoke profiles parity bench chaos train promote history trace-up

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
lint:      ## Lint + dependency rule (ADR-0002)
	uv run ruff check .
	uv run lint-imports --config services/scorer/pyproject.toml
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
bench:     ## Load protocol E1 (rate steps x3, p95/p99, sustained throughput); BENCH_ARGS to override
	uv run python -m fraud_bench.run e1 $(BENCH_ARGS)
chaos promote:   ## Implemented in later milestones (PLAN.md §11)
	@echo "'$@' arrives in a later milestone — see PLAN.md §11"; exit 1
