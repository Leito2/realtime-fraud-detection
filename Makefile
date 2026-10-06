.DEFAULT_GOAL := help
PROFILE ?= core
.PHONY: help doctor setup up down ps logs test lint fmt smoke bench chaos train promote

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
lint:      ## Lint
	uv run ruff check .
fmt:       ## Format
	uv run ruff format .
smoke train bench chaos promote:   ## Implemented in later milestones (PLAN.md §11)
	@echo "'$@' arrives in a later milestone — see PLAN.md §11"; exit 1
