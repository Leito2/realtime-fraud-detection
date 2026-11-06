# One Python image for every service; the command selects the service (PLAN.md §10.2).
# Training (xgboost, lightgbm, optuna) is not installed here: it runs on the host / in its own job.
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1 PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock ./
COPY packages ./packages
COPY services ./services
COPY training/pyproject.toml ./training/pyproject.toml
RUN uv sync --frozen --no-dev --package scorer --package generator --package latency \
    --package profile_builder --package feature_writer --package auditor --package explainer --package api --package fraud-bench
