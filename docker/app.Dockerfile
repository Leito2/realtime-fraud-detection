# One Python image for every service; the entrypoint selects the service (PLAN.md §10.2).
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1
COPY pyproject.toml ./
COPY packages ./packages
COPY services ./services
RUN uv sync --no-dev --all-packages
