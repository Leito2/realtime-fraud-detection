# On-demand job image (PLAN.md 13): backtests and drift reports. Heavier than the service image because it
# reuses the training code (XGBoost, LightGBM, Evidently), so it is built and run only when asked.
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1 PATH="/app/.venv/bin:$PATH"
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml uv.lock ./
COPY packages ./packages
COPY services ./services
COPY training ./training
COPY bench ./bench
COPY backtest ./backtest
RUN uv sync --frozen --no-dev --package fraud-backtest
ENTRYPOINT ["backtest"]
