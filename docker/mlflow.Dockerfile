# MLflow tracking server + model registry (SQLite backend, artifacts proxied through the server).
FROM python:3.12-slim
RUN pip install --no-cache-dir "mlflow==3.18.0"
ENV MLFLOW_DISABLE_AGENT_HINT=1 MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false
EXPOSE 5000
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000", \
     "--backend-store-uri", "sqlite:////mlflow/mlflow.db", \
     "--artifacts-destination", "/mlflow/artifacts", "--serve-artifacts", \
     "--workers", "1", "--disable-security-middleware"]
