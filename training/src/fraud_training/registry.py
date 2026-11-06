"""MLflow tracking + model registry, write side (PLAN.md sections 4.5, 7). The scorer reads it by alias."""
import tempfile
from pathlib import Path

import mlflow


def _flatten(prefix: str, value, out: dict[str, float]) -> None:
    if isinstance(value, dict):
        for k, v in value.items():
            _flatten(f"{prefix}.{k}" if prefix else str(k), v, out)
    elif isinstance(value, int | float) and not isinstance(value, bool):
        out[prefix] = float(value)


def register_model(*, tracking_uri: str, experiment: str, model_name: str, alias: str, onnx_bytes: bytes,
                   features: list[str], params: dict, metrics: dict, artifacts: dict | None = None,
                   tags: dict | None = None, artifact_root: str | None = None,
                   run_name: str | None = None, files: dict[str, bytes] | None = None) -> str:
    """Log a run, register its ONNX model as a new version and point ``alias`` at it. Returns the version.

    ``features`` is the model signature: the scorer refuses a model whose feature order is not its own.
    ``artifacts`` maps a file name to a JSON-serialisable dict stored next to the model; ``files`` does
    the same for raw bytes (e.g. the native XGBoost booster the explainer needs for SHAP).
    """
    mlflow.set_tracking_uri(tracking_uri)
    client = mlflow.MlflowClient(tracking_uri=tracking_uri)
    if client.get_experiment_by_name(experiment) is None:
        client.create_experiment(experiment, artifact_location=artifact_root)
    flat: dict[str, float] = {}
    _flatten("", metrics, flat)

    with mlflow.start_run(experiment_id=client.get_experiment_by_name(experiment).experiment_id,
                          run_name=run_name) as run:
        mlflow.log_params({k: str(v) for k, v in params.items()})
        mlflow.log_metrics(flat)
        mlflow.set_tags(tags or {})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "model.onnx"
            path.write_bytes(onnx_bytes)
            mlflow.log_artifact(str(path), "model")
        mlflow.log_dict(features, "model/features.json")
        for name, payload in (artifacts or {}).items():
            mlflow.log_dict(payload, f"model/{name}")
        for name, raw in (files or {}).items():
            with tempfile.TemporaryDirectory() as tmp:
                (Path(tmp) / name).write_bytes(raw)
                mlflow.log_artifact(str(Path(tmp) / name), "model")

    try:
        client.create_registered_model(model_name)
    except mlflow.exceptions.MlflowException:
        pass                                                   # already registered
    version_tags = {k: str(v) for k, v in (tags or {}).items()}
    mv = client.create_model_version(model_name, source=f"{run.info.artifact_uri}/model",
                                     run_id=run.info.run_id, tags=version_tags)
    client.set_registered_model_alias(model_name, alias, mv.version)
    return str(mv.version)
