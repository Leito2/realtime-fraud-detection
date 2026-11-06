"""Registry sabotage for F7: register broken model versions under the champion alias, then put things back."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import mlflow

URI = "http://localhost:5000"
NAME = "fraud-champion"


def champion_version(uri: str = URI) -> str:
    return str(mlflow.MlflowClient(tracking_uri=uri).get_model_version_by_alias(NAME, "champion").version)


def _register_broken(onnx_bytes: bytes, features: list[str], tag: str, uri: str) -> str:
    mlflow.set_tracking_uri(uri)
    client = mlflow.MlflowClient(tracking_uri=uri)
    experiment = client.get_experiment_by_name("fraud-models") or client.get_experiment_by_name("chaos")
    exp_id = experiment.experiment_id if experiment else client.create_experiment("chaos")
    with mlflow.start_run(experiment_id=exp_id, run_name=f"chaos-{tag}") as run, \
            tempfile.TemporaryDirectory() as tmp:
        (Path(tmp) / "model.onnx").write_bytes(onnx_bytes)
        (Path(tmp) / "features.json").write_text(json.dumps(features), encoding="utf-8")
        mlflow.log_artifacts(tmp, "model")
    mv = client.create_model_version(NAME, source=f"{run.info.artifact_uri}/model", run_id=run.info.run_id,
                                     tags={"chaos": tag})
    client.set_registered_model_alias(NAME, "champion", mv.version)
    return str(mv.version)


def register_corrupt_champion(uri: str = URI) -> str:
    """Bytes that are not an ONNX graph, with a plausible signature."""
    from fraudcore.features import FEATURE_ORDER

    return _register_broken(b"this is not an onnx model", list(FEATURE_ORDER), "corrupt-bytes", uri)


def register_skewed_champion(uri: str = URI) -> str:
    """A model whose feature order differs from fraudcore's: serving it would be silent train/serve skew."""
    from fraudcore.features import FEATURE_ORDER

    return _register_broken(b"irrelevant", list(reversed(FEATURE_ORDER)), "skewed-signature", uri)


def restore_champion(version: str, uri: str = URI) -> None:
    mlflow.MlflowClient(tracking_uri=uri).set_registered_model_alias(NAME, "champion", version)
