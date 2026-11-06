"""BoosterSource over the MLflow registry: the native booster registered next to the ONNX model."""
from pathlib import Path

import mlflow


class MlflowBoosterSource:
    def __init__(self, tracking_uri: str, model_name: str) -> None:
        self._client = mlflow.MlflowClient(tracking_uri=tracking_uri)
        self._uri, self._name = tracking_uri, model_name

    def fetch(self, version: str) -> bytes | None:
        try:
            mv = self._client.get_model_version(self._name, version)
            local = mlflow.artifacts.download_artifacts(artifact_uri=f"{mv.source}/booster.json",
                                                        tracking_uri=self._uri)
        except Exception:                       # registered without a booster (LightGBM, MLP) or no registry
            return None
        return Path(local).read_bytes()
