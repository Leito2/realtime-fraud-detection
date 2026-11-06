"""ModelRegistry on the MLflow server: resolve an alias, download the ONNX file and its feature signature.

Every downloaded version is cached on disk. When MLflow is unreachable (failure F7) the newest cached
version is served, so a registry outage or a restart during one never stops scoring.
"""
import json
import tempfile
from pathlib import Path

import mlflow
from mlflow.exceptions import MlflowException

from scorer.adapters.hot_swap import RegisteredModel


def _read_json(path: Path) -> dict | None:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


class MlflowRegistry:
    def __init__(self, tracking_uri: str, model_name: str, alias: str, cache_dir: Path | None = None) -> None:
        self._client = mlflow.MlflowClient(tracking_uri=tracking_uri, registry_uri=tracking_uri)
        self._uri, self._name, self._alias = tracking_uri, model_name, alias
        self._cache = Path(cache_dir) / alias if cache_dir else None

    def current_version(self) -> str | None:
        try:
            return str(self._client.get_model_version_by_alias(self._name, self._alias).version)
        except MlflowException as e:
            if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                return None                                          # nothing promoted yet
            return self._newest_cached_or_raise(e)
        except Exception as e:                                       # network errors: registry is down
            return self._newest_cached_or_raise(e)

    def fetch(self, version: str) -> RegisteredModel:
        cached = self._read_cache(version)
        if cached:
            return cached
        mv = self._client.get_model_version(self._name, version)
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(mlflow.artifacts.download_artifacts(artifact_uri=mv.source, dst_path=tmp,
                                                             tracking_uri=self._uri))
            model = RegisteredModel((local / "model.onnx").read_bytes(),
                                    json.loads((local / "features.json").read_text(encoding="utf-8")),
                                    _read_json(local / "thresholds.json"),
                                    _read_json(local / "score_reference.json"))
        self._write_cache(version, model)
        return model

    def _newest_cached_or_raise(self, error: Exception) -> str:
        root = self._cache
        versions = [int(p.name) for p in root.iterdir() if p.name.isdigit()] if root and root.exists() else []
        if not versions:
            raise error
        return str(max(versions))

    def _read_cache(self, version: str) -> RegisteredModel | None:
        if not self._cache:
            return None
        d = self._cache / version
        if not (d / "model.onnx").exists() or not (d / "features.json").exists():
            return None
        return RegisteredModel((d / "model.onnx").read_bytes(),
                               json.loads((d / "features.json").read_text(encoding="utf-8")),
                               _read_json(d / "thresholds.json"), _read_json(d / "score_reference.json"))

    def _write_cache(self, version: str, model: RegisteredModel) -> None:
        if not self._cache:
            return
        d = self._cache / version
        d.mkdir(parents=True, exist_ok=True)
        (d / "features.json").write_text(json.dumps(model.features), encoding="utf-8")
        if model.thresholds:
            (d / "thresholds.json").write_text(json.dumps(model.thresholds), encoding="utf-8")
        if model.reference:
            (d / "score_reference.json").write_text(json.dumps(model.reference), encoding="utf-8")
        (d / "model.onnx").write_bytes(model.onnx_bytes)       # written last: marks a complete entry
