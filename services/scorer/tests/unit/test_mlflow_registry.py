import json

import mlflow
import numpy as np
from scorer.adapters.hot_swap import HotSwapModel
from scorer.adapters.mlflow_registry import MlflowRegistry
from scorer.adapters.rule_model import RuleModel

from fraudcore.features import FEATURE_ORDER


def promote(uri: str, root: str, onnx: bytes, features: list[str]) -> None:
    """What the training job does: log the model files, register a version, move the alias."""
    client = mlflow.MlflowClient(tracking_uri=uri)
    if client.get_experiment_by_name("t") is None:
        client.create_experiment("t", artifact_location=root)
    mlflow.set_tracking_uri(uri)
    with mlflow.start_run(experiment_id=client.get_experiment_by_name("t").experiment_id) as run:
        mlflow.log_dict(features, "model/features.json")
        mlflow.log_text(json.dumps({}), "model/thresholds.json")
        mlflow.log_artifact(_write(onnx), "model")
    try:
        client.create_registered_model("fraud-champion")
    except mlflow.exceptions.MlflowException:
        pass
    mv = client.create_model_version("fraud-champion", f"{run.info.artifact_uri}/model", run.info.run_id)
    client.set_registered_model_alias("fraud-champion", "champion", mv.version)


def _write(data: bytes) -> str:
    import tempfile
    from pathlib import Path

    d = Path(tempfile.mkdtemp()) / "model.onnx"
    d.write_bytes(data)
    return str(d)


def test_alias_to_scores_through_a_real_mlflow_store(tmp_path, make_onnx):
    uri = f"sqlite:///{(tmp_path / 'm.db').as_posix()}"
    root = (tmp_path / "art").as_uri()
    registry = MlflowRegistry(uri, "fraud-champion", "champion")
    assert registry.current_version() is None                     # nothing promoted yet

    model = HotSwapModel(registry, "fraud-champion", RuleModel())
    X = np.ones((1, len(FEATURE_ORDER)))
    assert model.refresh() is False and model.predict(X).model == "rule-dummy"

    promote(uri, root, make_onnx(-0.5), list(FEATURE_ORDER))
    assert model.refresh() is True
    first = model.predict(X)
    assert first.version == "1" and first.scores[0] < 0.5

    promote(uri, root, make_onnx(0.5), list(FEATURE_ORDER))       # promotion moves the alias
    assert model.refresh() is True
    assert model.predict(X).version == "2" and model.predict(X).scores[0] > 0.5


def test_cache_keeps_the_scorer_alive_when_mlflow_is_down(tmp_path, make_onnx, monkeypatch):
    uri = f"sqlite:///{(tmp_path / 'm.db').as_posix()}"
    cache = tmp_path / "cache"
    promote(uri, (tmp_path / "art").as_uri(), make_onnx(0.5), list(FEATURE_ORDER))

    online = MlflowRegistry(uri, "fraud-champion", "champion", cache_dir=cache)
    HotSwapModel(online, "fraud-champion", RuleModel()).refresh()          # downloads and caches v1

    restarted = MlflowRegistry(uri, "fraud-champion", "champion", cache_dir=cache)

    def down(*_a, **_k):
        raise ConnectionError("mlflow down")

    monkeypatch.setattr(restarted._client, "get_model_version_by_alias", down)
    monkeypatch.setattr(restarted._client, "get_model_version", down)
    model = HotSwapModel(restarted, "fraud-champion", RuleModel())
    assert model.refresh() is True                                          # served from the cache
    assert model.predict(np.ones((1, len(FEATURE_ORDER)))).version == "1"


def test_without_cache_an_outage_is_an_error_the_model_logs_and_survives(tmp_path, monkeypatch):
    registry = MlflowRegistry(f"sqlite:///{(tmp_path / 'm.db').as_posix()}", "fraud-champion", "champion",
                              cache_dir=tmp_path / "empty")

    def down(*_a, **_k):
        raise ConnectionError("mlflow down")

    monkeypatch.setattr(registry._client, "get_model_version_by_alias", down)
    model = HotSwapModel(registry, "fraud-champion", RuleModel())
    assert model.refresh() is False and model.predict(np.ones((1, len(FEATURE_ORDER)))).model == "rule-dummy"
