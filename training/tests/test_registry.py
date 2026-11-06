import json
from pathlib import Path

import mlflow
from fraud_training.registry import register_model

from fraudcore.features import FEATURE_ORDER


def test_register_sets_alias_and_stores_model_features_thresholds(tmp_path):
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    kwargs = dict(
        tracking_uri=uri, artifact_root=(tmp_path / "artifacts").as_uri(), experiment="t", model_name="fraud",
        alias="champion", onnx_bytes=b"fake-onnx", features=FEATURE_ORDER, params={"max_depth": 4},
        metrics={"pr_auc": 0.8, "nested": {"x": 1.5}}, artifacts={"thresholds.json": {"review": 0.3}},
        tags={"dataset_digest": "abc"},
    )
    v1 = register_model(**kwargs)
    v2 = register_model(**kwargs)
    assert (v1, v2) == ("1", "2")

    client = mlflow.MlflowClient(tracking_uri=uri)
    mv = client.get_model_version_by_alias("fraud", "champion")
    assert str(mv.version) == "2" and mv.tags["dataset_digest"] == "abc"
    local = mlflow.artifacts.download_artifacts(artifact_uri=mv.source, dst_path=str(tmp_path / "dl"))
    assert (Path(local) / "model.onnx").read_bytes() == b"fake-onnx"
    run = client.get_run(mv.run_id)
    assert run.data.metrics["pr_auc"] == 0.8 and run.data.metrics["nested.x"] == 1.5
    assert json.loads((Path(local) / "features.json").read_text()) == FEATURE_ORDER
    assert json.loads((Path(local) / "thresholds.json").read_text()) == {"review": 0.3}
