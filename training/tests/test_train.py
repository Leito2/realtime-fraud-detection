import json
from datetime import UTC, datetime

import mlflow
import yaml
from fraud_training.train import Settings, run
from generator.logic import Population
from generator.offline import generate_history


def test_end_to_end_small_run_registers_a_verified_champion(tmp_path):
    hist = generate_history(Population(n_users=400, seed=5), seed=5, start=datetime(2026, 8, 1, tzinfo=UTC),
                            days=14, payments_per_day=1_500)
    path = tmp_path / "history.parquet"
    hist.write_parquet(path)
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    settings = Settings(history=path, tracking_uri=uri, artifact_root=(tmp_path / "art").as_uri(),
                        trials=0, valid_days=3, test_days=3, results_out=tmp_path / "results.json",
                        thresholds_out=tmp_path / "thresholds.yaml", max_review_rate=0.05)
    results = run(settings)

    assert {"rules", "xgboost", "xgboost_onnx", "lightgbm", "lightgbm_onnx"} <= set(results["models"])
    assert results["onnx_max_abs_diff"] < 1e-5
    assert results["champion"] in {"xgboost", "lightgbm"}

    mv = mlflow.MlflowClient(tracking_uri=uri).get_model_version_by_alias("fraud-champion", "champion")
    assert mv.tags["dataset_digest"] == results["dataset"]["digest"]
    th = yaml.safe_load((tmp_path / "thresholds.yaml").read_text())
    assert 0 < th["review"] < th["block"] <= 1 and th["threshold_set"].endswith(f"v{mv.version}")
    assert json.loads((tmp_path / "results.json").read_text())["champion"] == results["champion"]
