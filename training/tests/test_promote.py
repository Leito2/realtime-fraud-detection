import mlflow
import pytest
from fraud_training.promote import Criteria, PromotionRefused, evaluate_criteria, promote
from fraud_training.registry import register_model

from fraudcore.features import FEATURE_ORDER

CRITERIA = Criteria(min_labelled=1000, min_recall_gain=0.02, max_fpr_increase=0.005)


def confusion(model, tp, fn, fp, tn):
    return [(model, True, True, tp), (model, False, True, fn), (model, True, False, fp),
            (model, False, False, tn)]


def rows(champion, challenger):
    return confusion("champion", *champion) + confusion("challenger", *challenger)


def test_a_better_challenger_with_enough_labels_passes():
    verdict = evaluate_criteria(rows((70, 30, 100, 9800), (85, 15, 100, 9800)), CRITERIA)
    assert verdict.ok and verdict.recall_gain == pytest.approx(0.15)


def test_too_few_labelled_events_is_not_evidence():
    verdict = evaluate_criteria(rows((7, 3, 10, 80), (9, 1, 10, 80)), CRITERIA)
    assert not verdict.ok and "labelled" in verdict.reason


def test_a_small_gain_is_not_enough():
    verdict = evaluate_criteria(rows((70, 30, 100, 9800), (71, 29, 100, 9800)), CRITERIA)
    assert not verdict.ok and "recall" in verdict.reason


def test_more_recall_bought_with_much_more_friction_is_refused():
    verdict = evaluate_criteria(rows((70, 30, 100, 9800), (90, 10, 400, 9500)), CRITERIA)
    assert not verdict.ok and "FPR" in verdict.reason


def test_a_missing_model_is_refused_with_a_reason():
    verdict = evaluate_criteria(confusion("champion", 1, 1, 1, 1), CRITERIA)
    assert not verdict.ok and "challenger" in verdict.reason


def _register(tmp_path, uri, alias):
    register_model(tracking_uri=uri, artifact_root=(tmp_path / "a").as_uri(), experiment="t",
                   model_name="fraud", alias=alias, onnx_bytes=b"x", features=FEATURE_ORDER, params={},
                   metrics={})


@pytest.fixture()
def registry(tmp_path):
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    _register(tmp_path, uri, "champion")
    _register(tmp_path, uri, "challenger")
    return uri


def test_promote_moves_the_champion_alias_to_the_challenger_version(registry):
    result = promote(registry, "fraud")
    client = mlflow.MlflowClient(tracking_uri=registry)
    assert (result.version, result.previous) == ("2", "1")
    assert str(client.get_model_version_by_alias("fraud", "champion").version) == "2"
    assert client.get_model_version("fraud", "2").tags["promoted_from"] == "challenger"


def test_promote_without_a_challenger_is_refused(tmp_path):
    uri = f"sqlite:///{(tmp_path / 'm.db').as_posix()}"
    _register(tmp_path, uri, "champion")
    with pytest.raises(PromotionRefused):
        promote(uri, "fraud")
