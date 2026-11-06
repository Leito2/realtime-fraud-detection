"""Exact TreeSHAP from the native XGBoost booster of the model version that made the decision.

The ONNX graph cannot give attributions, so training registers the booster next to it. ``pred_contribs`` is
XGBoost's own TreeSHAP implementation (the algorithm behind ``shap.TreeExplainer``) with no extra dependency.
"""
import threading
from typing import Protocol

import numpy as np
import xgboost as xgb

from explainer.application.ports import NoExplanationModel
from fraudcore.features import FEATURE_ORDER


class BoosterSource(Protocol):
    def fetch(self, version: str) -> bytes | None: ...


class XgboostShap:
    def __init__(self, source: BoosterSource) -> None:
        self._source = source
        self._boosters: dict[str, xgb.Booster | None] = {}
        self._lock = threading.Lock()                 # workers must not fetch the same version in parallel

    def _booster(self, version: str) -> xgb.Booster:
        with self._lock:
            if version not in self._boosters:
                raw = self._source.fetch(version)
                self._boosters[version] = xgb.Booster(model_file=bytearray(raw)) if raw else None
            booster = self._boosters[version]
        if booster is None:
            raise NoExplanationModel(version)
        return booster

    def contributions(self, model_version: str, features: dict[str, float]) -> dict[str, float]:
        booster = self._booster(model_version)
        row = np.array([[features[f] for f in FEATURE_ORDER]], dtype=np.float32)
        contribs = booster.predict(xgb.DMatrix(row), pred_contribs=True)[0]        # last column is the bias
        return {name: float(value) for name, value in zip(FEATURE_ORDER, contribs[:-1], strict=True)}
