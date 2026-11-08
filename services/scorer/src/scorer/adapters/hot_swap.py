"""A ModelRuntime that follows a registry alias and swaps models without a restart (PLAN.md section 3.6).

The active model is one attribute. A swap is a single reference assignment, so a batch is scored entirely
by the old or entirely by the new model, and ``Prediction`` carries the version that produced it. Until a
champion exists (or if the registry is down) the current model keeps serving: promotion never costs an event.
"""
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

import numpy as np

from fraudcore.features import FEATURE_ORDER
from fraudcore.thresholds import Thresholds
from scorer.adapters.onnx_model import OnnxModel
from scorer.application.ports import ModelRuntime, Prediction

log = logging.getLogger("scorer.hot_swap")


@dataclass(frozen=True)
class RegisteredModel:
    onnx_bytes: bytes
    features: list[str]
    thresholds: dict | None = None            # {'review': x, 'block': y}, tuned for this model's scores
    reference: dict | None = None             # {'edges': [...], 'fractions': [...]} from training


class ModelRegistry(Protocol):
    def current_version(self) -> str | None: ...
    def fetch(self, version: str) -> RegisteredModel: ...


# Turns a registry model into something that can score: in-process ONNX by default, Triton when configured.
RuntimeBuilder = Callable[[RegisteredModel, str, str, Thresholds | None], ModelRuntime]


class HotSwapModel:
    def __init__(self, registry: ModelRegistry, name: str, fallback: ModelRuntime, poll_s: float = 30.0,
                 intra_op_threads: int = 1, build: RuntimeBuilder | None = None) -> None:
        self._registry, self._name, self._poll_s, self._threads = registry, name, poll_s, intra_op_threads
        self._build = build or self._build_onnx
        self._active: ModelRuntime = fallback
        self._version: str | None = None
        self._rejected: set[str] = set()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _build_onnx(self, loaded: RegisteredModel, name: str, version: str,
                    thresholds: Thresholds | None) -> ModelRuntime:
        return OnnxModel(loaded.onnx_bytes, name, version, self._threads, thresholds, loaded.reference)

    def predict(self, X: np.ndarray) -> Prediction:
        return self._active.predict(X)

    def refresh(self) -> bool:
        """Load the alias target if it changed. True when the active model was replaced."""
        try:
            version = self._registry.current_version()
            if version is None or version == self._version or version in self._rejected:
                return False
            loaded = self._registry.fetch(version)
        except Exception:
            log.exception("registry unavailable; keeping the current model")
            return False
        if loaded.features != FEATURE_ORDER:                       # train/serve skew guard
            log.error("model v%s has a different feature order; refusing to load it", version)
            self._rejected.add(version)
            return False
        try:
            thresholds = None
            if loaded.thresholds:
                cut = loaded.thresholds
                thresholds = Thresholds(f"{self._name}-v{version}", cut["review"], cut["block"])
            model = self._build(loaded, self._name, version, thresholds)
        except Exception:
            log.exception("model v%s could not be loaded; refusing it", version)
            self._rejected.add(version)
            return False
        self._active, self._version = model, version
        log.info("serving %s v%s", self._name, version)
        return True

    def start(self) -> None:
        self.refresh()
        self._thread = threading.Thread(target=self._loop, name="model-poller", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _loop(self) -> None:
        while not self._stop.wait(self._poll_s):
            self.refresh()
