"""ModelRuntime that scores through a Triton Inference Server over gRPC (PLAN.md 2.7, experiment E4).

The same micro-batch the in-process ONNX model would take goes out as one request. What this variant
measures is the price of the network hop and of leaving Python: nothing else in the pipeline changes.
"""
import time
from collections.abc import Callable

import numpy as np
import tritonclient.grpc as grpcclient

from fraudcore.thresholds import Thresholds
from scorer.adapters.hot_swap import RegisteredModel
from scorer.adapters.triton_repository import TritonRepository
from scorer.application.ports import ModelRuntime, Prediction


class TritonUnavailable(Exception):
    """Triton did not answer in time or refused the request."""


class TritonModel:
    def __init__(self, client, model: str, version: str, name: str, timeout_s: float = 0.05,
                 input_name: str = "input", output_name: str = "probabilities",
                 thresholds: Thresholds | None = None, reference: dict | None = None) -> None:
        self._client, self._model, self._version, self._timeout = client, model, version, timeout_s
        self._input, self._output = input_name, output_name
        self.name, self.version, self.thresholds, self.reference = name, version, thresholds, reference

    def predict(self, X: np.ndarray) -> Prediction:
        batch = np.ascontiguousarray(X, dtype=np.float32)
        request = grpcclient.InferInput(self._input, list(batch.shape), "FP32")
        request.set_data_from_numpy(batch)
        try:
            result = self._client.infer(
                model_name=self._model, inputs=[request],
                outputs=[grpcclient.InferRequestedOutput(self._output)],
                model_version=self._version, client_timeout=self._timeout)
        except Exception as exc:                       # grpc errors, timeouts, connection refused
            raise TritonUnavailable(f"triton inference failed: {exc}") from exc
        scores = result.as_numpy(self._output)[:, 1]
        return Prediction(scores, self.name, self.version, self.thresholds, self.reference)


class TritonRuntimeBuilder:
    """HotSwapModel builder: publish the model to the repository, wait for Triton, serve through it."""

    def __init__(self, client, repository: TritonRepository, model: str, timeout_s: float = 0.05,
                 ready_timeout_s: float = 60.0, poll_s: float = 1.0,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self._client, self._repository, self._model = client, repository, model
        self._timeout, self._ready_timeout = timeout_s, ready_timeout_s
        self._poll, self._sleep = poll_s, sleep

    def __call__(self, loaded: RegisteredModel, name: str, version: str,
                 thresholds: Thresholds | None) -> ModelRuntime:
        spec = self._repository.publish(self._model, version, loaded.onnx_bytes)
        deadline = time.monotonic() + self._ready_timeout
        while not self._ready(version):
            if time.monotonic() >= deadline:
                raise TritonUnavailable(f"triton did not load {self._model} v{version} in time")
            self._sleep(self._poll)
        return TritonModel(self._client, self._model, version, name, self._timeout, spec.input_name,
                           spec.probabilities_name, thresholds, loaded.reference)

    def _ready(self, version: str) -> bool:
        try:
            return bool(self._client.is_model_ready(self._model, version))
        except Exception:
            return False
