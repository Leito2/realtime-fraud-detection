"""ModelRuntime on ONNX Runtime: one batched call per micro-batch (PLAN.md section 7, ADR-2)."""
import numpy as np
import onnxruntime as ort

from fraudcore.thresholds import Thresholds
from scorer.application.ports import Prediction


class OnnxModel:
    def __init__(self, onnx_bytes: bytes, name: str, version: str, intra_op_threads: int = 1,
                 thresholds: Thresholds | None = None) -> None:
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = intra_op_threads
        opts.add_session_config_entry("session.intra_op.allow_spinning", "0")     # no busy-wait
        opts.log_severity_level = 3
        self._session = ort.InferenceSession(onnx_bytes, opts, providers=["CPUExecutionProvider"])
        self._input = self._session.get_inputs()[0].name
        self._probabilities = self._session.get_outputs()[1].name      # outputs: label, probabilities (N x 2)
        self.name, self.version, self.thresholds = name, version, thresholds

    def predict(self, X: np.ndarray) -> Prediction:
        batch = np.ascontiguousarray(X, dtype=np.float32)
        scores = self._session.run([self._probabilities], {self._input: batch})[0][:, 1]
        return Prediction(scores, self.name, self.version, self.thresholds)
