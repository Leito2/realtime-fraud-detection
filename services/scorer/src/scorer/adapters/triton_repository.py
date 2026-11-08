"""Writes the champion's ONNX into a Triton model repository (PLAN.md 2.7, experiment E4).

Triton runs with ``--model-control-mode=poll``: a new numeric version directory is loaded by itself, and
the previous version keeps serving until the new one is ready. The scorer therefore promotes a model by
writing it here and then waiting until Triton reports that version ready (``TritonRuntimeBuilder``).

``max_batch_size: 0`` on purpose: the scorer already sends a micro-batch (ADR-2), so the batch dimension is
part of the tensor shape and Triton's own dynamic batching has nothing to merge with a single client.
"""
import os
from dataclasses import dataclass
from pathlib import Path

import onnxruntime as ort

_TYPES = {"tensor(float)": "TYPE_FP32", "tensor(double)": "TYPE_FP64", "tensor(int64)": "TYPE_INT64",
          "tensor(int32)": "TYPE_INT32"}


@dataclass(frozen=True)
class TritonSpec:
    input_name: str
    probabilities_name: str


def _dims(shape: list) -> str:
    return "[ " + ", ".join(str(-1 if d is None or isinstance(d, str) else d) for d in shape) + " ]"


def _tensor(meta) -> str:
    return f'  {{ name: "{meta.name}" data_type: {_TYPES[meta.type]} dims: {_dims(meta.shape)} }}'


class TritonRepository:
    def __init__(self, root: Path | str) -> None:
        self._root = Path(root)

    def publish(self, model: str, version: str, onnx_bytes: bytes) -> TritonSpec:
        if not version.isdigit():
            raise ValueError(f"Triton versions are integers, got {version!r}")
        session = ort.InferenceSession(onnx_bytes, providers=["CPUExecutionProvider"])
        inputs, outputs = session.get_inputs(), session.get_outputs()
        spec = TritonSpec(inputs[0].name, outputs[1].name)       # outputs: label, probabilities (N x 2)
        directory = self._root / model
        directory.mkdir(parents=True, exist_ok=True)
        config = (
            f'name: "{model}"\nplatform: "onnxruntime_onnx"\nmax_batch_size: 0\n'
            "input [\n" + ",\n".join(_tensor(i) for i in inputs) + "\n]\n"
            "output [\n" + ",\n".join(_tensor(o) for o in outputs) + "\n]\n"
            "version_policy: { latest: { num_versions: 2 } }\n"
            "instance_group [ { kind: KIND_CPU count: 1 } ]\n")
        config_path = directory / "config.pbtxt"
        if not config_path.exists() or config_path.read_text() != config:
            config_path.write_text(config)
        target = directory / version
        if target.exists():
            return spec
        staged = directory / f"{version}.partial"               # not numeric: Triton ignores it
        staged.mkdir(exist_ok=True)
        (staged / "model.onnx").write_bytes(onnx_bytes)
        os.replace(staged, target)                              # appears complete or not at all
        return spec
