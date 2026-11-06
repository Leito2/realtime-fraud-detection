import numpy as np
import pytest
from onnx import TensorProto, helper, numpy_helper

from fraudcore.features import FEATURE_ORDER


def build_onnx(weight: float, n_features: int = len(FEATURE_ORDER)) -> bytes:
    """P(fraud) = sigmoid(weight * sum of features); outputs (label, probabilities) like the real export."""
    w = numpy_helper.from_array(np.full((n_features, 1), weight, dtype=np.float32), "w")
    one = numpy_helper.from_array(np.array([1.0], dtype=np.float32), "one")
    nodes = [
        helper.make_node("MatMul", ["input", "w"], ["z"]),
        helper.make_node("Sigmoid", ["z"], ["p1"]),
        helper.make_node("Sub", ["one", "p1"], ["p0"]),
        helper.make_node("Concat", ["p0", "p1"], ["probabilities"], axis=1),
        helper.make_node("ArgMax", ["probabilities"], ["label"], axis=1, keepdims=0),
    ]
    graph = helper.make_graph(
        nodes, "tiny", [helper.make_tensor_value_info("input", TensorProto.FLOAT, [None, n_features])],
        [helper.make_tensor_value_info("label", TensorProto.INT64, [None]),
         helper.make_tensor_value_info("probabilities", TensorProto.FLOAT, [None, 2])],
        initializer=[w, one])
    model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 15)])
    model.ir_version = 8
    return model.SerializeToString()


@pytest.fixture
def make_onnx():
    return build_onnx
