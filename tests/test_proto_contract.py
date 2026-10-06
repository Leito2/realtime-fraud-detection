"""The gRPC contract compiles and matches fraudcore's domain model (PLAN §14)."""
import importlib
import sys
from pathlib import Path

import pytest

grpc_tools = pytest.importorskip("grpc_tools")
from grpc_tools import protoc  # noqa: E402

from fraudcore.contracts import Payment  # noqa: E402

PROTO_ROOT = Path(__file__).resolve().parents[1] / "proto"


@pytest.fixture(scope="module")
def pb2(tmp_path_factory):
    out = tmp_path_factory.mktemp("gen")
    include = Path(grpc_tools.__file__).parent / "_proto"           # google/protobuf/*.proto
    code = protoc.main(["protoc", f"-I{PROTO_ROOT}", f"-I{include}", f"--python_out={out}",
                        f"--grpc_python_out={out}", str(PROTO_ROOT / "fraud/v1/scoring.proto")])
    assert code == 0, "protoc failed"
    sys.path.insert(0, str(out))
    try:
        yield importlib.import_module("fraud.v1.scoring_pb2")
    finally:
        sys.path.remove(str(out))


def test_service_exposes_unary_stream_and_lookup(pb2):
    methods = {m.name for m in pb2.DESCRIPTOR.services_by_name["FraudScoring"].methods}
    assert methods == {"Score", "ScoreStream", "GetDecision"}


def test_request_covers_every_payment_field(pb2):
    proto_fields = set(pb2.ScoreRequest.DESCRIPTOR.fields_by_name)
    measurement_only = {"t_scheduled_ns", "t_sent_ns"}              # load-test timestamps, not business data
    assert set(Payment.model_fields) - measurement_only <= proto_fields
