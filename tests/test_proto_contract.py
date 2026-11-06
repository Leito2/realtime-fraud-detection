"""The gRPC contract compiles, matches fraudcore's model, and the committed stubs are fresh (PLAN §14)."""
from pathlib import Path

import pytest

grpc_tools = pytest.importorskip("grpc_tools")
from grpc_tools import protoc  # noqa: E402

from fraud.v1 import scoring_pb2 as pb2  # noqa: E402
from fraudcore.contracts import Payment  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PROTO_ROOT = ROOT / "proto"
STUBS = ROOT / "packages/fraudproto/src/fraud/v1"


def test_service_exposes_unary_stream_and_lookup():
    methods = {m.name for m in pb2.DESCRIPTOR.services_by_name["FraudScoring"].methods}
    assert methods == {"Score", "ScoreStream", "GetDecision"}


def test_request_covers_every_payment_field():
    proto_fields = set(pb2.ScoreRequest.DESCRIPTOR.fields_by_name)
    measurement_only = {"t_scheduled_ns", "t_sent_ns"}              # load-test timestamps, not business data
    assert set(Payment.model_fields) - measurement_only <= proto_fields


def test_committed_stubs_are_what_protoc_generates_from_the_proto(tmp_path):
    """Edit the .proto without running `make proto` and this fails, so the stubs cannot drift."""
    include = Path(grpc_tools.__file__).parent / "_proto"
    code = protoc.main(["protoc", f"-I{PROTO_ROOT}", f"-I{include}", f"--python_out={tmp_path}",
                        f"--grpc_python_out={tmp_path}", f"--pyi_out={tmp_path}",
                        str(PROTO_ROOT / "fraud/v1/scoring.proto")])
    assert code == 0, "protoc failed"
    for name in ("scoring_pb2.py", "scoring_pb2.pyi", "scoring_pb2_grpc.py"):
        fresh = (tmp_path / "fraud/v1" / name).read_text(encoding="utf-8")
        assert (STUBS / name).read_text(encoding="utf-8") == fresh, f"{name} is stale: run `make proto`"
