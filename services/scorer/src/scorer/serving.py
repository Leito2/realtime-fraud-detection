"""How a registry model is served: in-process ONNX Runtime (default) or Triton over gRPC (PLAN.md 2.7)."""
from scorer.adapters.hot_swap import RuntimeBuilder
from scorer.config import Settings


def runtime_builder(cfg: Settings) -> RuntimeBuilder | None:
    """None keeps the default in-process ONNX path; ``serving.mode: triton`` swaps in the Triton client."""
    if cfg.serving_mode == "onnx":
        return None
    if cfg.serving_mode != "triton":
        raise ValueError(f"unknown serving mode {cfg.serving_mode!r} (expected onnx or triton)")
    import tritonclient.grpc as grpcclient

    from scorer.adapters.triton_model import TritonRuntimeBuilder
    from scorer.adapters.triton_repository import TritonRepository

    client = grpcclient.InferenceServerClient(cfg.triton_url)
    return TritonRuntimeBuilder(client, TritonRepository(cfg.triton_repository), cfg.triton_model,
                                timeout_s=cfg.triton_timeout_s, ready_timeout_s=cfg.triton_ready_s)
