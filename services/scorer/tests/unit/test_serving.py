from pathlib import Path

import pytest
from scorer import config
from scorer.adapters.triton_model import TritonRuntimeBuilder
from scorer.serving import runtime_builder

CONFIG_DIR = Path(__file__).resolve().parents[4] / "config"


def test_in_process_onnx_is_the_default(monkeypatch):
    monkeypatch.delenv("SERVING_MODE", raising=False)
    cfg = config.load(CONFIG_DIR)
    assert cfg.serving_mode == "onnx" and runtime_builder(cfg) is None


def test_the_environment_switches_to_triton_and_builds_the_client_side(monkeypatch):
    monkeypatch.setenv("SERVING_MODE", "triton")
    monkeypatch.setenv("TRITON_URL", "localhost:8001")
    cfg = config.load(CONFIG_DIR)
    assert (cfg.triton_url, cfg.triton_model, cfg.triton_timeout_s) == ("localhost:8001", "fraud_xgb", 0.05)
    assert isinstance(runtime_builder(cfg), TritonRuntimeBuilder)       # the channel connects lazily


def test_an_unknown_mode_is_an_error_not_a_silent_fallback(monkeypatch):
    monkeypatch.setenv("SERVING_MODE", "tritn")
    with pytest.raises(ValueError, match="unknown serving mode"):
        runtime_builder(config.load(CONFIG_DIR))
