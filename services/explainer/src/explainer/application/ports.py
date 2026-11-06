"""What the use case needs from the outside, from the use case's point of view (ADR-0002)."""
from typing import Protocol

from fraudcore.contracts import ExplanationEvent


class NoExplanationModel(Exception):
    """No native tree model for this version (e.g. a neural challenger): SHAP is not available."""


class LlmError(Exception):
    """Any failure of the language-model call; the core never sees HTTP details."""


class ShapEngine(Protocol):
    def contributions(self, model_version: str, features: dict[str, float]) -> dict[str, float]: ...


class LlmClient(Protocol):
    def complete(self, prompt: str) -> str: ...


class Metrics(Protocol):
    def observe(self, status: str, seconds: float, llm_seconds: float | None = None,
                rejected: bool = False) -> None: ...


class ExplanationSink(Protocol):
    def publish(self, event: ExplanationEvent) -> None: ...
