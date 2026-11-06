"""LLM clients behind one port: ``mock`` ($0) and any OpenAI-compatible endpoint."""
import re

import httpx

from explainer.application.ports import LlmError

FACT = re.compile(
    r"^- (?P<feature>\w+) \(.*?\) = (?P<value>\S+); effect on risk: (?P<effect>raises|lowers)$", re.M)
HEADER = re.compile(r"Decision: (?P<decision>\w+)\. Fraud score: (?P<score>\S+) ")
SYSTEM = "You are a concise assistant that explains fraud decisions to analysts."


class MockLlm:
    """Deterministic and faithful by construction: it re-states the facts of the prompt, nothing else."""

    def complete(self, prompt: str) -> str:
        head = HEADER.search(prompt)
        facts = FACT.findall(prompt)[:3]
        if not head or not facts:
            raise LlmError("prompt without facts")
        parts = [f"{feature} is {value} ({effect} the risk)" for feature, value, effect in facts]
        return f"{head['decision']} at score {head['score']}: " + ", ".join(parts) + "."


class OpenAiCompatLlm:
    def __init__(self, base_url: str, model: str, timeout_s: float = 20.0, api_key: str | None = None,
                 transport: httpx.BaseTransport | None = None) -> None:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._http = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout_s, headers=headers,
                                  transport=transport)
        self._model = model

    def complete(self, prompt: str) -> str:
        body = {"model": self._model, "temperature": 0.2, "max_tokens": 160,
                "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": prompt}]}
        try:
            response = self._http.post("/chat/completions", json=body)
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"].strip()
        except (httpx.HTTPError, KeyError, IndexError, ValueError) as exc:
            raise LlmError(str(exc)[:200]) from exc
