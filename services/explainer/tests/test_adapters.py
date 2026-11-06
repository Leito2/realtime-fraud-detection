import json

import httpx
import numpy as np
import pytest
import xgboost as xgb
from explainer.adapters.llm import MockLlm, OpenAiCompatLlm
from explainer.adapters.prometheus_metrics import PrometheusMetrics
from explainer.adapters.xgboost_shap import XgboostShap
from explainer.application.ports import LlmError, NoExplanationModel
from explainer.domain.contributions import top_contributions
from explainer.domain.faithfulness import unsupported_claims
from explainer.domain.prompt import build_prompt
from prometheus_client import CollectorRegistry

from fraudcore.features import FEATURE_ORDER


@pytest.fixture(scope="module")
def booster() -> xgb.Booster:
    rng = np.random.default_rng(0)
    X = rng.normal(size=(2000, len(FEATURE_ORDER))).astype(np.float32)
    y = (X[:, FEATURE_ORDER.index("f_cnt_10m")] + 0.2 * X[:, 0] > 1.0).astype(int)
    model = xgb.XGBClassifier(n_estimators=30, max_depth=3).fit(X, y)
    return model.get_booster()


class Source:
    def __init__(self, raw: bytes | None):
        self.raw, self.fetched = raw, 0

    def fetch(self, version: str) -> bytes | None:
        self.fetched += 1
        return self.raw


def test_shap_contributions_explain_the_margin_and_point_at_the_driving_feature(booster):
    engine = XgboostShap(Source(bytes(booster.save_raw(raw_format="json"))))
    row = {f: 0.0 for f in FEATURE_ORDER} | {"f_cnt_10m": 3.0}
    shap = engine.contributions("1", row)
    assert set(shap) == set(FEATURE_ORDER)
    top = max(shap, key=lambda k: abs(shap[k]))
    assert top == "f_cnt_10m" and shap[top] > 0


def test_each_version_is_fetched_once(booster):
    source = Source(bytes(booster.save_raw(raw_format="json")))
    engine = XgboostShap(source)
    row = {f: 0.0 for f in FEATURE_ORDER}
    engine.contributions("1", row)
    engine.contributions("1", row)
    assert source.fetched == 1


def test_parallel_first_calls_fetch_a_version_once(booster):
    from concurrent.futures import ThreadPoolExecutor

    source = Source(bytes(booster.save_raw(raw_format="json")))
    engine = XgboostShap(source)
    row = {f: 0.0 for f in FEATURE_ORDER}
    with ThreadPoolExecutor(4) as pool:
        list(pool.map(lambda _: engine.contributions("1", row), range(8)))
    assert source.fetched == 1


def test_a_version_without_a_native_booster_raises_no_explanation_model():
    with pytest.raises(NoExplanationModel):
        XgboostShap(Source(None)).contributions("2", {f: 0.0 for f in FEATURE_ORDER})


def facts():
    features = {"amount": 950.0, "f_cnt_10m": 7.0, "amount_to_avg_ratio": 9.5}
    return top_contributions({"amount": 0.4, "f_cnt_10m": 1.3, "amount_to_avg_ratio": 2.1}, features, 3)


def test_the_mock_llm_writes_only_what_the_prompt_says():
    prompt = build_prompt("BLOCK", 0.93, facts())
    text = MockLlm().complete(prompt)
    assert text and unsupported_claims(text, facts(), 0.93) == []
    assert "9.5" in text and "BLOCK" in text


def client(handler) -> OpenAiCompatLlm:
    return OpenAiCompatLlm("http://llm/v1", "tiny", timeout_s=2.0, transport=httpx.MockTransport(handler))


def test_the_openai_compatible_client_returns_the_message_content_and_sends_a_low_temperature():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content), url=str(request.url))
        return httpx.Response(200, json={"choices": [{"message": {"content": "  fine.  "}}]})

    assert client(handler).complete("explain") == "fine."
    assert seen["url"] == "http://llm/v1/chat/completions" and seen["temperature"] <= 0.3
    assert seen["messages"][-1]["content"] == "explain"


@pytest.mark.parametrize("handler", [
    lambda r: httpx.Response(503, text="circuit open"),
    lambda r: httpx.Response(200, json={"unexpected": 1}),
    lambda r: (_ for _ in ()).throw(httpx.ConnectTimeout("slow")),
])
def test_every_gateway_failure_becomes_an_llm_error(handler):
    with pytest.raises(LlmError):
        client(handler).complete("explain")


def test_metrics_export_status_counts_and_latencies():
    reg = CollectorRegistry()
    m = PrometheusMetrics(reg)
    m.observe("llm", 0.5, 0.4)
    m.observe("template", 0.1, 0.05, rejected=True)
    m.observe("skipped", 0.0)
    value = reg.get_sample_value
    assert value("explanations_total", {"status": "llm"}) == 1
    assert value("explanations_total", {"status": "skipped"}) == 1
    assert value("explanations_rejected_total") == 1
    assert value("explainer_llm_latency_seconds_count") == 2
