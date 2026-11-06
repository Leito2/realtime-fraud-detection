from explainer.domain.contributions import Contribution, top_contributions
from explainer.domain.faithfulness import unsupported_claims
from explainer.domain.policy import BackpressurePolicy, CircuitBreaker
from explainer.domain.prompt import build_prompt
from explainer.domain.template import render_template

FEATURES = {"amount": 950.0, "f_cnt_10m": 7.0, "amount_to_avg_ratio": 9.5, "is_foreign": 0.0,
            "n_known_devices": 2.0}
SHAP = {"amount": 0.4, "f_cnt_10m": 1.3, "amount_to_avg_ratio": 2.1, "is_foreign": -0.05,
        "n_known_devices": 0.01}


def contributions(k: int = 3) -> list[Contribution]:
    return top_contributions(SHAP, FEATURES, k)


def test_top_contributions_are_ordered_by_absolute_impact_and_carry_the_value():
    top = contributions(3)
    assert [c.feature for c in top] == ["amount_to_avg_ratio", "f_cnt_10m", "amount"]
    assert top[0].value == 9.5 and top[0].shap == 2.1


def test_a_negative_contribution_can_make_the_top_list_when_it_is_large():
    top = top_contributions({"a": 0.1, "b": -3.0}, {"a": 1.0, "b": 2.0}, 1)
    assert top[0].feature == "b" and top[0].shap < 0


def test_template_is_deterministic_and_names_the_decision_and_the_main_reasons():
    text = render_template("BLOCK", 0.93, contributions(3))
    assert text == render_template("BLOCK", 0.93, contributions(3))
    assert "BLOCK" in text and "0.93" in text
    assert "9.5" in text and "7" in text                         # the numbers behind the top reasons
    assert "lower" not in text.split(".")[0]


def test_prompt_lists_only_the_facts_and_forbids_inventing_more():
    prompt = build_prompt("REVIEW", 0.71, contributions(3))
    assert "amount_to_avg_ratio" in prompt and "9.5" in prompt
    assert "is_foreign" not in prompt                            # not in the top-3: the model never sees it
    assert "only" in prompt.lower()


def test_faithful_text_passes_the_gate():
    text = "Held for review: amount_to_avg_ratio is 9.5, f_cnt_10m is 7 and amount is 950.00 (score 0.71)."
    assert unsupported_claims(text, contributions(3), 0.71) == []


def test_invented_numbers_and_features_are_flagged():
    text = "Blocked because the card was used in 12 countries and is_foreign is 1 with amount 950."
    claims = unsupported_claims(text, contributions(3), 0.93)
    assert any("12" in c for c in claims) and any("is_foreign" in c for c in claims)


def test_rounding_within_tolerance_is_not_a_hallucination():
    assert unsupported_claims("The amount is about 9.4 times the usual.", contributions(3), 0.5) == []


def test_backpressure_sheds_when_the_llm_is_behind_or_busy():
    p = BackpressurePolicy(max_inflight=2, max_age_s=30.0)
    assert not p.should_skip(age_s=1.0, inflight=0)
    assert p.should_skip(age_s=1.0, inflight=2)                  # all slots busy
    assert p.should_skip(age_s=45.0, inflight=0)                 # an explanation nobody will read in time


def test_circuit_breaker_opens_after_consecutive_failures_and_half_opens_after_the_cooldown():
    now = [0.0]
    cb = CircuitBreaker(failures=3, cooldown_s=10.0, clock=lambda: now[0])
    assert cb.allow()
    for _ in range(3):
        cb.record(False)
    assert not cb.allow()                                        # open: skip the LLM, use the template
    now[0] = 11.0
    assert cb.allow()                                            # half-open: one probe
    assert not cb.allow()
    cb.record(True)
    assert cb.allow() and cb.allow()                             # closed again


def test_a_success_resets_the_failure_count():
    cb = CircuitBreaker(failures=2, cooldown_s=5.0, clock=lambda: 0.0)
    cb.record(False)
    cb.record(True)
    cb.record(False)
    assert cb.allow()
