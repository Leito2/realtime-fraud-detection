# ADR-0010: SHAP from the native XGBoost booster; the LLM only phrases facts

- **Status:** accepted
- **Date:** 2026-11-06
- **Origin:** Implementation, M7

## Context
The ONNX graph cannot give attributions, and an LLM that is allowed to reason about a payment can invent causes.

## Decision
Training registers the native booster next to the ONNX model. The explainer computes exact TreeSHAP with XGBoost's `pred_contribs` (no extra dependency), keeps the top five contributions as the only facts, asks the LLM to phrase them, and rejects any text that states a number or a feature that is not among the facts.

## Consequences
Explanations are faithful by construction up to the gate's reach and degrade to a deterministic template when the LLM is slow, down, or wrong (experiment F8). Models without a native booster (the MLP challenger) get a template that says no attribution exists.
