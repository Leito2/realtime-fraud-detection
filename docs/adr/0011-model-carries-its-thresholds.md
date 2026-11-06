# ADR-0011: A registered model carries its own thresholds and score reference

- **Status:** accepted
- **Date:** 2026-11-06
- **Origin:** Implementation, M6

## Context
Thresholds are tuned per model. A hot swap that changes the model but not the cutoffs silently changes the business behaviour, and a drift metric needs the score distribution the model was trained with.

## Decision
The registry version holds `thresholds.json` and `score_reference.json` next to `model.onnx`; the scorer swaps all three together and every decision records the threshold set it used.

## Consequences
A promotion is one alias move. The cost: `config/thresholds.yaml` is now only the fallback for the rule model and the degraded policy.
