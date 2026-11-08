# ADR-0016: Triton is a measured serving variant, not the default

- **Status:** accepted
- **Date:** 2026-11-08
- **Origin:** PLAN.md 2.7, M9 (experiment E4)

## Context
The scorer runs ONNX Runtime in-process. Triton Inference Server offers isolation, dynamic batching and a standard gRPC interface, at the price of a network hop and a second process. The plan asked for a measured comparison, not a migration.

## Decision
`serving.mode: triton` (or `SERVING_MODE=triton`) makes the scorer a Triton client. The registry stays the source of truth: when the champion alias moves, the scorer writes the ONNX file into the shared model repository, waits until Triton reports that version ready and only then swaps. A version Triton never loads is refused, so the previous model keeps serving. The repository config sets `max_batch_size: 0`: the scorer already sends a micro-batch, so Triton's dynamic batching has nothing to merge from a single client. Triton runs on CPU (profile `serving-triton`, about 1.5 GB of RAM, 35 GB of image).

## Consequences
In-process stays the default because the comparison (E4, `docs/results/`) is what decides it. On an 8 GB machine only low rates with one generator are measured, so the comparison under many concurrent clients, where dynamic batching helps, is not measured. If Triton is unreachable the scorer process fails and restarts with its offsets uncommitted, so no event is lost; it does not fall back to in-process scoring, which would hide the thing being measured.
