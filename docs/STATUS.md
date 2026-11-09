# Status (2026-11-09)

Milestones M0-M10 are built and measured on one 8 GB Windows laptop (WSL2 about 5 GB), one generator. Nothing was
pushed. Design source of truth: `PLAN.md` (findings in section 16, H1-H35); results: `docs/results/` and
`docs/resilience/`; the README has the tables.

## Done
* **M8 closed**: failure experiments F0 (control) to F10 rerun on a clean baseline; 10 of the 11 fault hypotheses
  confirmed, F1 (Flink restart 39 s vs 30 s) refuted; F5 refuted on a loaded host (130 s) and confirmed on rerun with only the pipeline up (88 s); 0 events
  lost in every run. Exactly-once sink (F10, E5), runbooks, ADR-0014/0015.
* **M7c**: `buf lint/breaking` in CI, README section, sync-vs-Flink residual-error report (exact for 89.1% of payments).
* **M9**: Triton serving variant (adapter, repository publisher, profile `serving-triton`, ADR-0016) and E4; E5
  (exactly-once cost), E6 (event-time cost), E7 (long windows: `slow_fraud` recall 1.1% -> 21.1%; option B operator
  logic in `flink/pyflink/user_state.py`), E10 (scaling at 400 ev/s).
* **M10**: README polished with measured results only, limitations and CV phrase; `v1.0` tag.
* Final measurements on a quiet machine: E3, E1, E2, E9, E8, E11.

## Measured headline
* **200 ev/s sustained at p95 38.6 ms** (E1, 3 reps); 300 ev/s p95 81 ms; 400+ not sustained. Bottleneck: Flink
  TaskManager CPU (100-131% of a core while the scorer stays near 30%).
* ONNX 0.021 ms vs native 0.468 ms per call at batch 1; Triton about 0.7-0.9 ms per call.
* gRPC unary p50 4.1 / p99 8.7-10.8 ms vs REST p50 6.3 / p99 15.4 ms at 100 req/s.
* Exactly-once: 0 duplicates after killing the TaskManager (1539 at-least-once); p95 25 ms -> 1.1 s (checkpoint 1 s).

## Open (nothing here blocks the rest)
* **Blocked outside the repo:** IEEE-CIS needs a Kaggle download with account credentials.
* F5: silence is load-dependent (130 s busy host vs 79.9 s quiet); the slow-rejoining stage is not isolated.
* Option B: the PyFlink thread-mode JVM-Python bridge is not measured (needs a Flink image with Python);
  `UserWindows` now rejects out-of-order timestamps and keeps exact integer sums.
* gRPC bidirectional streaming has no valid benchmark (the client falls behind its own schedule; 3 attempts).
* `tests/integration/test_feature_parity.py` failed intermittently once in three runs against the live Flink job;
  the full suite otherwise passed (391 passed, 1 skipped, 1 failed).
* `submit_flink` does not verify that its overrides were applied when a job is already running.
* E4 end to end, E8 and E9 tails are noisy (the host swapped during parts of the runs); the conclusions in the
  README rely on p50 and say so.
* Wider scaling (4 scorers, 16 GB, Triton with many clients) was out of reach on this machine.

## How to resume
`docker compose --profile core --profile ops --profile grpc up -d` (never `down`/`-v`); `make test`; `make lint`.
Measurements: `scripts/final_measurements.sh` (leaves dashboards and async scorers stopped).
