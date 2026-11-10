# Status (2026-11-09)

Milestones M0-M10 are built and measured on one 8 GB Windows laptop (WSL2 about 5 GB), one generator. Nothing was
pushed. Design source of truth: `PLAN.md` (findings in section 16, H1-H35); results: `docs/results/` and
`docs/resilience/`; the README has the tables.

## Done
* **M8 closed**: failure experiments F0 (control) to F10 rerun on a clean baseline; 10 of the 11 fault hypotheses
  confirmed, F1 (Flink restart 39 s vs 30 s) refuted; F5 refuted on a loaded host (130 s) and confirmed on three later runs (79.9 s with a polluted baseline, then 41.6 s and 40.9 s); 0 events
  lost in every run. Exactly-once sink (F10, E5), runbooks, ADR-0014/0015.
* **M7c**: `buf lint/breaking` in CI, README section, sync-vs-Flink residual-error report (exact for 89.1% of payments).
* **M9**: Triton serving variant (adapter, repository publisher, profile `serving-triton`, ADR-0016) and E4; E5
  (exactly-once cost), E6 (event-time cost), E7 (long windows: `slow_fraud` recall 1.1% -> 21.1%; option B operator
  logic in `flink/pyflink/user_state.py`), E10 (scaling at 400 ev/s).
* **M10**: README polished with measured results only, limitations and CV phrase; `v1.0` tag.
* Final measurements on a quiet machine: E3, E1, E2, E9, E8, E11.

## Measured headline
* **200 ev/s sustained at p95 38.6 ms** (E1, 3 reps, both full runs); 300 ev/s borderline (p95 64 ms in the final run, 81 ms in an earlier one); 400+ not sustained. Bottleneck: Flink
  TaskManager CPU (115-121% of a core at 300-400 ev/s in the final run while the scorer stays at 23-29%).
* ONNX 0.021 ms vs native 0.468 ms per call at batch 1; Triton about 0.7-0.9 ms per call.
* gRPC unary p50 3.5 / p99 8.6 ms vs REST p50 5.4 / p99 12.3 ms at 100 req/s (final block); E9 rerun discarded (all invalid), earlier E9 kept.
* Exactly-once: 0 duplicates after killing the TaskManager (1539 at-least-once); p95 25 ms -> 1.1 s (checkpoint 1 s).

## Open (nothing here blocks the rest)
* IEEE-CIS done offline (XGBoost PR-AUC 0.4723, recall@1%FPR 0.3925; `docs/results/ieee-cis-benchmark.md`). Nothing is blocked outside the repo.
* F5 closed: about 41 s of silence on a clean baseline (two clean reruns plus two isolation runs of 37-44 s); the 79.9 s run had a backlog warning in its baseline; the scorer group rejoin is the waiting stage.
* Option B: bridge measured in process mode (6.3k-7.9k ev/s, `docs/results/e7-option-b-bridge.md`); thread mode
  failed to start (pemja library not found, 4 attempts, documented).
* gRPC bidirectional streaming measured (3 valid runs, p50 9.2 ms); only 100 req/s from one client.
* `tests/integration/test_feature_parity.py` failed intermittently once in three runs against the live Flink job;
  the full suite otherwise passed (391 passed, 1 skipped, 1 failed).
* `submit_flink` does not verify that its overrides were applied when a job is already running.
* E4 end to end, E8 and E9 tails are noisy (the host swapped during parts of the runs); the conclusions in the
  README rely on p50 and say so.
* Wider scaling (4 scorers, 16 GB, Triton with many clients) was out of reach on this machine.

## How to resume
`docker compose --profile core --profile ops --profile grpc up -d` (never `down`/`-v`); `make test`; `make lint`.
Measurements: `scripts/final_measurements.sh` (leaves dashboards and async scorers stopped).

## Closed by decision
* E9: the final-block rerun was invalid (9 of 9 runs); the earlier valid measurement stays. No further runs requested.
* Bottleneck (Flink TaskManager, 115-121% of a core at 300-400 ev/s in the final E1 run) is accepted as measured; the alternatives (more Flink parallelism, cheaper per-event work, Avro/Protobuf, another engine) are listed as future work, not measured.
