# ADR-0003: Enriched events, not Redis lookups for velocity features

- **Status:** accepted
- **Date:** 2026-10-06
- **Origin:** PLAN.md ADR-1

## Context
If Flink wrote velocity features to Redis while the scorer consumed `payments` in parallel, the scorer could read features that do not yet include the current payment. That is a data race and it creates train/serve skew.

## Decision
Flink emits `payments-enriched`: one row per payment whose velocity features already include that payment (`f_cnt_10m >= 1`). The scorer consumes from there. Redis holds the last velocity state only for readers outside the critical path (the Feature Writer feeds it).

## Consequences
The critical path has no race and the offline twin (`fraudcore.velocity_reference`) can reproduce the online value exactly; a parity test compares the two against a live Flink job. The synchronous gRPC/REST path has no Flink window, so it rebuilds the same value from the stored state plus the current payment (`fraudcore.sync_velocity`, ADR-0012).
