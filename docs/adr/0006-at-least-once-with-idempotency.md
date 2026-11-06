# ADR-0006: At-least-once with idempotency on the critical path

- **Status:** accepted
- **Date:** 2026-10-06
- **Origin:** PLAN.md ADR-4

## Context
Flink's transactional Kafka sink makes `read_committed` consumers see data only after a checkpoint completes, which adds about one checkpoint interval (seconds) to every decision and cannot fit an 80 ms p95.

## Decision
The critical path uses `AT_LEAST_ONCE`. Downstream consumers deduplicate on `payment_id` (the auditor inserts with `ON CONFLICT DO NOTHING`). Exactly-once is measured as a separate experiment (E5, F10) to price the trade.

## Consequences
Replays after a failure produce bounded duplicates, never losses (F1, F2 count both). The trade-off is documented with numbers rather than argued.
