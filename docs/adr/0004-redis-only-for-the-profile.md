# ADR-0004: Redis on the critical path only for the user profile

- **Status:** accepted
- **Date:** 2026-10-06
- **Origin:** PLAN.md ADR-2

## Context
Features come in two kinds: velocity (changes with every event) and profile (30-day behaviour, changes slowly).

## Decision
Velocity is computed by Flink and travels inside the event. The profile is precomputed by a batch Profile Builder into Redis and read with one pipelined `HGETALL` per micro-batch, with a 5 ms timeout.

## Consequences
One Redis round trip per micro-batch instead of per event. A Redis failure is survivable: the scorer decides with neutral profile values and conservative thresholds and marks the decision `degraded` (experiments F3, F4).
