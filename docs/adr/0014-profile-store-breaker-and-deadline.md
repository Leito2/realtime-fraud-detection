# ADR-0014: The profile lookup has a hard deadline and a circuit breaker

- **Status:** accepted
- **Date:** 2026-11-08
- **Origin:** Implementation, M8 (experiments F3, F4)

## Context
With Redis stopped, each profile lookup blocked for about four seconds in name resolution, which no socket timeout covers, so the scorer stalled once per micro-batch instead of degrading (F3). A slow Redis (+20 ms, F4) cost its full latency on every batch. A first breaker that opened on one failed lookup turned each 5 ms blip into a second of degraded decisions with a healthy Redis (20-70% degraded without any fault).

## Decision
The lookup runs on a worker thread and the caller waits at most 30 ms (`DeadlineProfiles`). `BreakerProfiles` opens after three consecutive failures, answers "unavailable" at once while open and lets a single probe through every second. The use case already turns "unavailable" into the conservative degraded policy. Redis keeps its data with AOF `everysec` and tolerates a damaged AOF tail, so a restart does not replace real profiles with neutral defaults.

## Consequences
With Redis down the silence is bounded by the breaker (2.7 s in F3), and with Redis slow the p95 stays at the baseline (41 ms vs 42 ms) while about half of the decisions are degraded (F4). The numbers 30 ms / 3 failures / 1 s are configuration (`config/scorer.yaml`), chosen from the runs above, not derived. A longer degraded period is a business risk: the `ScorerDegraded` alert exists for it.
