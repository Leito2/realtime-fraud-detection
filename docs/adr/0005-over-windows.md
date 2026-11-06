# ADR-0005: OVER windows, not tumbling windows

- **Status:** accepted
- **Date:** 2026-10-06
- **Origin:** PLAN.md ADR-3

## Context
A tumbling window emits when it closes, which would add up to ten minutes of latency to a per-payment feature.

## Decision
`COUNT/SUM/MAX ... OVER (PARTITION BY user_id ORDER BY proc_time RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW)` emits one row per event with the aggregate as of that event.

## Consequences
Per-event latency. Known cost: rows processed in the same millisecond are peers in a RANGE window, so a burst from one user sees payments from its own future; the parity test sends payments one at a time on purpose (PLAN.md 3.3, experiment E7).
