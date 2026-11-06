# ADR-0012: Synchronous scoring rebuilds velocity from the stored state

- **Status:** accepted
- **Date:** 2026-11-06
- **Origin:** PLAN.md 14.2

## Context
gRPC and REST scoring have no Flink window, but the model was trained on windows that include the current payment.

## Decision
`fraudcore.sync_velocity` takes the Feature Writer's last window state for the user (with its event time), treats a state older than ten minutes as empty, and adds the current payment (count + 1, sum + amount, max, small-payment count).

## Consequences
Same feature semantics as the Kafka path without a second implementation of the model. It approximates Flink at the window edge (the stored state may still contain events that just expired) and lags by the Feature Writer's delay; the size of that gap is reported, not assumed.
