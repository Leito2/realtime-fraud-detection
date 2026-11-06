# ADR-0009: JSON serialization in v1

- **Status:** accepted
- **Date:** 2026-10-06
- **Origin:** PLAN.md ADR-7

## Context
Avro with a Schema Registry gives smaller payloads and enforced contracts, but it is one more container on an 8 GB laptop and Flink SQL handles JSON natively.

## Decision
JSON everywhere in v1, with the contracts as pydantic models in `fraudcore.contracts`. Avro stays a documented alternative, to be adopted only if E1 shows serialization as the bottleneck.

## Consequences
Simple operations and readable topics. Poison pills are possible (experiment F6) and need validation at the edge.
