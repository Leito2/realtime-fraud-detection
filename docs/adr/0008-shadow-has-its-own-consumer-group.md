# ADR-0008: The shadow scorer has its own consumer group

- **Status:** accepted
- **Date:** 2026-10-06
- **Origin:** PLAN.md ADR-6

## Context
A challenger must see real traffic without being able to slow the champion.

## Decision
The challenger runs the same binary with `--mode shadow`, in its own container and consumer group, reading `payments-enriched` and writing `shadow-scores`; the auditor joins both by `payment_id`.

## Consequences
The champion's p95 is independent of the challenger's cost (experiment E8 measures the residual). Promotion criteria are evaluated on the audit store's confusion view at an equal flag rate (PLAN.md 16, H11).
