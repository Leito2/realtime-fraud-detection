# ADR-0007: Fraud labels travel in their own topic

- **Status:** accepted
- **Date:** 2026-10-06
- **Origin:** PLAN.md ADR-5

## Context
A label inside `payments` would leak into the features and into anything that logs the event.

## Decision
The generator publishes truth to `labels`; only the auditor and training read it.

## Consequences
No label leakage by construction. Cost: feedback datasets are a join (`v_feedback_dataset`) and labels arrive later than decisions, which is also how it works in production.
