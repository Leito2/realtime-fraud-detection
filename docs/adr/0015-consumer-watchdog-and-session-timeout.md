# ADR-0015: Every Kafka consumer runs under a watchdog with a short session timeout

- **Status:** accepted
- **Date:** 2026-11-08
- **Origin:** Implementation, M8 (experiments F1, F2, F5)

## Context
After a broker restart, librdkafka consumers could stay in a group join that never completed: the process was alive, its lag grew and nothing in the log said why. Separately, a killed consumer is only noticed by the broker after `session.timeout.ms`, which defaults to 45 s: with one of two scorers killed the survivor was silent for 48 s (F2).

## Decision
`fraudcore.watchdog.guard()` wraps the configuration of every consumer (scorer, auditor, explainer, feature writer). It reads the statistics callback and exits with code 3 when the group state is not `up/steady` for 60 s, so the orchestrator restarts the service. It also sets `session.timeout.ms=10000` and `heartbeat.interval.ms=2000` unless the caller chose otherwise.

## Consequences
A wedged consumer recovers in about a minute instead of never. A dead consumer's partitions move in about ten seconds. The cost is a false rebalance if a process or the host stalls for more than ten seconds; the chaos runs on a busy 8 GB laptop show such stalls exist, which is why the value is not lower. Flink's heartbeat timeout (10 s) has the same trade-off and the same evidence.
