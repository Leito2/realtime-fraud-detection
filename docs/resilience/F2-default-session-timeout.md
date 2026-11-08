# F2 - One of two scorer processes dies

**Hypothesis (written before the run):** The consumer group rebalances in under 5 s and the survivor takes over the dead process's partitions; nothing is lost and duplicates are bounded by one micro-batch per partition.

**Result: hypothesis REFUTED.**

**Mechanism under test:** Consumer group with the cooperative-sticky assignor (incremental rebalance); offsets are committed after each micro-batch.

## Method

Steady load of 200 ev/s (about half of the sustained rate) for 60 s, then the fault, then 90 s more. Fault: 2 scorer processes were running; SIGKILL to 3edf47e8a160. After the load stopped the decisions topic was drained and every produced `payment_id` was compared with the decisions (`chaos/src/fraud_chaos/verify.py`).

## Numbers

| measure | value |
|---|---|
| payments produced | 54000 |
| lost (produced, never decided) | 0 |
| duplicated decisions (extra copies) | 2 |
| recovery (rate back and p95 back to normal for 5 s; normal = max(80 ms, 1.5 x the pre-fault median p95)) | 24 s |
| longest silence of the decisions topic | 48.1 s |
| p95 before the fault (last 60 s) | 189.9 ms |
| p95 after the fault, judged window | 44823.4 ms |
| p95 in the rest of the observation | nan ms |
| degraded decisions after the fault | 46% |
| records added to the DLQ | 0 |

## Checks

- PASS lost 0 (limit 0)
- PASS duplicated 2 (limit 6000)
- FAIL recovery 24 s (limit 15 s)

![decisions per second and p95 around the fault](F2-default-session-timeout.svg)

The dashed vertical line is the fault. Bars are decisions per second, the orange line is the p95 of end-to-end latency, the green dotted line is the 80 ms budget.
