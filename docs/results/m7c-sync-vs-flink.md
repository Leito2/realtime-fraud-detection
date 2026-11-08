# Synchronous velocity vs the Flink window (ADR-0012)

600,000 simulated payments replayed per writer delay. `exact` = all four velocity features equal to Flink's. `over` / `under` = share of payments where the count is too high / too low.

| writer delay (s) | exact | over-count | under-count | mean count error | p99 abs error |
|---|---|---|---|---|---|
| 0.0 | 89.06% | 10.94% | 0.00% | +0.1909 | 3 |
| 0.5 | 88.97% | 10.96% | 0.05% | +0.1907 | 3 |
| 1.0 | 88.89% | 10.98% | 0.10% | +0.1906 | 3 |
| 2.0 | 88.72% | 11.01% | 0.19% | +0.1904 | 3 |
