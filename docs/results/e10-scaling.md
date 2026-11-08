# E10 - scaling the scorer and Flink

400 ev/s offered, 90 s per cell after a 30 s warm-up, open loop.

| Flink parallelism | scorers | p50 (ms) | p95 (ms) | p99 (ms) | lag stable | lost | valid |
|---|---|---|---|---|---|---|---|
| 2 | 1 | 18.175 | 56.191 | 116.223 | True | 0 | True |
| 2 | 2 | 21.951 | 185.471 | 679.423 | True | 1 | False |
| 4 | 1 | 18.255 | 53.279 | 125.311 | True | 0 | True |
| 4 | 2 | 20.399 | 70.911 | 139.007 | True | 0 | True |
