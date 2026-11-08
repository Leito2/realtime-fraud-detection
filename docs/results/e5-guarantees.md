# E5 - cost of exactly-once (ADR-4)

200 ev/s, 90 s per row after a 30 s warm-up, open loop.

| configuration | p50 (ms) | p95 (ms) | p99 (ms) | lost | valid run |
|---|---|---|---|---|---|
| at-least-once, checkpoint 10 s | 15.055 | 25.055 | 44.063 | 0 | True |
| exactly-once, checkpoint 1 s | 642.047 | 1089.535 | 1146.879 | 0 | True |
| exactly-once, checkpoint 5 s | 2844.671 | 5787.647 | 8937.471 | 0 | True |
| exactly-once, checkpoint 10 s | 5562.367 | 10043.391 | 10649.599 | 0 | True |
