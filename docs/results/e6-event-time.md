# E6 - cost of event-time windows

200 ev/s, 90 s per row after a 30 s warm-up, open loop.

| window | p50 (ms) | p95 (ms) | p99 (ms) | lost | valid run |
|---|---|---|---|---|---|
| processing time | 15.927 | 29.935 | 75.967 | 0 | True |
| event time, 0.5 s out-of-orderness | 831.999 | 1055.743 | 1221.631 | 101 | True |
| event time, 2 s out-of-orderness | 2338.815 | 2562.047 | 2787.327 | 410 | True |
