# E7 - cost of option B (keyed operator, all windows)

2000 users, one payment per user every 600 s on average, 24 h buffers full before timing. Time of one `UserWindows.add` in CPython, no Flink around it.

| measure | value |
|---|---|
| p50 | 13.7 us |
| p95 | 24.9 us |
| p99 | 62.8 us |
| mean | 16.06 us |
| events/s on one core (mean) | 62,286 |
| buffer, median / max events per user | 145 / 180 |
