# E7 - cost of option B (keyed operator, all windows)

2000 users, one payment per user every 600 s on average, 24 h buffers full before timing. Time of one `UserWindows.add` in CPython, no Flink around it.

| measure | value |
|---|---|
| p50 | 32.8 us |
| p95 | 61.9 us |
| p99 | 96.6 us |
| mean | 36.06 us |
| events/s on one core (mean) | 27,734 |
| buffer, median / max events per user | 145 / 180 |
