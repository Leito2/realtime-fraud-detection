| rate (ev/s) | reps | p50 (ms) | p95 (ms) | p95 range | p99 (ms) | sustained | failing |
|---|---|---|---|---|---|---|---|
| 100 | 3 | 13.9 | 22.9 | 20.4-476 | 200 | no | gen_lag_valid, p95_within_budget |
| 200 | 3 | 15.6 | 38.6 | 34.6-45.4 | 231 | yes | - |
| 300 | 3 | 19.3 | 64.4 | 53.5-413 | 135 | yes (highest) | - |
| 400 | 3 | 25.1 | 326 | 122-1,409 | 704 | no | p95_within_budget |
| 500 | 3 | 63.2 | 35,848 | 578-49,644 | 42,533 | no | gen_lag_valid, p95_within_budget |
| 600 | 3 | 25,641 | 60,031 | 9,388-60,031 | 60,031 | no | gen_lag_valid, p95_within_budget |
