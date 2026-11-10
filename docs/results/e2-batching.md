# E2 - scorer micro-batch size x wait

200 ev/s (what this laptop sustains, see E1), 60 s per cell after a 20 s warm-up, one run per cell, one generator. Times in ms. One repetition per cell: differences of a few ms between cells are inside the run-to-run noise seen in E1.

| max messages | max wait (ms) | p50 | p95 | p99 | lag stable |
|---|---|---|---|---|---|
| 1 | 1 | 10.311 | 49.311 | 164.351 | True |
| 50 | 5 | 15.167 | 29.167 | 77.183 | True |
| 500 | 5 | 15.487 | 29.487 | 73.535 | True |
| 500 | 20 | 25.951 | 48.959 | 84.991 | True |
