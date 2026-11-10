# E2 - scorer micro-batch size x wait

200 ev/s (what this laptop sustains, see E1), 60 s per cell after a 20 s warm-up, one run per cell, one generator. Times in ms. One repetition per cell: differences of a few ms between cells are inside the run-to-run noise seen in E1.

| max messages | max wait (ms) | p50 | p95 | p99 | lag stable |
|---|---|---|---|---|---|
| 1 | 1 | 10.863 | 73.919 | 154.879 | True |
| 50 | 5 | 15.639 | 80.639 | 351.743 | True |
| 500 | 5 | 15.927 | 32.927 | 71.935 | True |
| 500 | 20 | 25.919 | 43.935 | 83.967 | True |
