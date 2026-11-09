# E8 - impact of the shadow challenger on the champion

200 ev/s, 90 s per run, 30 s warm-up. All times in ms.

| shadow | rep | p50 | p95 | p99 | lost | valid |
|---|---|---|---|---|---|---|
| off | 1 | 15.279 | 31.279 | 82.303 | 0 | True |
| on | 1 | 18.319 | 273.407 | 805.375 | 0 | True |
| off | 2 | 16.511 | 53.503 | 138.623 | 0 | True |
| on | 2 | 22.015 | 94.015 | 155.135 | 0 | True |
| off | 3 | 22.911 | 115.967 | 182.911 | 0 | True |
| on | 3 | 23.263 | 265.471 | 970.751 | 0 | False |

Median p95: off 53.503 ms, on 265.471 ms,
difference 211.968 ms.
