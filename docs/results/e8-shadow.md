# E8 - impact of the shadow challenger on the champion

200 ev/s, 90 s per run, 30 s warm-up. All times in ms.

| shadow | rep | p50 | p95 | p99 | lost | valid |
|---|---|---|---|---|---|---|
| off | 1 | 17.407 | 47.423 | 104.447 | 0 | True |
| on | 1 | 21.663 | 277.759 | 894.975 | 0 | False |
| off | 2 | 18.815 | 51.839 | 98.815 | 0 | True |
| on | 2 | 21.135 | 77.183 | 171.135 | 0 | True |
| off | 3 | 20.047 | 104.063 | 353.279 | 0 | False |
| on | 3 | 20.511 | 74.559 | 140.543 | 0 | True |

Median p95: off 51.839 ms, on 77.183 ms,
difference 25.344 ms.
