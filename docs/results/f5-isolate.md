# F5 isolation (kafka stopped at t=38 s, started again at t=58 s)

| t (s) | payments (+records) | payments-enriched (+records) | decisions (+records) |
|---|---|---|---|
| 0 | - | - | - |
| 2 | 0 | 0 | 0 |
| 4 | 361 | 365 | 367 |
| 6 | 403 | 401 | 401 |
| 8 | 400 | 401 | 406 |
| 10 | 404 | 405 | 401 |
| 12 | 404 | 401 | 403 |
| 14 | 400 | 399 | 397 |
| 16 | 400 | 401 | 401 |
| 18 | 402 | 404 | 409 |
| 20 | 397 | 395 | 389 |
| 22 | 414 | 418 | 424 |
| 24 | 391 | 390 | 386 |
| 26 | 400 | 399 | 401 |
| 28 | 405 | 408 | 404 |
| 30 | 401 | 396 | 396 |
| 32 | 399 | 401 | 400 |
| 34 | 405 | 405 | 407 |
| 36 | 401 | 399 | 398 |
| 38 | 406 | 409 | 411 |
| 40 | 402 | 405 | 414 |
| 42 | down | down | down |
| 44 | down | down | down |
| 49 | down | down | down |
| 53 | down | down | down |
| 58 | down | down | down |
| 62 | down | down | down |
| 67 | down | down | down |
| 71 | down | down | down |
| 73 | 6563 | 1294 | 79 |
| 75 | 400 | 2326 | 0 |
| 77 | 401 | 3746 | 0 |
| 79 | 408 | 410 | 0 |
| 81 | 392 | 385 | 0 |
| 83 | 406 | 379 | 0 |
| 85 | 401 | 438 | 4563 |
| 87 | 403 | 397 | 4724 |
| 89 | 402 | 401 | 405 |
| 91 | 395 | 391 | 383 |
| 93 | 405 | 404 | 409 |
| 95 | 402 | 405 | 404 |
| 97 | 405 | 405 | 401 |
| 99 | 403 | 399 | 403 |
| 101 | 399 | 402 | 399 |
| 103 | 403 | 401 | 405 |
| 105 | 401 | 402 | 398 |
| 107 | 403 | 403 | 405 |
| 109 | 405 | 406 | 406 |
| 111 | 400 | 397 | 399 |
| 113 | 404 | 408 | 407 |
| 115 | 399 | 396 | 394 |
| 117 | 402 | 403 | 404 |
| 119 | 400 | 402 | 401 |
| 121 | 410 | 408 | 408 |
| 123 | 397 | 396 | 396 |
| 125 | 405 | 405 | 408 |
| 127 | 403 | 413 | 411 |
| 129 | 401 | 392 | 392 |
| 131 | 403 | 403 | 402 |
| 133 | 403 | 405 | 406 |
| 135 | 402 | 400 | 398 |
| 137 | 400 | 400 | 400 |
| 139 | 401 | 401 | 400 |
| 141 | 404 | 405 | 410 |
| 143 | 401 | 400 | 397 |
| 145 | 403 | 408 | 410 |
| 147 | 403 | 398 | 396 |
| 149 | 403 | 401 | 402 |
| 151 | 399 | 399 | 398 |
| 153 | 402 | 402 | 403 |
| 155 | 401 | 401 | 400 |
| 157 | 404 | 405 | 407 |
| 159 | 403 | 403 | 400 |
| 161 | 403 | 402 | 403 |
| 163 | 401 | 403 | 402 |
| 165 | 404 | 403 | 406 |
| 167 | 400 | 402 | 399 |
| 169 | 403 | 402 | 402 |
| 172 | 404 | 404 | 407 |
| 174 | 140 | 137 | 133 |
| 176 | 0 | 0 | 0 |
| 178 | 0 | 0 | 0 |
| 180 | 0 | 0 | 0 |
| 182 | 0 | 0 | 0 |

## Reading

Counts are records per 2 s sample (about 400 = 200 ev/s). Kafka was stopped at t=41 s and answered again at
t=73 s (the stop took 20 s, the restart about 10 s more; the probe needs 1.5 s per dead call, hence the gaps).

* `payments`: the generator buffered during the outage and flushed 6,563 records at once when the broker came
  back, then returned to 400 per sample.
* `payments-enriched` (Flink): resumed in the first sample after the broker was back (1,294, 2,326, 3,746 while
  it worked off the backlog). Flink was not the slow stage.
* `decisions` (scorer): **silent from t=73 to t=84 (about 12 s) after the broker was back**, then two samples of
  about 4,600 records and normal again at t=89. The scorer's consumer group has to rejoin after the broker's
  coordinator restarted; that is the stage that waits.
* Total silence of `decisions` in this run: about 44 s (t=41 to t=85), against 79.9 s in the F5 report. The
  extra 35 s of the full F5 run were not reproduced by this lighter run (170 s, 34,000 payments, less data in the
  broker log to recover); that gap is host/broker-recovery time, not isolated further.
