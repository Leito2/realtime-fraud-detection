# E11 - REST/JSON vs gRPC unary vs gRPC bidirectional stream

Same model and the same scorer use case (`ScoreSync`), one client process at 100 req/s open loop (the rate one Python client drives on this laptop), 60 s after a 10 s warm-up, 3 repetitions per mode. Latency in ms from the scheduled send time. A run is valid only if the client's start lag p99 is <= 5 ms (otherwise the client, not the server, is the bottleneck).

| mode | rep | p50 | p95 | p99 | start lag p99 | valid | failed requests |
|---|---|---|---|---|---|---|---|
| rest | 1 | 6.32 | 9.68 | 13.35 | 1.59 | True | 0 |
| grpc | 1 | 4.16 | 6.27 | 10.16 | 1.30 | True | 0 |
| rest | 2 | 6.28 | 9.61 | 16.31 | 1.96 | True | 0 |
| grpc | 2 | 4.15 | 6.59 | 10.75 | 1.35 | True | 0 |
| rest | 3 | 6.23 | 9.88 | 15.38 | 1.86 | True | 0 |
| grpc | 3 | 4.09 | 6.06 | 8.66 | 1.26 | True | 0 |
| grpc-stream | 1 | 11.47 | 137.30 | 281.39 | 67.84 | False | 110 |
| grpc-stream | 2 | 9.67 | 21.85 | 121.21 | 14.76 | False | 0 |
| grpc-stream | 3 | 10.09 | 26.93 | 158.91 | 61.97 | False | 49 |

Median of the valid repetitions:

| mode | p50 | p95 | p99 |
|---|---|---|---|
| rest | 6.28 | 9.68 | 15.38 |
| grpc | 4.15 | 6.27 | 10.16 |

**gRPC bidirectional streaming has no valid result.** All six stream runs are in the table above: three with the first client (every request failed with `InvalidStateError`: grpc.aio allows one pending write per call), three after serializing the writes (the stream works, p50 about 10 ms, but the client's own start lag is 15-68 ms, so the run is invalid). The benchmark client, not the server, is the limit; a client with one write coroutine fed by a queue would be the next step. Not fixed after three attempts.
