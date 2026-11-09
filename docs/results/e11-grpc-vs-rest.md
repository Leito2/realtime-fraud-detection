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
| grpc-stream | 1 | 9.20 | 14.39 | 23.28 | 2.26 | True | 0 |
| grpc-stream | 2 | 9.29 | 12.79 | 19.28 | 2.05 | True | 0 |
| grpc-stream | 3 | 9.23 | 11.38 | 14.83 | 1.80 | True | 0 |

Median of the valid repetitions:

| mode | p50 | p95 | p99 |
|---|---|---|---|
| rest | 6.28 | 9.68 | 15.38 |
| grpc | 4.15 | 6.27 | 10.16 |
| grpc-stream | 9.23 | 12.79 | 19.28 |

**gRPC bidirectional streaming.** Two earlier client designs gave invalid runs (every request failed with `InvalidStateError`, because grpc.aio allows one pending write per call; then writes behind a lock, with a client start lag of 15-68 ms). The third design has one writer coroutine fed by a queue and one reader that resolves futures in order: three valid runs (start lag p99 1.8-2.3 ms, 0 failures; the client also fails every waiting request if the stream breaks). The stream is *slower* than unary at the same 100 req/s: p50 9.2 ms vs 4.1 ms. One stream is one server handler that scores requests in order, so each response waits for the previous one; unary calls run concurrently. Streaming would pay off by removing per-call setup at much higher rates from one connection, which this host cannot drive (the single Python client is the ceiling), so no advantage is claimed.
