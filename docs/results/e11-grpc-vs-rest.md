# E11 - REST/JSON vs gRPC unary vs gRPC bidirectional stream

Same model and the same scorer use case (`ScoreSync`), one client process at 100 req/s open loop (the rate one Python client drives on this laptop), 60 s after a 10 s warm-up, 3 repetitions per mode. Latency in ms from the scheduled send time. A run is valid only if the client's start lag p99 is <= 5 ms (otherwise the client, not the server, is the bottleneck).

| mode | rep | p50 | p95 | p99 | start lag p99 | valid | failed requests |
|---|---|---|---|---|---|---|---|
| rest | 1 | 39549 | 90139 | 103267 | 1677 | False | 1 |
| grpc | 1 | 4.97 | 14.50 | 38.36 | 3.66 | True | 0 |
| grpc-stream | 1 | 8.33 | 10.00 | 12.19 | 1.34 | True | 0 |
| rest | 2 | 5.30 | 8.56 | 12.78 | 1.33 | True | 0 |
| grpc | 2 | 3.49 | 5.39 | 8.59 | 1.26 | True | 0 |
| grpc-stream | 2 | 8.30 | 10.20 | 15.64 | 1.39 | True | 0 |
| rest | 3 | 5.40 | 8.03 | 11.76 | 1.41 | True | 0 |
| grpc | 3 | 3.54 | 5.18 | 6.88 | 1.26 | True | 0 |
| grpc-stream | 3 | 8.27 | 9.82 | 11.75 | 1.26 | True | 0 |

Median of the valid repetitions:

| mode | p50 | p95 | p99 |
|---|---|---|---|
| rest | 5.35 | 8.30 | 12.27 |
| grpc | 3.54 | 5.39 | 8.59 |
| grpc-stream | 8.30 | 10.00 | 12.19 |

Final measurement block, quiet machine. The first REST run is invalid (the client stalled right after the container started). An earlier set of runs (`e11-first-run.json`) gave rest p50 6.28 / grpc 4.15 / stream 9.23 ms.

**gRPC bidirectional streaming.** Two earlier client designs gave invalid runs (every request failed with `InvalidStateError`, because grpc.aio allows one pending write per call; then writes behind a lock, with a client start lag of 15-68 ms). The third design has one writer coroutine fed by a queue and one reader that resolves futures in order: three valid runs (start lag p99 1.8-2.3 ms, 0 failures; the client also fails every waiting request if the stream breaks). The stream is *slower* than unary at the same 100 req/s: p50 9.2 ms vs 4.1 ms. One stream is one server handler that scores requests in order, so each response waits for the previous one; unary calls run concurrently. Streaming would pay off by removing per-call setup at much higher rates from one connection, which this host cannot drive (the single Python client is the ceiling), so no advantage is claimed.
