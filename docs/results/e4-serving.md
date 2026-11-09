# E4 - the same model served three ways

Model `fraud-champion` v8, 1500 timed calls per cell. Parity ONNX in-process vs Triton: max difference 1.19e-07.

## Micro: time of the model call only (ms)

| batch | native p50 | native p99 | ONNX p50 | ONNX p99 | Triton p50 | Triton p99 |
|---|---|---|---|---|---|---|
| 1 | 0.468 | 11.7282 | 0.0205 | 0.0714 | 0.9255 | 1.8596 |
| 8 | 0.5774 | 20.2659 | 0.0333 | 0.0752 | 0.7279 | 1.9082 |
| 32 | 0.5424 | 9.2748 | 0.0747 | 0.2405 | 0.7032 | 1.463 |
| 128 | 0.8243 | 21.4424 | 0.2565 | 0.7743 | 0.9166 | 1.5966 |
| 500 | 1.7622 | 17.2914 | 0.7928 | 1.6515 | 1.4195 | 3.959 |

## End to end: scorer on ONNX in-process vs Triton client (one generator, ms)

Quiet machine (dashboards, exporters, async scorers and explainer stopped), 120 s per cell after a 45 s warm-up, 2 repetitions, rates 200 and 400 ev/s (this laptop sustains 200; 400 is above it and shows queueing). Read the p50: the p95/p99 tails move a lot between repetitions of the same cell.

| variant | rate (ev/s) | rep | p50 | p95 | p99 | lost | lag stable |
|---|---|---|---|---|---|---|---|
| onnx | 200 | 1 | 18.895 | 82.943 | 690.175 | 0 | True |
| onnx | 200 | 2 | 17.647 | 47.647 | 188.671 | 0 | True |
| onnx | 400 | 1 | 23.535 | 111.551 | 217.087 | 0 | True |
| onnx | 400 | 2 | 24.815 | 88.831 | 183.935 | 0 | True |
| triton | 200 | 1 | 19.903 | 1580.031 | 5787.647 | 0 | True |
| triton | 200 | 2 | 24.223 | 825.343 | 2639.871 | 0 | True |
| triton | 400 | 1 | 28.207 | 152.319 | 355.839 | 0 | True |
| triton | 400 | 2 | 32.511 | 204.543 | 400.639 | 0 | True |

Reading: with the model call itself costing 0.02-0.9 ms, the end-to-end p50 differs by 2-8 ms (Triton is slower: an extra gRPC hop plus a second container sharing the 8 GB). Triton at 200 ev/s shows p95 of 0.8-1.6 s in both repetitions, much worse than at 400 ev/s; the cause was not isolated (suspect: the Triton model warming up under the first requests, since the 400 ev/s cells ran after it), so no claim is made beyond the numbers. A first, noisier run on a busy host (ONNX p50 16-23 ms, Triton p50 19-26 ms) points the same way for the p50 and is superseded by this table.
