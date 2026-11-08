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

Measured on a busy 8 GB laptop with a noisy host; repetitions of the same cell differ by a lot, so read the p50, not the tails. Repeated on a quiet machine in the final measurement block if time allows.

| variant | rate (ev/s) | rep | p50 | p95 | p99 | lost | lag stable |
|---|---|---|---|---|---|---|---|
| onnx | 200 | 1 | 16.703 | 330.751 | 1102.847 | 0 | True |
| onnx | 200 | 2 | 16.415 | 35.423 | 84.415 | 0 | True |
| onnx | 400 | 1 | 20.511 | 73.023 | 174.591 | 0 | True |
| onnx | 400 | 2 | 23.263 | 217.343 | 1167.359 | 0 | True |
| triton | 200 | 1 | 19.343 | 42.367 | 83.391 | 0 | True |
| triton | 200 | 2 | 19.967 | 45.983 | 92.991 | 0 | True |
| triton | 400 | 1 | 25.967 | 103.999 | 280.575 | 0 | True |
| triton | 400 | 2 | 25.567 | 78.591 | 135.167 | 0 | True |
