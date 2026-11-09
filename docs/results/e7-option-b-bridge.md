# E7 option B - the JVM-Python bridge (PyFlink 2.0.0, local mini-cluster, one container, 2 GB)

Same `UserWindows` logic as `docs/results/e7-option-b.md` (16.06 us/event, 62,286 ev/s in plain Python after the maximum became a monotonic deque), now inside a
PyFlink `KeyedProcessFunction` (`flink/pyflink/bridge_bench.py`, image `docker/flink-py.Dockerfile`). Bounded
source of 100,000 payments over 2,000 users, parallelism 1, per-key state is a Python dict (Flink keyed-state
API, checkpoint and state-backend cost not included: the numbers are an upper bound for a real operator). Quiet host: dashboards and async scorers stopped.

| mode | run | identity map (s) | windows (s) | windows (ev/s) | windows extra over identity (us/event) |
|---|---|---|---|---|---|
| process | 1 | 7.07 | 15.93 | 6,277 | 88.62 |
| process | 2 | 6.27 | 13.80 | 7,249 | 75.21 |
| process | 3 | 6.28 | 12.62 | 7,924 | 63.41 |
| thread | - | - | - | - | failed |

* Process mode (separate Python worker): 6.3k-7.9k ev/s per slot, about 8-10x below the 62k ev/s of the same logic
  called in plain Python. The bridge, not the windows code, is the cost. These are the three runs after the
  benchmark was corrected to warm up both jobs (a first set, with only the stateful job warmed, gave 3.7k-6.9k
  ev/s because the identity baseline carried the cold start; it was discarded).
* Thread mode (pemja, embedded interpreter) did not start: `RuntimeException: Failed to find PemJa Library`
  from `pemja.utils.CommonUtils.getLibraryPathWithPattern`. Four attempts (Configuration API, PYTHONPATH and
  LD_LIBRARY_PATH, `site-packages` symlink); not solved, so thread mode is **not measured**.
* Reading: at the 200 ev/s this laptop sustains, even the slowest process-mode run has 31x headroom, so option B
  would be viable on volume; the argument against it is operational (a second runtime in the Flink image), not
  throughput. Not adopted in v1.
