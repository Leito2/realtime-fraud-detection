"""Option B, the part not measured before: what the JVM-Python bridge costs (PLAN.md 3.3, E7).

Runs inside ``fraud/flink-py:bench`` (docker/flink-py.Dockerfile) on a local mini-cluster, one job per mode:

    docker run --rm -v <repo>/flink/pyflink:/opt/pyflink fraud/flink-py:bench \
        python /opt/pyflink/bridge_bench.py --mode thread --events 200000

A bounded source of payments goes through ``key_by(user_id)`` into a ``KeyedProcessFunction`` that runs the
same ``UserWindows`` class the unit tests and ``fraud_bench.option_b`` use. Per-key state is a Python dict
(the cost of Flink's keyed-state API, checkpoints and the state backend is not part of this number, so it is
an upper bound on what a real operator would reach). Each mode also runs a baseline job with an
identity map that crosses the bridge too, so the difference is the cost of the windows logic itself. Prints
one JSON line.
"""

import argparse
import json
import random
import sys
import time

sys.path.insert(0, "/opt/pyflink")

from pyflink.common import Configuration, Types  # noqa: E402
from pyflink.datastream import KeyedProcessFunction, StreamExecutionEnvironment  # noqa: E402
from user_state import UserWindows  # noqa: E402


class Windows(KeyedProcessFunction):
    def open(self, runtime_context):
        self.users: dict[str, UserWindows] = {}

    def process_element(self, value, ctx):
        user, ts, amount = value
        state = self.users.get(user)
        if state is None:
            state = self.users[user] = UserWindows()
        yield state.add(ts, amount)["f_cnt_10m"] + 0.0


def events(n: int, users: int, seed: int = 3) -> list[tuple[str, float, float]]:
    rnd = random.Random(seed)
    t, out = 0.0, []
    for _ in range(n):
        t += rnd.expovariate(1 / 0.05)
        out.append((f"u{rnd.randrange(users)}", t, round(rnd.uniform(1, 200), 2)))
    return out


def run(mode: str, data: list, with_state: bool) -> float:
    config = Configuration()
    config.set_string("python.execution-mode", mode)
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    ds = env.from_collection(data, type_info=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.DOUBLE()]))
    if with_state:
        keyed = ds.key_by(lambda r: r[0], key_type=Types.STRING())
        out = keyed.process(Windows(), output_type=Types.DOUBLE())
    else:
        out = ds.map(lambda r: r[2], output_type=Types.DOUBLE())
    out.filter(lambda x: x < 0).print()
    start = time.perf_counter()
    env.execute(f"bridge-{mode}-{'state' if with_state else 'identity'}")
    return time.perf_counter() - start


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["thread", "process"], default="thread")
    ap.add_argument("--events", type=int, default=200_000)
    ap.add_argument("--users", type=int, default=2000)
    args = ap.parse_args()
    data = events(args.events, args.users)
    run(args.mode, data[:2000], False)                      # warm-up of both jobs
    run(args.mode, data[:2000], True)
    base = run(args.mode, data, False)
    full = run(args.mode, data, True)
    print(json.dumps({
        "mode": args.mode, "events": args.events, "identity_s": round(base, 2), "windows_s": round(full, 2),
        "windows_ev_per_s": round(args.events / full),
        "windows_extra_us_per_event": round((full - base) / args.events * 1e6, 2),
    }))


if __name__ == "__main__":
    main()
